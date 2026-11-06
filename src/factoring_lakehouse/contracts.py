"""Data contracts: one pydantic model per source entity.

A contract is the single source of truth for an entity. From it we derive:
  * the Spark read schema (``spark_schema``): explicit, never inferred;
  * the data-quality rules (``derive_rules``): not-null, range, accepted values, uniqueness and
    foreign keys;
  * row-level validation inside the extractors (``Model.model_validate``).

Read schemas are fully nullable on purpose. Bad rows have to *reach* the quality layer so they
can be quarantined with a reason, instead of being silently nulled or dropped by the reader.
"""

from __future__ import annotations

import datetime as dt
import types
import typing
from decimal import Decimal
from typing import Annotated, ClassVar, Literal

from pydantic import BaseModel, Field
from pyspark.sql import types as T

Money = Annotated[Decimal, Field(max_digits=18, decimal_places=2)]
Rate = Annotated[float, Field(ge=0, le=1)]


class Contract(BaseModel):
    """Base class; subclasses declare a primary key and foreign keys."""

    __entity__: ClassVar[str]
    __primary_key__: ClassVar[str]
    __foreign_keys__: ClassVar[dict[str, tuple[str, str]]] = {}  # column -> (entity, column)
    __pii__: ClassVar[tuple[str, ...]] = ()


class Client(Contract):
    __entity__ = "clients"
    __primary_key__ = "client_id"

    client_id: str
    client_name: str
    industry: str
    advance_rate: Annotated[float, Field(ge=0.5, le=0.95)]
    reserve_rate: Rate
    fee_rate: Annotated[float, Field(ge=0, le=0.10)]
    credit_limit: Annotated[Decimal, Field(ge=0, max_digits=18, decimal_places=2)]
    updated_at: dt.datetime


class Debtor(Contract):
    __entity__ = "debtors"
    __primary_key__ = "debtor_id"
    __pii__ = ("tax_id",)

    debtor_id: str
    debtor_name: str
    tax_id: str
    industry: str
    country: Annotated[str, Field(min_length=2, max_length=2)]
    credit_limit: Annotated[Decimal, Field(ge=0, max_digits=18, decimal_places=2)]
    risk_rating: Literal["A", "B", "C", "D"]
    updated_at: dt.datetime


class Invoice(Contract):
    __entity__ = "invoices"
    __primary_key__ = "invoice_id"
    __foreign_keys__ = {"client_id": ("clients", "client_id"), "debtor_id": ("debtors", "debtor_id")}

    invoice_id: str
    client_id: str
    debtor_id: str
    invoice_number: str
    invoice_date: dt.date
    due_date: dt.date
    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Literal["USD"]
    status: Literal["OPEN", "FUNDED", "PAID", "DISPUTED", "CANCELLED"]
    advance_amount: Annotated[Decimal, Field(ge=0, max_digits=18, decimal_places=2)] | None = None
    funded_at: dt.datetime | None = None
    updated_at: dt.datetime


class Payment(Contract):
    __entity__ = "payments"
    __primary_key__ = "payment_id"
    __foreign_keys__ = {"invoice_id": ("invoices", "invoice_id")}

    payment_id: str
    invoice_id: str
    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    paid_at: dt.datetime
    method: Literal["ACH", "WIRE", "CHECK"]
    event_ts: dt.datetime


CONTRACTS: dict[str, type[Contract]] = {m.__entity__: m for m in (Client, Debtor, Invoice, Payment)}

# --------------------------------------------------------------------------------------------------
# Contract -> Spark schema
# --------------------------------------------------------------------------------------------------


def _unwrap(annotation: typing.Any) -> tuple[typing.Any, bool, list]:
    """Return (inner_type, is_optional, extra_metadata) for ``Annotated[X, ...] | None`` and friends.

    Pydantic lifts constraints of a top-level ``Annotated`` into ``FieldInfo.metadata``, but not
    when the ``Annotated`` sits inside an ``Optional``, so we collect those here.
    """
    optional = False
    metadata: list = []
    if typing.get_origin(annotation) in (typing.Union, types.UnionType):
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        annotation, optional = args[0], True
    if typing.get_origin(annotation) is Annotated:
        annotation, *extras = typing.get_args(annotation)
        for extra in extras:
            metadata.extend(getattr(extra, "metadata", [extra]))
    return annotation, optional, metadata


def _spark_type(py_type: typing.Any) -> T.DataType:
    if typing.get_origin(py_type) is Literal:
        return T.StringType()
    mapping: dict[typing.Any, T.DataType] = {
        str: T.StringType(),
        int: T.LongType(),
        float: T.DoubleType(),
        bool: T.BooleanType(),
        Decimal: T.DecimalType(18, 2),
        dt.date: T.DateType(),
        dt.datetime: T.TimestampType(),
    }
    if py_type not in mapping:
        raise TypeError(f"No Spark mapping for {py_type!r}")
    return mapping[py_type]


def spark_schema(model: type[Contract]) -> T.StructType:
    fields = []
    for name, info in model.model_fields.items():
        inner, _, _ = _unwrap(info.annotation)
        fields.append(T.StructField(name, _spark_type(inner), nullable=True))
    return T.StructType(fields)


# --------------------------------------------------------------------------------------------------
# Contract -> data-quality rules
# --------------------------------------------------------------------------------------------------


class RuleSpec(typing.NamedTuple):
    name: str
    kind: str  # not_null | range | accepted_values | length | unique | foreign_key
    column: str
    params: dict


def derive_rules(model: type[Contract]) -> list[RuleSpec]:
    rules: list[RuleSpec] = []
    for name, info in model.model_fields.items():
        inner, optional, extra_meta = _unwrap(info.annotation)
        if info.is_required() and not optional:
            rules.append(RuleSpec(f"{name}_not_null", "not_null", name, {}))
        if typing.get_origin(inner) is Literal:
            rules.append(
                RuleSpec(f"{name}_accepted", "accepted_values", name, {"values": list(typing.get_args(inner))})
            )
        bounds: dict[str, float] = {}
        for meta in [*info.metadata, *extra_meta]:
            for attr in ("ge", "gt", "le", "lt", "min_length", "max_length"):
                value = getattr(meta, attr, None)
                if value is not None:
                    bounds[attr] = float(value) if attr in ("ge", "gt", "le", "lt") else int(value)
        range_bounds = {k: v for k, v in bounds.items() if k in ("ge", "gt", "le", "lt")}
        if range_bounds:
            rules.append(RuleSpec(f"{name}_range", "range", name, range_bounds))
        length_bounds = {k: v for k, v in bounds.items() if k in ("min_length", "max_length")}
        if length_bounds:
            rules.append(RuleSpec(f"{name}_length", "length", name, length_bounds))

    rules.append(RuleSpec(f"{model.__primary_key__}_unique", "unique", model.__primary_key__, {}))
    for column, (ref_entity, ref_column) in model.__foreign_keys__.items():
        rules.append(
            RuleSpec(f"{column}_fk_{ref_entity}", "foreign_key", column, {"entity": ref_entity, "column": ref_column})
        )
    return rules
