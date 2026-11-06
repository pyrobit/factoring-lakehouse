-- SCD2 integrity: exactly one current version per debtor, and no overlapping validity windows.
with current_versions as (
    select debtor_id
    from {{ ref('dim_debtor') }}
    group by debtor_id
    having count_if(is_current) != 1
),

overlaps as (
    select a.debtor_id
    from {{ ref('dim_debtor') }} as a
    inner join {{ ref('dim_debtor') }} as b
        on
            a.debtor_id = b.debtor_id
            and a.debtor_version_key != b.debtor_version_key
            and a.valid_from < b.valid_to
            and b.valid_from < a.valid_to
)

select debtor_id from current_versions
union all
select debtor_id from overlaps
