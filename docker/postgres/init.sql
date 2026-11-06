-- Operational (OLTP) schema of the factoring platform: the source for incremental extraction.
create schema if not exists factoring;

create table factoring.clients (
    client_id varchar(16) primary key,
    client_name text not null,
    industry text not null,
    advance_rate numeric(4, 3) not null check (advance_rate between 0.5 and 0.95),
    reserve_rate numeric(4, 3) not null,
    fee_rate numeric(5, 4) not null,
    credit_limit numeric(18, 2) not null,
    updated_at timestamptz not null default now()
);

create table factoring.debtors (
    debtor_id varchar(16) primary key,
    debtor_name text not null,
    tax_id text not null,
    industry text not null,
    country char(2) not null,
    credit_limit numeric(18, 2) not null,
    risk_rating char(1) not null check (risk_rating in ('A', 'B', 'C', 'D')),
    updated_at timestamptz not null default now()
);

create table factoring.invoices (
    invoice_id varchar(24) primary key,
    client_id varchar(16) not null references factoring.clients,
    debtor_id varchar(16) not null references factoring.debtors,
    invoice_number text not null,
    invoice_date date not null,
    due_date date not null,
    amount numeric(18, 2) not null check (amount > 0),
    currency char(3) not null default 'USD',
    status text not null,
    advance_amount numeric(18, 2),
    funded_at timestamptz,
    updated_at timestamptz not null default now()
);

-- Watermark extraction scans by updated_at: index it.
create index invoices_updated_at_idx on factoring.invoices (updated_at);
create index debtors_updated_at_idx on factoring.debtors (updated_at);
create index clients_updated_at_idx on factoring.clients (updated_at);

-- Keep updated_at honest even when the application forgets to set it.
create or replace function factoring.touch_updated_at() returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

create trigger invoices_touch before update on factoring.invoices
for each row execute function factoring.touch_updated_at();
create trigger debtors_touch before update on factoring.debtors
for each row execute function factoring.touch_updated_at();

insert into factoring.clients values
('C0001', 'Client 1 LLC', 'Trucking', 0.85, 0.15, 0.02, 500000, now()),
('C0002', 'Client 2 LLC', 'Staffing', 0.80, 0.20, 0.025, 250000, now());

insert into factoring.debtors values
('D0001', 'Debtor 1 Corp', '12-3456789', 'Wholesale', 'US', 250000, 'A', now()),
('D0002', 'Debtor 2 Corp', '98-7654321', 'Manufacturing', 'MX', 100000, 'C', now());

insert into factoring.invoices values
('INV-900001', 'C0001', 'D0001', 'C0001-900001', current_date - 20, current_date + 25, 12500.00, 'USD',
 'FUNDED', 10625.00, now() - interval '19 days', now()),
('INV-900002', 'C0002', 'D0002', 'C0002-900002', current_date - 70, current_date - 25, 8300.00, 'USD',
 'FUNDED', 6640.00, now() - interval '69 days', now());
