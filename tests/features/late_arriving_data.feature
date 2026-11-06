Feature: Out-of-order CDC events never overwrite newer data
  As a data engineer
  I want the invoice merge to be guarded by the source update timestamp
  So that replays and late-arriving events cannot roll back an invoice's state

  Background:
    Given the client "C1" exists
    And the debtor "D1" exists with risk rating "A"

  Scenario: A stale "DISPUTED" event arrives after the invoice was paid
    Given a landing batch "b1" with invoices:
      | invoice_id | client_id | debtor_id | amount  | currency | status | updated_at           |
      | INV-1      | C1        | D1        | 1000.00 | USD      | PAID   | 2026-10-30T12:00:00Z |
    And the pipeline loads batch "b1" into silver
    And a landing batch "b2" with invoices:
      | invoice_id | client_id | debtor_id | amount  | currency | status   | updated_at           |
      | INV-1      | C1        | D1        | 1000.00 | USD      | DISPUTED | 2026-07-01T00:00:00Z |
    When the pipeline loads batch "b2" into silver
    Then invoice "INV-1" has status "PAID" in silver
    And 1 stale update was skipped

  Scenario: Re-running the same batch is idempotent
    Given a landing batch "b1" with invoices:
      | invoice_id | client_id | debtor_id | amount  | currency | status |
      | INV-1      | C1        | D1        | 1000.00 | USD      | FUNDED |
      | INV-2      | C1        | D1        | 500.00  | USD      | FUNDED |
    When the pipeline loads batch "b1" into silver
    And the pipeline loads batch "b1" into silver
    Then silver has 2 invoices
