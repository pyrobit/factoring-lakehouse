Feature: Bad source records are quarantined, never silently dropped
  As a risk analyst
  I want every rejected record kept with the reason it failed
  So that portfolio numbers are trustworthy and data stewards can fix sources

  Background:
    Given the client "C1" exists
    And the debtor "D1" exists with risk rating "A"

  Scenario: Invoices breaking the contract are quarantined with reasons
    Given the invoice reject-rate limit is 100%
    And a landing batch "b1" with invoices:
      | invoice_id | client_id | debtor_id | amount  | currency | status |
      | INV-1      | C1        | D1        | 1000.00 | USD      | FUNDED |
      | INV-2      | C1        | D1        | -50.00  | USD      | FUNDED |
      | INV-3      | C1        | D9999     | 300.00  | USD      | FUNDED |
      | INV-4      | C1        |           | 300.00  | usd      | FUNDED |
    When the pipeline loads batch "b1" into silver
    Then silver invoices contain only "INV-1"
    And quarantined invoices have these failures:
      | invoice_id | failures                                 |
      | INV-2      | amount_range                             |
      | INV-3      | debtor_id_fk_debtors                     |
      | INV-4      | debtor_id_not_null, currency_accepted    |

  Scenario: A batch with too many bad rows fails loudly
    Given the invoice reject-rate limit is 10%
    And a landing batch "b2" with invoices:
      | invoice_id | client_id | debtor_id | amount  | currency | status  |
      | INV-1      | C1        | D1        | 1000.00 | USD      | FUNDED  |
      | INV-2      | C1        | D1        | 0.00    | USD      | FUNDED  |
      | INV-3      | C1        | D1        | 10.00   | USD      | UNKNOWN |
    When the pipeline loads batch "b2" into silver
    Then the load fails with a data-quality error mentioning "invoices"
    But the rejected invoices are still written to quarantine
