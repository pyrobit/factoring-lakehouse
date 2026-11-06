Feature: Portfolio aging for risk reporting
  As a portfolio manager
  I want each open invoice placed in the right aging bucket as of a date
  So that collections can prioritise overdue receivables

  Scenario Outline: Open invoice aging bucket
    Given an open invoice of 1000.00 due on "<due_date>"
    When the portfolio snapshot is built as of "2026-10-31"
    Then the invoice is in aging bucket "<bucket>" with <dpd> days past due

    Examples:
      | due_date   | bucket  | dpd |
      | 2026-11-15 | CURRENT | -15 |
      | 2026-10-31 | CURRENT | 0   |
      | 2026-10-21 | 1-30    | 10  |
      | 2026-09-15 | 31-60   | 46  |
      | 2026-08-10 | 61-90   | 82  |
      | 2026-06-30 | 90+     | 123 |

  Scenario: A short-paid invoice is closed and its shortfall counted as dilution
    Given a paid invoice of 1000.00 with payments totalling 920.00
    When the portfolio snapshot is built as of "2026-10-31"
    Then the invoice has outstanding 0.00 and dilution 80.00
