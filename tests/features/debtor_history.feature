Feature: Debtor attributes keep full history (SCD Type 2)
  As a credit officer
  I want to see what a debtor's risk rating was on any past date
  So that funding decisions can be audited against the information available at the time

  Scenario: A rating downgrade closes the old version and opens a new one
    Given the debtor "D1" exists with risk rating "A"
    When the debtor "D1" is updated to risk rating "C" at "2026-11-01T10:00:00Z"
    Then debtor "D1" has 2 versions
    And the current rating of debtor "D1" is "C"
    And the closed version of debtor "D1" ends at "2026-11-01T10:00:00Z"

  Scenario: Replaying an unchanged debtor does not create a new version
    Given the debtor "D1" exists with risk rating "A"
    When the debtor "D1" is updated to risk rating "A" at "2026-11-01T10:00:00Z"
    Then debtor "D1" has 1 versions
