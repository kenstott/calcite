# FEC Schema Documentation

## Overview

The `fec` schema provides access to Federal Election Commission campaign finance data. It covers
federal candidates, political committees, contribution linkages, individual contributions,
committee-to-committee transfers, operating expenditures, independent expenditures, electioneering
communications, communication costs, and pre-aggregated candidate/committee summaries.

The schema is served by `FecSchemaFactory` via `GovDataSchemaFactory` and is driven by
`fec-schema.yaml`. All data is sourced from FEC bulk ZIP downloads (updated after each election
cycle filing deadline).

---

## Tables

| Table | Description | Primary source | Cadence |
|---|---|---|---|
| `candidates` | Registered federal candidates: ID, name, party, office (H/S/P), state, district, election year, principal committee ID, and mailing address | FEC bulk `cn{year}.zip` | Annual (by election year) |
| `committees` | Political committees: ID, name, type, designation, connected org, treasurer, and mailing address | FEC bulk `cm{year}.zip` | Annual |
| `candidate_committee_linkages` | Links candidates to their authorized committees (principal, authorized, joint fundraising, etc.) | FEC bulk `ccl{year}.zip` | Annual |
| `individual_contributions` | Itemized contributions from individuals to committees: date, amount, contributor name/address/employer/occupation, transaction type | FEC bulk `indiv{year}.zip` | Annual |
| `committee_contributions` | Contributions from committees to candidates (PAC-to-candidate): date, amount, filing/amendment info | FEC bulk `pas2{year}.zip` | Annual |
| `intercommittee_transactions` | Non-contribution transfers between committees (loans, reattributions, other transactions) | FEC bulk `oth{year}.zip` | Annual |
| `operating_expenditures` | Itemized committee operating expenditures: payee, purpose, date, amount | FEC bulk `oppexp{year}.zip` | Annual |
| `independent_expenditures` | Independent expenditures (electioneering without coordination): candidate supported/opposed, amount, date | FEC independent expenditures bulk | Annual |
| `electioneering_communications` | Electioneering communication disclosures (broadcast ads mentioning federal candidates within 30/60 days of election) | FEC electioneering bulk | Annual |
| `communication_costs` | Corporate/union communication costs to members: candidate supported/opposed, amount | FEC communication costs bulk | Annual |
| `candidate_summaries` | Pre-aggregated candidate financial summary: total receipts, disbursements, cash on hand, debt | FEC candidate summary bulk | Annual |
| `committee_summaries` | Pre-aggregated committee financial summary: total receipts, disbursements, independent expenditures, cash on hand | FEC committee summary bulk | Annual |

---

## Views

| View | Description | Depends on |
|---|---|---|
| `candidate_district_profile` | Enriches FEC candidates with TIGER congressional district boundaries; House candidates (office=H) matched to district geometry | `candidates`, `geo.tiger_congressional_districts` |

---

## Environment Variables

### Required

| Variable | Description |
|---|---|
| `GOVDATA_PARQUET_DIR` | Root Parquet directory |

---

## Key Fields

- `candidate_id` — FEC candidate ID (e.g., `H0AL01234`, `S0AK00567`, `P80000722`)
- `committee_id` — FEC committee ID (e.g., `C00000042`)
- `transaction_date` — MMDDYYYY format in source; stored as string
- `amount` — USD; negative values indicate refunds
- `office` — `H` (House), `S` (Senate), `P` (President)
- `fec_election_year` — even-numbered election cycle year

---

## Sample Queries

```sql
-- Top individual donors by total contribution amount (latest cycle)
SELECT contributor_name, SUM(amount) AS total_contributed
FROM fec.individual_contributions
WHERE fec_election_year = 2024 AND amount > 0
GROUP BY contributor_name
ORDER BY total_contributed DESC
LIMIT 20;

-- Total PAC spending by party (committee contributions to candidates)
SELECT c.party, SUM(cc.amount) AS total_pac_to_candidate
FROM fec.committee_contributions cc
JOIN fec.candidates c ON cc.candidate_id = c.candidate_id
WHERE cc.fec_election_year = 2024
GROUP BY c.party
ORDER BY total_pac_to_candidate DESC;

-- Candidates by total receipts (presidential race)
SELECT candidate_name, party, total_receipts, cash_on_hand_close
FROM fec.candidate_summaries
WHERE office = 'P' AND election_year = 2024
ORDER BY total_receipts DESC
LIMIT 10;

-- Independent expenditure spending for vs. against (latest cycle)
SELECT candidate_id, support_oppose_indicator,
       SUM(expenditure_amount) AS total
FROM fec.independent_expenditures
WHERE election_year = 2024
GROUP BY candidate_id, support_oppose_indicator
ORDER BY total DESC
LIMIT 20;
```
