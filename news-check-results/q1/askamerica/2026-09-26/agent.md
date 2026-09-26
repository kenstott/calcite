## Verdict: Claim checks out (with one gap the corpus can't fill)

**Data source used:** `ag.ers_farm_income` (USDA ERS Farm Income and Wealth Statistics), US-total rows, vintage `publication_date = "Data as of September 3, 2026"` — matching the article's September 2026 forecast exactly.

### Figures computed vs. claimed

| Claim | Computed from ag.ers_farm_income | Verdict |
|---|---|---|
| 2026 net farm income = $158.4B | **$158.354B** (category='Net income', subcategory='Farm income', year=2026) | Matches |
| 2.6% nominal decline from 2025 | 2025 = $162.615B → 2026 = $158.354B = **-2.62%** | Matches |
| 5.5% real decline from 2025 | Using the table's own `gdp_deflator` (2026=100.0, 2025=97.089): 2025 restated in 2026 dollars = $167.49B → 2026 $158.354B = **-5.46%** | Matches |
| ~70% rise in government payments | 2025 = $27.885B → 2026 = $47.362B = **+69.86%** (category='Government payments', subcategory='Total') | Matches |
| Production costs +4.5% to $492.8B | 2025 = $471.570B → 2026 = **$492.813B** = **+4.50%** | Matches |
| $5B higher than USDA's Feb 2026 forecast | No February 2026 vintage stored in this table | **Not checkable here** |

### Method notes
- Net farm income = category `'Net income'`, subcategory `'Farm income'` — distinct from subcategory `'Cash income'`, which is *net cash* farm income, a different ERS series.
- The table carries **two** parallel USDA total-production-expense series under category `'Production expenses'`, subcategory `'All'`: `artificial_key EXAUSPE--EXP` ($489.464B in 2026, $468.357B in 2025, also +4.5%) and `EXAUSPE--INP` ($492.813B in 2026, $471.570B in 2025, +4.50%). The **INP** series is the one that reproduces the article's exact $492.8B/4.5% figures, so that's the one cited; the EXP series is close but not an exact match.
- Real decline was computed using the table's own `gdp_deflator` column rather than an external CPI series, since it's the deflator ERS itself pairs with these income figures.

### Why the $5B-vs-February claim can't be checked here
`ag.ers_farm_income` is a **single-vintage snapshot**: for 2026 it holds exactly one `publication_date` value ("Data as of September 3, 2026"). It does not retain USDA ERS's earlier 2026 forecast releases (e.g., February 2026), so there is no stored prior figure to compare the September number against. This is a genuine coverage gap in this corpus, not a defect in the article and not evidence the claim is wrong — the article's $5B revision figure would require either an archived February 2026 vintage (not present) or a fetch from USDA ERS's own published archive (out of scope for this connector-only check).

### Bottom line
Every figure in the claim that this corpus *can* check — 2026 net farm income level, nominal and real year-over-year change, the government-payments jump, and the production-cost increase — matches USDA ERS's own September 2026 release precisely or to within rounding. The only unverified sub-claim (the $5B revision from February) is a stated data-availability gap, not a contradiction.

**Report (local, ephemeral — dies when this session ends):** http://127.0.0.1:49561/a/e43e8baadffde568732284507cd0e38b.html
**Saved copy:** `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q1/askamerica/2026-09-26/report.html`

### Queries used (for reproducibility)
```sql
-- Net farm income
SELECT "year", amount, gdp_deflator FROM ag.ers_farm_income
WHERE state='US' AND category='Net income' AND subcategory='Farm income' AND "year">=2023;

-- Government payments
SELECT "year", amount FROM ag.ers_farm_income
WHERE state='US' AND category='Government payments' AND subcategory='Total' AND "year">=2023;

-- Production expenses
SELECT "year", subcategory, amount, artificial_key FROM ag.ers_farm_income
WHERE state='US' AND category='Production expenses' AND "year">=2023;

-- Vintage check
SELECT DISTINCT publication_date FROM ag.ers_farm_income WHERE state='US' AND "year"=2026;
```
