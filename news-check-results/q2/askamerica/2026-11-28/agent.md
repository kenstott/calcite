# The Fortune article's claims about corporate profits as a share of GDP are somewhat biased and mostly accurate.

Article: https://fortune.com/2026/11/28/corporate-profits-gdp-wall-street-bad-bet/
Rating: 1 Pinocchio.
Local report: http://127.0.0.1:53424/a/42e3efb7ccdc990e242257bb5c55caaa.html (saved at /Users/kennethstott/.askamerica/reports/20261128-063217-the-fortune-article-s-claims-about-corpo.html)

## Summary
The headline numbers hold. After-tax profits (FRED CP, identical to warehouse econ.national_accounts T11200 line 45 for 2025Q2-Q4) rose from $3,590B in Q3 2025 to $4,419B in Q3 2026 (+23.1%). Nominal GDP rose 6.0% ($31,180B to $33,056B, warehouse econ.fred_indicators). The ratio was 13.37% in Q3 and 13.14% in Q2. BEA's second estimate was released Nov. 25, 2026.

## Claim grades
- Q3 ratio 13.37% vs 13.14%, released Nov 25 (central): true.
- GDP +6%, $31.18T to $33.06T: true.
- Profits +23%, $3.59T to $4.42T, "nearly four times" GDP growth: true (ratio 3.9).
- Ratio "exploded" from mid-2025: true (11.0% in Q2 2025 to 13.4% in Q3 2026).
- 2010 to Q2 2025 averaged about 11%: true (10.8%).
- "6-7% from 1945 to 2003": mostly true (1947-2003 average 6.3%, range 3.7-9.9%).
- "Shot to 10% in the dot.com run-up": partially false. The ratio was about 5.4-7.0% in 1997-2000; about 10% came in 2005-06 (peak 10.8% in 2006Q3).
- 15.7% scenario, "twice the 80-year average", 40% above 11%: mostly true (1.43x of 11% is right; vs 6.3% it is about 2.5x, so "twice" understates).
- Reversion to 11% gives $3.9T, 12% below $4.42T: true as arithmetic (-11.8%).
- "Never-before-seen": mostly true for after-tax profits (prior high 12.7%, 2021Q2); on profits with IVA/CCAdj the warehouse already shows 13.5% in 2025Q4.
- Arnott's Syzygy "over $32 billion": mostly true ($32.2B regulatory AUM per Private Fund Data; other reports say about $30B).
- "Consensus: profits will rise another 25% by the close of 2027": partially false. Published consensus is about 15% S&P 500 EPS growth for 2027 (LSEG IBES via Reuters, 24/7 Wall St, Oct 3 2026). The linked Fortune Nov 21 article has no 25% figure; a Nov 20 Fortune piece's 25% is Goldman's estimate that AI lifted S&P earnings above trend, a different quantity.
- Kapoor "most mean-reverting series in finance": unsupported (quote not located; mean reversion of profitability is documented in the literature).
- Zandi "permanently reset": opinion, not checkable.
- Friedman phone anecdote and quote: personal recollection, not independently checkable (not in structured claims).

## Misleading framing
- The text says "earnings to national income" but computes against GDP.
- The series is after-tax profits without inventory/capital-consumption adjustments, never stated. The "mid-2025 explosion" depends on that choice.
- Part of the rise comes from lower corporate taxes, which only Zandi mentions.
- The 15.7% scenario leans on an overstated forward consensus.

## Provenance
Warehouse: econ.national_accounts has profits only through 2025Q4; econ.fred_indicators has GDP through 2026Q3. Q3 2026 profits ($4,419.0B) and Q2 ($4,279.9B) come from FRED CP (WebFetch), cross-validated because the warehouse line 45 matches FRED exactly for 2025Q2-Q4. The BEA release page (Nov 25, GDP +6.2% annualized, profits +$206.0B) was read via WebFetch; it states no profit level. Historic ratios were computed from FRED fredgraph.csv (CP, GDP). All 13 article links were fetched; none changed a grade other than the Nov 20/21 pages noted above. Kapoor, Zandi and Friedman quotes could not be independently located.
