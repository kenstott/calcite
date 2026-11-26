# Judge: AP via News4JAX, "US plans to hike tariffs on China, EU, Brazil and others over subsidized production", 2026-11-26

**Piece:** https://www.news4jax.com/news/politics/2026/11/25/trump-plans-a-20-tariff-on-chinese-goods-and-new-import-taxes-for-the-eu-and-a-dozen-other-major-trading-partners/. The task was to fact-check it, extract its claims, and grade it on the 0-4 Pinocchio scale.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. The everyman and expert prompt.md files carry both composer markers. Askamerica ran the verbatim news-check prompt on askamerica-news-desktop. Neither unaided arm mentions askamerica.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Article text | WebFetch summary | Full fetch, plus the Chicago Tribune's AP version | WebFetch summary |
| Primary policy docs | None (CNBC, Boston Report) | USTR Nov 25 release and the China notice | USTR Nov 25 release |
| "Roughly three-fifths of U.S. imports" | Not checkable | **True. Warehouse `econ.usitc_tariffs` 2025: the 14 economies are 60.3% of import value.** Effective duty rate 7.65% (2025) vs 2.36% (2024) | Not found. Notes that CNBC's 99.4% figure refers to a different action |
| China "20% stacked, 45% total" | Sources disagree | **Partially false.** "Up to 20%, net of" the forced-labor duty; 10% for Annex A; up-to-40% alternative; 45% not in the notice | "Up to 20%," net of; 45% rests on officials |
| $30B exemption / AP VoteCast two-thirds | Supported / not checkable | Both true (DTN "30 for 30"; syndicated AP poll writeup) | Not checkable |
| Goldman 7.8-9.2% | Not checkable | Unsupported | Not checkable |
| Grade | 1 | 1 (engine-computed; 17 claims: 11 author, 4 subject, 2 fidelity) | 1 |

## Verification

- The article's own trade-share claim can only be checked by summing 2025 import value across the 14 named economies. Only askamerica could do that, and it did, from the warehouse. 60.3% matches "roughly three-fifths". This is a clean case of the connector reaching data that is not on the open web.
- All three arms flag the "20%" headline figure as a proposal capped at "up to," net of other duties. Askamerica and expert read the notice. Askamerica is the only arm that graded the "45% total" as partially false, which is the most precise call.
- The claim shape was already split on the first publish: subject_claims with speakers (Williams, Wyden, Scissors, Greer) and fidelity for Goldman and AP VoteCast. No resume was needed.

## Ranking rationale

1. **askamerica** verified the most claims (17), including the two that need data or primary documents: the import share from the warehouse and the stacking language from the notice. It also verified the $30B exemption and the voter-poll figure, which both unaided arms left unchecked.
2. **expert** read the USTR release and caught the "net of" language and the CNBC mix-up. It left the three-fifths share and several attributed figures unchecked.
3. **everyman** used secondary sources only and left most attributed figures unchecked.

What would change the ranking: nothing material. Askamerica's lead rests on two verifications that the other arms could not replicate.

## Recipe check

Nothing new to teach. Askamerica used `econ.usitc_tariffs` for the share without prompting. Tariff data ending in 2025 is the expected publication lag, not a gap.
