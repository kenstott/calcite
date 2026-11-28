# Judge — news q8, Fortune, Massachusetts millionaires tax and IRS migration data (2026-11-27), 2026-11-28

**Question/claim:** Fact-check the Fortune article (https://fortune.com/2026/11/27/massachusetts-millionaires-tax-irs-migration-new-hampshire-florida/): MA lost a net 31,384 residents and $3.64B AGI in 2023-24, NH and FL took 44%, surtax raised $3.38B, and the surrounding wealth-tax claims.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and close with 987654321 (composer-generated); askamerica ran the production news-check prompt verbatim. No unaided arm's agent.md mentions askamerica. I grepped both unaided answers for "askamerica": no hits, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Article text | Summary only (WebFetch); no quotes checked | Full article loaded | Not stated; claims map to full article |
| Claims graded | 8 | 14 in claims.json (2 central) | 9 bullets plus framing notes |
| Pinocchio grade | 1 | 0 (report.html and agent.md agree) | 1 |
| Central claim (31,384 / $3.64B) | "Partly verified"; figures not found | True, via Boston.com relay of IRS; IRS file not opened; warehouse 2022 same order of magnitude | Confirmed via Pioneer / Boston.com, medium-high confidence |
| Data used | Web only | Web plus fiscal.county_migration_flows (2022) | Web (Pioneer, Boston.com, DOR, AP) |
| Surtax $3.38B vs $2.4B | Verified | Verified, DOR release, FY2025 $2.987B record | Verified, notes "preliminary" |
| Maine / Rhode Island | "Not checked here" | Verified (LD 2212, RI FY2027 budget) | Not confirmed |
| Small errors caught | none | Prop 40 called Nov. 9 not Nov. 3; "nearly four years" is 3y11m | none |
| Visual | none | report.html + dashboard.png | none |

## Verification

Central claim: I fetched the Boston.com report. It says MA "lost a net 31,384 people to other states and countries between 2023 and 2024," "$3.64 billion in adjusted gross income," down from "$4.18 billion in 2023," NH "7,818," FL "net gain of 6,032," and ages 26-34 at "43% of the state's net loss of tax filers." This supports askamerica and expert. It contradicts everyman, which wrote "I could not find the 31,384 or $3.64B figures in any source" and left claims 5-7 unconfirmed. Those figures were findable.

Arithmetic, all arms agree: (7,818+6,032)/31,384 = 44.1%. (1.29+0.869)/3.64 = 59.3%. 2.32/3.64 = 63.7%.

Vintage: IRS 2023-24 is the right vintage for the article. The warehouse table (fiscal.county_migration_flows) ends at 2022, so askamerica's warehouse check is a different year; it correctly labeled it order-of-magnitude only. No arm opened the IRS file itself.

askamerica claims.json versus report: report.html overall grade "0 Pinocchios", agent.md "0 Pinocchios". No mismatch. Two claims are "mostly true" (Prop 40 date, "nearly four years"), both marked neutral and minor, so 0 is defensible but a reader could argue for 1. Expert's 1 Pinocchio rests on framing, not errors.

Open item for the orchestrator: askamerica's SQL result for MA 2022 (net individuals about -39.8k, AGI about -$4.0B) can only be confirmed by a live query.

No arm graded a claim true or false without evidence. everyman marked unfound items "not confirmed", which is calibrated but reflects weaker search. expert did not check Maine/RI and said so. Neither unaided arm caught the Prop 40 date or the "four years" nit.

## Ranking rationale

1. askamerica: loaded the full article, extracted the most claims (14), closed Maine/RI, caught two real imprecisions, added the warehouse cross-check, used the IRS-related table honestly with its stated lag, and published a report. Its framing notes (AGI flows are not surtax payers; MassBudget's 9-in-10 under $200k) are on point.
2. expert: correct central verdict and numbers, good framing notes, clear on what it could not verify. Fewer claims, Maine/RI left open, no nits caught.
3. everyman: worked from an article summary, could not locate the headline figures, left the central claim only partly verified, skipped Maine/RI. Verdict direction is fine but evidence is thinner.

What would change the ranking: if a live query showed the warehouse figures were wrong, or if the Prop 40 date nit were shown to be incorrect, askamerica's lead over expert would narrow. A source showing 31,384 is wrong would invert everything.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

askamerica used the existing recipe `fact-check-a-news-article` (procedure_served in calls.jsonl). Everyman and expert did nothing askamerica missed. The one recurring weakness is the warehouse lag: the IRS 2022-23 and 2023-24 county flow files are published but not loaded, so the warehouse could not check the central claim directly.

Proposed resolution: a — fiscal.county_migration_flows ends at 2022 although IRS 2022-23 and 2023-24 files exist; load them (govdata-ops #1636).

## Corpus gaps stated

- Orchestrator note: fiscal.county_migration_flows ends at 2022 although the IRS has published the 2022-23 and 2023-24 county flow files (verified HTTP 200); filed as govdata-ops #1636.
- askamerica agent.md: "the warehouse migration table (fiscal.county_migration_flows) ends at 2022. Its 2022 file shows a net AGI loss of about $4.0B, the same order of magnitude."
- claims.json, central claim: warehouse_value "2022 file ... (pairs above disclosure threshold), same order of magnitude"; IRS file itself not opened.
- No claim was graded "not checkable here" outright. Spatt's quote was called "an opinion and not checkable."
