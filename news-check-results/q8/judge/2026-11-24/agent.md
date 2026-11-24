# Judge — News q8 (wjla-water-systems-cyberattacks-states), 2026-11-24

**Question/claim:** Fact-check the WJLA/National News Desk article (2026-11-23) on water-system cyberattacks: Walkinshaw says attacks are "exploding"; 12 states reportedly attacked, 9 confirmed; FBI/EPA July warning on internet-facing PLCs; CSIS says Iran-linked CyberAv3ngers claimed responsibility; Clayton and Vance remarks.

Rank: [askamerica, everyman, expert]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md carry both markers; askamerica received the product's verbatim news prompt, which carries no markers by design. A grep of the everyman and expert answers for "askamerica" found no match, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web search, syndicated article copy; PSA only via summaries | Web; PSA read in full at ic3.gov; TheWrap, Reuters, IBTimes. Warehouse had nothing relevant | Web; PSA via Tenable/Nextgov; Fortune, ABC transcript, The Hill |
| Vintage | Nov 2026 | Nov 2026 | Nov 2026 |
| Pinocchio grade | 1 | 0 (engine score) | 1 |
| Claims graded | 10 | 10 (claims.json) | 10 |
| Central claim verdict | PSA supported; 12/9 tally unverified | PSA true; "exploding" unsupported; 9 of 12 unsupported | PSA supported; 12/9 press-sourced; "exploding" supported via search coverage |
| Headline figures | FBI "at least 7" vs 12; Minnesota 30+ | FBI "at least 7"; PSA I-073026-PSA, Jul 30 | PSA Jul 30; 7 vs 12; hearing Nov 18 |
| Visual | none | report.html with engine board | agent-report.html + dashboard png |

## Verification

- Central claim (FBI/EPA July warning on internet-facing PLCs). A web search shows a joint FBI/EPA PSA dated 30 July 2026 (I-073026-PSA), at least seven states, IP address and password changes, mitigations of gateways/firewalls, strong passwords and ACLs, and no named actor. I could not read the PDF body (compressed stream), so this rests on search summaries of the PSA. All three arms support the claim. askamerica's "nearly verbatim" match to the PSA is consistent with the summaries.
- "12 states, 9 confirmed": all three note the PSA says at least seven; none found primary support. Consistent.
- CyberAv3ngers via CSIS: CSIS returned 403 for all arms. everyman graded it a correctly worded claim; expert "not checkable"; askamerica "unsupported" and added the Tenable/Claroty disagreement. The law-firm summary agrees the PSA names no actor.
- Walkinshaw "exploding": askamerica found no transcript and graded it unsupported. expert graded it "Supported (Nov 18 hearing)" from search coverage; no evidence of the quote itself is shown. That is a grade without evidence. everyman did not verify it and said so.
- Clayton/Vance: all arms agree the quote and role are real and that the remarks concerned general outages. askamerica and expert cite Reuters/ABC for Clayton saying outages were not coordinated attacks.
- Engine rating vs agent.md: report.html says 0 Pinocchios; agent.md says 0 Pinocchios. No mismatch. The opening sentence of agent.md ("biased and accurate") is awkward. A 0 is lenient given that both central claims-level items (Walkinshaw, Iran attribution) went unsupported and the 12/9 count is unconfirmed. 1 is the better-calibrated grade.
- Missed claims: askamerica did not note Minnesota's 30+ systems (everyman did). expert did not grade "aging infrastructure" as unsupported where askamerica flagged no named source; expert called it supported via Clayton's "bumps in the road".

## Ranking rationale

1. askamerica. Only arm that read the primary PSA in full. Strongest evidence on the central claim and the ACL/password text. Calibrated "unsupported" and "not checkable" on Walkinshaw, CSIS and the state list, with stated reasons. Good misleading-context finding (outage remarks placed in a water story). Weakness: 0 Pinocchio grade is lenient.
2. everyman. Good claim coverage, honest about what it did not read, useful context (Minnesota scale, FBI 7 vs 12). No primary PSA read. Grade of 1 is reasonable.
3. expert. Terse, secondary sources only, and graded the Walkinshaw quote supported without shown evidence. Context point on outages is sound.

What would change the ranking: a live check that the PSA text lacks the ACL line, or evidence the Walkinshaw quote is on the record (would lift expert's grade and mildly dent askamerica's "unsupported").

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Recipe `fact-check-a-news-article` covers this pattern (read the primary document, grade per claim). askamerica followed it. A small tuning note: when central claims are unsupported, the engine's 0 Pinocchio mapping looks generous; this is a scoring matter, not a recipe gap. The warehouse has no water-sector cyber incident table, which is a legitimate scope gap.

Proposed resolution: d — askamerica ranks first; no actionable cause.

## Corpus gaps stated

- "The warehouse has no table on water-sector cyber incidents. Catalog searches turned up only MITRE ATT&CK and IOC tables, so grading rests on web sources."
- Walkinshaw "exploding": graded unsupported, "No count of attack..." / no transcript found.
- CSIS CyberAv3ngers claim: "csis.org returned HTTP 403; no other loaded page states the claim." (unsupported)
- Slogan claim on critical systems managing water flow: "not checkable here" (opinion).
- Nine confirmed states list: "No page found confirming Utah, Oregon, Arkansas or Alabama."
