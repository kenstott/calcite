# Judge: Axios, "Congress gets a historic overhaul from every direction", 2026-11-26

**Piece:** https://www.axios.com/2026/11/20/house-senate-freshmen-class-largest-decades. The task was to fact-check it, extract its claims, and grade it on 0-4 Pinocchios.

Rank: [expert, askamerica, everyman]

All three personas ran fresh today. The everyman and expert prompt.md files carry both composer markers. Askamerica ran the verbatim news-check prompt on askamerica-news-desktop. No unaided arm mentions askamerica.

**Access caveat:** Axios returned HTTP 403 (a Cloudflare challenge) to all three arms, including the connector's own fetch, and no archive copy existed. Every verdict below rests on the headline and on secondary summaries of the piece, not on its text.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Article claims reconstructed from | Nothing usable | Headline thesis only, giving 3 claims | Search summary plus Political Wire write-up, giving about 6 claims |
| Senate class | Quorum: 16 senators-elect | 19 (CBS); Wilson Center table shows 20 in 1979 and 18 in 1981, so largest since 1979; definition-dependent (17 on Jan 3) | 19 (CBS), 16 (Quorum), 21 (summary of Axios); "since 1946" unverified and conflicts with CBS's "almost five decades" |
| House class vs 1992's 108 | 104 (Quorum) | 105 lottery chips (Cronkite) vs 108; partially false unless the final count reaches 109 or more; House History 108/116 | 105 (Cronkite) vs 108, provisional |
| Other claims | none | none | Gallup 14% approval confirmed (Oct 1-19, lowest pre-midterm since 1974); quote not checkable; causes not verified |
| Warehouse | n/a | `officials.members` ends at the 119th Congress (members-elect not yet seated); no data route | n/a |
| Grade | None | 1 | 1 (provisional) |

## Verification

- Askamerica and expert agree on the House class: 105 is below 1992's 108, so "biggest since 1992" holds only if pending contests push the class past 108. Askamerica stated that condition explicitly.
- On the Senate, askamerica's historical baseline (the Wilson Center freshman table, 20 in 1979) is the more rigorous check. Expert found the conflicting counts (16, 19, 21) and the conflicting baselines ("since 1946" vs "five decades") but did not resolve them against a historical table.
- `officials.members` not yet holding the 120th Congress is expected: members-elect are not seated until January 3. This is not a gap worth filing.
- Claim shape: 3 author claims reconstructed from the headline. With no readable text there are no attributed claims to split, so the single shape is acceptable here.

## Ranking rationale

1. **expert** extracted and checked more of the piece's claims from secondary write-ups. It confirmed the Gallup 14% figure, which no other arm checked, and flagged the shakiest comparison ("since 1946").
2. **askamerica** gave the most rigorous verification of the two central class-size claims, against historical tables. It covered only the headline thesis, so several claims went unexamined.
3. **everyman** gave no grade and checked no claims.

This is a close call between expert and askamerica. If historical-baseline rigor on the central claims outweighed breadth, askamerica would lead. Askamerica's second place is not a data gap: all three arms were blocked from the text.

## Recipe check

Nothing to teach. The loss to expert comes from article access: expert found a secondary write-up (Political Wire) that summarized more of the piece. It is not a warehouse or tool issue. No issue filed.
