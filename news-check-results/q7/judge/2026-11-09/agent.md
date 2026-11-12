# Judge: Radio Oklahoma News, "PSO Rate Increase Approved for December 2026 in Oklahoma" (q7), 3-persona 2026-11-09

Rank: [askamerica, everyman, expert]

Source: https://www.radiooklahomanews.com/post/pso-rate-increase-approved-for-december-2026-in-oklahoma
(no named reporter; publication date inferred as about Nov. 6-8, 2026).

**Engine build:** the private dev jar, `~/.askamerica/engine-dev/askamerica-engine.jar` (same code as
27c4b39413, all gates). **Data server:** the patched local overlay on port 45533.

## Judge's own verification

I had two local reports read in full: KOKH/FOX 25 (updated Nov. 9) and the Tulsa Flyer (Nov. 5). I did
not open OCC Order No. 761871.

- **Interim Order:** KOKH says the decision "is formally titled an 'Interim Order'" because power-supply
  issues for large users remain open, and that it "allows rate changes to proceed". So "interim" concerns
  the large-load matter; the residential rate change itself proceeds. "Permanent" is loose rather than
  flatly wrong, since it replaces the interim charge.
- **3.55%:** an OCC spokesperson says it is what PSO "has indicated" to staff, and staff must review the
  updated financials. KOKH warns not to read it as another 3.55% on current bills; Chair David says the
  same. So "the percentage increase has been finalized" is wrong.
- **Refunds:** PSO says residential customers get none; OCC says it cannot yet confirm. The article states
  it as settled.
- **Six days:** KOKH says six calendar days for updated accounting and a revised study, with revised
  tariffs after an objection period. The article's "revised tariffs" within six days is imprecise.

## Why

**`askamerica` is first.** It found the Interim Order title, that the 3.55% is unconfirmed by staff, the
six-day and tariff sequence, the OCC's caution on refunds, and the omitted dissent and settlement context.
"Mostly false" on "finalized" holds. "Partially false" on "permanent" is somewhat strong, and the
"biased and partly false" page headline with 2 Pinocchios runs a little hot for an article whose core
facts check out. It used no warehouse data, since no table covers rate orders; its one catalog search was
sound. It did not open the order.

**`everyman` is second.** It worked from a summary, yet flagged "permanent", that December is only PSO's
expectation, and the 90-day clock, and added the 15% request and 1% settlement. It left the
Data Center act not checkable and, like the others, did not open the order.

**`expert` is third.** It found HB 2992 for the act and the vote breakdown, but listed the 3.55%, December
timing and six-day requirement as verified facts while noting elsewhere that staff must still verify
the 3.55%. It made nothing of the Interim Order, and read only a summary.

## Severity invariant

`askamerica` ranks above `everyman`. Not triggered.

## Gate check

No gate refused: no claim was graded 'unsupported', and the 'partially false' and 'mostly false' claims name
a date. `askamerica` filled `windows_tested` on two claims with entries such as "Nov 5 vote" and "order
status", which are not time windows. The field can be filled to no purpose.

## Recipe check

`askamerica` is #1. No change warranted.
