# Judge: Grist, black lung surge and the MSHA silica proposal (news q8, 2026-11-21)

Rank: [askamerica, expert, everyman]

Story: https://grist.org/health/as-black-lung-disease-surges-the-trump-administration-proposes-deregulatory-protections-for-miners/ (2026-11-19). Claimants: Grist reporting, MSHA (Federal Register, Nov 12), NIOSH-led study, NIH, Appalachian Citizens' Law Center (Brendan Muckian-Bates).

Verdicts: all three 1 Pinocchio (askamerica engine-scored).

## Ranking

**1. askamerica (narrow).** Checked the rule claims against the Federal Register (50 µg/m³ kept, uniform 21% error factor, voluntary exams for new metal/nonmetal miners, $28.7M savings with $2.43M coal, dates, Eighth Circuit stay of April 11, 2025). Graded the "more than 1 in 3 over three years" prevalence claim mostly false against NIOSH's 32.5% over five years among 25+-year underground miners. Caught an error neither other arm found: the 2024 rule did not require air sampling every six months; it required six-month periodic evaluations with sampling frequency tied to exposure level. Named omissions in both directions: the proposal adds mandatory exams for overexposed sampled miners, MSHA says the 21% factor matches current enforcement, and the industry's past-exposure argument is absent.

**2. expert.** Same rule verifications and the same prevalence finding (32.5%, 91 of 280, "nearly 50 years", not a 50-year high), plus a useful emphasis point askamerica did not make: 92% of the savings go to metal/nonmetal mines, so the story's coal framing over-weights coal's share. It only partly confirmed the sampling-frequency claim and did not catch the six-month error.

**3. everyman.** Confirmed dates, the stay and the prevalence figure's imprecision, but could not parse the Federal Register notice, so the 21% factor, savings and exam changes went unchecked.

What would change the order: the top two are close; a reader weighting expert's savings-split point over askamerica's sampling-schedule correction could swap them.

## Severity invariant

askamerica ranks first. No defect flag.

## Claim-shape check

Compliant: Grist's own claims as `author_claims`, NIH/MSHA-cited figures as `fidelity`, Brendan Muckian-Bates as `subject_claims` with speaker. Rating 1 in report.html and agent.md.

## Gaps and resolution

askamerica found no black lung, silica sampling or rulemaking table (only MSHA coal production). Filed govdata-ops **#1412**, `health: add MSHA respirable dust/silica samples and NIOSH black lung (CWHSP) prevalence` (type:sourcing, kind:gap, schema:health, status:open). askamerica ranked first, so this is a gap filing, not a loss resolution.

## Recipe check

expert's decomposition of the regulatory savings by sector is the kind of check a recipe could prompt, but it applies to Federal Register cost tables, which are not in the warehouse. No recipe.
