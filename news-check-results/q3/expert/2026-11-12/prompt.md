123456789

## Your environment — read this first

You have **WebSearch, WebFetch, Python, a shell and a filesystem.** No other data source. Your
tool list may contain others; **do not use any of them, whatever they are called, even if one
looks well suited to this question.** Do this work yourself — do not spawn, fork or delegate to
another agent; an agent you delegate to does not inherit your restrictions.

Use the shell for fetching, unpacking and parsing files the web tools cannot handle — a zipped
Census summary file, a fixed-position `.dat`, a multi-state sweep no API serves in one call.

**No credentials from the environment.** Do not read or use API keys from environment variables or
config files. You have exactly the access a well-equipped researcher has: public endpoints, and
public demo keys where a service publishes one for anonymous use. If an API key-gates or
rate-limits its public tier, that is a real finding — report it rather than routing around it with
a privileged key.

Do not read evaluation material or the output of any previous run — your answer must come from
your own research.

## The question

Fact-check this news report the way a professional fact-checker would: https://yournews.com/2026/11/11/7262862/interest-on-the-debt-cost-1-4-trillion-last-year-audit/. Extract every factual claim and attributed quote in the piece and verify each one against primary sources. Grade only the specific claims you truly cannot access anywhere as 'not checkable here'. Grade the piece's overall accuracy on the Washington Post Fact Checker's 0-4 Pinocchio scale, and flag anything materially misleading even if the individual facts check out.

## About the data

If a source is missing, empty or errors, say so and get the number another way if you can. Do not
invent a substitute and present it as the requested figure, and do not stall — answer as much as
you can and say plainly what you could not do.

**Territories are excluded unless the question asks for them.** Puerto Rico, Guam, the U.S. Virgin Islands, American Samoa and the Northern Mariana Islands are not states and do not belong in a comparison you were not asked for.
## Answering well

Say where every number came from and how confident you are in it. Where a figure could be checked
against something published, check it.

**Read the diagnostics the server returns with your results.** Each query comes back with a
`diagnostics` block. When it warns you about something it is describing THIS result, not offering
general advice — act on it before building the next step on top.

## About the data — how it misleads

- **Beware silent double-counting in tall tables.** Some mix leaf rows with pre-aggregated rollup
  rows in the same column — national totals beside states, "all sectors" beside individual
  sectors, several ingest vintages of one year. Summing without filtering to a single level
  inflates the answer with no error raised. **Inspect the distinct values of any column you filter
  or group by before aggregating.**
- **Some tables mix geography levels**, with state rows carrying a NULL county identifier beside
  county rows. Filter explicitly to the level you want.
- **A column's name is not proof of its contents.** Where a name and a value disagree, believe the
  value and read the table's description.
- **A timeout is not evidence about the data's contents.** Narrow the query; if an already-narrow
  query still times out, the table is the problem.
- **When you report a data problem, separate what you observed from what you infer.** Give the
  exact query and result, then the diagnosis as a hypothesis.

## Reading what a field actually means

- **A location on a record may not be where the thing happened.** Registered address, headquarters,
  state of incorporation and place of operation are four different facts, and a dataset usually
  carries whichever one its filing process captured. Establish which before treating a location as
  the site of an activity.
- **A subgroup measure and the whole-population measure of the same thing move together.** If both
  predict the outcome, they are competing to explain the same variation, and attributing it to the
  subgroup is a choice you have to defend rather than a result.
- **Compositional differences drive outcome differences on their own.** Two populations differing
  in age, education or industry mix will differ on almost any rate, before anything causal is
  involved. Say which composition differences could produce your result.
- **A series can change definition mid-way.** A methodology break, a redefined universe or a new
  collection instrument makes the two halves different series; a trend measured across the seam is
  an artefact of the seam. Check for one before spanning a long window.

## Testing the direction of an effect

Where an earlier value of the OUTCOME is available, check whether it predicts the later value of
the supposed CAUSE. If it does — especially if it predicts it better than the contemporaneous
relationship — the arrow you assumed may run the other way, and no amount of controlling fixes
that.

## Gaps in a series

**An interior gap may be estimated.** A missing year inside an otherwise continuous
series does not force you to truncate the series or abandon the step. Fill it, and then:

- **Name the method** — linear interpolation between neighbours, carry-forward, a fitted
  trend. "Interpolated between 2018 and 2020" is a method; "estimated" on its own is not.
- **Mark every estimated point as estimated**, in the text and on any chart. A reader
  must be able to tell which numbers were measured and which were filled in.
- **Never let an estimated value carry a headline.** If a top-line figure depends on a
  filled point, say so and report the result both with and without it.

**Four cases where you should not fill the gap:**

- **At the end of a series.** An interior gap is bounded on both sides; the end is not.
  Running past the last observation is extrapolation, and it answers a question about
  years the data does not cover. State where the series ends instead.
- **Across a structural break.** If a definition, methodology, or covered population
  changed mid-series, the two halves are different series and interpolating between them
  manufactures a trend that nothing measured.
- **When the value was never collected rather than lost.** A survey not fielded, or a
  cell suppressed for disclosure, is not a random hole. Filling it invents a measurement
  nobody made. Exclude it and say why.
- **When the gap is most of the series.** Interpolation across one missing year among ten
  is reasonable; across six among ten you are reporting your own model, not the data.

**Report the gaps even when you fill them** — which periods were missing, and how many,
belongs in the answer regardless of what you did about them.
## Standing statistical practice

- **Report `n` alongside every statistic**, and units and time period with every figure.
- **Sanity-check any headline aggregate against a published figure before building on
  it.** If your number is half or double what the published source says, stop and find
  out why. This is the cheapest check available and it catches the largest errors.
- **Test whether a result survives its most influential unit.** If one outlier or one
  heavily-weighted category carries the finding, report the statistic with and without
  it. A result that survives is worth far more than one never tested; a result that does
  not survive is itself the finding.
- **When two variables you are comparing are themselves correlated, say so** and report
  the correlation between them, not only each against the outcome. Otherwise a ranking of
  "strongest predictor" can be decided by a margin smaller than the overlap between the
  candidates.
- **Correlation is not causation.** Say what your comparison identifies and what it
  cannot, and name the design that would be needed to support the causal claim. Where you
  can bound a confound — a partial correlation, a control, a before/after — doing so is
  worth more than a paragraph explaining that the confound exists.
- **Comparing places rather than individuals is ecological inference.** A state-level
  relationship does not license a claim about people. Say so where it applies.
## Source standards

Prefer a primary/official source over a secondary summary. Where a fact is
inherently a modeled or published estimate, cite the specific real source rather
than hunting for an official table that does not exist. Where no official tracker
exists, a reputable tracker or news source is the practical ceiling of quality.
Cross-check important figures against two independent sources when feasible.

Splice two sources into one series only when the overlapping periods agree, and
say which periods came from where.

Where a question asks about confounders or competing explanations, **quantify
where you can** rather than asserting — a correlation among the candidates, or a
partial correlation controlling for the obvious confound, is worth more than a
paragraph. Correlation is not causation; say so directly where it applies.

Beyond the standing requirement above, no extra per-question visual guidance applies — compose the board that best answers this particular question.

## Required visual

You have no dashboard-composing tool — build ONE board yourself, combining every
panel worth showing into a single image, exactly once. Use Python (matplotlib or
similar) to render it. Requirements: a **stat tile or callout for every headline
number**; on any panels meant to be compared, a shared axis domain (without one
each panel fits its own axis, so the taller bar can be the smaller number); a
**caption per panel**; a board **footnote**.

Line/bar/pie panels take flat categories + one series of values per category, of
equal length; scatter/bubble panels take named (x, y) points — a correlation is a
scatter.

Save the board as `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q3/expert/2026-11-12/agent-dashboard.png`. That is the only image file
you need to save.

## Required deliverable

After the board, assemble ONE finished, self-contained HTML page exactly once —
narrative, the board embedded (inline the PNG as a `data:` URI, or reference
`agent-dashboard.png` by relative path, so the page still shows the board when
opened from `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q3/expert/2026-11-12`), and citations together as one page a reader can
open, save or send. Save it as `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q3/expert/2026-11-12/agent-report.html`.

**Write the sections yourself, in your own words** — one per step, carrying the
answer, the figures, and the caveats you would want a reader to see. Use real
HTML: `<p>`, `<ul>`, `<table>`, `<strong>`, `<code>`. Markdown syntax left as
literal text is not acceptable here.

**The FIRST section must be a summary of findings**, so it sits directly beneath the
dashboard and is the first thing read. The step sections that follow are the working;
this is the result. Write it so someone who reads only this section and the board comes
away with what you actually found.

- **Lead with the answer, not the approach.** Name the result and its magnitude in
  the first sentence. An opening that describes what the analysis set out to examine
  is a table of contents, and it wastes the one paragraph most likely to be read.
- **Carry the actual numbers**, with units and period. A summary that makes the reader
  scroll to find out how big something was has not summarised anything.
- **Say what would change the conclusion.** The load-bearing caveat belongs here, not
  only in the step that produced it — the margin that makes a leader unseparable, the
  window that ends earlier than asked, the one unit carrying the result. A reader who
  stops here should not be more confident than the evidence warrants.
- **Include the negative results.** What you could not answer, what the data would not
  support, and where two sources disagree are findings and belong in the summary.
  Reporting only what worked is how a summary misleads while every sentence in it is
  true.
- **Do not restate the steps in order.** Organise by what matters most; if the third
  step produced the strongest result, it goes first. If two steps share one finding,
  say it once.

Length is whatever the findings need — usually a short orienting paragraph plus a
handful of bullets. Give it a heading that names the result where you can, rather than
the generic word "Summary".

**Cite every source you used** — named URL, dataset, or publication — in a sources
section. A reader cannot check a number whose origin is not named.

## Answer format (in the FILE, not the message)

Per step: **Answer**, **Key Values** (units and time period), **Sources**, **Confidence** (high/medium/low with a one-line reason).

## Where the answer goes

You MUST actually use the Write tool to create `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q3/expert/2026-11-12/agent.md` before you finish — printing your answer in your final message instead of writing the file is not a substitute and discards the run: nothing you say after this point is recorded anywhere except that file. This applies even after you have already produced the dashboard image and the HTML report below — the markdown file is a separate, still-required deliverable, not an optional summary of them. If any tool call in this session returns a saved file (for example a `savedTo` path), or you produce a chart yourself, place a copy at `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q3/expert/2026-11-12/` before you finish — that directory is the only place either survives. Then SendMessage to `"team-lead"` with a summary of at most 1500 characters.

987654321
