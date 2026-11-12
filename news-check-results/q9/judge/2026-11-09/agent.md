# Judge: Baltimore Sun via WBFF, "Mayor Scott blames media, surrounding counties for Baltimore's negative image" (q9), 3-persona 2026-11-09

Rank: [expert, askamerica, everyman]

Source: https://foxbaltimore.com/news/local/sun-mayor-scott-blames-media-surrounding-counties-for-baltimores-negative-image
(Chevall Pryce, The Baltimore Sun, Oct. 29, 2026). **11 days old, outside the seven-day default**, used after
no in-window story with checkable figures could be found.

**Engine build:** the private dev jar, from 0af22733bd, all gates. **Data server:** the rebuilt
`~/.askamerica/pgwire-dev` on port 45533, current `main` code, fresh catalog cache. `spawn.log` shows
`socket-claimed port=45533` and `catalog-cache-hit`.

## Judge's own verification

- **UMBC poll, read from the PDF:** n=666 city residents, April 14-19, 2026. "Sixty-two percent say they can
  trust their local government 'never' or 'only some of the time'"; "April 2025: 65 percent". So the article's
  65% is last year's figure. Right direction 41%, wrong track 45%, and 61% have "a lot" or "some" trust in
  local news. Those last three match the article.
- **Downtown $1 billion:** coverage of April 2026 Baltimore Sun reporting puts the loss at more than $1 billion
  over about six years ($496M Downtown, $363M Inner Harbor, $215M Downtown West). The article's "last year" looks
  wrong. Most of what I found is aggregators reposting the Sun; I did not read the original.
- **27%, 44%, 21%:** do not appear in the Part 1 PDF. I did not check the demographic tables.
- **102 vs 108, 259 vs 254:** not verified at source. Consistent with the city's Oct. 1 and JNS's later counts.

## Why

**`expert` is first.** The 62% against 65% catch, the $1.07 billion support and its timing doubt, the PILOT claims
via Banner and Brew, and the most useful context: the homicide lead narrowed from about -26% in August to about
-6%, and shootings flipped to up. It marked confidence claim by claim. It leaned on the article's own outlet for
the 27%, which is weak.

**`askamerica` is second.** It read the primary PDFs and caught the same error with windows stated, added
that the news-source items ask about use rather than primary source, gave the official 11,558 vacants, and noted
the 61% trust figure undercuts the mayor's thesis. But it left the $1 billion and West Lexington PILOT "not
checkable" although ordinary search finds reporting on both, and it did not draw out the narrowing trend.

**`everyman` is third.** It caught the 65% via CBS and raised a fair point about the Sun and Fox45 reporting on
criticism of themselves. It read a summary, wrongly suggested the 61% conflicts with 62% (the poll says 61%),
and left much unchecked.

## Severity invariant

`askamerica` ranks above `everyman`. Not triggered.

## Gate check

The first live refusals. The named-subject gate and the not-checkable gate each refused one draft, and
`askamerica` went on to fetch more before publishing. The limit also showed: a page that loaded but lacked the
figure satisfies the check, so the $1 billion claim passed as not checkable with `why_no_page` after one
unrelated article.

## Recipe check

No change from this run. `askamerica` was not last, and the remaining miss is search effort rather than a new
failure.
