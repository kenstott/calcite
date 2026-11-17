123456789

## Your environment — read this first

These tools are available to you: **WebSearch, WebFetch, Bash**, and the AskAmerica MCP server
tools (names starting `mcp__askengine__`). You do **not** have a shell or a filesystem beyond
writing your answer: use `Bash` for exactly one purpose, running `python3`, and nothing else — no
`cat`/`ls`/`curl`/file reads or writes, no other command.

**`python3` here means Desktop's actual sandbox, not a full local install — restrict yourself to
its package list.** This task runs your Python through a real system interpreter with every locally
installed package on the machine reachable, which is more capable than the real thing it stands
in for. Desktop's sandbox ships a fixed set of about 367 packages; the ones that matter for
data/document work are:

`pandas`, `numpy`, `scipy`, `sklearn`, `matplotlib`, `openpyxl`, `xlsxwriter`, `docx`, `pptx`,
`pypdf`, `pdfplumber`, `camelot`, `tabula`, `bs4`, `lxml`, `cv2`, `PIL`, `skimage`, `sympy`,
`playwright`

You MUST use `python3` whenever it can improve the efficiency or accuracy of the response — a
calculation, a statistical test, extracting a table from a fetched document — rather than doing
that work by hand or skipping it.

**Import from that list, plus the standard library — except the modules that would undo the
restrictions already stated above.** Specifically, never import `os`, `subprocess`, `shutil`,
`socket`, `ctypes`, `urllib.request`, `http.client`, or anything else that shells out, touches the
filesystem beyond your answer, or reaches the network outside WebFetch/the connector — those would
make "no shell, no filesystem" a fiction the moment Python is involved, not a real constraint. If a
task seems to need something outside the allowed list — `requests` to bypass WebFetch, a
statistics package not listed, anything from PyPI — that is Desktop genuinely not having it, not a
gap to route around. Say so and work within what's listed instead; reaching for an unlisted or
excluded module is not something the real client you are modeling could do.

**Never fetch a `127.0.0.1` / loopback URL.** Artifact links are for the human reader; you cannot
view your own chart, and trying will block you on a permission prompt.

Do not read evaluation material or the output of any previous run — discover it yourself or leave
it out.

## The question

Fact-check this news report the way a professional fact-checker would: https://www.dotnews.com/2026/11/12/bps-enrollment-continues-to-decline-nearly-2000-fewer-students-this-year/. Extract every factual claim and attributed quote in the piece and verify each one against primary sources. Grade only the specific claims you truly cannot access anywhere as 'not checkable here'. Grade the piece's overall accuracy on the Washington Post Fact Checker's 0-4 Pinocchio scale, and flag anything materially misleading even if the individual facts check out.

## Where the answer goes

You have no filesystem tool, so you cannot write a file yourself. You MUST call `preview_report`, passing `run_subpath="q4/askamerica/2026-11-17"` in that same call, before you finish — its `http://127.0.0.1/...` link dies the moment this session ends, and that argument is what makes the server save the actual published page before that happens. This is not optional and not conditional on whether you produced a chart: a session that ends without a `preview_report` call has not delivered a usable report, full stop. Separately, when you are completely done, also call the `deliver_report` tool with `run_subpath="q4/askamerica/2026-11-17"` and `markdown` set to your complete final answer (the same content you would otherwise write to a file). The server has a real filesystem and saves it for you there, along with the most recent chart you produced (`render_chart`/`compose_dashboard`/`preview_report`), if any. Do both calls yourself — do not just print the answer and stop. If your finding is that the data doesn't exist at the grain asked for, that is still worth a chart — use `render_chart` or `compose_dashboard` to show what you DID check (sources consulted, categories ruled in or out, a bound or proxy figure) rather than skipping the visual because there is no clean series to plot; a negative finding with no chart reads as less finished than the same finding with one.

Someone asked you this question. Answer it.

987654321
