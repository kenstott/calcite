# q14 estimate run 1: not delivered

Jar 9324e0f802. The engine's copy search used one query, and Bing returned only the usnews and
detroitnews copies, neither of which loads. The agent found no copy with its own web_search,
could not read the article, and preview_report refused (no article_copy_url). Undelivered
provisional verdict: 1 Pinocchio, headline only. Fixed in 993825d887 (several query variants).
