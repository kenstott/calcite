# Stooq Stock Price Downloader - Detail Design

## Overview

This document describes the design for replacing Yahoo Finance and Alpha Vantage stock price downloaders with Stooq.com integration in the GovData SEC adapter.

## Problem Statement

### Current State Issues

**Yahoo Finance (`YahooFinanceDownloader`)**:
- Terms of Service concerns for commercial use
- Aggressive rate limiting (429 errors)
- Bulk downloading operates in a legal gray area
- Paginated responses require multiple requests per ticker

**Alpha Vantage (`AlphaVantageDownloader`)**:
- Requires API key (currently: `ALPHA_VANTAGE_KEY`)
- Free tier severely limited: 5 requests/minute, 25 requests/day
- 12.5 second delay between requests required
- Impractical for bulk downloads (~6,000 tickers would take ~250 days on free tier)
- Rate limit errors cause cascade failures

### Why Stooq.com?

[Stooq.com](https://stooq.com) is a Polish financial data site that explicitly provides free bulk historical stock price downloads.

| Aspect | Yahoo Finance | Alpha Vantage | Stooq.com |
|--------|---------------|---------------|-----------|
| ToS for commercial use | Gray area | OK with key | Explicitly free |
| Rate limiting | Aggressive (429 errors) | Severe (25/day free) | Permissive (~1 req/sec) |
| API key required | No | Yes | Optional (for premium) |
| Bulk download friendly | No | No | Yes |
| Full history per request | No (paginated) | Yes | Yes |
| Data quality | Good | Good | Good |
| US stock coverage | Full | Full | Full |
| Time for 6,000 tickers | Hours (with backoff) | 250 days (free tier) | ~100 minutes |

## API Specification

### Endpoint

```
https://stooq.com/q/d/l/?s={ticker}.us&i=d
```

### Request Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `s` | Stock symbol with exchange suffix | `aapl.us` (US stocks), `msft.us` |
| `i` | Interval: `d` (daily), `w` (weekly), `m` (monthly) | `d` |
| `d1` | Start date (optional): YYYYMMDD format | `20200101` |
| `d2` | End date (optional): YYYYMMDD format | `20241231` |

### Response Format

**Content-Type**: `text/csv`

**Headers**:
```csv
Date,Open,High,Low,Close,Volume
```

**Example Response**:
```csv
Date,Open,High,Low,Close,Volume
2024-12-13,248.49,251.14,247.48,248.13,49286500
2024-12-12,247.96,251.35,246.35,247.96,47989300
2024-12-11,246.67,248.59,245.84,246.49,44169700
...
1984-09-07,0.0996047,0.100827,0.0984022,0.0996047,98811715
```

### Ticker Format Reference

| Exchange | Suffix | Example |
|----------|--------|---------|
| US Stocks | `.us` | `aapl.us`, `msft.us` |
| US Indices | `^` prefix | `^spx`, `^ndq`, `^dji` |
| US ETFs | `.us` | `spy.us`, `qqq.us` |
| Warsaw SE | No suffix | `pkn`, `pko` |

### Error Handling

| Scenario | HTTP Status | Response |
|----------|-------------|----------|
| Valid ticker | 200 | CSV data |
| Invalid/delisted ticker | 200 | Empty or single header row |
| Rate limited | 429 | Empty response |
| Server error | 500 | Error page |

## Authentication

### Free Tier (Default)

No authentication required. Rate limited to approximately 1 request/second.

### Premium Tier (Optional)

For higher rate limits, Stooq offers premium subscriptions. The adapter supports optional authentication:

**Environment Variables**:
```bash
STOOQ_USERNAME=kenstott
STOOQ_PASSWORD=14May1959
```

**Authentication Method**: HTTP Basic Auth or session cookie (TBD based on testing)

## Data Schema

### Avro Schema (Compatible with existing `AlphaVantageDownloader`)

```json
{
  "type": "record",
  "name": "StockPrice",
  "fields": [
    {"name": "cik", "type": "string"},
    {"name": "date", "type": "string"},
    {"name": "open", "type": ["null", "double"], "default": null},
    {"name": "high", "type": ["null", "double"], "default": null},
    {"name": "low", "type": ["null", "double"], "default": null},
    {"name": "close", "type": ["null", "double"], "default": null},
    {"name": "adj_close", "type": ["null", "double"], "default": null},
    {"name": "volume", "type": ["null", "long"], "default": null}
  ]
}
```

**Notes**:
- `adj_close`: Stooq returns split-adjusted prices, so `close` equals `adj_close`
- `cik`: Added during conversion from ticker-based download
- `volume`: May be null for some historical dates

### Parquet Storage Structure

```
{basePath}/stock_prices/
├── ticker=AAPL/
│   ├── year=2020/
│   │   └── aapl_prices.parquet
│   ├── year=2021/
│   │   └── aapl_prices.parquet
│   └── ...
├── ticker=MSFT/
│   └── ...
└── ...
```

## Class Design

### `StooqDownloader.java`

```java
package org.apache.calcite.adapter.govdata.sec;

/**
 * Downloads historical stock price data from Stooq.com and stores it in Parquet format.
 *
 * <p>Stooq provides free bulk historical stock data with permissive rate limits,
 * making it ideal for downloading prices for thousands of tickers.
 *
 * <p>Features:
 * <ul>
 *   <li>Full historical data in single request (no pagination)</li>
 *   <li>Permissive rate limiting (~1 req/sec)</li>
 *   <li>Split-adjusted prices</li>
 *   <li>Optional authentication for premium accounts</li>
 * </ul>
 */
public class StooqDownloader {

    private static final String STOOQ_BASE_URL = "https://stooq.com/q/d/l/";
    private static final String USER_AGENT = "Apache Calcite SEC Adapter";

    // Parallel downloads - keep low to avoid triggering rate limits
    private static final int MAX_PARALLEL_DOWNLOADS = 1;  // Sequential recommended

    private final StorageProvider storageProvider;
    private final String username;  // Optional, from STOOQ_USERNAME
    private final String password;  // Optional, from STOOQ_PASSWORD
    private final ExecutorService downloadExecutor;
    private final RateLimiter rateLimiter = new RateLimiter();  // Thread-safe rate limiter

    /**
     * Creates a StooqDownloader.
     *
     * @param storageProvider Storage provider for Parquet output
     * @param username Optional Stooq username (null for free tier)
     * @param password Optional Stooq password (null for free tier)
     */
    public StooqDownloader(StorageProvider storageProvider,
                           String username,
                           String password);

    /**
     * Downloads stock prices for multiple tickers.
     *
     * @param basePath Base directory for stock_prices partition
     * @param tickerCikPairs List of ticker-CIK pairs to download
     * @param startYear Start year for data range
     * @param endYear End year for data range
     */
    public void downloadStockPrices(String basePath,
                                    List<TickerCikPair> tickerCikPairs,
                                    int startYear,
                                    int endYear);

    /**
     * Downloads data for a single ticker.
     */
    private void downloadTickerData(String basePath,
                                    TickerCikPair pair,
                                    int startYear,
                                    int endYear);

    /**
     * Fetches CSV data from Stooq API with rate limiting and retry.
     * See Rate Limiting section for implementation details.
     */
    private List<StockPriceRecord> fetchWithRateLimiting(String ticker, int year)
            throws IOException, InterruptedException;

    /**
     * Parses CSV response into StockPriceRecord objects.
     */
    private List<StockPriceRecord> parseCsvResponse(BufferedReader reader,
                                                     int year) throws IOException;

    /**
     * Writes records to Parquet file.
     */
    private void writeToParquet(String path,
                                List<StockPriceRecord> prices,
                                String cik) throws IOException;
}
```

### Public API

```java
// Reuse existing TickerCikPair from AlphaVantageDownloader
public static class TickerCikPair {
    public final String ticker;
    public final String cik;

    public TickerCikPair(String ticker, String cik) {
        this.ticker = ticker;
        this.cik = cik;
    }
}
```

## Implementation Details

### URL Construction

```java
private String buildStooqUrl(String ticker, Integer startYear, Integer endYear) {
    StringBuilder url = new StringBuilder(STOOQ_BASE_URL);
    url.append("?s=").append(ticker.toLowerCase()).append(".us");
    url.append("&i=d");  // Daily interval

    if (startYear != null) {
        url.append("&d1=").append(startYear).append("0101");
    }
    if (endYear != null) {
        url.append("&d2=").append(endYear).append("1231");
    }

    return url.toString();
}
```

### CSV Parsing

```java
private List<StockPriceRecord> parseCsvResponse(BufferedReader reader, int filterYear)
        throws IOException {
    List<StockPriceRecord> prices = new ArrayList<>();
    String line;
    boolean headerSkipped = false;

    while ((line = reader.readLine()) != null) {
        if (!headerSkipped) {
            headerSkipped = true;
            continue;  // Skip header: "Date,Open,High,Low,Close,Volume"
        }

        String[] parts = line.split(",");
        if (parts.length < 5) {
            continue;  // Invalid row
        }

        String dateStr = parts[0];  // YYYY-MM-DD format

        // Filter by year if specified
        if (!dateStr.startsWith(String.valueOf(filterYear))) {
            continue;
        }

        StockPriceRecord record = new StockPriceRecord();
        record.date = dateStr;
        record.open = parseDouble(parts[1]);
        record.high = parseDouble(parts[2]);
        record.low = parseDouble(parts[3]);
        record.close = parseDouble(parts[4]);
        record.adjClose = record.close;  // Stooq returns adjusted prices
        record.volume = parts.length > 5 ? parseLong(parts[5]) : null;

        prices.add(record);
    }

    return prices;
}
```

### Rate Limiting

Stooq enforces a 1-second rate limit. The implementation uses a two-pronged strategy:

1. **Proactive Throttling**: Track time between calls and wait if needed
2. **Reactive Detection**: Detect rate limiting responses and retry with backoff

#### Rate Limiter Class

```java
/**
 * Thread-safe rate limiter that enforces minimum delay between requests
 * and handles rate limit responses with exponential backoff.
 */
private static class RateLimiter {
    private static final long BASE_RATE_LIMIT_MS = 1000;  // 1 second minimum
    private static final long MAX_BACKOFF_MS = 30000;     // 30 second max backoff
    private static final int MAX_RETRIES = 3;

    private long lastRequestTime = 0;
    private long currentBackoffMs = BASE_RATE_LIMIT_MS;

    /**
     * Wait until rate limit window has passed.
     * Call this BEFORE making each request.
     */
    public synchronized void waitForRateLimit() throws InterruptedException {
        long now = System.currentTimeMillis();
        long timeSinceLastRequest = now - lastRequestTime;
        long waitTime = currentBackoffMs - timeSinceLastRequest;

        if (waitTime > 0) {
            LOGGER.debug("Rate limiter: waiting {}ms before next request", waitTime);
            Thread.sleep(waitTime);
        }

        lastRequestTime = System.currentTimeMillis();
    }

    /**
     * Call this after a successful request to reset backoff.
     */
    public synchronized void onSuccess() {
        currentBackoffMs = BASE_RATE_LIMIT_MS;
    }

    /**
     * Call this when rate limited. Increases backoff exponentially.
     */
    public synchronized void onRateLimited() {
        currentBackoffMs = Math.min(currentBackoffMs * 2, MAX_BACKOFF_MS);
        LOGGER.warn("Rate limited. Increasing backoff to {}ms", currentBackoffMs);
    }

    public int getMaxRetries() {
        return MAX_RETRIES;
    }
}

private final RateLimiter rateLimiter = new RateLimiter();
```

#### Rate-Limited Fetch with Retry

```java
/**
 * Fetches data with automatic rate limiting and retry on rate limit errors.
 */
private List<StockPriceRecord> fetchWithRateLimiting(String ticker, int year)
        throws IOException, InterruptedException {

    int attempts = 0;
    while (attempts < rateLimiter.getMaxRetries()) {
        attempts++;

        // Proactive: wait for rate limit window
        rateLimiter.waitForRateLimit();

        try {
            HttpURLConnection conn = createConnection(buildStooqUrl(ticker, year, year));
            int responseCode = conn.getResponseCode();

            // Reactive: detect rate limiting
            if (isRateLimited(conn, responseCode)) {
                LOGGER.warn("Rate limited on attempt {} for ticker {}", attempts, ticker);
                rateLimiter.onRateLimited();
                conn.disconnect();
                continue;  // Retry with increased backoff
            }

            if (responseCode != 200) {
                throw new IOException("HTTP error code: " + responseCode);
            }

            // Success - parse and reset backoff
            List<StockPriceRecord> records;
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()))) {
                records = parseCsvResponse(reader, year);
            }

            rateLimiter.onSuccess();
            return records;

        } catch (SocketTimeoutException e) {
            LOGGER.warn("Timeout on attempt {} for ticker {}: {}", attempts, ticker, e.getMessage());
            rateLimiter.onRateLimited();
            // Fall through to retry
        }
    }

    throw new IOException("Failed to fetch " + ticker + " after " + attempts + " attempts");
}

/**
 * Detects if response indicates rate limiting.
 * Stooq may return 429, empty response, or specific error patterns.
 */
private boolean isRateLimited(HttpURLConnection conn, int responseCode) throws IOException {
    // Explicit 429 Too Many Requests
    if (responseCode == 429) {
        return true;
    }

    // Check for empty or suspiciously small response (may indicate soft rate limit)
    if (responseCode == 200) {
        String contentLength = conn.getHeaderField("Content-Length");
        if (contentLength != null) {
            long length = Long.parseLong(contentLength);
            // Header-only response (~40 bytes) may indicate rate limiting
            if (length < 50) {
                LOGGER.debug("Suspiciously small response ({}B), may be rate limited", length);
                return true;
            }
        }
    }

    // Check for rate limit headers (if Stooq provides them)
    String retryAfter = conn.getHeaderField("Retry-After");
    if (retryAfter != null) {
        LOGGER.info("Server requested Retry-After: {}", retryAfter);
        return true;
    }

    return false;
}
```

#### Rate Limiting Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    fetchWithRateLimiting()                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  STEP 1: Proactive Wait                                      │
│  ─────────────────────                                       │
│  rateLimiter.waitForRateLimit()                              │
│  - Calculate: timeSinceLastRequest = now - lastRequestTime   │
│  - If timeSinceLastRequest < currentBackoffMs: sleep         │
│  - Update: lastRequestTime = now                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  STEP 2: Make Request                                        │
│  ────────────────────                                        │
│  conn = createConnection(url)                                │
│  responseCode = conn.getResponseCode()                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  STEP 3: Check Rate Limiting                                 │
│  ───────────────────────────                                 │
│  if isRateLimited(conn, responseCode):                       │
│    - rateLimiter.onRateLimited() → double backoff            │
│    - if attempts < MAX_RETRIES: goto STEP 1                  │
│    - else: throw IOException                                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  STEP 4: Process Success                                     │
│  ───────────────────────                                     │
│  records = parseCsvResponse(reader, year)                    │
│  rateLimiter.onSuccess() → reset backoff to 1000ms           │
│  return records                                              │
└─────────────────────────────────────────────────────────────┘
```

#### Backoff Progression

| Attempt | Backoff | Cumulative Wait |
|---------|---------|-----------------|
| 1 | 1000ms | 1s |
| 2 | 2000ms | 3s |
| 3 | 4000ms | 7s |
| 4 | 8000ms | 15s |
| 5 | 16000ms | 31s |
| 6+ | 30000ms (max) | ... |

### Caching Strategy

```java
private boolean needsDownload(String fullPath, int year, int currentYear,
                               ZonedDateTime nowEst, ZonedDateTime todayMarketClose)
        throws IOException {
    if (!storageProvider.exists(fullPath)) {
        return true;
    }

    // Historical years: use cached data
    if (year < currentYear) {
        LOGGER.info("Using cached stock prices for {} year {} (historical)", ticker, year);
        return false;
    }

    // Current year: refresh after market close (4:30 PM EST)
    StorageProvider.FileMetadata metadata = storageProvider.getMetadata(fullPath);
    ZonedDateTime fileModifiedEst = Instant.ofEpochMilli(metadata.getLastModified())
        .atZone(ZoneId.of("America/New_York"));

    if (nowEst.isAfter(todayMarketClose) && fileModifiedEst.isBefore(todayMarketClose)) {
        LOGGER.info("Stock prices need refresh (modified {}, market closed at {})",
            fileModifiedEst, todayMarketClose);
        return true;
    }

    return false;
}
```

### Optional Authentication

```java
private HttpURLConnection createConnection(String urlString) throws IOException {
    URL url = new URL(urlString);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("User-Agent", USER_AGENT);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);

    // Add authentication if credentials provided
    if (username != null && password != null && !username.isEmpty()) {
        String auth = username + ":" + password;
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
        conn.setRequestProperty("Authorization", "Basic " + encodedAuth);
    }

    return conn;
}
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `STOOQ_USERNAME` | Optional Stooq username for premium | `null` |
| `STOOQ_PASSWORD` | Optional Stooq password for premium | `null` |
| `STOCK_PRICE_SOURCE` | Stock price provider: `stooq`, `alphavantage`, `yahoo` | `stooq` |
| `STOCK_PRICE_BASE_RATE_LIMIT_MS` | Base milliseconds between requests | `1000` |
| `STOCK_PRICE_MAX_BACKOFF_MS` | Maximum backoff on rate limiting | `30000` |
| `STOCK_PRICE_MAX_RETRIES` | Maximum retry attempts per ticker | `3` |

### Model File Configuration

```json
{
  "schemas": [{
    "name": "SEC",
    "factory": "org.apache.calcite.adapter.govdata.sec.SecSchemaFactory",
    "operand": {
      "fetchStockPrices": true,
      "stockPriceSource": "stooq",
      "stooqUsername": "${STOOQ_USERNAME}",
      "stooqPassword": "${STOOQ_PASSWORD}",
      "ciks": ["AAPL", "MSFT", "GOOGL"],
      "startYear": 2020,
      "endYear": 2024
    }
  }]
}
```

### Connection URL

```java
String url = "jdbc:govdata:source=sec&ciks=AAPL&fetchStockPrices=true&stockPriceSource=stooq";
```

## Migration Plan

### Phase 1: Parallel Implementation

1. Create `StooqDownloader.java` as new downloader class
2. Add `stockPriceSource` configuration parameter
3. Default to `stooq`, fallback to `alphavantage`
4. Maintain backward compatibility with existing Parquet schema

### Phase 2: Testing & Validation

1. Run integration tests comparing data quality:
   - Stooq vs Alpha Vantage for same ticker/date range
   - Verify price accuracy within acceptable tolerance
2. Test rate limiting behavior under load
3. Validate authentication with premium credentials

### Phase 3: Deprecation

1. Mark `AlphaVantageDownloader` as `@Deprecated`
2. Mark `YahooFinanceDownloader` as `@Deprecated` (if exists)
3. Log deprecation warnings when legacy sources used
4. Update documentation

### Phase 4: Removal

1. Remove deprecated downloaders after validation period (suggest: 2 releases)
2. Remove related configuration options
3. Clean up tests

## Error Handling

### Error Classification

| Error Type | Detection | Recovery Strategy |
|------------|-----------|-------------------|
| Rate Limited | HTTP 429, small response, Retry-After header | Exponential backoff, retry up to 3 times |
| Network Timeout | SocketTimeoutException | Treat as rate limit, backoff and retry |
| Invalid Ticker | HTTP 200 with empty CSV body | Log warning, skip ticker, no retry |
| Server Error | HTTP 5xx | Log error, skip ticker, no retry |
| Connection Failed | IOException | Log error, skip ticker, no retry |

### Integrated Error Handling in fetchWithRateLimiting()

The rate-limited fetch method (see Rate Limiting section) handles all recoverable errors:

```java
private List<StockPriceRecord> fetchWithRateLimiting(String ticker, int year)
        throws IOException, InterruptedException {

    int attempts = 0;
    IOException lastException = null;

    while (attempts < rateLimiter.getMaxRetries()) {
        attempts++;
        rateLimiter.waitForRateLimit();

        try {
            HttpURLConnection conn = createConnection(buildStooqUrl(ticker, year, year));
            int responseCode = conn.getResponseCode();

            // Rate limited - recoverable
            if (isRateLimited(conn, responseCode)) {
                LOGGER.warn("Rate limited on attempt {}/{} for {}",
                    attempts, rateLimiter.getMaxRetries(), ticker);
                rateLimiter.onRateLimited();
                conn.disconnect();
                continue;
            }

            // Server error - not recoverable
            if (responseCode >= 500) {
                LOGGER.error("Server error {} for ticker {}", responseCode, ticker);
                throw new IOException("Server error: " + responseCode);
            }

            // Client error (except 429) - not recoverable
            if (responseCode >= 400) {
                LOGGER.error("Client error {} for ticker {}", responseCode, ticker);
                throw new IOException("Client error: " + responseCode);
            }

            // Success - parse response
            List<StockPriceRecord> records;
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()))) {
                records = parseCsvResponse(reader, year);
            }

            rateLimiter.onSuccess();
            return records;

        } catch (SocketTimeoutException e) {
            // Timeout - treat as rate limit, recoverable
            LOGGER.warn("Timeout on attempt {}/{} for {}: {}",
                attempts, rateLimiter.getMaxRetries(), ticker, e.getMessage());
            lastException = e;
            rateLimiter.onRateLimited();
        } catch (ConnectException e) {
            // Connection failed - not recoverable
            LOGGER.error("Connection failed for {}: {}", ticker, e.getMessage());
            throw e;
        }
    }

    throw new IOException("Failed after " + attempts + " attempts: " +
        (lastException != null ? lastException.getMessage() : "rate limited"));
}
```

### Empty Response Handling

Empty responses after successful fetch indicate invalid/delisted ticker, not rate limiting:

```java
List<StockPriceRecord> prices = fetchWithRateLimiting(ticker, year);

if (prices.isEmpty()) {
    // Ticker may be delisted or invalid - this is NOT rate limiting
    // (rate limiting detected earlier in isRateLimited())
    LOGGER.warn("No data returned for {} year {} - ticker may be delisted or invalid",
        ticker, year);
    return;  // Don't create empty Parquet file, don't retry
}
```

### Per-Ticker Error Isolation

Errors for one ticker should not affect others:

```java
for (TickerCikPair pair : tickerCikPairs) {
    try {
        downloadTickerData(basePath, pair, startYear, endYear);
    } catch (IOException e) {
        // Log and continue with next ticker
        LOGGER.error("Failed to download {}: {}. Continuing with remaining tickers.",
            pair.ticker, e.getMessage());
        failedTickers.add(pair.ticker);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Download interrupted", e);
    }
}

if (!failedTickers.isEmpty()) {
    LOGGER.warn("Failed to download {} tickers: {}", failedTickers.size(), failedTickers);
}
```

## Testing Strategy

### Unit Tests

```java
@Test
void testUrlConstruction() {
    String url = downloader.buildStooqUrl("AAPL", 2020, 2024);
    assertEquals("https://stooq.com/q/d/l/?s=aapl.us&i=d&d1=20200101&d2=20241231", url);
}

@Test
void testCsvParsing() {
    String csv = "Date,Open,High,Low,Close,Volume\n" +
                 "2024-12-13,248.49,251.14,247.48,248.13,49286500\n";
    List<StockPriceRecord> records = downloader.parseCsvResponse(
        new BufferedReader(new StringReader(csv)), 2024);
    assertEquals(1, records.size());
    assertEquals("2024-12-13", records.get(0).date);
    assertEquals(248.13, records.get(0).close, 0.01);
}
```

### Integration Tests

```java
@Test
@Tag("integration")
void testStooqDownloadApple() {
    StooqDownloader downloader = new StooqDownloader(storageProvider, null, null);
    List<TickerCikPair> pairs = Arrays.asList(
        new TickerCikPair("AAPL", "0000320193")
    );

    downloader.downloadStockPrices(basePath, pairs, 2024, 2024);

    String parquetPath = basePath + "/stock_prices/ticker=AAPL/year=2024/aapl_prices.parquet";
    assertTrue(storageProvider.exists(parquetPath));
}

@Test
@Tag("integration")
void testStooqRateLimiting() {
    // Download 10 tickers and verify requests are spaced ~1 second apart
    long startTime = System.currentTimeMillis();
    downloader.downloadStockPrices(basePath, tenTickers, 2024, 2024);
    long elapsed = System.currentTimeMillis() - startTime;

    assertTrue(elapsed >= 9000, "Should take at least 9 seconds for 10 requests");
}
```

### Data Quality Validation

```java
@Test
@Tag("integration")
void testDataQualityVsAlphaVantage() {
    // Compare Stooq data with Alpha Vantage for same ticker/date
    List<StockPriceRecord> stooqData = stooqDownloader.fetchData("AAPL", 2024);
    List<StockPriceRecord> avData = alphaVantageDownloader.fetchData("AAPL", 2024);

    // Find matching dates and compare prices
    for (StockPriceRecord stooq : stooqData) {
        StockPriceRecord av = findByDate(avData, stooq.date);
        if (av != null) {
            // Allow 1% tolerance for price differences
            assertEquals(stooq.close, av.close, av.close * 0.01);
        }
    }
}
```

## Performance Estimates

### Download Time for Full SEC Coverage

With the rate limiter enforcing 1 request/second (sequential):

| Tickers | Base Rate | With Retries (5%) | Total Time |
|---------|-----------|-------------------|------------|
| 100 | 1 req/sec | ~5 retries @ 2s avg | ~2 min |
| 1,000 | 1 req/sec | ~50 retries @ 2s avg | ~18 min |
| 6,000 | 1 req/sec | ~300 retries @ 2s avg | ~110 min |

**Notes**:
- Sequential downloads recommended to avoid triggering rate limits
- Retries add ~2 seconds average per retry (exponential backoff)
- Assuming 5% of requests hit rate limits and require 1 retry

### Worst Case (Heavy Rate Limiting)

If Stooq is heavily rate limiting:

| Scenario | Backoff Pattern | Impact |
|----------|-----------------|--------|
| Occasional (1%) | 1s → 2s → success | +1% time |
| Moderate (10%) | 1s → 2s → 4s → success | +30% time |
| Severe (25%) | 1s → 2s → 4s → 8s → success | +100% time |

### Storage Requirements

| Data | Estimate |
|------|----------|
| Per ticker (full history) | ~50 KB compressed Parquet |
| 6,000 tickers | ~300 MB |
| With yearly partitions | ~350 MB (slight overhead) |

## Gotchas and Limitations

1. **Delisted Stocks**: Not available (survivorship bias - same as other sources)
2. **Ticker Suffix**: `.us` required for US stocks
3. **ETF Handling**: Some ETFs may need different suffix or no suffix
4. **Weekend/Holiday Gaps**: No trading data (expected)
5. **Split Adjustments**: Prices are split-adjusted, no separate unadjusted column
6. **Dividend Adjustments**: Close price includes split adjustments only, not dividend adjustments

## Index Support

For market indices, use special symbols:

| Index | Stooq Symbol | Description |
|-------|--------------|-------------|
| S&P 500 | `^spx` | Standard & Poor's 500 |
| NASDAQ Composite | `^ndq` | NASDAQ Composite Index |
| Dow Jones | `^dji` | Dow Jones Industrial Average |
| Russell 2000 | `^rut` | Russell 2000 Small Cap |

**URL Example**:
```
https://stooq.com/q/d/l/?s=^spx&i=d
```

## References

- [Stooq.com](https://stooq.com) - Data source
- [STOCK_PRICES.md](./STOCK_PRICES.md) - Current stock price architecture
- [AlphaVantageDownloader.java](./src/main/java/org/apache/calcite/adapter/govdata/sec/AlphaVantageDownloader.java) - Existing implementation to replace
- [StorageProvider](../file/src/main/java/org/apache/calcite/adapter/file/storage/StorageProvider.java) - S3-compatible storage abstraction
