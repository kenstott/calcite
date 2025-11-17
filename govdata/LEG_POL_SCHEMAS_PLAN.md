# Legislative and Political Data Schemas - Implementation Plan

**Status**: PLANNING - Future Consideration
**Created**: 2025-10-29
**Estimated Effort**: 8-10 weeks
**Priority**: Not yet scheduled

## Executive Summary

This document outlines a potential expansion of the GovData adapter to include comprehensive federal legislative and political data through two new schemas:

- **LEG (Legislative)** - Congressional bills, votes, committees, hearings, nominations
- **POL (Political)** - Political offices, office holders, campaigns, judicial appointments

These schemas would provide SQL-queryable access to the full legislative process, voting records, campaign finance, and political office data across all three branches of federal government, plus state-level executive offices.

## Business Value

### Use Cases

**Legislative Analysis**:
- Track bill progression through Congress
- Analyze voting patterns by party, state, or member
- Monitor committee activity and membership changes
- Research nomination confirmations
- Study hearing schedules and witness testimony

**Political Research**:
- Track political office holders across federal and state levels
- Analyze presidential executive orders over time
- Study Supreme Court composition changes
- Monitor cabinet appointments and turnover
- Research campaign finance patterns
- Analyze political party leadership evolution

**Cross-Domain Analytics**:
- Correlate legislative activity with economic indicators (ECON schema)
- Link bills to geographic regions (GEO schema)
- Connect regulatory changes to securities performance (SEC schema)

## Schema Design

### LEG Schema (8 Tables)

#### `congress_members`
Congressional members with biographical and service information.

```sql
CREATE TABLE congress_members (
  bioguide_id VARCHAR(7) PRIMARY KEY,  -- e.g., "S000033" (Bernie Sanders)
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  middle_name VARCHAR(100),
  suffix VARCHAR(20),
  nickname VARCHAR(100),
  party VARCHAR(50),                    -- "Democratic", "Republican", "Independent"
  state_code CHAR(2),                   -- Foreign key to GEO schema
  district INTEGER,                      -- NULL for senators, 0 for at-large
  chamber VARCHAR(20),                   -- "House" or "Senate"
  date_of_birth DATE,
  gender VARCHAR(1),
  url VARCHAR(500),
  twitter_account VARCHAR(100),
  youtube_account VARCHAR(100),
  facebook_account VARCHAR(100),
  leadership_role VARCHAR(200),         -- e.g., "Speaker", "Majority Leader"
  next_election INTEGER,
  total_votes INTEGER,
  missed_votes INTEGER,
  missed_votes_pct DECIMAL(5,2),
  votes_with_party_pct DECIMAL(5,2),
  votes_against_party_pct DECIMAL(5,2)
);
```

#### `bills`
Legislative bills from all Congresses.

```sql
CREATE TABLE bills (
  bill_id VARCHAR(50) PRIMARY KEY,      -- e.g., "hr1-118" (congress number included)
  bill_slug VARCHAR(100),                -- e.g., "hr1-118"
  bill_type VARCHAR(10),                 -- "hr", "s", "hjres", "sjres", "hconres", etc.
  bill_number INTEGER,
  congress INTEGER,                      -- Congress number (e.g., 118 for 2023-2024)
  title TEXT,
  short_title TEXT,
  sponsor_id VARCHAR(7),                 -- Foreign key to congress_members
  sponsor_name VARCHAR(200),
  sponsor_party VARCHAR(50),
  sponsor_state CHAR(2),                 -- Foreign key to GEO schema
  introduced_date DATE,
  latest_major_action_date DATE,
  latest_major_action TEXT,
  house_passage_date DATE,
  senate_passage_date DATE,
  enacted_date DATE,
  vetoed_date BOOLEAN,
  active BOOLEAN,
  subjects TEXT[],                       -- Array of subject tags
  committees TEXT[],                     -- Array of committee names
  primary_subject VARCHAR(500),
  summary TEXT,                          -- Summary description
  govtrack_url VARCHAR(500),
  congressdotgov_url VARCHAR(500)
);
```

#### `votes`
Roll call votes in House and Senate.

```sql
CREATE TABLE votes (
  roll_call_id VARCHAR(50) PRIMARY KEY,  -- e.g., "h123-118-2023"
  chamber VARCHAR(20),                   -- "House" or "Senate"
  congress INTEGER,
  session INTEGER,
  roll_call INTEGER,
  vote_date DATE,
  vote_time TIME,
  question TEXT,                         -- Description of vote (e.g., "On Passage")
  description TEXT,                      -- Full vote description
  vote_type VARCHAR(50),                 -- "On Passage", "On Motion to Recommit", etc.
  result VARCHAR(50),                    -- "Passed", "Failed", "Agreed to"
  bill_id VARCHAR(50),                   -- Foreign key to bills (nullable)
  total_yes INTEGER,
  total_no INTEGER,
  total_not_voting INTEGER,
  total_present INTEGER,
  democratic_yes INTEGER,
  democratic_no INTEGER,
  democratic_not_voting INTEGER,
  republican_yes INTEGER,
  republican_no INTEGER,
  republican_not_voting INTEGER,
  independent_yes INTEGER,
  independent_no INTEGER,
  url VARCHAR(500)
);
```

#### `member_votes`
Individual member votes on roll calls.

```sql
CREATE TABLE member_votes (
  roll_call_id VARCHAR(50),              -- Foreign key to votes
  bioguide_id VARCHAR(7),                -- Foreign key to congress_members
  vote_position VARCHAR(20),             -- "Yes", "No", "Not Voting", "Present"
  PRIMARY KEY (roll_call_id, bioguide_id)
);
```

#### `committees`
Congressional committees and subcommittees.

```sql
CREATE TABLE committees (
  committee_id VARCHAR(10) PRIMARY KEY,  -- e.g., "SSFI" (Senate Finance)
  name VARCHAR(300),
  chamber VARCHAR(20),                   -- "House", "Senate", or "Joint"
  parent_committee_id VARCHAR(10),       -- For subcommittees
  url VARCHAR(500)
);
```

#### `committee_members`
Committee membership over time.

```sql
CREATE TABLE committee_members (
  committee_id VARCHAR(10),              -- Foreign key to committees
  bioguide_id VARCHAR(7),                -- Foreign key to congress_members
  congress INTEGER,                      -- Congress number
  rank_in_party INTEGER,
  title VARCHAR(100),                    -- "Chair", "Ranking Member", "Vice Chair"
  party VARCHAR(50),
  begin_date DATE,
  end_date DATE,
  PRIMARY KEY (committee_id, bioguide_id, congress)
);
```

#### `nominations`
Presidential nominations requiring Senate confirmation.

```sql
CREATE TABLE nominations (
  nomination_id VARCHAR(50) PRIMARY KEY,
  nomination_number VARCHAR(20),
  congress INTEGER,
  nominee_name VARCHAR(200),
  position VARCHAR(500),
  organization VARCHAR(300),             -- e.g., "Department of State"
  received_date DATE,
  committee_referral_date DATE,
  committee_id VARCHAR(10),              -- Foreign key to committees
  latest_action_date DATE,
  latest_action TEXT,
  confirmed_date DATE,
  rejected_date DATE,
  returned_date DATE,
  withdrawn_date DATE
);
```

#### `hearings`
Congressional hearings.

```sql
CREATE TABLE hearings (
  hearing_id VARCHAR(100) PRIMARY KEY,
  congress INTEGER,
  chamber VARCHAR(20),
  committee_id VARCHAR(10),              -- Foreign key to committees
  title TEXT,
  hearing_date DATE,
  location VARCHAR(500),
  description TEXT,
  witnesses TEXT[],                      -- Array of witness names
  url VARCHAR(500)
);
```

### POL Schema (11 Tables)

#### `presidents`
U.S. Presidents with term information.

```sql
CREATE TABLE presidents (
  president_number INTEGER PRIMARY KEY,
  full_name VARCHAR(200),
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  party VARCHAR(50),
  term_start DATE,
  term_end DATE,
  birth_date DATE,
  death_date DATE,
  birth_state CHAR(2),                   -- Foreign key to GEO schema
  vice_president VARCHAR(200),
  prior_office VARCHAR(300),
  education VARCHAR(500),
  notes TEXT
);
```

#### `vice_presidents`
U.S. Vice Presidents.

```sql
CREATE TABLE vice_presidents (
  vp_number INTEGER PRIMARY KEY,
  full_name VARCHAR(200),
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  party VARCHAR(50),
  term_start DATE,
  term_end DATE,
  president_number INTEGER,              -- Foreign key to presidents
  birth_date DATE,
  death_date DATE,
  birth_state CHAR(2),                   -- Foreign key to GEO schema
  prior_office VARCHAR(300),
  notes TEXT
);
```

#### `cabinet_members`
Cabinet secretaries and other cabinet-level positions.

```sql
CREATE TABLE cabinet_members (
  cabinet_member_id VARCHAR(50) PRIMARY KEY,
  full_name VARCHAR(200),
  position VARCHAR(200),                 -- e.g., "Secretary of State"
  department VARCHAR(200),               -- e.g., "State Department"
  president_number INTEGER,              -- Foreign key to presidents
  party VARCHAR(50),
  confirmed_date DATE,
  start_date DATE,
  end_date DATE,
  state_of_origin CHAR(2),               -- Foreign key to GEO schema
  prior_office VARCHAR(500),
  notes TEXT
);
```

#### `governors`
State governors (current and historical).

```sql
CREATE TABLE governors (
  governor_id VARCHAR(50) PRIMARY KEY,
  state_code CHAR(2),                    -- Foreign key to GEO schema
  full_name VARCHAR(200),
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  party VARCHAR(50),
  term_start DATE,
  term_end DATE,
  term_number INTEGER,                   -- Which term (1st, 2nd, etc.)
  birth_date DATE,
  birth_state CHAR(2),
  prior_office VARCHAR(300),
  notes TEXT
);
```

#### `supreme_court_justices`
Supreme Court justices (current and historical).

```sql
CREATE TABLE supreme_court_justices (
  justice_id VARCHAR(50) PRIMARY KEY,
  full_name VARCHAR(200),
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  title VARCHAR(100),                    -- "Chief Justice", "Associate Justice"
  nominated_by INTEGER,                  -- Foreign key to presidents
  nomination_date DATE,
  confirmation_date DATE,
  start_date DATE,
  end_date DATE,
  end_reason VARCHAR(100),               -- "Death", "Retirement", etc.
  birth_date DATE,
  death_date DATE,
  birth_state CHAR(2),                   -- Foreign key to GEO schema
  law_school VARCHAR(300),
  prior_positions TEXT[],                -- Array of prior judicial/legal positions
  notes TEXT
);
```

#### `federal_judges`
Federal district and circuit court judges.

```sql
CREATE TABLE federal_judges (
  judge_id VARCHAR(50) PRIMARY KEY,
  full_name VARCHAR(200),
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  court_type VARCHAR(50),                -- "District", "Circuit", "Bankruptcy"
  court_name VARCHAR(300),               -- e.g., "U.S. Court of Appeals for the 9th Circuit"
  nominated_by INTEGER,                  -- Foreign key to presidents
  nomination_date DATE,
  confirmation_date DATE,
  start_date DATE,
  end_date DATE,
  senior_status_date DATE,
  birth_date DATE,
  death_date DATE,
  birth_state CHAR(2),                   -- Foreign key to GEO schema
  law_school VARCHAR(300),
  notes TEXT
);
```

#### `campaigns`
Campaign finance data from FEC.

```sql
CREATE TABLE campaigns (
  campaign_id VARCHAR(50) PRIMARY KEY,   -- FEC committee ID
  candidate_name VARCHAR(200),
  bioguide_id VARCHAR(7),                -- Foreign key to congress_members (if applicable)
  office VARCHAR(50),                    -- "House", "Senate", "President"
  state_code CHAR(2),                    -- Foreign key to GEO schema
  district INTEGER,                      -- For House races
  party VARCHAR(50),
  election_year INTEGER,
  total_receipts DECIMAL(15,2),
  total_disbursements DECIMAL(15,2),
  cash_on_hand DECIMAL(15,2),
  total_contributions DECIMAL(15,2),
  individual_contributions DECIMAL(15,2),
  pac_contributions DECIMAL(15,2),
  candidate_contributions DECIMAL(15,2),
  last_updated DATE
);
```

#### `contributions`
Individual campaign contributions (large contributions only, per FEC reporting).

```sql
CREATE TABLE contributions (
  contribution_id VARCHAR(100) PRIMARY KEY,
  campaign_id VARCHAR(50),               -- Foreign key to campaigns
  contributor_name VARCHAR(300),
  contributor_city VARCHAR(200),
  contributor_state CHAR(2),             -- Foreign key to GEO schema
  contributor_zip VARCHAR(10),
  contributor_employer VARCHAR(300),
  contributor_occupation VARCHAR(200),
  contribution_date DATE,
  contribution_amount DECIMAL(10,2),
  contribution_type VARCHAR(50),         -- "Individual", "PAC", etc.
  election_year INTEGER
);
```

#### `party_leadership`
Congressional party leadership positions over time.

```sql
CREATE TABLE party_leadership (
  leadership_id VARCHAR(50) PRIMARY KEY,
  bioguide_id VARCHAR(7),                -- Foreign key to congress_members
  chamber VARCHAR(20),                   -- "House" or "Senate"
  congress INTEGER,
  party VARCHAR(50),
  position VARCHAR(200),                 -- "Speaker", "Majority Leader", "Whip", etc.
  start_date DATE,
  end_date DATE
);
```

#### `caucuses`
Congressional caucuses and coalitions.

```sql
CREATE TABLE caucuses (
  caucus_id VARCHAR(50) PRIMARY KEY,
  name VARCHAR(300),
  chamber VARCHAR(20),                   -- "House", "Senate", "Both"
  description TEXT,
  founded_date DATE,
  active BOOLEAN
);
```

#### `caucus_members`
Caucus membership over time.

```sql
CREATE TABLE caucus_members (
  caucus_id VARCHAR(50),                 -- Foreign key to caucuses
  bioguide_id VARCHAR(7),                -- Foreign key to congress_members
  congress INTEGER,
  join_date DATE,
  leave_date DATE,
  PRIMARY KEY (caucus_id, bioguide_id, congress)
);
```

#### `executive_orders`
Presidential executive orders.

```sql
CREATE TABLE executive_orders (
  executive_order_number INTEGER PRIMARY KEY,
  president_number INTEGER,              -- Foreign key to presidents
  president_name VARCHAR(200),
  signing_date DATE,
  title TEXT,
  subject TEXT,
  summary TEXT,
  revoked_by INTEGER,                    -- Foreign key to executive_orders (self-reference)
  amended_by TEXT[],                     -- Array of EO numbers that amended this
  url VARCHAR(500)
);
```

## Data Sources

### Legislative Data (LEG Schema)

**ProPublica Congress API** (Primary source)
- **Coverage**: 113th Congress forward (2013-present)
- **Data**: Members, bills, votes, committees, nominations
- **API Key**: Required (free for non-commercial use)
- **Rate Limits**: 5,000 requests per day
- **Documentation**: https://projects.propublica.org/api-docs/congress-api/

**Congress.gov API** (Supplementary)
- **Coverage**: 117th Congress forward (2021-present)
- **Data**: Bills, amendments, summaries, actions
- **API Key**: Required (free)
- **Rate Limits**: Reasonable use
- **Documentation**: https://api.congress.gov/

**GovInfo API** (Historical supplement)
- **Coverage**: 1973-present (94th Congress forward)
- **Data**: Bill text, Congressional Record, hearings
- **API Key**: Required (free)
- **Rate Limits**: None specified
- **Documentation**: https://api.govinfo.gov/docs/

### Political Data (POL Schema)

**Ballotpedia API**
- **Coverage**: Current and historical office holders
- **Data**: Presidents, governors, state officials, judicial appointments
- **API Key**: Required (commercial pricing)
- **Rate Limits**: Varies by plan
- **Documentation**: https://ballotpedia.org/API-documentation

**Federal Election Commission (FEC) API**
- **Coverage**: 1980-present
- **Data**: Campaign finance, contributions, expenditures
- **API Key**: Required (free)
- **Rate Limits**: 1,000 requests per hour
- **Documentation**: https://api.open.fec.gov/developers/

**Federal Judicial Center**
- **Coverage**: Complete historical data for federal judges
- **Data**: Biographical data, service dates, nominations
- **API Key**: Not required (web scraping with permission)
- **Documentation**: https://www.fjc.gov/history/judges

**White House API / Archives**
- **Coverage**: All presidencies
- **Data**: Executive orders, proclamations, presidential documents
- **API Key**: Not required
- **Documentation**: https://www.federalregister.gov/api/v1

## Implementation Phases

### Phase 1: LEG Schema - Core Legislative Data (3 weeks)

**Week 1-2: Data downloaders and schema setup**
- Create `LegislativeDataDownloader.java`
- Implement ProPublica Congress API integration
- Create JSON cache structure for members, bills, votes
- Build basic parquet conversion pipeline
- Create SQL table definitions

**Week 2-3: Core tables implementation**
- `congress_members` table (with current member data)
- `bills` table (117th-118th Congress initially)
- `votes` and `member_votes` tables
- Integration tests for data quality

### Phase 2: LEG Schema - Extended Legislative Data (2 weeks)

**Week 3-4: Committees and leadership**
- `committees` table
- `committee_members` table
- `nominations` table
- `hearings` table (basic implementation)
- Historical data backfill (113th Congress forward)

### Phase 3: POL Schema - Political Offices (2 weeks)

**Week 5-6: Executive and judicial data**
- Create `PoliticalDataDownloader.java`
- `presidents` table (complete historical data)
- `vice_presidents` table
- `cabinet_members` table
- `supreme_court_justices` table
- `federal_judges` table (district and circuit courts)
- Integration tests

### Phase 4: POL Schema - Electoral Data (2 weeks)

**Week 6-7: Campaign finance and governors**
- FEC API integration
- `campaigns` table (congressional candidates)
- `contributions` table (large contributions)
- `governors` table (current and historical)
- `party_leadership` table
- `caucuses` and `caucus_members` tables

### Phase 5: Advanced Features (1 week)

**Week 8: Cross-schema integration and queries**
- `executive_orders` table
- Foreign key validation across LEG, POL, GEO schemas
- Example analytical queries
- Performance optimization
- Documentation and examples

### Phase 6: Historical Backfill (Ongoing)

**Post-implementation**
- Backfill bills to 94th Congress (1973) using GovInfo API
- Complete campaign finance data (1980-present)
- Historical committee membership (1973-present)
- Older judicial appointments (pre-1950)

## Technical Architecture

### Storage Structure

```
source=econ/
  type=congress_members/year=2024/congress_members.json
  type=bills/congress=118/bills.json
  type=votes/year=2024/chamber=house/votes.json
  type=votes/year=2024/chamber=senate/votes.json
  type=committees/congress=118/committees.json
  type=nominations/year=2024/nominations.json
  type=hearings/year=2024/hearings.json
  type=presidents/presidents.json
  type=governors/state=CA/year=2024/governors.json
  type=cabinet/president=46/cabinet.json
  type=supreme_court/justices.json
  type=federal_judges/court_type=circuit/judges.json
  type=campaigns/year=2024/election_type=house/campaigns.json
  type=executive_orders/president=46/executive_orders.json
```

### Parquet Conversion

New converter classes:
- `LegRawToParquetConverter.java` - Handles LEG schema conversions
- `PolRawToParquetConverter.java` - Handles POL schema conversions

Integration with `FileSchema`:
```java
LegRawToParquetConverter legConverter = new LegRawToParquetConverter(legislativeDownloader);
PolRawToParquetConverter polConverter = new PolRawToParquetConverter(politicalDownloader);
fileSchema.registerRawToParquetConverter(legConverter);
fileSchema.registerRawToParquetConverter(polConverter);
```

### Cross-Schema Foreign Keys

**LEG → GEO**:
- `congress_members.state_code` → `states.state_code`
- `bills.sponsor_state` → `states.state_code`

**POL → GEO**:
- `governors.state_code` → `states.state_code`
- `campaigns.state_code` → `states.state_code`
- `presidents.birth_state` → `states.state_code`

**POL → LEG**:
- `campaigns.bioguide_id` → `congress_members.bioguide_id`

**LEG → POL**:
- `supreme_court_justices.nominated_by` → `presidents.president_number`
- `cabinet_members.president_number` → `presidents.president_number`

## Example Queries

### Legislative Analysis

**Find all bills sponsored by Bernie Sanders in 118th Congress:**
```sql
SELECT bill_id, short_title, introduced_date, latest_major_action
FROM bills
WHERE sponsor_id = 'S000033'
  AND congress = 118
ORDER BY introduced_date DESC;
```

**Analyze voting patterns on climate legislation:**
```sql
SELECT
  m.state_code,
  m.party,
  COUNT(*) as total_climate_votes,
  SUM(CASE WHEN mv.vote_position = 'Yes' THEN 1 ELSE 0 END) as yes_votes,
  ROUND(100.0 * SUM(CASE WHEN mv.vote_position = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) as yes_pct
FROM member_votes mv
JOIN congress_members m ON mv.bioguide_id = m.bioguide_id
JOIN votes v ON mv.roll_call_id = v.roll_call_id
JOIN bills b ON v.bill_id = b.bill_id
WHERE 'Climate' = ANY(b.subjects)
GROUP BY m.state_code, m.party
ORDER BY yes_pct DESC;
```

**Track bill progress through committee:**
```sql
SELECT
  b.bill_id,
  b.short_title,
  b.introduced_date,
  c.name as committee_name,
  cm.title as member_role,
  m.full_name as member_name,
  m.party
FROM bills b
JOIN committee_members cm ON cm.congress = b.congress
JOIN committees c ON cm.committee_id = c.committee_id
JOIN congress_members m ON cm.bioguide_id = m.bioguide_id
WHERE b.bill_id = 'hr1-118'
  AND c.name = ANY(b.committees)
ORDER BY cm.rank_in_party;
```

### Political Research

**Presidential cabinet turnover by administration:**
```sql
SELECT
  p.full_name as president,
  p.term_start,
  p.term_end,
  COUNT(DISTINCT cm.cabinet_member_id) as total_cabinet_members,
  AVG(EXTRACT(DAYS FROM (COALESCE(cm.end_date, CURRENT_DATE) - cm.start_date))) as avg_tenure_days
FROM presidents p
LEFT JOIN cabinet_members cm ON p.president_number = cm.president_number
GROUP BY p.president_number, p.full_name, p.term_start, p.term_end
ORDER BY p.term_start DESC;
```

**Supreme Court composition over time:**
```sql
SELECT
  j.full_name,
  j.title,
  p.full_name as nominated_by,
  j.confirmation_date,
  j.start_date,
  j.end_date,
  EXTRACT(YEAR FROM AGE(COALESCE(j.end_date, CURRENT_DATE), j.start_date)) as years_served
FROM supreme_court_justices j
JOIN presidents p ON j.nominated_by = p.president_number
WHERE j.end_date IS NULL OR j.end_date > '2000-01-01'
ORDER BY j.start_date;
```

**Campaign finance by state and election cycle:**
```sql
SELECT
  s.state_name,
  c.election_year,
  c.office,
  SUM(c.total_receipts) as total_raised,
  SUM(c.individual_contributions) as individual_total,
  SUM(c.pac_contributions) as pac_total,
  COUNT(*) as num_campaigns
FROM campaigns c
JOIN geo.states s ON c.state_code = s.state_code
WHERE c.election_year >= 2020
GROUP BY s.state_name, c.election_year, c.office
ORDER BY total_raised DESC;
```

### Cross-Schema Analytics

**Correlate unemployment rates with congressional voting on economic bills:**
```sql
SELECT
  ur.state_code,
  s.state_name,
  ur.year,
  ur.month,
  ur.unemployment_rate,
  COUNT(DISTINCT b.bill_id) as economic_bills_introduced,
  AVG(CASE WHEN mv.vote_position = 'Yes' THEN 1.0 ELSE 0.0 END) as avg_yes_vote_rate
FROM econ.unemployment_rate ur
JOIN geo.states s ON ur.state_code = s.state_code
JOIN congress_members m ON m.state_code = ur.state_code
JOIN bills b ON b.sponsor_id = m.bioguide_id
  AND EXTRACT(YEAR FROM b.introduced_date) = ur.year
  AND 'Economics' = ANY(b.subjects)
LEFT JOIN votes v ON v.bill_id = b.bill_id
LEFT JOIN member_votes mv ON mv.roll_call_id = v.roll_call_id
  AND mv.bioguide_id = m.bioguide_id
WHERE ur.year >= 2020
GROUP BY ur.state_code, s.state_name, ur.year, ur.month, ur.unemployment_rate
ORDER BY ur.unemployment_rate DESC, ur.year, ur.month;
```

**Federal judges appointed during economic recessions:**
```sql
SELECT
  fj.full_name,
  fj.court_name,
  p.full_name as president,
  fj.confirmation_date,
  gdp.value as gdp_growth_rate
FROM federal_judges fj
JOIN presidents p ON fj.nominated_by = p.president_number
JOIN econ.gdp_growth gdp ON EXTRACT(YEAR FROM fj.confirmation_date) = gdp.year
WHERE gdp.value < 0  -- Recession years (negative GDP growth)
ORDER BY fj.confirmation_date DESC;
```

**Governor elections during high inflation periods:**
```sql
SELECT
  g.state_code,
  s.state_name,
  g.full_name,
  g.party,
  g.term_start,
  i.year,
  i.inflation_rate
FROM governors g
JOIN geo.states s ON g.state_code = s.state_code
JOIN econ.inflation_metrics i ON EXTRACT(YEAR FROM g.term_start) = i.year
WHERE i.inflation_rate > 5.0  -- High inflation threshold
ORDER BY i.inflation_rate DESC, g.term_start DESC;
```

## Risks and Considerations

### Data Quality Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|---------|------------|
| API rate limiting impacts downloads | Medium | Medium | Implement exponential backoff, request caching |
| Historical data inconsistencies | Medium | Medium | Data validation, manual review of edge cases |
| Bioguide ID mismatches across sources | Low | High | Cross-reference multiple sources, maintain mapping table |
| FEC data completeness issues | Medium | Low | Document known gaps, provide data quality metrics |

### Technical Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|---------|------------|
| ProPublica API changes | Low | High | Version API calls, monitor API announcements |
| Large parquet files (10+ years of votes) | High | Medium | Partition by congress/year, implement lazy loading |
| Cross-schema join performance | Medium | Medium | Add appropriate indexes, query optimization |
| Complex historical backfill | High | Medium | Incremental approach, automated validation |

### Legal/Compliance Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|---------|------------|
| API terms of service violations | Low | High | Review ToS for all sources, obtain necessary permissions |
| Campaign finance data sensitivity | Low | Medium | Follow FEC disclosure guidelines, document data source |
| Data licensing restrictions | Low | High | Use only public domain/open data sources |

## Success Metrics

**Coverage Metrics**:
- All 27 current senators from major states represented
- All 435 House members represented
- Bills from 113th Congress forward (2013+) available
- Campaign finance data from 2016 forward available
- Historical presidents, VPs, cabinet members complete
- Supreme Court complete historical roster (1789+)

**Quality Metrics**:
- 99%+ data accuracy (validated against official sources)
- Zero duplicate records (PK uniqueness enforced)
- Foreign key integrity maintained across LEG/POL/GEO schemas
- All API calls complete successfully (< 1% failure rate)

**Performance Metrics**:
- Query response time < 2 seconds for typical analytical queries
- Parquet conversion completes in < 30 minutes per year
- Full data refresh completes in < 4 hours
- Cross-schema joins perform efficiently (< 5 second response)

**Usability Metrics**:
- Example queries demonstrate all major use cases
- Schema documentation complete and accurate
- Integration tests cover all table relationships
- Error messages provide actionable guidance

## Dependencies

**External APIs**:
- ProPublica Congress API (free tier)
- Congress.gov API (free)
- GovInfo API (free)
- Ballotpedia API (requires subscription)
- FEC API (free)

**Calcite Components**:
- FileSchema (existing)
- GovDataAdapter (existing)
- RawToParquetConverter interface (existing)
- StorageProvider (existing)

**New Components Required**:
- LegislativeDataDownloader.java
- PoliticalDataDownloader.java
- LegRawToParquetConverter.java
- PolRawToParquetConverter.java
- JSON schema definitions for all 19 tables
- Integration test suite (LegIntegrationTest, PolIntegrationTest)

## Open Questions

1. **Historical depth priority**: Should we prioritize recent data (117th-118th Congress) with complete details, or broader historical coverage (94th+ Congress) with limited details?

2. **Campaign finance granularity**: Should we include individual-level contributions (very large dataset) or just campaign-level summaries?

3. **Update frequency**: How often should data be refreshed? Daily for current Congress, weekly for historical?

4. **State-level offices**: Should we include state legislators, or limit to federal + state executives (governors)?

5. **International data**: Should POL schema eventually include international political leaders for cross-national comparisons?

6. **Real-time vs batch**: Should we support real-time vote notifications, or stick to batch updates?

## References

### API Documentation
- ProPublica Congress API: https://projects.propublica.org/api-docs/congress-api/
- Congress.gov API: https://api.congress.gov/
- GovInfo API: https://api.govinfo.gov/docs/
- FEC API: https://api.open.fec.gov/developers/
- Federal Judicial Center: https://www.fjc.gov/history/judges

### Government Data Sources
- Ballotpedia: https://ballotpedia.org/
- Federal Register: https://www.federalregister.gov/
- Congressional Research Service: https://crsreports.congress.gov/

### Related Projects
- GovTrack: https://www.govtrack.us/
- OpenSecrets: https://www.opensecrets.org/
- LegiScan: https://legiscan.com/

## Appendix A: API Rate Limits and Costs

| Source | Rate Limit | Cost | Notes |
|--------|-----------|------|-------|
| ProPublica | 5,000 req/day | Free | Sufficient for daily updates |
| Congress.gov | Reasonable use | Free | No hard limit specified |
| GovInfo | None specified | Free | Batch downloads available |
| Ballotpedia | Varies | $500-2000/mo | Commercial pricing required |
| FEC | 1,000 req/hour | Free | ~24,000 requests per day |

**Total estimated cost**: $500-2,000/month (primarily for Ballotpedia API access)

**Alternative**: For POL schema tables, could use web scraping with permission instead of Ballotpedia API to eliminate licensing costs. Trade-off is maintenance burden.

## Appendix B: Sample Data Volumes

**LEG Schema**:
- `congress_members`: ~540 rows (current Congress)
- `bills`: ~12,000 bills per Congress × 6 Congresses = 72,000 rows
- `votes`: ~1,500 votes per year × 10 years = 15,000 rows
- `member_votes`: 15,000 votes × 435 members avg = 6,525,000 rows
- `committees`: ~200 committees/subcommittees
- `committee_members`: ~4,000 memberships per Congress × 6 = 24,000 rows
- `nominations`: ~500 per Congress × 6 = 3,000 rows
- `hearings`: ~1,000 per year × 10 years = 10,000 rows

**POL Schema**:
- `presidents`: 46 rows
- `vice_presidents`: 49 rows
- `cabinet_members`: ~1,500 rows (historical)
- `governors`: ~2,500 rows (50 states × 50 years)
- `supreme_court_justices`: 115 rows (historical)
- `federal_judges`: ~3,000 rows (active judges)
- `campaigns`: ~1,500 campaigns per cycle × 5 cycles = 7,500 rows
- `contributions`: ~5,000,000 rows (large contributions only)
- `party_leadership`: ~200 rows per Congress × 6 = 1,200 rows
- `caucuses`: ~150 caucuses
- `caucus_members`: ~5,000 memberships
- `executive_orders`: ~14,000 rows (historical)

**Total estimated storage**: ~15-20 GB parquet files after compression

---

**Document Status**: PLANNING - Not yet approved for implementation
**Next Steps**: Review with stakeholders, prioritize against other schema expansions (SAFETY, PUB, HEALTH, WEATHER)
