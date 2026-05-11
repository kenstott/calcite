# Claude Development Guidelines - Teams

Team definitions, agent mappings, and coordination workflows for the Calcite project.

## 👥 TEAM DEFINITIONS

### Core Engine Team
**Scope**: Calcite internals, query planning, adapter framework, SQL parsing
**Modules**: `core/`, `babel/`, `file/`, `splunk/`, `sharepoint-list/`, `salesforce/`, `cloud-ops/`
**Primary Agents**: architect, data-engine-dev, sql-analyst
**Secondary Agents**: refactorer, code-reviewer
**Commands**: `/project:test:adapter`, `/project:regression`
**Guidelines**: CLAUDE-CORE.md (Java 8), CLAUDE-ADAPTERS.md (all adapters)

### Data Team
**Scope**: GovData ETL pipelines, data sources (SEC, Census, Crime, Econ, Geo, Weather, FEC), Iceberg materialization, S3 storage, tracker state
**Modules**: `govdata/`, `govdata/scripts/parallel/`, S3/MinIO configuration
**Primary Agents**: data-engine-dev, debugger
**Secondary Agents**: sql-analyst, test-strategist
**Commands**: `/project:test:adapter govdata`, `/project:test:quick`
**Guidelines**: CLAUDE-ADAPTERS.md (govdata), CLAUDE-TROUBLESHOOTING.md

### Infrastructure Team
**Scope**: Build system, CI/CD, parallel worker orchestration, Docker, deployment
**Modules**: `build.gradle.kts`, `settings.gradle.kts`, `buildSrc/`, `docker-compose*.yml`, `govdata/scripts/parallel/`
**Primary Agents**: architect, debugger
**Secondary Agents**: doc-writer
**Commands**: `/project:test:all`
**Guidelines**: CLAUDE-CORE.md, CLAUDE-TROUBLESHOOTING.md

### Quality Team
**Scope**: Testing strategy, code review, documentation, refactoring
**Modules**: Cross-cutting (all `src/test/` directories, all documentation)
**Primary Agents**: test-strategist, code-reviewer, doc-writer, refactorer
**Secondary Agents**: debugger
**Commands**: `/project:test:all`, `/project:regression`, `/project:reconstitute-commits`
**Guidelines**: CLAUDE-TESTING.md, CLAUDE-CORE.md (verification checklist)

## 📊 AGENT-TO-TEAM MATRIX

| Agent | Core Engine | Data | Infrastructure | Quality |
|-------|:-----------:|:----:|:--------------:|:-------:|
| architect | **P** | | **P** | |
| data-engine-dev | **P** | **P** | | |
| sql-analyst | **P** | S | | |
| debugger | | **P** | **P** | S |
| test-strategist | | S | | **P** |
| code-reviewer | S | | | **P** |
| doc-writer | | | S | **P** |
| refactorer | S | | | **P** |
| marketer | — | — | — | — |

**P** = Primary, **S** = Secondary, **—** = Available to all teams on demand

## 🎯 TASK ROUTING

```
New task?
├─ core/, babel/, adapter framework code?
│  └─ Core Engine Team
├─ govdata/, ETL, data sources, S3, Parquet data?
│  └─ Data Team
├─ build system, CI, Docker, deployment, parallel workers?
│  └─ Infrastructure Team
├─ tests, docs, code review, refactoring?
│  └─ Quality Team
└─ Spans multiple teams?
   └─ architect agent triages → assigns owning team
```

## 🔄 INTER-TEAM WORKFLOWS

### Feature Request Flow
```
1. Owning team receives request
2. architect agent evaluates design impact
3. If cross-cutting: architect identifies affected teams
4. Owning team implements
5. code-reviewer validates (Java 8, DRY, security)
6. test-strategist verifies coverage
```

### Breaking Change Flow
```
1. Core Engine team proposes change
2. Data team assesses ETL pipeline impact
3. Infrastructure team assesses build/deploy impact
4. Coordinated rollout with regression testing
```

### Incident/Debug Flow
```
1. debugger agent investigates root cause
2. Routes to owning team based on module
3. Owning team fixes
4. Quality team validates fix + regression
```

## 📏 COORDINATION RULES

1. **One owner per task** — every task has exactly one owning team
2. **Cross-cutting work** — owning team leads, others contribute via secondary agents
3. **Universal rules** — all teams follow CLAUDE-CORE.md unconditionally (Java 8, honesty, completion)
4. **Review authority** — Quality team has final say on merge readiness
5. **Escalation** — use architect agent to resolve cross-team design disputes
6. **Market viability** — use marketer agent when evaluating feature ROI for any team

## 📚 CROSS-REFERENCES
- Core rules → CLAUDE-CORE.md
- Testing → CLAUDE-TESTING.md
- Adapters → CLAUDE-ADAPTERS.md
- Debugging → CLAUDE-TROUBLESHOOTING.md
