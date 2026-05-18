# 🦉 trading-agents-service — Python Multi-Agent Engine

> **Tech** | Python 3.13 · FastAPI · LangGraph · OpenAI GPT-4o · SQLAlchemy 2.x async + asyncpg · Alembic · Redis (SSE pub-sub) · FMP · yfinance · SEC EDGAR · Sentry

The Python service powering [Lyceum Fund](https://github.com/Two-Trees-Digital/lyceum-fund)'s multi-agent analysis pipeline and financial-model construction. A FastAPI wrapper around a fork of [TauricResearch/TradingAgents](https://github.com/TauricResearch/TradingAgents) — adds service-grade persistence, HMAC-auth, observability, structured-output extraction, and the Generate-Critique-Reconcile model-build pipeline.

---

## Overview

This service has two responsibilities:

1. **Multi-agent ticker analysis** — Take a `(user_id, ticker, trade_date)` request, run the full TradingAgents debate pipeline (fundamentals → sentiment → news → technical → bull/bear → trader → risk committee → portfolio manager), persist agent reports + structured metadata, and stream live progress via Server-Sent Events.

2. **Financial-model construction** — Take a ticker and build a complete DCF + comps + earnings model from FMP data + 10-K parsing. Specialized Generate agents own different model fields; Critique agents flag fragile assumptions; Reconciler synthesizes consensus fields; Auditor spot-checks the final output.

The service is invoked by Lyceum Fund's Node-side worker over HMAC-signed POST. SSE streams are authenticated via short-lived query-param tokens. All state lives in PostgreSQL — checkpoints, decisions, agent reports, model versions, critique entries — so a container restart resumes cleanly and every decision is reproducible.

Built to template-grade quality: the `app/` shell (db, auth, observability, routes scaffolding) is intentionally separable from the trading-specific `tradingagents/` library, and will be extracted as `python-temp-pro` once a second Python-service spawned app validates the abstraction.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│ HTTP layer (FastAPI on Railway)                                          │
│                                                                          │
│   GET  /health              — liveness; Railway healthcheck target      │
│   GET  /ready               — DB + Redis ping; 503 on either failure    │
│   POST /analyze             — kick agent pipeline; returns 202          │
│   POST /build-model         — kick model-build pipeline; returns 202    │
│   GET  /stream/{run_id}     — SSE stream of agent progress              │
│                                                                          │
│   Auth: HMAC on POST (X-Signature + X-Timestamp + body hash)            │
│         Short-lived query token on /stream (minted by apps/api)         │
└────────────┬─────────────────────────────────────────────────────────────┘
             │
             ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ Agent pipelines (LangGraph + OpenAI)                                     │
│                                                                          │
│ Analyze pipeline (POST /analyze):                                        │
│   1. Analyst team (parallel)                                            │
│      Fundamentals · Sentiment · News · Technical                         │
│   2. Researcher debate                                                   │
│      Bull · Bear · Research Manager (rounds configurable)                │
│   3. Trader                                                              │
│      Composes inputs → BUY/SELL/HOLD + sizing + tax framing             │
│   4. Risk committee                                                      │
│      Risk Manager · Portfolio Manager (final approval signal)            │
│   5. Post-pipeline structured extraction                                 │
│      GPT-4o-mini extracts metrics + sentiment from each agent report    │
│                                                                          │
│ Model-build pipeline (POST /build-model):                                │
│   1. Generate phase (parallel specialized agents)                       │
│      Fundamentals · Sector · Capital Structure · Valuation · Growth     │
│      Each owns its slice; structured-output JSON enforced               │
│   2. Validation (deterministic, three layers)                           │
│      Schema bounds → Math consistency → Citation enforcement            │
│   3. Critique phase                                                      │
│      Bear · Risk Manager · Devil's Advocate flag fragile assumptions    │
│   4. Reconciliation (consensus fields)                                   │
│      Research Manager synthesizes terminal growth, ERP, etc.             │
│   5. LLM auditor (post-validation spot check)                           │
│      Sanity + internal-consistency flags surface to admin dashboard     │
└────────────┬─────────────────────────────────────────────────────────────┘
             │
             ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ Data + persistence                                                       │
│                                                                          │
│   PostgreSQL (Neon, shared with Node side via SQLAlchemy)               │
│   ├─ runs / decisions / agent_reports / agent_invocations               │
│   ├─ company_models / model_versions / model_critiques                  │
│   └─ Read-only access to portfolio context (positions, tax_lots)        │
│                                                                          │
│   Redis (Upstash, pub-sub for SSE)                                      │
│   ├─ Channel per run_id; agents publish events as they progress         │
│   └─ /stream/{run_id} subscribes and pipes to the browser EventSource   │
│                                                                          │
│   FMP Premium ($79/mo)                                                  │
│   ├─ Income / balance / cash-flow statements (5y history)               │
│   ├─ Analyst consensus estimates                                         │
│   ├─ 10-K filings text                                                  │
│   ├─ Earnings transcripts                                               │
│   └─ Insider trades · institutional holdings                            │
│                                                                          │
│   Plus: yfinance (price data) · SEC EDGAR (filings) · Finnhub (calendar)│
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Tech stack

| Layer | Technology |
|---|---|
| **Language** | Python 3.13 |
| **Web framework** | FastAPI + uvicorn |
| **Agent orchestration** | LangGraph (forked from TauricResearch/TradingAgents) |
| **LLM** | OpenAI GPT-4o (analysts + trader + critique); GPT-4o-mini (extraction + auditor) |
| **Database** | SQLAlchemy 2.x async + asyncpg; Alembic for migrations. Shared schema with Node-side Prisma — Node owns DDL, Python reads/writes through SQLAlchemy models that mirror the Prisma schema |
| **Queue + pub-sub** | Redis (Upstash). SSE event channels per `run_id` |
| **Auth** | HMAC signature middleware on POST. Stream-token query auth on SSE |
| **Market data** | `MarketDataProvider` interface; FMP Premium as primary impl; yfinance + SEC EDGAR as supplements |
| **Observability** | Sentry (shared `two-trees-shared-python` project, tagged per-app) + structured JSON logging |
| **Container** | Docker; deployed to Railway |
| **CI/CD** | GitHub Actions — ruff + pytest + Docker-build |

---

## Getting started

### Prerequisites

- Python 3.13
- PostgreSQL (local or Neon)
- Redis (local or Upstash)
- OpenAI API key
- FMP API key (Premium tier)

### 1. Clone and install

```sh
git clone https://github.com/Two-Trees-Digital/trading-agents-service.git
cd trading-agents-service
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 2. Configure environment

Copy `.env.example` to `.env` and fill in:

```env
# Database (shared with Lyceum Fund)
DATABASE_URL="postgres://...pooler.neon.tech:5432/..."

# Service-to-service auth (shared with apps/api + apps/worker)
SERVICE_HMAC_SECRET="..."
STREAM_TOKEN_SECRET="..."

# Pub-sub for SSE
REDIS_URL="rediss://...upstash.io"

# LLM
OPENAI_API_KEY="sk-..."

# Market data
FMP_API_KEY="..."
ALPHA_VANTAGE_API_KEY=""        # optional fallback

# Identification (set by Two Trees Platform on deploy)
APP_SLUG="lyceum-fund"
NODE_ENV="development"

# Observability
SENTRY_DSN="https://...@o....ingest.sentry.io/..."

# CORS — comma-separated origins allowed to hit /stream/{run_id}
CORS_ALLOW_ORIGINS="https://lyceum-fund-app.vercel.app,http://localhost:3000"
```

### 3. Run migrations

```sh
alembic upgrade head
```

The schema is owned by the Node side's Prisma (Lyceum Fund repo). The Alembic migrations here mirror it and are used for local dev + new-environment bootstrap. In production, Prisma's `migrate deploy` is the source of truth for DDL.

### 4. Start the service

```sh
uvicorn app.main:app --reload --port 8000
```

Health check:

```sh
curl http://localhost:8000/health
# {"ok": true, "service": "trading-agents-service", "env": "development", "app": "lyceum-fund"}
```

### 5. Run an analysis (via HMAC)

The service is normally invoked by Lyceum Fund's worker. For local testing, sign a request manually:

```python
import hmac, hashlib, time, json, requests

body = json.dumps({"run_id": "test-uuid", "user_id": "...", "ticker": "AAPL", "trade_date": "2026-05-17"})
ts = str(int(time.time()))
sig = hmac.new(b"<SERVICE_HMAC_SECRET>", f"{ts}.{body}".encode(), hashlib.sha256).hexdigest()

requests.post(
    "http://localhost:8000/analyze",
    data=body,
    headers={"X-Signature": sig, "X-Timestamp": ts, "Content-Type": "application/json"},
)
```

---

## API surface

### POST /analyze

Kick off a multi-agent analysis. Returns 202 immediately; the pipeline runs in the background and streams progress to `/stream/{run_id}`.

**Request body:**
```json
{
  "run_id": "uuid",
  "user_id": "uuid",
  "ticker": "AAPL",
  "trade_date": "2026-05-17"
}
```

**Response:** `202 Accepted` with `{"run_id": "...", "status": "queued"}`.

The service writes the `runs` row, then runs the LangGraph pipeline. Agent reports persist to `agent_reports` with extracted metadata in JSONB. Final decision lands in `decisions` with `model_version_id` FK linkage.

### POST /build-model

Build or refresh a master financial model for a ticker.

**Request body:**
```json
{
  "ticker": "AAPL",
  "trigger": "watchlist_add | quarterly_refresh | event:earnings"
}
```

**Response:** `202 Accepted` with `{"company_model_id": "...", "build_status": "queued"}`.

Pipeline runs Generate → Validate → Critique → Reconcile → Audit. New `model_versions` row written on success; `company_models.current_version_id` updated atomically.

### GET /stream/{run_id}

Server-Sent Events stream of pipeline progress. Browser EventSource auth via `?token=<short-lived JWT>` query param. Token minted by `apps/api` from the same `STREAM_TOKEN_SECRET`.

**Event types:**
- `agent_start` — `{"agent": "fundamentals_analyst"}`
- `agent_token` — streamed content from the agent's LLM call
- `agent_end` — `{"agent": "...", "report_id": "..."}`
- `decision` — final `{"decision": "BUY", "size_pct": 1.5, ...}`
- `error` — `{"reason": "..."}` for transient failures
- `done` — pipeline terminated (success or fail)

### GET /health and /ready

Standard liveness + readiness probes. Railway's healthcheck points at `/health`. Load-balancer config should use `/ready` for traffic gating.

---

## Database

Python service writes through SQLAlchemy 2.x async models. Schema mirrors the Lyceum Fund Prisma schema; DDL is owned by Node side. Python tables (writes):

| Table | Purpose |
|---|---|
| **runs** | Top-level analyze invocation. Status state machine: pending → running → complete \| cancelled \| timed_out \| error |
| **agent_invocations** | Per-LLM-call audit: prompt template version, prompt hash, model version string (e.g. `gpt-4o-2024-08-06`), config, seed |
| **agent_reports** | Markdown content + structured `metadata` JSONB per-agent. Schemas defined in `app/services/extractors/schemas.py` |
| **decisions** | Final BUY/SELL/HOLD + sizing + tax_consideration + `model_version_id` FK |
| **model_versions** | Versioned JSONB financial model state |
| **model_critiques** | Structured dissent rows per field path |
| **model_quality_alerts** | Auditor flags surfaced to admin |

Python tables (reads only — Node owns writes):

`portfolios`, `accounts`, `positions`, `tax_lots`, `transactions`, `assets`, `user_budgets`, `watchlists`. The pipeline reads portfolio context for prompt injection; never mutates portfolio state.

---

## Project structure

```
trading-agents-service/
├── app/
│   ├── main.py                          ← FastAPI entrypoint, lifecycle, /health, /ready
│   ├── config.py                        ← pydantic-settings Settings class
│   ├── db.py                            ← async engine, session, dependency
│   ├── auth.py                          ← HMAC verification middleware
│   ├── observability.py                 ← Sentry init
│   ├── logging_config.py                ← structured JSON logging
│   ├── models.py                        ← SQLAlchemy 2.x ORM (mirrors Prisma)
│   ├── routes/
│   │   ├── analyze.py                   ← POST /analyze
│   │   ├── build_model.py               ← POST /build-model
│   │   └── stream.py                    ← GET /stream/{run_id} (SSE)
│   └── services/
│       ├── trading_agents_runner.py     ← analyze-pipeline orchestration
│       ├── model_builder.py             ← Generate→Validate→Critique→Reconcile→Audit
│       ├── callbacks.py                 ← LangGraph callback handler — writes reports
│       ├── pubsub.py                    ← Redis pub-sub for SSE
│       ├── stream_token.py              ← short-lived SSE token verification
│       ├── redis_client.py              ← async redis factory
│       └── extractors/
│           ├── extractor.py             ← GPT-4o-mini structured extraction
│           └── schemas.py               ← per-agent metadata Pydantic schemas
├── tradingagents/                       ← upstream library, mostly unmodified
│   ├── agents/                          ← analyst + researcher + trader + risk
│   ├── graph/                           ← LangGraph wiring
│   └── default_config.py
├── alembic/
│   ├── env.py
│   └── versions/                        ← migration scripts
├── tests/
├── pyproject.toml
├── Dockerfile
├── alembic.ini
└── .github/workflows/python-deploy.yml
```

---

## Deployment

**Railway** — auto-deploys on push to `main`. Dockerfile builds the Python service + bundles `tradingagents/` upstream library. Env vars set in Railway service Variables tab. Healthcheck against `/health` with a 30s window.

**GitHub Actions** (`python-deploy.yml`):

| Step | Purpose |
|---|---|
| Ruff lint | Style + import-order check |
| pytest | Unit + integration tests (with a synthetic OpenAI mock) |
| Docker build | Verifies the image builds; uses BuildKit cache to keep CI under 90s |

**Sentry** — shared `two-trees-shared-python` project for now (works because every event is tagged with `app:<APP_SLUG>`). Dedicated per-app project when traffic justifies it.

---

## Roadmap

The Python service moves in lockstep with [Lyceum Fund](https://github.com/Two-Trees-Digital/lyceum-fund)'s strategic roadmap. The service's work is concentrated in Phases A, D, and E.

**Where we are today:** v1 shipped. Multi-agent analyze pipeline live with structured metadata extraction, SSE streaming, runaway-protection (timeout + iteration cap + cancel-poll). Currently on the Pre-Phase-A: Polish & Operations milestone — token-based cost tracking + heuristic-to-extractor migration. Phase A (decision journal + agent versioning) is the next strategic milestone.

### Pre-Phase-A: Polish & Operations

Operational debt: extract `/analyze` background work to a dedicated Arq worker for restart-safety, replace the `_extract_action` regex heuristic with the structured extractor (TT-296), and adopt real token-usage cost tracking from the LangChain callback (TT-294 — coordinates with Lyceum Fund's UserBudget settlement).

### Phase A — Decision Journal & Outcome Tracking

The service-side contribution: capture full per-invocation context (prompt template version, prompt-rendered hash, OpenAI model version string, config, seed) on every LangGraph node call, write to `agent_invocations` for reproducibility. Two years from now, any decision can be replayed against the exact prompt + model + data snapshot.

### Phase B — Watchlist & Calendar

Minimal service-side work. Event-triggered re-analysis (TT-315) reuses the existing `/analyze` flow via worker enqueue on earnings calendar events.

### Phase C — Portfolio & Brokerage

Service injects portfolio context into agent prompts: a compact JSON summary (top holdings, sector tilts, concentration, recent transactions, tax-lot state) is added to every analyst system prompt. Trader agent's decision schema extends with `position_size_pct`, `max_position_size_pct`, and `tax_consideration` for tax-aware sell recommendations.

### Phase D — Financial Models

The biggest service-side build. Three sub-phases:

- **D-1 Foundation** — Ship the `MarketDataProvider` interface + FMP implementation, the model JSON schema + deterministic validators (schema bounds + math consistency), the single Fundamentals Generate agent that builds a complete model from FMP data + 10-K parse, and citation recording on every value (populated, not yet enforced).

- **D-2 Multi-agent + Dissent** — Split the single Generate agent into specialized owners (Fundamentals · Sector · Capital Structure · Valuation · Growth), each owning specific model fields. Add the Critique phase with structured dissent storage (`model_critiques` table). Flip citation enforcement on. Wire event-driven refresh from FMP earnings calendar + 8-K feed.

- **D-3 Platform Maturity** — Reconciliation agents for consensus fields (terminal growth, equity risk premium). LLM auditor agents above deterministic validation. Sensitivity analysis API for the dashboard's what-if UI.

### Phase E — Risk & Backtesting

Drawdown-aware agent reasoning. When portfolio state is in `WARNING` or `CIRCUIT_BREAKER`, the trader agent's prompt includes a drawdown-context block and the decision is constrained (HOLD-only during circuit breaker). Otherwise the service is mostly read-only from Phase E's perspective — risk metrics and backtest replay live in apps/worker.

### Icebox

Options-aware agent layer — same trigger as Lyceum's options icebox: needs Phase A outcome tracking + Phase C portfolio context + a real options-data subscription (Polygon or Schwab API) before it's worth building.

Full ticket-level detail: [Linear project →](https://linear.app/two-trees-digital/project/lyceum-fund-1427cdb98f49)

---

## References

- [Lyceum Fund](https://github.com/Two-Trees-Digital/lyceum-fund) — the Node-side app that calls this service
- [TauricResearch/TradingAgents](https://github.com/TauricResearch/TradingAgents) — upstream open-source framework
- [FastAPI docs](https://fastapi.tiangolo.com) · [LangGraph docs](https://langchain-ai.github.io/langgraph/) · [SQLAlchemy 2.x async](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html) · [Alembic](https://alembic.sqlalchemy.org)
- [OpenAI Python SDK](https://github.com/openai/openai-python) · [Financial Modeling Prep API](https://site.financialmodelingprep.com/developer/docs)

---

**Built by Two Trees Digital** 🌲 | [Lyceum Fund](https://lyceum-fund-app.vercel.app) | [GitHub](https://github.com/Two-Trees-Digital/trading-agents-service)
