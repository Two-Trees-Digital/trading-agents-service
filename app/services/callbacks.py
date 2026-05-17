"""
LangChain callback handler that bridges per-agent output to:
  1. The `agent_reports` Postgres table (durable record for replay/UI)
  2. The `run:<run_id>` Redis pub-sub channel (live SSE stream)

Wired into the LangGraph stream via the `callbacks` config option in
`trading_agents_runner.run_analysis`. Each chain end-event corresponds
roughly to one agent's output (the TradingAgents pipeline structures
each agent as its own LangChain chain).

Treat the BASE (RunRecorderHandler) as a python-temp-pro template — the
shape works for any LangGraph chain. The trading-specific
interpretation (mapping chain names to agent_reports rows) is the only
app-specific code here.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from langchain_core.callbacks.base import AsyncCallbackHandler
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.models import AgentReport
from app.services.pubsub import publish_event


logger = logging.getLogger(__name__)


# TT-295: previously this only looked at `serialized.name`, which under
# LangGraph returns generic strings like "RunnableSequence" and lost the
# real agent identity. The dashboard's agent_reports list rendered every
# row as "unknown" because of this.
#
# Lookup priority:
#   1. metadata.langgraph_node  — LangGraph sets this for each node call.
#                                 Most accurate for TradingAgents since
#                                 it constructs the pipeline as a graph.
#   2. name kwarg               — LangChain passes the explicit name when
#                                 a chain was constructed with one.
#   3. serialized.name / .id    — legacy fallback for non-LangGraph chains.
#   4. "unknown"                — last resort.
_GENERIC_NAMES = {"runnablesequence", "runnable", "chain", "agent"}


def _normalize_agent_name(
    serialized: dict[str, Any] | None,
    metadata:   dict[str, Any] | None = None,
    name_kw:    str | None            = None,
) -> str:
    # 1. LangGraph metadata.
    if metadata:
        node = metadata.get("langgraph_node")
        if node:
            return _to_snake_case(str(node))

    # 2. Explicit name kwarg.
    if name_kw:
        cleaned = _to_snake_case(_strip_suffixes(str(name_kw)))
        if cleaned and cleaned not in _GENERIC_NAMES:
            return cleaned

    # 3. serialized.name / serialized.id legacy.
    if serialized:
        name = serialized.get("name") or serialized.get("id", [""])[-1]
        if name:
            cleaned = _to_snake_case(_strip_suffixes(str(name)))
            if cleaned and cleaned not in _GENERIC_NAMES:
                return cleaned

    return "unknown"


def _strip_suffixes(name: str) -> str:
    """Drop a single trailing 'Chain' / 'Agent' / 'Runnable' suffix if present."""
    for suffix in ("Chain", "Agent", "Runnable"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


# TT-295 P2 fix: TradingAgents' LangGraph nodes come through with mixed
# casing — analyst nodes as "Market Analyst" (Title Case with spaces),
# tool nodes as "tools_market" (already snake_case). The extractor's
# SCHEMA_FOR_AGENT mapping uses snake_case keys, so every Title-Case
# name silently failed the schema lookup and metadata stayed null.
# Normalizing to snake_case here makes both formats hit the same key.
def _to_snake_case(name: str) -> str:
    """'Market Analyst' → 'market_analyst'. Idempotent on already-snake_case input."""
    return name.lower().strip().replace(" ", "_").replace("-", "_")


class RunRecorderHandler(AsyncCallbackHandler):
    """
    Captures each agent's output as the TradingAgents graph runs. One
    instance per analysis run — owns the run_id for tagging.

    Tradeoff note: we capture on `on_chain_end` rather than per LLM call.
    A single agent typically makes multiple LLM calls (one for thinking,
    one for response, etc.); capturing per-LLM would flood agent_reports
    with intermediate steps. Per-chain gives one row per agent's full
    output cycle, which matches the user's mental model of "this agent
    finished its analysis."
    """

    # AsyncCallbackHandler requires this flag for it to be invoked in
    # async runs.
    run_inline = False

    # TT-298: per-agent iteration cap. When the same agent fires more
    # than this many times in one run, the handler sets `runaway_detected`
    # and the main astream loop aborts on its next chunk-boundary check.
    # Defends against TradingAgents' internal retry chains looping a
    # failing tool call indefinitely. Tunable later via env var if
    # the legitimate ceiling needs to grow.
    MAX_AGENT_INVOCATIONS = 20

    def __init__(
        self,
        run_id: str,
        sessionmaker: async_sessionmaker,
    ):
        super().__init__()
        self._run_id = run_id
        self._sessionmaker = sessionmaker
        # Track chain run UUIDs so we can correlate start/end pairs and
        # know what agent each chain belongs to.
        self._active: dict[UUID, str] = {}
        # TT-298: agent invocation counts + runaway flag. on_chain_start
        # increments per-agent count; main loop checks `runaway_detected`
        # between chunks and aborts the run if set.
        self._agent_counts: dict[str, int] = {}
        self.runaway_detected: bool = False
        self.runaway_agent: str | None = None

    async def on_chain_start(
        self,
        serialized: dict[str, Any] | None,
        inputs: dict[str, Any],
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        **kwargs: Any,
    ) -> None:
        # TT-295: pull langgraph_node from metadata + explicit name kwarg.
        agent_name = _normalize_agent_name(
            serialized,
            metadata=kwargs.get("metadata"),
            name_kw=kwargs.get("name"),
        )
        self._active[run_id] = agent_name

        # TT-298: per-agent invocation cap. Increment count; flag a
        # runaway when the cap is exceeded. The flag is checked at
        # chunk boundaries in the main astream loop — we don't raise
        # here because exceptions from callbacks can leak into LangGraph's
        # retry-on-error path and create the very loop we're trying to
        # detect. Setting a flag + breaking the outer loop is robust.
        count = self._agent_counts.get(agent_name, 0) + 1
        self._agent_counts[agent_name] = count
        if count > self.MAX_AGENT_INVOCATIONS and not self.runaway_detected:
            self.runaway_detected = True
            self.runaway_agent = agent_name
            logger.warning(
                "Run %s: agent %s exceeded %d invocations — runaway flagged",
                self._run_id, agent_name, self.MAX_AGENT_INVOCATIONS,
            )
        # Live event: agent started. Browser uses this to show a "thinking..."
        # row in the UI immediately, before any output exists.
        await publish_event(
            self._run_id,
            {
                "type": "agent_started",
                "agent": agent_name,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    async def on_chain_end(
        self,
        outputs: dict[str, Any],
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        **kwargs: Any,
    ) -> None:
        agent_name = self._active.pop(run_id, "unknown")
        # Stringify the chain output for storage. LangChain outputs are
        # dicts of varying shape — JSON.dumps preserves structure when
        # consumers want to parse, but the agent_reports.content column
        # is plain Text since most consumers will render directly.
        content = _outputs_to_text(outputs)

        # TT-298 fix: skip intermediate steps that produced no usable
        # content. These are tool-call AIMessages (content="" with
        # additional_kwargs.tool_calls), state-clearing nodes
        # (msg_clear_*), and similar plumbing. Persisting them flooded
        # the agent_reports list with noise rows and made MetadataCard
        # render empty shells. Real analyst output always has non-empty
        # content; skipping empty ones cleans up both the row list and
        # the post-pipeline extraction set.
        if not content.strip():
            return

        # TT-298 — extraction moved out of the callback path. We write
        # the row with null metadata here; the post-pipeline phase in
        # trading_agents_runner.py reads agent_reports back and runs the
        # extractor in a clean async context (no LangGraph callback
        # context = no cross-loop bugs).
        async with self._sessionmaker() as session:
            try:
                session.add(
                    AgentReport(
                        run_id=self._run_id,
                        agent_name=agent_name,
                        content=content,
                        report_metadata=None,
                    )
                )
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.warning(
                    "AgentReport persist failed for run %s agent %s: %s",
                    self._run_id, agent_name, e,
                )

        # Live event: agent finished, here's its output. Browser appends
        # this to the scrolling agent-output panel.
        await publish_event(
            self._run_id,
            {
                "type": "agent_finished",
                "agent": agent_name,
                "content": content,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    async def on_chain_error(
        self,
        error: BaseException,
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        **kwargs: Any,
    ) -> None:
        agent_name = self._active.pop(run_id, "unknown")
        logger.error("Chain error in agent %s: %s", agent_name, error)
        await publish_event(
            self._run_id,
            {
                "type": "agent_error",
                "agent": agent_name,
                "error": str(error),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )


def _outputs_to_text(outputs: dict[str, Any]) -> str:
    """
    Best-effort string serialization of a chain's output dict. Common
    shapes:
      - {"output": "..."}             → just the output
      - {"messages": [Message(...)]}  → last message content (may be "")
      - {"final_trade_decision": "..."} → that string
      - everything else                → "" (caller filters)

    TT-298 fix: empty AIMessage content (tool-call or state-mgmt
    intermediate steps) now returns "" instead of falling through to
    str(outputs). Falling through dumped the AIMessage __repr__()
    into agent_reports.content, which the extractor then tried to
    parse and got nothing from — leaving rows with all-null metadata
    that rendered as empty MetadataCard shells in the dashboard.
    Empty string is a meaningful signal — the on_chain_end caller
    skips persistence + event publishing for empty-content rows.
    """
    if not outputs:
        return ""
    if "output" in outputs and isinstance(outputs["output"], str):
        return outputs["output"]
    if "messages" in outputs and outputs["messages"]:
        last = outputs["messages"][-1]
        content = getattr(last, "content", "")
        # Return content even when empty — caller skips empty-content
        # rows. Empty content is the tool-call/intermediate-step signal.
        if isinstance(content, str):
            return content
        return str(content)
    # TradingAgents-specific terminal state key.
    if "final_trade_decision" in outputs and outputs["final_trade_decision"]:
        return str(outputs["final_trade_decision"])
    # Unknown output shape — empty means "skip" rather than dumping a
    # confusing dict repr into the row.
    return ""
