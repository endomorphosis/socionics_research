from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import discord
from discord import app_commands

from .config import settings
from .logging_setup import configure_logging
from .guardrails import apply_guardrails
from .ingest import Ingestor
from .utils import RateLimiter, parse_time_range, audit_log, build_context_snippet, has_admin_access
from .metrics import inc, start_server
from .knowledge_loader import load_doc_embeddings

configure_logging()
log = logging.getLogger("socionics_bot")
if not settings.json_logs:
    logging.basicConfig(level=logging.INFO)

INTENTS = discord.Intents.none()
INTENTS.message_content = False
INTENTS.guilds = True
INTENTS.members = False

class SocionicsBot(discord.Client):
    def __init__(self) -> None:
        super().__init__(intents=INTENTS)
        self.tree = app_commands.CommandTree(self)
        self.ingestor = Ingestor()
        self.cmd_limiter = RateLimiter(settings.rate_limit_per_min)
        self.search_limiter = RateLimiter(settings.search_rate_limit_per_min)
        # Set of admin role IDs allowed for privileged commands
        self.admin_roles = set(settings.admin_role_ids or [])

    async def setup_hook(self) -> None:
        await self.tree.sync()
        log.info("Commands synced")
        # Start metrics server once client is ready (optional)
        if settings.enable_metrics:
            try:
                start_server()
                log.info("Metrics server started on %s:%s", settings.metrics_host, settings.metrics_port)
            except Exception:  # pragma: no cover - non-critical
                log.exception("Failed to start metrics server")


def summarize_theory(topic: str, model, docs: list[dict]) -> str:
    """Return summary string for a theory topic with optional doc augmentation.

    Pure function for unit tests (no discord dependency)."""
    key = topic.lower()
    base_msg: str | None = None
    if "model" in key:
        base_msg = (
            "Model A: Eight functional positions describing information metabolism roles "
            "(program, creative, role, vulnerable, suggestive, mobilizing, ignoring, demonstrative). "
            "Empirical status: structural arrangement needs validation."
        )
    elif "duality" in key:
        base_msg = (
            "Duality: Hypothesized complementary relation maximizing support. Evidence: anecdotal; "
            "design dyadic task studies to test coordination efficiency vs. random pairs."
        )
    aug_lines: list[str] = []
    if docs:
        try:
            import numpy as np
            raw_q = model.encode(topic)
            # Support both list and numpy array returns
            if hasattr(raw_q, "tolist"):
                raw_q = raw_q.tolist()
            q_vec = np.array(raw_q, dtype=float)
            mat = np.vstack([
                (d["embedding"].tolist() if hasattr(d["embedding"], "tolist") else d["embedding"])  # type: ignore[index]
                for d in docs
            ]).astype(float)
            # Normalize cosine similarity
            denom = (np.linalg.norm(mat, axis=1) * (np.linalg.norm(q_vec) + 1e-9))
            scores = (mat @ q_vec) / (denom + 1e-9)
            top_idx = np.argsort(-scores)[: settings.retrieval_top_k]
            for i in top_idx:
                doc = docs[int(i)]
                aug_lines.append(f"Doc:{doc['path']} sim:{scores[int(i)]:.3f}")
        except Exception:
            # Provide a soft fallback if docs exist but similarity failed
            aug_lines.append("(Doc embeddings available; similarity calc failed)")
    if not base_msg and not aug_lines:
        return "Topic not found in beta glossary."
    composed = base_msg or "No canned summary; closest docs:"
    if aug_lines:
        composed += "\nRelated docs (semantic):\n" + "\n".join(aug_lines)
    return composed

bot_client = SocionicsBot()

@bot_client.tree.command(name="about", description="Neutral overview of Socionics & empirical status")
async def about_socionics(interaction: discord.Interaction) -> None:  # type: ignore[type-arg]
    text = (
        "Socionics is a theoretical framework describing information processing styles and intertype relations. "
        "This project evaluates its claims empirically. Constructs are exploratory and not diagnostic."
    )
    await interaction.response.send_message(text, ephemeral=True)
    inc("about_calls")

@bot_client.tree.command(name="my_type", description="Guided self-observation checklist (no type assignment)")
async def my_type_help(interaction: discord.Interaction) -> None:  # type: ignore[type-arg]
    checklist = (
        "Self-Observation Dimensions:\n"
        "1. Information Seeking: Do you widen options quickly or narrow to causal sequences?\n"
        "2. Structural vs. Relational Focus: Do you default to systems or interpersonal context first?\n"
        "3. Comfort vs. Force Orientation: Track weekly notes on environment optimization vs. exerting influence.\n"
        "4. Expression vs. Evaluation: Notice expressive emotional broadcasting vs. internal valuation language.\n"
        "5. Temporal Narration: Future scenario weaving vs. divergent brainstorming bursts.\n"
        "6. Feedback Sensitivity: Which kinds of input energize vs. drain you?\n\n"
        "Record examples; compare patterns before consulting any type labels."
    )
    await interaction.response.send_message(checklist, ephemeral=True)
    inc("my_type_calls")

@bot_client.tree.command(name="intertype", description="Summarize canonical intertype relation (beta)")
@app_commands.describe(type1="First type (e.g., ILE)", type2="Second type (e.g., LII)")
async def intertype(interaction: discord.Interaction, type1: str, type2: str) -> None:  # type: ignore[type-arg]
    allowed = {"ILE","LII","ESE","SEI","SLE","LSI","EIE","IEI","LIE","ILI","SEE","ESI","IEE","EII","LSE","SLI"}
    t1, t2 = type1.upper(), type2.upper()
    if t1 not in allowed or t2 not in allowed:
        await interaction.response.send_message("Unknown type code(s).", ephemeral=True)
        return
    # Placeholder relation logic
    if t1 == t2:
        relation = "Identity: Similar strengths, potential blind spot overlap; test hypothesis via redundancy in task role allocation."
    else:
        relation = "Relation description placeholder; empirical validation pending. Formulate a falsifiable interaction metric."
    await interaction.response.send_message(relation, ephemeral=True)
    inc("intertype_calls")

@bot_client.tree.command(name="theory", description="Retrieve concise explanation of a theory topic (beta)")
@app_commands.describe(topic="Keyword, e.g., 'Model A', 'intertype relations'")
async def theory(interaction: discord.Interaction, topic: str) -> None:  # type: ignore[type-arg]
    guard = apply_guardrails(topic)
    if guard.blocked:
        await interaction.response.send_message(
            "Request blocked (type assignment or prohibited topic). This bot cannot assign types.", ephemeral=True
        )
        return
    docs = load_doc_embeddings()
    msg = summarize_theory(topic, bot_client.ingestor.model, docs)
    await interaction.response.send_message(msg, ephemeral=True)
    inc("theory_calls")

@bot_client.tree.command(name="ingest_channel", description="Ingest recent messages from a channel (no raw text stored)")
@app_commands.describe(limit="Max messages to fetch (default 200)")
async def ingest_channel(interaction: discord.Interaction, limit: Optional[int] = 200) -> None:  # type: ignore[type-arg]
    if not bot_client.cmd_limiter.allow(interaction.user.id):  # type: ignore[arg-type]
        await interaction.response.send_message("Rate limit exceeded.", ephemeral=True)
        return
    # Role-based admin override if configured
    member = interaction.user  # type: ignore[assignment]
    if not has_admin_access(member, bot_client.admin_roles):
        await interaction.response.send_message("Admin role required.", ephemeral=True)
        return
    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel):
        await interaction.response.send_message("Not a text channel.", ephemeral=True)
        return
    if settings.allowed_channel_ids and channel.id not in settings.allowed_channel_ids:
        await interaction.response.send_message("Channel not allowed for ingestion.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    messages = []
    async for msg in channel.history(limit=limit):  # type: ignore[attr-defined]
        messages.append(msg)
    added = await bot_client.ingestor.ingest_messages(messages)
    audit_log("ingest", channel_id=channel.id, added=added, user=interaction.user.id)
    await interaction.followup.send(f"Ingested vectors for {added} new messages.", ephemeral=True)
    inc("ingest_calls", added)

@bot_client.tree.command(name="search_vectors", description="Vector similarity search over ingested messages")
@app_commands.describe(query="Search text", top_k="Results to return", channel_id="Restrict to channel id",
                       author_hash="Filter by hashed author id", start_ts="Unix start timestamp", end_ts="Unix end timestamp")
async def search_vectors(
    interaction: discord.Interaction,
    query: str,
    top_k: Optional[int] = 5,
    channel_id: Optional[str] = None,
    author_hash: Optional[str] = None,
    start_ts: Optional[float] = None,
    end_ts: Optional[float] = None,
) -> None:  # type: ignore[type-arg]
    if not bot_client.search_limiter.allow(interaction.user.id):  # type: ignore[arg-type]
        await interaction.response.send_message("Search rate limit exceeded.", ephemeral=True)
        return
    model = bot_client.ingestor.model  # reuse loaded model
    q_vec = model.encode(query).tolist()
    cid_int = int(channel_id) if channel_id else None
    results = bot_client.ingestor.search(
        q_vec,
        top_k=top_k or 5,
        channel_id=cid_int,
        author_hash=author_hash,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    if not results:
        await interaction.response.send_message("No matches.", ephemeral=True)
        return
    lines = [
        f"Channel {r['channel_id']} Msg {r['message_id']} Author {r['author_hash'][:8]} Score {r['score']:.3f} TS {datetime.utcfromtimestamp(r['created_ts']).isoformat()}Z"
        for r in results
    ]
    audit_log("search", q_len=len(query), user=interaction.user.id, n=len(results))
    await interaction.response.send_message("\n".join(lines), ephemeral=True)
    inc("search_calls")

@bot_client.tree.command(name="context_window", description="Build context snippet (metadata only) for a query + optional time range like '24h'")
@app_commands.describe(query="Semantic query", time_range="e.g., 24h or 90m", channel_id="Restrict channel")
async def context_window(
    interaction: discord.Interaction,
    query: str,
    time_range: Optional[str] = None,
    channel_id: Optional[str] = None,
) -> None:  # type: ignore[type-arg]
    if not bot_client.search_limiter.allow(interaction.user.id):  # type: ignore[arg-type]
        await interaction.response.send_message("Rate limit exceeded.", ephemeral=True)
        return
    model = bot_client.ingestor.model
    q_vec = model.encode(query).tolist()
    start_ts = end_ts = None
    if time_range:
        start_ts, end_ts = parse_time_range(time_range)
    cid_int = int(channel_id) if channel_id else None
    results = bot_client.ingestor.search(q_vec, top_k=10, channel_id=cid_int, start_ts=start_ts, end_ts=end_ts)
    snippet = build_context_snippet(results)
    audit_log("context_window", q_len=len(query), user=interaction.user.id, n=len(results))
    await interaction.response.send_message(snippet or "No context.", ephemeral=True)
    inc("context_window_calls")

@bot_client.tree.command(name="keyword_search", description="Keyword + semantic search (hashed tokens)")
@app_commands.describe(keywords="Space-separated keywords", query="Semantic query (optional)", top_k="Results", channel_id="Restrict channel")
async def keyword_search(
    interaction: discord.Interaction,
    keywords: str,
    query: Optional[str] = None,
    top_k: Optional[int] = 5,
    channel_id: Optional[str] = None,
) -> None:  # type: ignore[type-arg]
    if not bot_client.search_limiter.allow(interaction.user.id):  # type: ignore[arg-type]
        await interaction.response.send_message("Rate limit exceeded.", ephemeral=True)
        return
    kw_list = [k for k in keywords.split() if k]
    hashed = bot_client.ingestor.hash_query_tokens(kw_list)
    message_ids = bot_client.ingestor.keyword_filter(hashed)
    if not message_ids:
        await interaction.response.send_message("No keyword matches.", ephemeral=True)
        return
    model = bot_client.ingestor.model
    q_vec = model.encode(query).tolist() if query else [0.0] * len(bot_client.ingestor.model.encode("test").tolist())  # semantic neutral vector if no query
    cid_int = int(channel_id) if channel_id else None
    results = bot_client.ingestor.search(
        q_vec,
        top_k=top_k or 5,
        channel_id=cid_int,
        message_ids=message_ids,
    )
    if not results:
        await interaction.response.send_message("No semantic matches after keyword filter.", ephemeral=True)
        return
    lines = [
        f"Msg {r['message_id']} ch:{r['channel_id']} author:{r['author_hash'][:8]} score:{r['score']:.3f}" for r in results
    ]
    audit_log("keyword_search", kw=len(kw_list), user=interaction.user.id, n=len(results))
    await interaction.response.send_message("\n".join(lines), ephemeral=True)
    inc("keyword_search_calls")

@bot_client.tree.command(name="purge_message", description="Purge a message vector by message id (admin)")
@app_commands.describe(message_id="Discord message ID")
async def purge_message(interaction: discord.Interaction, message_id: str) -> None:  # type: ignore[type-arg]
    member = interaction.user  # type: ignore[assignment]
    if not has_admin_access(member, bot_client.admin_roles):
        await interaction.response.send_message("Admin role required.", ephemeral=True)
        return
    removed = bot_client.ingestor.purge_message(int(message_id))
    audit_log("purge", message_id=message_id, removed=removed, user=interaction.user.id)
    inc("purge_calls", removed)
    await interaction.response.send_message(f"Removed {removed} entries.", ephemeral=True)

@bot_client.tree.command(name="llm_context", description="JSON context assembly for LLM (metadata only)")
@app_commands.describe(query="Semantic query", top_k="Max results (<= configured limit)")
async def llm_context(interaction: discord.Interaction, query: str, top_k: Optional[int] = 10) -> None:  # type: ignore[type-arg]
    if not bot_client.search_limiter.allow(interaction.user.id):  # type: ignore[arg-type]
        await interaction.response.send_message("Rate limit exceeded.", ephemeral=True)
        return
    model = bot_client.ingestor.model
    q_vec = model.encode(query).tolist()
    limit = min(top_k or 10, settings.max_context_results)
    results = bot_client.ingestor.search(q_vec, top_k=limit)
    # Build minimal JSON for injection
    payload = {
        "query": query,
        "results": [
            {
                "message_id": r["message_id"],
                "channel_id": r["channel_id"],
                "author_hash": r["author_hash"],
                "created_ts": r["created_ts"],
                "similarity": r["score"],
            }
            for r in results
        ],
        "disclaimer": "No raw content stored; metadata only.",
    }
    import orjson
    audit_log("llm_context", q_len=len(query), n=len(results), user=interaction.user.id)
    inc("llm_context_calls")
    await interaction.response.send_message(f"```json\n{orjson.dumps(payload).decode()}\n```", ephemeral=True)

def run() -> None:
    bot_client.run(settings.discord_token.get_secret_value())

if __name__ == "__main__":  # pragma: no cover
    run()
