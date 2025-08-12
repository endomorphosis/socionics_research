from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import discord
from discord import app_commands

from .config import settings
from .guardrails import apply_guardrails

log = logging.getLogger("socionics_bot")
logging.basicConfig(level=logging.INFO)

INTENTS = discord.Intents.none()
INTENTS.message_content = False
INTENTS.guilds = True
INTENTS.members = False

class SocionicsBot(discord.Client):
    def __init__(self) -> None:
        super().__init__(intents=INTENTS)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self) -> None:
        await self.tree.sync()
        log.info("Commands synced")

bot_client = SocionicsBot()

@bot_client.tree.command(name="about_socionics", description="Neutral overview of Socionics & empirical status")
async def about_socionics(interaction: discord.Interaction) -> None:  # type: ignore[type-arg]
    text = (
        "Socionics is a theoretical framework describing information processing styles and intertype relations. "
        "This project evaluates its claims empirically. Constructs are exploratory and not diagnostic."
    )
    await interaction.response.send_message(text, ephemeral=True)

@bot_client.tree.command(name="my_type_help", description="Guided self-observation checklist (no type assignment)")
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

@bot_client.tree.command(name="theory", description="Retrieve concise explanation of a theory topic (beta)")
@app_commands.describe(topic="Keyword, e.g., 'Model A', 'intertype relations'")
async def theory(interaction: discord.Interaction, topic: str) -> None:  # type: ignore[type-arg]
    guard = apply_guardrails(topic)
    if guard.blocked:
        await interaction.response.send_message(
            "Request blocked (type assignment or prohibited topic). This bot cannot assign types.", ephemeral=True
        )
        return
    # For now: simple keyword mapping placeholder
    key = topic.lower()
    if "model" in key:
        msg = "Model A: Eight functional positions describing information metabolism roles (program, creative, role, vulnerable, suggestive, mobilizing, ignoring, demonstrative). Empirical status: structural arrangement needs validation."
    elif "duality" in key:
        msg = "Duality: Hypothesized complementary relation maximizing support. Evidence: anecdotal; design dyadic task studies to test coordination efficiency vs. random pairs."
    else:
        msg = "Topic not found in beta glossary."
    await interaction.response.send_message(msg, ephemeral=True)


def run() -> None:
    bot_client.run(settings.discord_token.get_secret_value())

if __name__ == "__main__":  # pragma: no cover
    run()
