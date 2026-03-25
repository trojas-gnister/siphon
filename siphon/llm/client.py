"""Async LLM client wrapper for OpenAI-compatible endpoints."""

from __future__ import annotations

import json
import re

from openai import AsyncOpenAI

from siphon.config.schema import LLMConfig
from siphon.utils.errors import ExtractionError


class LLMClient:
    """Async client for any OpenAI-compatible LLM endpoint (OpenAI, Ollama, vLLM, LM Studio)."""

    def __init__(self, config: LLMConfig) -> None:
        self._config = config
        self._client = AsyncOpenAI(
            base_url=config.base_url,
            api_key=config.api_key or "not-needed",  # Ollama doesn't need a key
        )
        self._model = config.model

    async def complete(self, prompt: str) -> str:
        """Send a prompt and return the text response.

        Raises ExtractionError on API failure.
        """
        try:
            response = await self._client.chat.completions.create(
                model=self._model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
            )
            return response.choices[0].message.content or ""
        except Exception as e:
            raise ExtractionError(f"LLM call failed: {e}") from e

    async def extract_json(self, prompt: str) -> list[dict]:
        """Send a prompt and parse the response as a JSON array of objects.

        Handles common LLM response quirks:
        1. Bare JSON array: [{"a": 1}, ...]
        2. Code-fenced: ```json\\n[...]\\n```
        3. Wrapped dict: {"data": [...]} or {"results": [...]}
        4. Extra text before/after the JSON

        Returns list[dict].
        Raises ExtractionError if parsing fails.
        """
        text = await self.complete(prompt)
        return self._parse_json_response(text)

    @staticmethod
    def _parse_json_response(text: str) -> list[dict]:
        """Parse LLM response text into list[dict]."""
        # Strip code fences
        text = text.strip()
        fence_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?\s*```", text, re.DOTALL)
        if fence_match:
            text = fence_match.group(1).strip()

        # Find first [ or { to locate the start of JSON
        start_bracket = None
        for i, c in enumerate(text):
            if c in "[{":
                start_bracket = i
                break

        if start_bracket is None:
            raise ExtractionError(f"No JSON found in LLM response: {text[:200]}")

        text = text[start_bracket:]

        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            # Try progressively shorter substrings to handle extra trailing text
            parsed = None
            for end in range(len(text), 0, -1):
                try:
                    parsed = json.loads(text[:end])
                    break
                except json.JSONDecodeError:
                    continue
            if parsed is None:
                raise ExtractionError(
                    f"Failed to parse JSON from LLM response: {text[:200]}"
                )

        # If it's a dict, unwrap by finding the first key whose value is a list
        if isinstance(parsed, dict):
            for key, value in parsed.items():
                if isinstance(value, list):
                    parsed = value
                    break
            else:
                raise ExtractionError(
                    f"LLM returned a JSON object with no array value: {list(parsed.keys())}"
                )

        if not isinstance(parsed, list):
            raise ExtractionError(f"Expected JSON array, got {type(parsed).__name__}")

        return parsed
