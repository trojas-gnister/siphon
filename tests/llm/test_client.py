"""Tests for the async LLM client wrapper."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from siphon.config.schema import LLMConfig
from siphon.llm.client import LLMClient
from siphon.utils.errors import ExtractionError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def llm_config() -> LLMConfig:
    return LLMConfig(base_url="http://localhost:11434/v1", model="llama3")


@pytest.fixture
def client(llm_config: LLMConfig) -> LLMClient:
    return LLMClient(llm_config)


def _make_mock_response(content: str) -> MagicMock:
    """Build a mock OpenAI chat completion response returning `content`."""
    message = MagicMock()
    message.content = content
    choice = MagicMock()
    choice.message = message
    response = MagicMock()
    response.choices = [choice]
    return response


# ---------------------------------------------------------------------------
# complete()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_complete_returns_text(client: LLMClient) -> None:
    mock_response = _make_mock_response("Hello, world!")

    with patch.object(
        client._client.chat.completions,
        "create",
        new=AsyncMock(return_value=mock_response),
    ):
        result = await client.complete("Say hello")

    assert result == "Hello, world!"


@pytest.mark.asyncio
async def test_complete_empty_content_returns_empty_string(client: LLMClient) -> None:
    mock_response = _make_mock_response(None)  # type: ignore[arg-type]

    with patch.object(
        client._client.chat.completions,
        "create",
        new=AsyncMock(return_value=mock_response),
    ):
        result = await client.complete("prompt")

    assert result == ""


@pytest.mark.asyncio
async def test_complete_raises_extraction_error_on_api_failure(
    client: LLMClient,
) -> None:
    with patch.object(
        client._client.chat.completions,
        "create",
        new=AsyncMock(side_effect=RuntimeError("connection refused")),
    ):
        with pytest.raises(ExtractionError, match="LLM call failed"):
            await client.complete("prompt")


# ---------------------------------------------------------------------------
# _parse_json_response() — tested as a static method directly
# ---------------------------------------------------------------------------


def test_parse_bare_json_array() -> None:
    result = LLMClient._parse_json_response('[{"a": 1}, {"b": 2}]')
    assert result == [{"a": 1}, {"b": 2}]


def test_parse_code_fenced_json() -> None:
    text = '```json\n[{"name": "Alice"}]\n```'
    result = LLMClient._parse_json_response(text)
    assert result == [{"name": "Alice"}]


def test_parse_code_fenced_no_language_tag() -> None:
    text = "```\n[{\"x\": 42}]\n```"
    result = LLMClient._parse_json_response(text)
    assert result == [{"x": 42}]


def test_parse_wrapped_dict_data_key() -> None:
    text = '{"data": [{"id": 1}, {"id": 2}]}'
    result = LLMClient._parse_json_response(text)
    assert result == [{"id": 1}, {"id": 2}]


def test_parse_wrapped_dict_results_key() -> None:
    text = '{"results": [{"name": "Acme"}]}'
    result = LLMClient._parse_json_response(text)
    assert result == [{"name": "Acme"}]


def test_parse_extra_text_before_json() -> None:
    text = 'Here is the JSON you asked for:\n[{"key": "value"}]'
    result = LLMClient._parse_json_response(text)
    assert result == [{"key": "value"}]


def test_parse_extra_text_after_json() -> None:
    text = '[{"key": "value"}]\nHope that helps!'
    result = LLMClient._parse_json_response(text)
    assert result == [{"key": "value"}]


def test_parse_raises_on_no_json() -> None:
    with pytest.raises(ExtractionError, match="No JSON found"):
        LLMClient._parse_json_response("There is no JSON here at all.")


def test_parse_raises_on_invalid_json() -> None:
    with pytest.raises(ExtractionError, match="Failed to parse JSON"):
        LLMClient._parse_json_response("[{broken json")


def test_parse_raises_on_dict_with_no_array_value() -> None:
    with pytest.raises(ExtractionError, match="no array value"):
        LLMClient._parse_json_response('{"status": "ok", "count": 5}')


def test_parse_raises_on_non_array_json() -> None:
    # A bare string or number after bracket-search won't be a list
    with pytest.raises(ExtractionError):
        LLMClient._parse_json_response('"just a string"')


# ---------------------------------------------------------------------------
# extract_json() — end-to-end with mocked complete()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extract_json_bare_array(client: LLMClient) -> None:
    with patch.object(
        client,
        "complete",
        new=AsyncMock(return_value='[{"company": "Acme"}]'),
    ):
        result = await client.extract_json("extract companies")

    assert result == [{"company": "Acme"}]


@pytest.mark.asyncio
async def test_extract_json_code_fenced(client: LLMClient) -> None:
    fenced = '```json\n[{"name": "OpenAI"}]\n```'
    with patch.object(client, "complete", new=AsyncMock(return_value=fenced)):
        result = await client.extract_json("extract names")

    assert result == [{"name": "OpenAI"}]


@pytest.mark.asyncio
async def test_extract_json_wrapped_dict(client: LLMClient) -> None:
    wrapped = '{"data": [{"val": 99}]}'
    with patch.object(client, "complete", new=AsyncMock(return_value=wrapped)):
        result = await client.extract_json("extract data")

    assert result == [{"val": 99}]


@pytest.mark.asyncio
async def test_extract_json_raises_on_bad_response(client: LLMClient) -> None:
    with patch.object(
        client, "complete", new=AsyncMock(return_value="not valid json at all")
    ):
        with pytest.raises(ExtractionError):
            await client.extract_json("extract something")
