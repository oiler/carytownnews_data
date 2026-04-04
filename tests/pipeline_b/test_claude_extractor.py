import json
import pytest
from unittest.mock import MagicMock, patch
from anthropic import APIStatusError
import httpx

from pipelines.pipeline_b.claude_extractor import extract_page, ExtractResult

VALID_RESPONSE_JSON = json.dumps({
    "expenditures": [
        {"fund": "General", "department": "Police", "division": None,
         "amount_type": "adopted", "amount": 45230000}
    ],
    "revenues": [],
    "fund_summaries": []
})

FAKE_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100


def _make_api_response(content_text: str, input_tokens: int = 1500, output_tokens: int = 300):
    """Build a mock Anthropic message response."""
    msg = MagicMock()
    msg.content = [MagicMock(text=content_text)]
    msg.usage.input_tokens = input_tokens
    msg.usage.output_tokens = output_tokens
    return msg


def _make_client(response):
    client = MagicMock()
    client.messages.create.return_value = response
    return client


def test_extract_page_returns_extract_result():
    client = _make_client(_make_api_response(VALID_RESPONSE_JSON))
    result = extract_page(FAKE_PNG, "budget prompt", client)
    assert isinstance(result, ExtractResult)


def test_extract_page_parses_json_correctly():
    client = _make_client(_make_api_response(VALID_RESPONSE_JSON))
    result = extract_page(FAKE_PNG, "budget prompt", client)
    assert len(result.data["expenditures"]) == 1
    assert result.data["expenditures"][0]["department"] == "Police"
    assert result.data["expenditures"][0]["amount"] == 45230000


def test_extract_page_returns_token_counts():
    client = _make_client(_make_api_response(VALID_RESPONSE_JSON, input_tokens=2000, output_tokens=400))
    result = extract_page(FAKE_PNG, "budget prompt", client)
    assert result.input_tokens == 2000
    assert result.output_tokens == 400


def test_extract_page_returns_empty_data_on_invalid_json():
    client = _make_client(_make_api_response("this is not json"))  # default: input=1500, output=300
    result = extract_page(FAKE_PNG, "budget prompt", client)
    assert result.data == {"expenditures": [], "revenues": [], "fund_summaries": []}
    assert result.input_tokens == 1500
    assert result.output_tokens == 300


def test_extract_page_strips_json_code_fence():
    fenced = "```json\n" + VALID_RESPONSE_JSON + "\n```"
    client = _make_client(_make_api_response(fenced))
    result = extract_page(FAKE_PNG, "budget prompt", client)
    assert result.data["expenditures"][0]["department"] == "Police"


def test_extract_page_strips_bare_code_fence():
    fenced = "```\n" + VALID_RESPONSE_JSON + "\n```"
    client = _make_client(_make_api_response(fenced))
    result = extract_page(FAKE_PNG, "budget prompt", client)
    assert result.data["expenditures"][0]["department"] == "Police"


def _make_status_error(message: str, status_code: int) -> APIStatusError:
    """Build an APIStatusError with a properly mocked httpx.Response."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = status_code
    mock_response.headers = MagicMock()
    mock_response.headers.get = MagicMock(return_value=None)
    return APIStatusError(message, response=mock_response, body={})


def test_extract_page_retries_on_api_status_error():
    good_response = _make_api_response(VALID_RESPONSE_JSON)
    error = _make_status_error("rate limit", 429)
    client = MagicMock()
    client.messages.create.side_effect = [error, good_response]

    with patch("time.sleep"):  # don't actually sleep in tests
        result = extract_page(FAKE_PNG, "budget prompt", client)

    assert client.messages.create.call_count == 2
    assert result.data["expenditures"][0]["department"] == "Police"


def test_extract_page_raises_after_three_failures():
    error = _make_status_error("server error", 500)
    client = MagicMock()
    client.messages.create.side_effect = error

    with patch("time.sleep"):
        with pytest.raises(APIStatusError):
            extract_page(FAKE_PNG, "budget prompt", client)

    assert client.messages.create.call_count == 3
