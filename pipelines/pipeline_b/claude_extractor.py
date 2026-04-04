import base64
import json
import logging
import time
from dataclasses import dataclass

from anthropic import Anthropic, APIStatusError

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
_MAX_RETRIES = 3
_RETRY_BASE_SECONDS = 2


@dataclass
class ExtractResult:
    data: dict
    input_tokens: int
    output_tokens: int


def extract_page(image_bytes: bytes, prompt: str, client: Anthropic) -> ExtractResult:
    """
    Send a PNG image to Claude and return structured financial data.
    Retries up to 3 times on APIStatusError with exponential backoff.
    Returns empty data (not raises) on invalid JSON response.
    """
    b64 = base64.standard_b64encode(image_bytes).decode()

    for attempt in range(_MAX_RETRIES):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=2048,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": "image/png",
                                    "data": b64,
                                },
                            },
                            {"type": "text", "text": prompt},
                        ],
                    }
                ],
            )
            text = response.content[0].text
            # Strip markdown code fences if present (e.g. ```json ... ```)
            stripped = text.strip()
            if stripped.startswith("```"):
                lines = stripped.splitlines()
                # Drop the opening fence line and the closing ``` line
                inner_lines = lines[1:]
                if inner_lines and inner_lines[-1].strip() == "```":
                    inner_lines = inner_lines[:-1]
                stripped = "\n".join(inner_lines)
            try:
                data = json.loads(stripped)
            except json.JSONDecodeError:
                logger.warning(f"Claude returned invalid JSON: {text[:200]!r}")
                data = {"expenditures": [], "revenues": [], "fund_summaries": []}

            return ExtractResult(
                data=data,
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
            )

        except APIStatusError as e:
            if attempt < _MAX_RETRIES - 1:
                wait = _RETRY_BASE_SECONDS * (2 ** attempt)
                logger.warning(
                    f"API error (attempt {attempt + 1}/{_MAX_RETRIES}): {e}. Retrying in {wait}s."
                )
                time.sleep(wait)
            else:
                raise
