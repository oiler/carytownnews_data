_SHARED_INSTRUCTIONS = """
Return ONLY valid JSON. No markdown, no explanation, no code fences.
Return null for any field you cannot find — do not hallucinate values.
Return all dollar amounts as plain numbers without dollar signs or commas (e.g. 45230000).
"""

_BUDGET_PROMPT = """
This is a page from a Town of Cary, North Carolina adopted operating budget document.
Extract all financial data visible on this page.

{shared}

Return this exact JSON structure:
{{
  "expenditures": [
    {{
      "fund": "General",
      "department": "Police",
      "division": null,
      "amount_type": "adopted",
      "amount": 45230000
    }}
  ],
  "revenues": [
    {{
      "fund": "General",
      "source": "Property Tax",
      "amount_type": "adopted",
      "amount": 120000000
    }}
  ],
  "fund_summaries": [
    {{
      "fund": "General",
      "total_revenues": 200000000,
      "total_expenditures": 195000000,
      "transfers_in": 5000000,
      "transfers_out": 10000000,
      "beginning_balance": 50000000,
      "ending_balance": 50000000
    }}
  ]
}}

amount_type must be one of: "adopted", "actual", "prior_year_actual", "recommended".
If a page shows multiple years, include one entry per year per department with the correct amount_type.
If a section is not present on this page, return an empty array for that key.
""".format(shared=_SHARED_INSTRUCTIONS)

_QUARTERLY_PROMPT = """
This is a page from a Town of Cary, North Carolina Council Quarterly Report.
Extract the financial highlights data visible on this page.

{shared}

Important: Dollar amounts in this document are displayed in millions. A table value of 183.7 means $183,700,000. Return the full dollar amount (e.g. return 183700000, not 183.7).

Return this exact JSON structure:
{{
  "expenditures": [],
  "revenues": [],
  "fund_summaries": [
    {{
      "fund": "General Fund",
      "total_revenues": null,
      "total_expenditures": 130000000,
      "transfers_in": null,
      "transfers_out": null,
      "beginning_balance": null,
      "ending_balance": null
    }}
  ]
}}

If a section is not present on this page, return an empty array for that key.
""".format(shared=_SHARED_INSTRUCTIONS)

_ACFR_PROMPT = """
This is a page from the Town of Cary, North Carolina Annual Comprehensive Financial Report (ACFR).
Extract all financial statement data visible on this page.

{shared}

Return this exact JSON structure:
{{
  "expenditures": [
    {{
      "fund": "General",
      "department": "Public Safety",
      "division": null,
      "amount_type": "actual",
      "amount": 45230000
    }}
  ],
  "revenues": [
    {{
      "fund": "General",
      "source": "Property Tax",
      "amount_type": "actual",
      "amount": 120000000
    }}
  ],
  "fund_summaries": [
    {{
      "fund": "General",
      "total_revenues": 200000000,
      "total_expenditures": 195000000,
      "transfers_in": 5000000,
      "transfers_out": 10000000,
      "beginning_balance": 50000000,
      "ending_balance": 50000000
    }}
  ]
}}

amount_type for ACFR documents is always "actual" (these are audited final figures).
If a section is not present on this page, return an empty array for that key.
""".format(shared=_SHARED_INSTRUCTIONS)

_PROMPTS = {
    "budget": _BUDGET_PROMPT,
    "quarterly": _QUARTERLY_PROMPT,
    "acfr": _ACFR_PROMPT,
}


def get_prompt(doc_type: str) -> str:
    if doc_type not in _PROMPTS:
        raise ValueError(
            f"Unknown doc_type: {doc_type!r}. Expected one of: {list(_PROMPTS)}"
        )
    return _PROMPTS[doc_type]
