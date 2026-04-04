from dataclasses import dataclass


@dataclass
class SectionAnchor:
    section: str       # logical name, e.g. "budget_overview"
    keyword: str       # text to search for in PDF pages
    pages_after: int   # how many pages after the match to include (0 = only the match page)
    collect_all: bool  # if True, collect every page containing the keyword (vs. first match only)


_BUDGET_ANCHORS = [
    SectionAnchor(
        section="budget_overview",
        keyword="BUDGET OVERVIEW",
        pages_after=1,
        collect_all=False,
    ),
    SectionAnchor(
        section="dept_profiles",
        keyword="DEPARTMENT PROFILE",
        pages_after=0,
        collect_all=True,
    ),
]

_QUARTERLY_ANCHORS = [
    SectionAnchor(
        section="financial_highlights",
        keyword="FINANCIAL HIGHLIGHTS",
        pages_after=10,
        collect_all=False,
    ),
]

_ACFR_ANCHORS = [
    SectionAnchor(
        section="financial_statements",
        keyword="Statement of Net Position",
        pages_after=30,
        collect_all=False,
    ),
    SectionAnchor(
        section="statistical",
        keyword="STATISTICAL SECTION",
        pages_after=50,
        collect_all=False,
    ),
]

_ANCHORS = {
    "budget": _BUDGET_ANCHORS,
    "quarterly": _QUARTERLY_ANCHORS,
    "acfr": _ACFR_ANCHORS,
}


def get_anchors(doc_type: str) -> list[SectionAnchor]:
    """Return keyword anchors for the given doc_type."""
    if doc_type not in _ANCHORS:
        raise ValueError(
            f"Unknown doc_type: {doc_type!r}. Expected one of: {list(_ANCHORS)}"
        )
    return _ANCHORS[doc_type]
