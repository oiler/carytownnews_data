# Project

A civic transparency research tool for investigating local government. Pulls data from official government APIs, PDFs, and meeting transcripts to surface insights about local government activity.

# Commands

```bash
# Install dependencies: uv pip install -r requirements.txt
# Run tests: pytest
# Run single test: pytest tests/test_file.py::test_name
# Lint + format: ruff check . && ruff format .
```

# Gotchas

- API keys and credentials required — copy `.env.example` to `.env` and fill in values before running
- Never commit `.env` or any file containing credentials

# Claude Permissions

**Mode: B** — Standard

# Git Mode

**Mode: C** — Automatic

- Never delete the project folder
- Never delete databases
- Never delete and start over — always work incrementally

# Skills

- Use `writing-style` skill when writing documents
- Use `superpowers:brainstorming` when asked to brainstorm or ideate
- Use `superpowers:writing-plans` when asked to create a plan or design
- Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` when asked to build
- Use `python` skill for all Python code
- Use `web-security` skill when writing any HTTP, API, or scraping code
