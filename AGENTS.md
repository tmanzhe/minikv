# Working standards

- Keep changes minimal and focused; prefer small, logical commits. The user
  commits: never write to `.git` or create commits.
- Test observable behavior, not implementation details. Keep all existing tests
  green, run the full suite at the end, and report pass/fail/skip counts and key
  numbers.
- Prove changes work against the real artifact: build the distribution, install
  it without editable mode, and run the tests against that installed package.
- Never read or write `.env*` files or secrets. Never include secrets in code,
  logs, fixtures, or commits.
- Make no paid or real API calls; use mocks for external APIs. Local artifact
  checks must not contact live services.
- Keep this scaffold free of server code until explicitly requested.
