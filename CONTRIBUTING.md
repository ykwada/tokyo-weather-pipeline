# Contributing Guide

## Branching strategy (GitHub Flow)

`main` is the single source of truth. All changes go through
feature branches and pull requests.

- `feature/<issue>-<description>` — new functionality
- `fix/<issue>-<description>` — bug fixes
- `docs/<description>` — documentation only
- `chore/<description>` — maintenance tasks

## Commit conventions

Follows Conventional Commits:
`<type>(<scope>): <description>` with `refs/closes #<issue>` in the footer.
Types: feat / fix / refactor / docs / test / chore / ci
One commit = one logical change.

## Pull requests

- One PR per issue, base branch is `main`
- Title: `[#<issue>] <description>`
- Include `closes #<issue>` to auto-close the issue
- CI must pass before merge