# Migration Project

## Domain

Migrating a data warehouse to **Vibedata Managed Fabric Lakehouse**. Source system: **Microsoft SQL Server** (T-SQL stored procedures).

Migration target: silver and gold dbt transformations on the Fabric Lakehouse endpoint. Bronze ingestion layers, ADF pipelines, and Power BI are out of scope.

## Stack

| Layer | Technology | Notes |
|---|---|---|
| Source DDL access | DDL file MCP (`ddl_mcp`) | Pre-extracted `.sql` files; no live DB required |
| Live source DB access | `mssql` MCP via genai-toolbox | Requires `toolbox` binary on PATH |
| Transformation target | **dbt** (dbt-fabric adapter) | SQL models on Lakehouse endpoint |
| Storage | **Delta tables** on OneLake | Managed by Fabric Lakehouse |
| Orchestration | dbt build pipeline | |
| Platform | **Microsoft Fabric** on Azure | |

## Directory Layout

See `repo-map.json` for the full directory structure and agent notes.

## Skills

| Skill | Purpose |
|---|---|
| `/setup-ddl` | Extract DDL from live SQL Server and write local artifact files |
| `/listing-objects` | Browse the DDL catalog — list, show, refs |
| `/analyzing-table` | Writer discovery, procedure analysis, scope resolution, and catalog persistence |
| `/profiling-table` | Interactive single-table profiling with approval gates |
| `/generating-tests` | Generate ground truth test fixtures for a table |
| `/reviewing-tests` | Quality gate for test generation output |
| `/generate-model` | Generate dbt models from stored procedures |

## MCP Servers

| Server | Transport | Purpose |
|---|---|---|
| `ddl_mcp` | stdio | Structured DDL parsing from local `.sql` files |
| `mssql` | HTTP (genai-toolbox) | Live SQL Server queries |

## Git Workflow

Worktree conventions and PR format are in `.claude/rules/git-workflow.md`.

## Guardrails

- Never log or commit secrets (API keys, passwords, connection strings)
- All SQL parsing uses sqlglot — no regex fallbacks for DDL analysis
- Validate at system boundaries; trust internal library guarantees
- Pre-commit hook blocks common credential patterns (see `.githooks/pre-commit`)

## Maintenance

Update `repo-map.json` whenever files are added, removed, or renamed in structural directories (ddl/, catalog/, dbt/).

## Skill Reasoning

Before answering any LLM judgment step (classification, writer selection, statement tagging, profiling question), state your reasoning in 1–2 sentences. This trace must appear in the conversation log so reviewers and batch-run debuggers can see *why* a decision was made, not just *what* it was.

## Output Framing

Present results so the reader understands the output without mental overhead. Lead with the decision, then supporting evidence. At approval gates, the user should see the answer first and the reasoning second — not the other way around.

## Commit Discipline

Commit at logical checkpoints so work is never lost mid-session.

| Checkpoint | When to commit |
|---|---|
| After DDL extraction | `setup-ddl` completes writing DDL files and catalog |
| After discovery | `discover` produces new analysis or annotations |
| After scoping | Scoping agent finalises scope configuration |
| After model generation | A dbt model is written or updated |
| After config changes | Manifest, project config, or schema changes |

Commit messages: `type: short description` (e.g. `feat: extract DDL from AdventureWorks`).

If not a git repository, skip commit steps silently.
