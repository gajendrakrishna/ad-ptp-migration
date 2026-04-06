# Migration Project

Data warehouse migration from Microsoft SQL Server to Vibedata Managed Fabric Lakehouse using dbt.

## Prerequisites

- **Python 3.11+**
- **uv** — Python package manager ([install](https://astral.sh/uv))
- **direnv** — credential management ([install](https://direnv.net)) — recommended
- **genai-toolbox** — for live SQL Server access ([releases](https://github.com/googleapis/genai-toolbox/releases)) — optional

### Credential setup with direnv

1. Copy the `.envrc` template and fill in your values:

   ```bash
   # .envrc (gitignored)
   export MSSQL_HOST=localhost
   export MSSQL_PORT=1433
   export MSSQL_DB=YourDatabase
   export SA_PASSWORD=YourPassword
   ```

2. Run `direnv allow` to load the variables.

These values are passed to the `mssql` MCP server at startup via environment inheritance — they must be set before launching `claude`.

## Workflow

1. **`/init-ad-migration`** — verify prerequisites and scaffold project files
2. **`/setup-ddl`** — extract DDL from live SQL Server into local artifact files
3. **`/listing-objects`** — browse the DDL catalog (list objects by type)
4. **`/profiling-table`** — profile individual tables interactively
5. **`/generate-model`** — generate dbt models from stored procedures

## Directory Structure

```text
.
├── CLAUDE.md          # Agent instructions
├── README.md          # This file
├── repo-map.json      # Directory structure for agent discovery
├── .envrc             # Credentials (gitignored)
├── .gitignore         # Git ignore rules
├── .githooks/         # Git hooks (pre-commit secret blocking)
├── ddl/               # Extracted DDL files (from setup-ddl)
├── catalog/           # Catalog JSON files (from setup-ddl)
├── manifest.json      # Extraction manifest (from setup-ddl)
└── dbt/               # dbt project (from init-dbt)
```

## Git Safety

A pre-commit hook in `.githooks/` blocks commits containing:

- Anthropic API keys (`sk-ant` prefix)
- `SA_PASSWORD` in `.mcp.json`
- MSSQL credentials in `.env` or `.envrc` files

The hook is a safety net — `.env`, `.envrc`, and `.mcp.json` are also in `.gitignore`.

## Commit Conventions

Commit messages use the format: `type: short description`

Examples: `feat: extract DDL from AdventureWorks`, `fix: correct column type mapping`
