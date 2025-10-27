### AMOS Core (dbt project)

Private markets teams often live with different versions of the truth—numbers vary between CRM exports, fund admin portals, and spreadsheet models. AMOS Core gives you a single, reliable foundation for performance, exposure, and operations. It standardizes how funds, portfolios, deals, accounts, and legal entities fit together so everyone asks—and answers—the same questions with the same data. Use it to ship reports and dashboards faster, gain better insights, and power AI assistants on trusted tables.

### The problem this solves

- One question, many answers: metrics differ across spreadsheets and systems
- Slow time-to-insight: every dashboard requires bespoke wrangling
- Hard to scale: each new fund, region, or system multiplies complexity

### How it works

1. Connect your existing systems and files
2. Align key fields to AMOS standard entities
3. Run AMOS Core to build clean, consistent tables
4. Use your preferred tools (dashboards, notebooks, apps) on those tables

### Quickstart

```bash
cd amos_core
dbt deps
dbt seed
dbt build
```

### What you get

- Standardized tables for funds, portfolios, deals, accounts, and entities
- Consistent keys and KPI definitions
- Reference data for harmonization and lookups

### What’s inside

- Models: curated marts in `models/marts/core`
- Seeds: reference tables in `seeds/`

### When to use AMOS Core

- You need a single, reliable view across CRM, portfolio, fund admin, and finance
- You’re standardizing KPIs and definitions across teams and service providers
- You want a durable foundation for dashboards, reporting, and AI assistants

## Related Projects

- **[AMOS Starter](https://github.com/open-amos/starter)** – Orchestrator and entry point
- **[AMOS Source Example](https://github.com/open-amos/source-example)** – Source integration patterns and example data

### Docs

For detailed setup and technical docs, visit [docs.amos.tech](https://docs.amos.tech).


