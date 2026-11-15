# Lakehouse context

<!--
  Maintained by the agent's provider (you). Loaded into the agent's system prompt on every run.
  Keep it short and factual: what the data MEANS, which copy is canonical, and known traps.
  The agent can find table/column NAMES itself - don't list those here.
  Promote confirmed entries from notes.md (written by the agent) into this file.
-->

## Canonical datasets

| Topic | Use this | Notes |
|---|---|---|
| TPC-H benchmark (stored) | `icedemocatalog.tpchdbiceberg` | Default when a user says "TPC-H" or "tpch" without naming a schema. Other stored copies (`icedemocatalog.tpch_t3`, `icedemocatalog.tpchdbhive`) are not canonical. |
| TPC-H benchmark (generated) | `tpch.<scale>` (e.g. `tpch.sf1`) | Presto's built-in generator: data is synthesized at query time. Only use it when the user explicitly asks for a scale factor or "the generator". |
| NBA history | `ignition_catalog.nba` | Main tables: `game` (one row per game), `team`, `team_details`, `team_history`, `player`, `common_player_info`. `ignition.nba_dataset` is a smaller subset. |

## Known data issues

- `icedemocatalog.tpchdbiceberg.customer` is **under-loaded**: ~30.0M rows, while every other
  table in the schema is at scale factor 1000 (orders = 1.5B, so customer should be 150M).
  Customer keys range up to ~150M, so ~80% of customers are missing. Joins to `customer` will
  drop most orders; say so whenever an answer depends on customers.
- In `icedemocatalog.tpchdbiceberg`, **date columns are stored as varchar** (e.g.
  `lineitem.l_shipdate`). Compare with `CAST(l_shipdate AS DATE)` or ISO-string comparison
  (`l_shipdate >= '1995-01-01'`), never against a `DATE` literal directly.
- `icedemocatalog.tpchdbiceberg.lineitem` has ~6.0B rows: full scans exceed the scan budget.
  Filter, sample, or use EXPLAIN estimates for counts.
- TPC-H data is synthetic and uniformly distributed: differences between regions/nations are
  random noise. Say so when ranking them.

## Dataset conventions

- `ignition_catalog.nba.game.season_id` encodes type + year: `2YYYY` = regular season starting
  in year YYYY (e.g. `22022` = 2022-23), `4YYYY` = playoffs, `1YYYY` = pre-season,
  `3YYYY` = All-Star. Filter `season_type = 'Regular Season'` for standings questions.
- Season-over-season comparisons in NBA data should use **win percentage**, not raw wins:
  1998-99, 2011-12, 2019-20 and 2020-21 were shortened seasons.

## Metric definitions

<!-- Fill these in with your organization's real definitions. Examples: -->
- **Revenue** (TPC-H): `SUM(l_extendedprice * (1 - l_discount))` - net of discount, before tax.
- **Win percentage** (NBA): wins / (wins + losses), regular season only.
