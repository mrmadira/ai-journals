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

- `icedemocatalog.tpchdbiceberg.orders.o_orderkey` **overflowed at load**. It is `integer`
  (32-bit) while `lineitem.l_orderkey` is `bigint` (up to 6B), so keys above 2,147,483,647 became
  NULL: ~64% of `o_orderkey` values are NULL (963M of 1.5B). Joins `lineitem` → `orders` on order
  key match only ~36% of line items, and per-order answers are incomplete. Say so whenever an
  answer depends on joining orders by key. (verified 2026-11-15: 1994-06-20 partition has 399,755
  NULL keys of 622,741 rows, matching `$partitions` null_count)
- **Silent partial counts happened.** Until midday 2026-11-15, `COUNT(*)` on
  `icedemocatalog.tpchdbiceberg.customer` returned 29,995,372 on both Presto engines with no
  error. That's one of 5 partitions; the true count is 150,000,000 and the table hasn't changed
  since 2024. Root cause unknown. When a count matters, cross-check it against
  `SUM(row_count)` from `"<table>$partitions"`, and report both if they disagree.
- `icedemocatalog.tpchdbiceberg.lineitem.l_shipdate` is **varchar** (ISO `YYYY-MM-DD`), because
  it's the partition column. Compare it as a string (`l_shipdate >= '1995-01-01'`), never with a
  `DATE` literal. Other date columns such as `l_commitdate` and `l_receiptdate` are real `DATE`s;
  check other tables with describe-table.
- `icedemocatalog.tpchdbiceberg.lineitem` has ~6.0B rows: full scans exceed the scan budget.

## Physical layout (use it: it's the biggest performance lever)

- `icedemocatalog.tpchdbiceberg.lineitem` is **partitioned by `l_shipdate`, one partition per
  day** (2,526 partitions). Always filter on `l_shipdate` when the question has a time range;
  only the matching days are read.
- **Row counts by ship date come free from metadata.** The Iceberg metadata table
  `icedemocatalog.tpchdbiceberg."lineitem$partitions"` has one row per day with `row_count`, so
  `SELECT SUM(row_count) FROM icedemocatalog.tpchdbiceberg."lineitem$partitions" WHERE
  l_shipdate >= '1995-01-01' AND l_shipdate < '1996-01-01'` returns the exact count (verified:
  910,208,622 in ~3s, vs ~60s scanning). Prefer it for any "how many line items" by ship date.
  (Quote the table name; it reflects data files, so it's exact unless rows were deleted.)
- `$partitions` also has per-partition min/max/null counts for every column, which is useful for
  cheap range and null checks.
- Other partitioned tables in `icedemocatalog.tpchdbiceberg`: `orders` by `o_orderdate` (a real
  `DATE`, one partition per day, 1992-01-01..1998-08-02), `customer` by `c_mktsegment`
  (5 partitions of ~30M rows each), `part` by `p_brand` (25 partitions). `partsupp`, `supplier`,
  `nation`, `region` are unpartitioned. (profiled + verified 2026-11-15)
- Exact sizes (2026-11-15): lineitem 5,999,989,709 · orders 1,500,000,000 · partsupp 800M ·
  part 200M · customer 150M · supplier 10M · nation 25 · region 5. This is a full TPC-H scale
  factor 1000 load.
- TPC-H data is synthetic and uniformly distributed: differences between regions/nations are
  random noise. Say so when ranking them.

## Dataset conventions

- `ignition_catalog.nba.game.season_id` encodes type + year: `2YYYY` = regular season starting
  in year YYYY (e.g. `22022` = 2022-23), `4YYYY` = playoffs, `1YYYY` = pre-season,
  `3YYYY` = All-Star. Filter `season_type = 'Regular Season'` for standings questions.
- Season-over-season comparisons in NBA data should use **win percentage**, not raw wins:
  1998-99, 2011-12, 2019-20 and 2020-21 were shortened seasons.
- `ignition_catalog.nba.game` has **no regular-season rows for 7 seasons**: 1960-61, 1961-62,
  1966-67, 1970-71, 1975-76, 1976-77 and 2012-13 (70 of 77 seasons from 1946-47 to 2022-23). For
  "previous season" comparisons, check that the seasons are consecutive.
- All-Star games carry two `season_type` spellings, `'All Star'` and `'All-Star'`, often for the
  same game. Filter on both, or on `season_id` prefix 3.
- The NBA data is team-level and biographical only: no awards/MVP data and no per-player game or
  season statistics.

## Metric definitions

<!-- Fill these in with your organization's real definitions. Examples: -->
- **Revenue** (TPC-H): `SUM(l_extendedprice * (1 - l_discount))` - net of discount, before tax.
- **Win percentage** (NBA): wins / (wins + losses), regular season only.
