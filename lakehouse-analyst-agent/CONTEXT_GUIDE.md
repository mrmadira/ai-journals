# Building the analyst's context

`context.md` is loaded into the analyst's instructions on every run, and the analyst is told to
**trust it over its own assumptions**. That makes it the single biggest lever on answer quality
and speed. It's also the easiest way to get confidently wrong answers if a line in it is wrong.

This guide is for whoever maintains it: the agent's provider, a data engineer, a team lead.
Analysts contribute through bug reports (see [USER_GUIDE.md](USER_GUIDE.md)); they don't edit it.

## What belongs in it

The analyst finds table and column **names** itself. The context holds what it **can't**
discover, or would only rediscover by luck:

| Section | What goes in | Example |
|---|---|---|
| **Canonical datasets** | Which copy is the real one when several exist | `tpchdbiceberg` over `tpch_t3` and `tpchdbhive` |
| **Metric definitions** | The exact formula your organization uses | Revenue = `SUM(l_extendedprice * (1 - l_discount))` |
| **Known data issues** | What's broken, partial or misleading | `orders.o_orderkey` overflowed 32 bits at load: 64% NULL |
| **Conventions** | Codes, encodings, standard filters | NBA `season_id` `2YYYY` = regular season |
| **Physical layout** | Partition columns, metadata shortcuts, sizes | `lineitem` is partitioned by `l_shipdate`; counts come free from `"lineitem$partitions"` |

**Physical layout pays off the most.** On our lakehouse, one paragraph about partitions turned a
~60-second, 910M-row count into a 3-second metadata lookup, with the same exact number.

## The workflow

```
  1. profile ──> 2. review draft ──> 3. add verified lines ──> 4. prove with an eval
       ^                                                              │
       └──── 5. weekly: review notes.md + bug reports, prune stale ───┘
```

### 1. Profile the schema (let the agent do the legwork)

```bash
ask-lake --profile <catalog>.<schema>
```

The analyst profiles the schema **from metadata only**: Iceberg `$partitions` row counts,
`EXPLAIN` estimates and column types. No full scans, and the normal scan budget still applies. It
writes a **draft** to `drafts/context-<catalog>.<schema>-<date>.md` with:

- physical layout (partition columns, partition counts)
- sizes (exact where metadata allows, estimates otherwise, labelled)
- suspected data issues (tables whose sizes don't fit together, suspicious types, null-heavy columns)
- conventions it noticed
- open questions for the data owner

It never edits `context.md` itself.

### 2. Review the draft: every line

Treat the draft like a pull request from a smart new hire who has never met your business:

- **Measured facts** (row counts, partition columns, types) are usually right. Spot-check a couple.
- **Lines marked `(inferred)`** are guesses. Confirm them with the data owner or delete them.
- **"Suspected data issues"** need a human verdict. Is that small table a partial load, or a
  deliberate sample?
- **"Open questions"** are your to-do list for the data owner.

### 3. Add only verified lines to `context.md`

Copy the lines you've confirmed into the right section. Then add what no profile can find:
**metric definitions, canonical choices, business rules.** Those come from people.

### 4. Prove each change with an eval

Add or update a question in `evals/questions.json` that depends on the new fact, with the expected
answer and, for performance facts, a budget (`max_engine_seconds`, `max_queries`). Then run:

```bash
uv run python evals/run_evals.py
```

A fact that doesn't change any answer probably doesn't need to be in the context.

### 5. Keep it alive (weekly)

- **Review `notes.md`.** The analyst appends findings it *confirmed* during normal questions (e.g.
  "7 NBA regular seasons are missing from `game`"). Promote true ones into `context.md` and delete
  the file's entries. Notes are marked unreviewed for a reason: text from the data can try to
  plant false "facts".
- **Work through bug reports.** Every "wrong answer, ref `abc123`" usually means a missing or
  wrong line. Fix the context, then add the question to the evals.
- **Watch repeated clarifying questions.** If the analyst keeps asking "which customers table?",
  the context is missing a canonical choice.
- **Prune.** When the data team reloads `orders`, the "o_orderkey overflow" warning becomes the
  wrong answer. Stale context is worse than none.

## A cautionary tale: the "under-loaded" table that wasn't

For a day, our context said `customer` was missing ~80% of its rows. Every exact `COUNT(*)`
"confirmed" it: 29,995,372 of an expected 150M, on both engines. It went into `context.md`, and
the analyst repeated it confidently in every answer.

It was wrong. The table had 150M rows all along. For most of that day both engines silently
returned one partition's count (29,995,372 is exactly the `AUTOMOBILE` segment), with no error.
Then they started returning the right number. The table's snapshot history showed it hadn't
changed since 2024.

What caught it:

- **`notes.md`**: an analyst run got 150M, saw the contradiction with the context, and recorded
  it. Nobody had reviewed the notes yet.
- **`--profile`**: it computed sizes from `$partitions` metadata, a second, independent source,
  and flagged the contradiction.
- **A human check**: `GROUP BY c_mktsegment` on the real rows, plus `"customer$snapshots"`.

What didn't catch it: the eval. It checked that "29,995,372" appeared *somewhere* in the
answer, and it kept passing after the analyst gave the right answer, because 29,995,372 was
still in the table as one segment's size.

Lessons:

1. **One query result isn't verification.** Confirm load-bearing facts from two independent
   sources (a scan and `$partitions`; two engines; metadata and data).
2. **Review `notes.md` promptly.** It had the correction hours before anyone looked.
3. **Make eval checks exact.** Check for the answer itself, not a number that can appear for
   other reasons.
4. **Re-profile periodically** and diff against the context. Sources disagreeing is the signal.

## Rules for good context

- **Facts, not instructions.** Write "`o_orderkey` is 64% NULL", not "be careful with
  orders". The analyst decides how to act on a fact.
- **Say why, briefly.** "`l_shipdate` is varchar *because it's the partition column*". The
  reason helps the analyst generalize. (It applied the `$partitions` trick to `customer` on its
  own after reading the `lineitem` example.)
- **Be precise about scope.** We once wrote "date columns are varchar"; only `l_shipdate` was.
  Over-broad lines produce wrong SQL.
- **Verify before adding.** If you can't point to the query that proves it, it's not ready.
- **Keep it short.** Every line is sent with every question. Today's file is ~1.5k tokens and
  cached, which is cheap. If it grows past a few thousand lines, split it by domain.
- **No secrets, no personal data.** The file goes into every prompt and probably into git.
- **Date and own fragile facts:** "(as of 2026-11; owner: data-eng)". Load issues get fixed,
  layouts change.

## Useful metadata queries

These are all cheap. They read Iceberg metadata, not data:

```sql
-- Partition column(s), per-partition row counts and column stats (quote the name)
SELECT * FROM catalog.schema."table$partitions" LIMIT 1;

-- Exact table size, even for unpartitioned tables
SELECT SUM(row_count) AS rows, SUM(file_count) AS files FROM catalog.schema."table$partitions";

-- Exact counts for a time range, if partitioned by date
SELECT SUM(row_count) FROM catalog.schema."table$partitions"
WHERE partition_col >= '1995-01-01' AND partition_col < '1996-01-01';
```

Column types come from `describe-table`. Estimated row counts for any query come from
`explain-query` (`type: distributed`; look for `outputRowCount`).
