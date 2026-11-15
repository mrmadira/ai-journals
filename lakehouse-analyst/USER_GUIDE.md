# Lakehouse Analyst: user guide

Ask questions about data in the watsonx.data lakehouse in plain English. The analyst finds the
right tables, writes the SQL, checks its work, and shows you how it got the answer.

```bash
ask-lake "Which region has the most suppliers in tpchdbiceberg?"   # one question
ask-lake                                                           # a conversation, with follow-ups
```

Inside Claude Code: `@ask-lake <your question>`, or `! ask-lake "<your question>"`.

> In zsh, put questions that contain `?` in quotes.

## What the analyst does for you

- **It knows the official datasets and known data problems.** You don't need to explain which
  copy of a dataset to use or which tables are incomplete.
- **It asks when your question could mean more than one thing**, instead of guessing.
- **It protects the shared cluster.** Queries that would scan too much data are refused
  automatically.
- **It shows its work, every time**: the answer, the SQL it ran, what it checked, and the caveats.
- **It leaves a trail.** Every answer ends with a `ref` code linked to a full record of how it was
  produced.

## How to get good answers

1. **Be specific when it matters.** You don't *have* to be, because the analyst will ask. But
   naming the dataset, time range and metric up front skips the back-and-forth:
   - Instead of: "revenue by region"
   - Try: "net revenue by region for 1995 in `tpchdbiceberg`"
2. **Read the Checks and Caveats sections, not just the answer.** That's where it tells you what it
   couldn't verify, what it estimated and what it assumed. If Checks says "didn't verify X", the
   number is less certain.
3. **Look at the SQL before you reuse a number elsewhere.** Make sure the filters, joins and table
   are what you expected. It takes 30 seconds and catches most misunderstandings.
4. **Push back when something looks off.** In a conversation (`ask-lake` with no question), try:
   - "Do these totals match the table's row count?"
   - "What if you exclude X?"
   - "Why that table and not Y?"

   It's good at checking itself when you ask.
5. **If a query is refused as too large, narrow the question.** Add a time range, pick a smaller
   slice, or accept an estimate. Don't try to get around the refusal: the limit protects everyone
   sharing the engine.
6. **Rely on it for exploration and first drafts; double-check anything that matters.** It's
   excellent for "where is the data and roughly what does it say" and for a first pass at the SQL.
   Anything going into a decision, a customer deliverable or an executive deck still gets a
   human check.

## Warning signs

Slow down and double-check when you see:

- ⚠️, "estimate", "couldn't verify" or "assumed" in the answer
- a dataset in **How I got it** that you didn't expect
- the answer mentioning a **possible prompt-injection attempt**. Report it with the ref
- **different numbers** for the same question on two runs: ask which is right, and report it

## When something goes wrong

| You see | Do this |
|---|---|
| "watsonx.data rejected the credentials" | Your token expired: get a fresh one and `export WXD_AUTH=...` |
| "Bedrock rejected the credentials" | Your AWS token expired: refresh it |
| A clarifying question instead of an answer | Answer it, or ask again with the detail included |
| "Query refused by the scan budget" | Narrow the question: time range, filter, or a smaller table |
| "Gave up after 25 steps" | The question is probably too broad: split it into smaller questions |
| An answer you think is wrong | Report it (below) |

## Reporting a problem

Send the agent's owner:

1. the **ref** from the bottom of the answer (for example `ref a3f9c2`)
2. what you asked
3. what you think the right answer is, and why

The ref lets them see exactly which tables and SQL produced the answer. Without it, it's
guesswork. Wrong answers you report become test cases, so the same mistake doesn't come back.

## What you don't need to do

- Explain which copy of a dataset is the official one, or quirks like dates stored as text.
  The analyst's context file covers that.
- Worry about running a huge query by accident. The scan budget blocks it.
- Copy answers into a document to keep a record. Every answer is logged under its ref.
