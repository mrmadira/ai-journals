"""Run the eval set: real questions with known answers. THIS CALLS THE MODEL (costs tokens).

    uv run python evals/run_evals.py            # all questions
    uv run python evals/run_evals.py tpch-tables nba-most-improved   # a subset

Re-run after every prompt, context or model change, and compare pass rates before shipping.
Checks are deliberately simple (substring matches on the final answer): good enough to catch
regressions, cheap to maintain. Each run is appended to evals/results.jsonl.
"""
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
AGENT = HERE.parent / "agent.py"


def grade(case: dict, output: str) -> list[str]:
    answer = output.split("📊 Answer", 1)[-1] if "📊 Answer" in output else output
    problems = [f"missing {s!r}" for s in case.get("must_contain", []) if s not in answer]
    if case.get("must_contain_any") and not any(s in answer for s in case["must_contain_any"]):
        problems.append(f"none of {case['must_contain_any']}")
    problems += [f"should not contain {s!r}" for s in case.get("must_not_contain", []) if s in answer]
    if "max_table_rows" in case:  # Markdown table rows in the answer (header + separator excluded)
        rows = sum(1 for ln in answer.splitlines() if ln.strip().startswith("|")) - 2
        if rows > case["max_table_rows"]:
            problems.append(f"{rows} table rows (max {case['max_table_rows']})")
    return problems


def main() -> int:
    cases = json.loads((HERE / "questions.json").read_text())
    if len(sys.argv) > 1:
        cases = [c for c in cases if c["id"] in sys.argv[1:]]
    env = {**os.environ, "NO_COLOR": "1"}
    results = []
    for case in cases:
        start = time.time()
        proc = subprocess.run([sys.executable, str(AGENT), case["question"]], capture_output=True,
                              text=True, env=env, timeout=900)
        output = proc.stdout + proc.stderr
        problems = grade(case, output) if proc.returncode == 0 else [f"exit code {proc.returncode}"]
        ref = next((ln.split()[2] for ln in output.splitlines() if ln.startswith("— ref ")), None)
        results.append({"id": case["id"], "pass": not problems, "problems": problems,
                        "seconds": round(time.time() - start), "ref": ref})
        print(f"{'PASS' if not problems else 'FAIL'}  {case['id']:22} {results[-1]['seconds']:>4}s  "
              f"ref {ref}  {'; '.join(problems)}")
    passed = sum(r["pass"] for r in results)
    print(f"\n{passed}/{len(results)} passed")
    with (HERE / "results.jsonl").open("a") as f:
        f.write(json.dumps({"ts": dt.datetime.now().isoformat(timespec="seconds"),
                            "passed": passed, "total": len(results), "results": results}) + "\n")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
