# WPS POC — Working Agreement

Read `POC-PROMPT.md` for the full brief. This file is the short version that
governs how work proceeds, especially unattended.

## What this is

A proof of concept for a config-driven translation layer that absorbs four
mutually incompatible internal-service dialects and emits one standardized
quarterly performance model. Feeds dashboards behind billion-dollar financial,
architecture, and org decisions at a fintech. **This is the POC, not the
production build.**

## Non-negotiables

1. **Mappings live in configuration, never in code.** If a mapping is about to
   be hardcoded, stop and fix the config surface. Hardcoding moves the
   fragmentation one layer up into code where the business cannot audit it —
   that is the thesis failing, even if everything runs.
2. **Zero cost.** Local or always-free only. No paid cloud calls. Cloud CLIs
   are denied in `.claude/settings.json` on purpose.
3. **No real data.** The corpus is 100% synthetic.
4. **`TRUST-BOUNDARY.md` is a first-class deliverable**, written as stubs are
   created — not reconstructed at the end. The AVP presents this work onward;
   anything overclaimed becomes their exposure in a room we are not in.

## Definition of done

The happy path runs end to end and the trust boundary is honest. That is the
whole bar. **Further iteration is scope creep** — this matters because there
is no deadline, and an unbounded loop degrades the work rather than improving
it: more surface, more assumptions, a longer untrustable-list.

## Unattended-loop rules

- Long-running processes (Airflow, Spark) go to background; poll with Monitor.
- Blocked on something? Write it into `TRUST-BOUNDARY.md` and route around it.
  Never spin on one obstacle — the corpus generator, SSOT schema, mapping
  config, and grading harness are all plain Python and need no Spark.
- Report honestly. A run that skipped a step is not a green run.
- Do not silently resolve the open questions in `POC-PROMPT.md` — argue them
  explicitly and record the decision.

## Environment

Python 3.14.6, Java 26, Docker 29.5.3 available. Python 3.14 is likely ahead
of PySpark/delta-spark wheel support — see `.claude/skills/wps-run/SKILL.md`
for the fallback ladder.
