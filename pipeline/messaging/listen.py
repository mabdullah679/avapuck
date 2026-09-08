"""The trigger-side filter for the control topic.

Runs INSIDE the Airflow triggerer, once per message, as the `apply_function`
of the Kafka MessageQueueTrigger that backs the consumer DAG's Asset
(docs/DECISIONS.md D7). Returning a truthy value wakes the DAG with that value
as the asset event's payload; returning None keeps polling.

Kept deliberately small and dependency-light: this is the one place a
non-conforming or negative verdict is dropped before any DAG runs, and it has
to import cleanly in a process that is not a task.
"""
from __future__ import annotations

import logging

from pipeline.messaging.contracts import ContractError
from pipeline.messaging.envelope import EVENT_RUN_VERDICT, parse

log = logging.getLogger(__name__)


def accept_verdict(*args, **_kwargs) -> dict | None:
    """confluent_kafka.Message -> parsed message dict, or None to keep waiting.

    Three things earn a wake-up, all required: the message parses against its
    contract, it is a run.verdict, and the verdict says proceed. Anything else
    is logged and skipped -- the DAG never sees a message it would then have
    to decide about, which is what "the producer owns the predicate" means.

    WHY THE ODD SIGNATURE. On the asset-watcher path Airflow's serializer
    hands the trigger's EMPTY apply_function_args/kwargs through in its own
    encoded form: the call arrives as f("__var", "list", message, __var={},
    __type="dict") rather than f(message) (seen live, 2026-09-08, three
    times, one shape at a time). So the message is found by what it IS -- the
    one argument with a callable .value() -- never by position.
    """
    message = next((a for a in args if callable(getattr(a, "value", None))), None)
    if message is None:
        log.warning("control topic: apply_function called without a Kafka message (args=%r)",
                    [type(a).__name__ for a in args])
        return None
    value = message.value()
    if value is None:
        return None
    try:
        msg = parse(value)
    except ContractError as e:
        log.warning("control topic: dropping message that violates its contract: %s", e)
        return None
    env = msg["envelope"]
    if env["event_type"] != EVENT_RUN_VERDICT:
        return None
    if not msg["payload"].get("proceed"):
        log.info("control topic: verdict for %s %s says proceed=false; not waking",
                 env["dataset"], env["run_slot"])
        return None
    log.info("control topic: verdict for %s %s says proceed; waking consumer",
             env["dataset"], env["run_slot"])
    return msg
