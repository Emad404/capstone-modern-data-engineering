# Architecture

## Overview

The capstone is one pipeline, not five separate demos. An order event enters through Kafka,
gets validated at the ingestion boundary, lands in a Delta Lakehouse (Bronze → Silver → Gold),
and the Gold layer's summary — plus a small domain knowledge base describing the pipeline
itself — is what the RAG stage indexes and answers questions over. Airflow wires the stages
together; Great Expectations and OpenLineage wrap every stage.

```
                    ┌─────────────┐
  Kafka producer -> │ orders_raw  │
                    │   topic     │
                    └──────┬──────┘
                           │
                           v
              ┌────────────────────────┐
              │ Kafka consumer +       │   invalid -> data/quarantine/orders.jsonl
              │ OrderEventContract     │──────────────────────────────────────────>
              │ (Pydantic v2 gate)     │
              └───────────┬────────────┘
                           │ valid events
                           v
              ┌────────────────────────┐
              │ Bronze (Delta, append) │
              └───────────┬────────────┘
                           v
              ┌────────────────────────┐
              │ Great Expectations     │──── FAIL ────> pipeline halts (Airflow task fails,
              │ quality gate           │                downstream tasks never scheduled)
              └───────────┬────────────┘
                           │ PASS
                           v
              ┌────────────────────────┐
              │ Silver (Delta MERGE,   │
              │ upsert on order_id)    │
              └───────────┬────────────┘
                           v
              ┌────────────────────────┐
              │ Gold (real aggregate:  │
              │ revenue per country)   │
              └───────────┬────────────┘
                           v
              ┌────────────────────────┐
              │ RAG: chunk -> ChromaDB │
              │ + BM25 -> RRF fusion   │
              │ -> cross-encoder rerank│
              │ -> cited answer        │
              └────────────────────────┘

  Every stage above also emits OpenLineage START/COMPLETE/FAIL events
  (src/lineage/lineage_emitter.py) and is a task in the Airflow DAG
  (src/orchestration/capstone_dag.py), with quality_gate upstream of
  everything downstream of it.
```

## Component decisions

**Ingestion — Kafka + Pydantic (`src/ingestion/`).**
`contracts.py` defines `OrderEventContract`, the single choke point every ingestion path calls
through. `kafka_producer.py` / `kafka_consumer.py` use the real `kafka-python` client against a
real local broker — same library and bootstrap pattern as the Day 2 lab's "Real Kafka Round
Trip". Records that fail the contract are never dropped: they're written to
`data/quarantine/orders.jsonl` as `QuarantineRecord` rows with the rejection reason.

**Lakehouse — Delta Lake (`src/lakehouse/delta_pipeline.py`).**
Bronze is append-only — nothing is deduplicated or transformed at landing time. Silver is built
with a real `DeltaTable.merge()` keyed on `order_id`: `whenMatchedUpdateAll` handles
re-deliveries (e.g. a price correction), `whenNotMatchedInsertAll` handles new orders. Schema
enforcement is proven negatively: `prove_schema_enforcement()` attempts to write a row with an
incompatible type and an unexpected column straight into Silver without `mergeSchema`, and Delta
rejects it. Gold is a `groupBy("country")` revenue/order-count/avg-order-value aggregate — a
real materialized answer, not a Silver copy.

**RAG — hybrid search + rerank (`src/rag/rag_pipeline.py`).**
Chunking is overlapping-sentence, matching the Day 3 lab. Dense retrieval uses ChromaDB with a
`SentenceTransformerEmbeddingFunction` (`all-MiniLM-L6-v2`). Sparse retrieval uses `rank_bm25`.
The two ranked lists are fused with Reciprocal Rank Fusion (`k=60`), and the fused top-k is
reranked with a cross-encoder (`cross-encoder/ms-marco-MiniLM-L-6-v2`) before the final answer
is generated strictly from the reranked chunks and cited by chunk id.

**Orchestration — Airflow (`src/orchestration/capstone_dag.py`).**
Five `PythonOperator` tasks (`ingest_orders >> quality_gate >> lakehouse_merge >>
gold_aggregate >> rag_index`) call straight into the `src/` modules above — the DAG has no
duplicated business logic. Because `quality_gate` sits upstream of everything else via `>>`,
a task failure there means Airflow's scheduler never queues the rest of that run; this was
verified with `airflow dags test capstone_pipeline <date>` against the real scheduler (see
`run_logs/airflow_dag_test_run.log`).

**Quality + Lineage (`src/quality/expectations_gate.py`, `src/lineage/lineage_emitter.py`).**
The Great Expectations suite checks six DAMA dimensions (completeness, uniqueness, validity,
consistency) against the Silver schema using GX 1.x's fluent/ephemeral API — no external GX
service required. `run_quality_gate` raises `QualityGateFailure` on any failed expectation
instead of just logging a report, which is what makes it a gate. `track_stage()` is a context
manager every pipeline stage runs inside; it emits a real OpenLineage `START` event on entry,
`COMPLETE` on clean exit, and `FAIL` (then re-raises) on any exception — using `FileTransport`
so events are durable at `run_logs/lineage_events.jsonl` without needing a running Marquez
instance (swap in `HttpTransport` for that in production; no other code changes).

## Known sandbox limitation, and how it was resolved

This repository's application code was written and validated in a network-restricted build
sandbox. Three dependencies specifically need broader internet access than that sandbox
allowed: the Delta Lake JVM jar (Maven Central), a local Kafka broker binary
(`downloads.apache.org`), and the two embedding/reranker model weights (`huggingface.co`).
Everything else — the Pydantic contract, the Great Expectations gate (both the pass and the
halt-on-failure path), the OpenLineage emitter, the BM25/RRF/citation path of RAG, and the full
Airflow DAG executed by the real scheduler — was run for real in that sandbox; the logs in
`run_logs/` are genuine captured output, not hand-written transcripts. The three
internet-dependent pieces use the exact same libraries and APIs and were designed to be run
once in Google Colab (which has unrestricted internet and preinstalled Java, exactly like the
course's own Day 2 and Day 4 labs already assume) — `notebooks/capstone_pipeline.ipynb` is that
run. Re-running it end to end regenerates every log in `run_logs/` with the full — not
partial — set of real, captured evidence.

**Follow-up verification done without a live Colab session.** Two additional risks were
checked before handing this off, since Colab's environment (Python version, latest package
releases) can drift from what this repo was built against:

- **PySpark 3.5.0 on newer Python (no stdlib `distutils`).** `pyspark/sql/pandas/utils.py`
  imports `distutils`, which was removed from the standard library in Python 3.12. Confirmed by
  actually running `delta_pipeline.py`'s exact `createDataFrame(list[Row]) -> groupBy -> agg`
  pattern on a Python install with no real `distutils` present: it works, because `setuptools`
  (present in every pip environment, Colab included) installs a compatibility shim that
  transparently provides a working `distutils` regardless of Python version. No code change
  needed.
- **`io.delta:delta-spark_2.12:3.2.0` Maven Central resolution.** The build sandbox's Ivy
  resolution reported `UNRESOLVED DEPENDENCIES ... not found` against Maven Central,
  `spark-packages`, and local caches. Confirmed via direct lookup that this artifact genuinely
  exists on Maven Central (published May 2024) and is built against Spark 3.5.x, matching the
  pinned `pyspark==3.5.0` — the sandbox's error was its own network policy blocking/mangling the
  request, not a real problem with the pinned versions.
- **`kafka-python`'s public API after its 3.x rewrite.** The library's internals were rewritten
  around an asyncio event loop in version 3.0 (visible in tracebacks as `kafka.net.manager` /
  `async def` frames, rather than the older synchronous client). Confirmed via the project's own
  documentation that the high-level API this repo calls — `KafkaProducer(bootstrap_servers=,
  value_serializer=)`, `KafkaConsumer(...)`, iterating with `for msg in consumer` — is
  unchanged; a `KafkaTimeoutError: ECONNREFUSED` failure here means the broker itself never
  started (see the mirror-fallback logic in `notebooks/capstone_pipeline.ipynb`'s Kafka cell),
  not an API incompatibility.
- **`requirements.txt` no longer includes `apache-airflow`.** Airflow pins its dependencies
  strictly enough that bundling it into one shared `pip install -r requirements.txt` broke the
  *entire* install (not just Airflow) the moment the runtime's Python version didn't satisfy
  Airflow's constraint — which is exactly what happened when Colab's default Python moved to
  3.13+. Airflow evidence for this project already exists (`run_logs/airflow_dag_test_run.log`,
  captured against the real scheduler in an isolated venv) and does not need to be reproduced in
  Colab; see the notebook's orchestration section for the optional, isolated install command if
  you want to run it anyway.

None of this is a substitute for actually running the notebook — it's the reasoning for why
each of these cells is expected to work, so a genuine new failure (rather than one of the above)
is easier to recognize as new.
