# Submission Checklist

Use this before submitting the GitHub link for automated grading. Every rubric line item is
listed with exactly where it's satisfied in this repo, plus the physical actions still on you.

## 1. Capstone Rubric (100 pts)

### Ingestion — 20 pts
- [x] Kafka producer + consumer using the real `kafka-python` library — `src/ingestion/kafka_producer.py`, `src/ingestion/kafka_consumer.py`
- [x] Schema validation at the ingestion boundary (Pydantic data contract) — `src/ingestion/contracts.py::OrderEventContract`
- [x] Malformed records routed to a quarantine/dead-letter store with the rejection reason recorded — `kafka_consumer.py::_write_quarantine` → `data/quarantine/orders.jsonl`
- [ ] **Action required:** run `notebooks/capstone_pipeline.ipynb`'s Kafka cells in Colab once, so the real broker round-trip has captured output in your submitted repo (the contract logic itself is already proven offline in `run_logs/ingestion_contract_test.log`)

### Delta Lakehouse — 25 pts
- [x] Bronze/Silver/Gold layers on real Delta Lake — `src/lakehouse/delta_pipeline.py`
- [x] Real `MERGE` (upsert) keyed on a business key (`order_id`) — `merge_into_silver()`
- [x] Demonstrated schema enforcement (a rejected incompatible write) — `prove_schema_enforcement()`
- [x] Gold is a genuine aggregate, not a copy of Silver — `build_gold()` (revenue/order-count/avg per country)
- [ ] **Action required:** run the lakehouse cell in Colab once — blocked in the build sandbox because Delta's JVM jar needs Maven Central (see `docs/ARCHITECTURE.md`); the code itself is complete and unmodified

### RAG Pipeline — 25 pts
- [x] Document chunking — `src/rag/rag_pipeline.py::chunk_documents`
- [x] Embeddings + a real vector store (ChromaDB) — `build_vector_index`, `dense_search`
- [x] Hybrid search: dense + keyword/BM25 fused with RRF — `BM25Index`, `reciprocal_rank_fusion`
- [x] Reranking (cross-encoder) — `rerank()`
- [x] Answers grounded in retrieved context with citations — `answer_with_citations()`
- [x] BM25 + RRF + citation path already proven with real output — `run_logs/rag_bm25_run.log`
- [ ] **Action required:** run the RAG cell in Colab once to capture the dense+hybrid+reranked output (needs `huggingface.co` for `all-MiniLM-L6-v2` and the cross-encoder, unavailable in the build sandbox)

### Orchestration — 15 pts
- [x] Airflow DAG wiring every stage with correct task dependencies — `src/orchestration/capstone_dag.py`
- [x] A failed quality gate halts the pipeline before downstream stages run — `quality_gate >> lakehouse_merge >> gold_aggregate >> rag_index`, verified structurally and by the real scheduler
- [x] Already executed against the real Airflow scheduler with captured output — `run_logs/airflow_dag_test_run.log` (all 5 tasks `SUCCESS`, `DagRun Finished ... state=success`)

### Quality Gate + Lineage — 15 pts
- [x] Great Expectations checks that actually gate the pipeline (raise + halt on failure) — `src/quality/expectations_gate.py`
- [x] Both the pass path and the halt path proven with real output — `run_logs/quality_gate_run.log`
- [x] OpenLineage START/COMPLETE/FAIL events emitted per stage — `src/lineage/lineage_emitter.py`
- [x] Already proven with real output, including a FAIL event — `run_logs/lineage_run.log`, `run_logs/lineage_events.jsonl`

## 2. GitHub & Documentation Requirements

- [x] Clear, comprehensive project description visible from the repo landing page — `README.md`, "What this project does"
- [x] Professional README: idea, prerequisites, install/setup, how to run, expected output — `README.md`
- [x] Proper technical documentation: architecture/pipeline overview, key components, config/env vars — `docs/ARCHITECTURE.md`, `README.md` "Configuration"
- [x] Good Git version-control practices: meaningful, incremental commits (not one bulk upload), sensible structure, `.gitignore` excluding secrets/generated files — see `git log --oneline` (8 staged commits, one per deliverable)
- [x] Training program attribution (program name + cohort/session dates) — `README.md`, "Training program attribution"
- [x] Link to SDAIA Academy on GitHub — `README.md` references `https://github.com/SDAIAAcademy`
- [ ] **Action required:** create the GitHub repo and push (see below) — "not published to GitHub... is not a complete submission"

## 3. Actions still required from you (in order)

1. **Create the GitHub repo.** If you don't already have `capstone-modern-data-engineering` (or your preferred name) under your account, create it on github.com. If your account username differs from `Emad404`, update the two hardcoded URLs (`README.md`'s clone command, `docs/ARCHITECTURE.md`/`notebooks` producer URL) to match.
2. **Push this history as-is.** The 8 commits are already staged with meaningful, incremental messages — don't squash them; the rubric explicitly checks for incremental history over a single bulk upload.
   ```bash
   git remote add origin https://github.com/<your-username>/<repo-name>.git
   git branch -M main
   git push -u origin main
   ```
3. **Open `notebooks/capstone_pipeline.ipynb` in Google Colab** (upload it or open it directly from your pushed GitHub repo via Colab's "Open from GitHub"). Run all cells top to bottom — takes a few minutes, mostly waiting on the Kafka broker (20s) and the two model downloads (~90MB, one-time).
4. **Save the executed notebook back into the repo** (`File > Save a copy in GitHub`, or download the `.ipynb` with outputs and commit it over the current file) so the pushed repo contains a notebook with real captured output for every cell, not just runnable code.
5. **Commit and push the newly-populated `run_logs/`** (the Colab run also regenerates `lakehouse_run.log` and adds Kafka + full RAG logs) with a commit message like:
   ```
   docs(evidence): add Colab-executed run logs for Kafka, Delta, and full hybrid RAG
   ```
6. Double-check the repo landing page renders `README.md` cleanly and that no `data/`, `kafka_2.13-*/`, or `airflow_home/` artifacts got committed by accident (`.gitignore` already covers these, but confirm with `git status` before your final push).
7. Submit the GitHub repo URL.
