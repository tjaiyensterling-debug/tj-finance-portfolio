# Airflow + Astronomer Cosmos orchestration

Production-style orchestration for the [`dbt_finance_variance`](../dbt_finance_variance) project:
**Cosmos renders the dbt project into native Airflow tasks** — every seed, model, and test becomes its
own task with isolated retries — then hands the fresh mart to the
[Claude variance agent](../claude_finance_agent) for narrative drafting.

```
finance_variance_pipeline (DAG, weekday 06:00)
└── dbt_finance_variance (Cosmos DbtTaskGroup)
    ├── raw_financials_seed
    ├── stg_financials  (+ schema tests)
    └── fct_account_variance  (+ schema & relationship tests)
        └── draft_variance_narrative  (DuckDB read → Claude agent if API key set)
```

## Why Cosmos instead of `dbt build` in a BashOperator

| Concern | BashOperator `dbt build` | Cosmos task group |
|---|---|---|
| Observability | One opaque log | Per-model logs & lineage in the Airflow UI |
| Failure recovery | Re-run the whole project | Retry the single failed model |
| Test visibility | Buried in stdout | Tests are first-class task failures |
| Scheduling granularity | All-or-nothing | Model-level |

Trade-off: Cosmos loads the dbt project at DAG-parse time. Negligible here; at enterprise scale
(thousands of models across business domains) you would split DAGs by dbt tag and bridge
cross-domain dependencies with `ExternalTaskSensor`s instead of one monolithic DAG.

## Design decisions

- **DuckDB + committed example profile** — runnable anywhere, no warehouse or secrets.
- **The AI step degrades gracefully**: without `ANTHROPIC_API_KEY` the hand-off task still
  succeeds and logs the deterministic variance summary. The pipeline's correctness never
  depends on the LLM — same guardrail philosophy as the agent itself.
- **Retries (2, 5-min delay) at the task level**, where Cosmos makes them actually useful:
  a flaky model retries alone instead of forcing a full `dbt build`.

## Run it locally (Linux/macOS/WSL2)

```bash
pip install -r requirements.txt
export AIRFLOW_HOME=$(mktemp -d)
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false
airflow standalone        # UI at localhost:8080 → trigger finance_variance_pipeline
```

CI validates the DAG on every push (import test via `DagBag` — see
[`.github/workflows/dbt-ci.yml`](../.github/workflows/dbt-ci.yml)), so a broken DAG can't land on `main`.
