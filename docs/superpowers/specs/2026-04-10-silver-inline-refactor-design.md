# Silver Queue Removal + App Cutover — Design

**Date:** 2026-04-10
**Author:** Claude + Bryan
**Status:** Approved for implementation planning
**Related:** Migration from `func-infinitspace-datawarehouse` to `func-infinitspace-etl` (Flex Consumption)

---

## Context

The Nexudus silver layer is currently built as a queue fanout:

- `functions/silver_nexudus.py` — a timer trigger that enqueues 7 messages onto the `silver-sync-tasks` Azure Storage Queue and exits in under 1 second.
- `functions/silver_worker.py` — a queue trigger that processes one entity per invocation.
- `shared/azure_clients/queue_client.py` — the `SilverTaskQueue` client used only by the silver orchestrator.

This architecture was introduced to dodge the 10-minute `functionTimeout` on the previous Consumption plan: running all 7 entity transformations serially in a single invocation routinely exceeded the limit on larger datasets. Splitting the work across 7 queue messages gave each entity its own 10-minute budget and enabled parallel execution.

The data warehouse is now being migrated to a new Function App, `func-infinitspace-etl`, running on Flex Consumption. Flex Consumption supports a `functionTimeout` of up to 60 minutes per invocation, which eliminates the original reason for the queue fanout. The user has decided to simplify the pipeline by inlining the silver transformations back into a single timer-triggered function and retiring the queue worker entirely, then cutting traffic over from the old app to the new one.

App settings (secrets, connection strings, feature flags) have already been set on the new app out of band — this design does **not** cover settings migration.

## Goals

1. Collapse the silver orchestrator + queue worker into a single timer-triggered function that runs all 7 Nexudus silver entity transformations inline.
2. Preserve the resilience property that a failure in one entity does not prevent the other 6 from running that night.
3. Preserve per-entity telemetry in `meta.sync_runs` exactly as it exists today.
4. Remove the silver queue infrastructure (`queue_client.py`, `silver_worker.py`, the `silver-sync-tasks` queue) as cleanly as possible, with no impact on the unrelated CoStar queue used by the Real Estate extractor.
5. Cut production traffic over from the old app (`func-infinitspace-datawarehouse`) to the new app (`func-infinitspace-etl`) in a single, reversible cutover.

## Non-Goals

- Migrating app settings (already done out of band).
- Introducing concurrency between silver entities (`asyncio.gather` etc.). Sequential execution matches the bronze pattern and is simpler to reason about; wall-clock speed is not a constraint at 02:30 UTC.
- Modifying any silver writer (`shared/azure_clients/silver_writer_*.py`, `silver_write_locations.py`). They already take a `sync_run_id` and run standalone — this is the key reason the refactor is small.
- Modifying bronze, AVA, Xero, BambooHR, Reply.io, finance dashboard, admin, or Real Estate functions. None of them touch silver fanout.
- Touching the CoStar queue architecture (`costar_queue_client.py`, `real_estate_costar_worker.py`). It is completely separate and stays untouched.
- Copying app settings, creating new Azure resources, or changing network configuration.

## Design

### Changes

1. **Rewrite `functions/silver_nexudus.py`** to run all 7 Nexudus entity transformations inline, sequentially, inside the single `bronze_to_silver` timer trigger. Each entity is wrapped in its own try/except so a failure in one entity does not prevent the remaining entities from running. After the loop, if any entity failed, the function raises an aggregated `RuntimeError` so the invocation shows red in App Insights.

2. **Delete `functions/silver_worker.py`.** No longer needed.

3. **Delete `shared/azure_clients/queue_client.py`.** Only used by silver; `costar_queue_client.py` is a separate file and stays.

4. **Update `function_app.py`:** remove the `silver_worker_bp` import and its `app.register_functions(silver_worker_bp)` call. Nothing else in `function_app.py` changes.

5. **Update `host.json`:**
   - Raise `functionTimeout` from `00:10:00` to `01:00:00` (the Flex Consumption ceiling).
   - Remove the `extensions.queues.messageEncoding` block — no queue trigger remains in the ETL surface, so the setting is dead config. The CoStar queue (still in use) is configured via its own trigger metadata and does not need this global setting.

6. **Update `CLAUDE.md`:**
   - Function registry table: remove the `silver_entity_worker` row.
   - Runtime topology diagram: remove the fanout-to-queue arrows; silver becomes a single timer-triggered step.
   - Expected log lines: replace the current queue-era entries
     (`Bronze -> Silver orchestrator started`, `Bronze -> Silver: 7 tasks enqueued`, `Silver worker received: ...`, `Silver worker complete: ...`)
     with the new inline shape:
     - `Bronze -> Silver sync started [sync_run_id=<uuid>]`
     - `Silver <entity>: result=<dict>` (one line per entity)
     - `Bronze -> Silver sync complete [sync_run_id=<uuid>]`
     - On per-entity failure: `Silver <entity> failed: <exc>` at ERROR level.
     - On any failure at end: `Silver sync completed with N/7 failures: ...` raised as `RuntimeError`.
   - Repository structure listing: remove `functions/silver_worker.py` and `shared/azure_clients/queue_client.py`.
   - Key Technical Behaviors → Silver Fanout: rewrite to describe inline sequential execution with per-entity try/except.
   - Update `Last updated` to 2026-04-10 with a note about the silver inline refactor.

7. **Cutover:** deploy the refactored code to `func-infinitspace-etl` and stop the old app `func-infinitspace-datawarehouse` with `az functionapp stop`. Stop, not delete — the stop is reversible in seconds and the old app remains as a rollback safety net for ~1–2 weeks before eventual deletion.

### New `silver_nexudus.py` shape

```python
ENTITIES: list[tuple[str, type, Callable[[dict], int]]] = [
    ("locations",         SilverLocationsWriter,         lambda r: r.get("locations", 0) + r.get("location_hours", 0)),
    ("products",          SilverProductsWriter,          lambda r: r.get("products", 0)),
    ("contracts",         SilverContractsWriter,         lambda r: r.get("contracts", 0)),
    ("coworker_invoices", SilverCoworkerInvoicesWriter,  lambda r: r.get("coworker_invoices", 0)),
    ("coworkers",         SilverCoworkersWriter,         lambda r: r.get("coworkers", 0)),
    ("resources",         SilverResourcesWriter,         lambda r: r.get("resources", 0)),
    ("extra_services",    SilverExtraServicesWriter,     lambda r: r.get("extra_services", 0)),
]

@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def bronze_to_silver(timer: func.TimerRequest) -> None:
    sync_run_id = uuid.uuid4()
    logger.info(f"Bronze -> Silver sync started [sync_run_id={sync_run_id}]")

    failures: list[tuple[str, Exception]] = []

    for entity_name, writer_cls, rows_fn in ENTITIES:
        try:
            async with RunTracker(
                "nexudus",
                entity_name,
                "silver",
                triggered_by="timer",
                metadata=str(sync_run_id),
            ) as run:
                writer = writer_cls(sync_run_id)
                result = writer.run()
                run.rows_written = rows_fn(result)
                logger.info(f"Silver {entity_name}: result={result}")
        except Exception as exc:
            logger.error(f"Silver {entity_name} failed: {exc}", exc_info=True)
            failures.append((entity_name, exc))

    if failures:
        summary = ", ".join(f"{name}: {type(exc).__name__}" for name, exc in failures)
        raise RuntimeError(
            f"Silver sync completed with {len(failures)}/{len(ENTITIES)} failures: {summary}"
        )

    logger.info(f"Bronze -> Silver sync complete [sync_run_id={sync_run_id}]")
```

**Key properties:**

- Per-entity telemetry preserved. `RunTracker` is an async context manager that writes a `meta.sync_runs` row on `__aexit__`, setting `status='failed'` and populating `error_message` when an exception is in flight. The outer try/except catches the exception **after** `RunTracker.__aexit__` has already run, so the failed row is durably committed before we append to `failures`.
- Invocation goes red on any failure. The final `raise RuntimeError` ensures App Insights marks the invocation as failed, so alerts fire normally. All successful entities have already committed their writes — they are not rolled back.
- `triggered_by` changes from `"queue"` to `"timer"` in the RunTracker metadata — reflects the new reality. This is the one observable change in `meta.sync_runs` for silver rows.
- Entity order is preserved from the current code. There are no write-time dependencies between silver entities (each writes to its own tables), but keeping the order stable makes logs predictable and matches operator muscle memory.

### `host.json` after changes

```json
{
  "version": "2.0",
  "logging": {
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": true,
        "maxTelemetryItemsPerSecond": 20
      }
    }
  },
  "extensionBundle": {
    "id": "Microsoft.Azure.Functions.ExtensionBundle",
    "version": "[4.*, 5.0.0)"
  },
  "functionTimeout": "01:00:00"
}
```

The `extensions.queues.messageEncoding: "none"` block is removed. The CoStar queue trigger is unaffected because its encoding is handled at the trigger level, not globally.

### `function_app.py` after changes

The file shrinks by two lines (one import, one `register_functions` call). Order and structure of the remaining registrations are preserved so the diff is minimal and reviewable.

## Error Handling

### Per-entity failure

- Caught by the inner try/except.
- `RunTracker` writes a `failed` row to `meta.sync_runs` with the exception message.
- Logged at `ERROR` level with full traceback via `exc_info=True`.
- Appended to the `failures` list.
- Loop continues with the next entity.

### Multiple-entity failure

- All failures are collected.
- Aggregated `RuntimeError` with a summary like `"Silver sync completed with 2/7 failures: coworkers: TimeoutError, resources: ProgrammingError"` is raised at the end.
- App Insights shows the invocation as failed.

### Loss of queue auto-retry

- The queue pattern previously gave 5 automatic retries with exponential backoff per entity. The inline design does not retry within the same invocation.
- **Consequence:** a transient SQL deadlock that the queue would have retried silently within minutes will now wait until the next night's 02:30 run to retry.
- **Accepted.** The user considered and accepted this tradeoff. Silver writers are idempotent MERGEs, so the next night's retry will correctly converge. If transient failures become a real operational problem, the follow-up fix is in-process retry (e.g., 2 attempts with a short backoff) per entity — scoped as a separate change, not part of this design.

## Testing

1. **Local smoke test of writers.** Run the existing scripts `test_locations_silver.py --write`, `test_products_silver.py --write`, `test_contracts_silver.py --write`, `test_extra_services_silver.py --write`. These bypass the orchestrator and exercise the writers directly, confirming that nothing broke from the `queue_client.py` / `silver_worker.py` deletions (e.g., no stray imports).
2. **Unit test for orchestrator failure handling** (optional but recommended). A small test that injects a failing writer as one of the 7 entities and confirms:
   - The loop continues through the remaining entities.
   - A `RuntimeError` is raised at the end summarizing the failures.
   - `RunTracker` writes the failed row before the outer catch.
   Can be added during implementation or deferred — the risk is low because the loop is ~30 lines.
3. **Production first-run verification.** After cutover, check `meta.sync_runs` the next morning for 7 silver rows with matching `metadata` UUID, all `status='succeeded'`, all `triggered_by='timer'`.

## Cutover Plan

Steps are ordered with rollback available at every point up to step 8.

1. **Create branch `refactor/silver-inline`.** All code changes happen here.
2. **Apply the refactor.** Rewrite `silver_nexudus.py`, delete `silver_worker.py` and `queue_client.py`, update `function_app.py`, update `host.json`, update `CLAUDE.md`. Commit.
3. **Run local smoke tests** as described in the Testing section.
4. **Pre-flight: verify `ENABLE_ETL_FUNCTIONS=1` on `func-infinitspace-etl`.** One command:
   ```powershell
   az functionapp config appsettings list `
     -g infinitspace-prod-northeurope-data-rg `
     -n func-infinitspace-etl `
     --query "[?name=='ENABLE_ETL_FUNCTIONS'].value" -o tsv
   ```
   If it returns empty or `0`, set it to `1` before merging.
5. **Merge `refactor/silver-inline` to `main` outside the 02:00–05:00 UTC sync window** (any daytime CET is fine). GitHub Actions deploys to `func-infinitspace-etl` automatically. During the ~2-minute deploy both apps are technically enabled; merging outside the sync window avoids the double-run risk entirely.
6. **Verify the new app's function list in the Azure portal.** All ETL functions (`nexudus_to_bronze`, `bronze_to_silver`, `refresh_ava_availability`, `xero_invoice_sync`, `bamboohr_sync`, `finance_dashboard_refresh`, `replyio_sync`) should be listed. `silver_entity_worker` should NOT be listed — its absence is a positive signal that the refactor landed. If zero functions appear, debug from the Log stream before proceeding to step 7.
7. **Stop the old app:**
   ```powershell
   az functionapp stop -g infinitspace-prod-northeurope-data-rg -n func-infinitspace-datawarehouse
   ```
   Reversible with `az functionapp start`. Keep the app stopped for 1–2 weeks as a rollback safety net.
8. **Monitor the first real run.** Next morning (02:00 UTC onwards), verify in `meta.sync_runs`:
   - 7 silver rows with matching `metadata` UUID, all `succeeded`, all `triggered_by='timer'`.
   - Bronze, AVA, Xero, BambooHR, finance dashboard, Reply.io runs all show up.
   - No runs attributed to the old app.
9. **After 1–2 weeks of clean runs:**
   - Delete the old app: `az functionapp delete -g infinitspace-prod-northeurope-data-rg -n func-infinitspace-datawarehouse`.
   - Delete the `silver-sync-tasks` and `silver-sync-tasks-poison` queues from storage account `staccinfinitspaceprod001`.
   - Delete `oldapp-settings.json` from the local working directory (contains plaintext secrets).

### Rollback

At any point before step 9:

- **Silver run fails on the first night (or earlier verification fails):**
  - `az functionapp start -g infinitspace-prod-northeurope-data-rg -n func-infinitspace-datawarehouse`
  - `az functionapp stop -g infinitspace-prod-northeurope-data-rg -n func-infinitspace-etl`
  - Old app resumes its normal cron the next night. Debug on the new app at leisure.
- **Something else misbehaves** (e.g., Xero or BambooHR fails on the new app): same move. The old app has working code for all ETL functions, so a full rollback is always safe.
- **The refactor itself needs to be reverted in code:** `git revert` the merge commit on main, GitHub Actions redeploys the previous shape. The old app remains the source of truth until rollback is complete.

## Risks and Mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Silver sync exceeds 60-minute Flex Consumption ceiling | Low | Silver is SQL-only (no API calls), strictly faster per entity than bronze, which already runs 7 entities serially without issue. If this ever becomes a real risk, the fix is windowed incremental processing for the slow entity, not concurrency. |
| Transient SQL failures no longer auto-retry within the night | Medium | Accepted tradeoff. MERGE idempotency guarantees next-night convergence. Follow-up: in-process retry if operational pain appears. |
| Merge-to-main deploy races the 02:30 cron | Low | Merge outside 02:00–05:00 UTC. ~2-minute deploy window makes this a non-issue for any daytime merge. |
| `ENABLE_ETL_FUNCTIONS` not set on new app, repeat of the zero-functions deploy | Low | Explicit pre-flight check in step 4 of the cutover. |
| Deletion of `queue_client.py` breaks a stray import somewhere | Low | `grep` for `queue_client` / `SilverTaskQueue` in implementation step. Only silver references came up in exploration; `costar_queue_client.py` is a separate file. |
| Loss of visibility into per-entity runtimes (previously each worker was a separate invocation in App Insights) | Low | Per-entity `meta.sync_runs` rows still exist with `started_at` / `finished_at` timestamps. App Insights will show one invocation for silver instead of seven, but per-entity timings remain queryable via SQL. |

## Open Questions

None. All clarifying questions have been answered during brainstorming:

- Architecture: inline sequential (not concurrent, not queue-kept).
- Error handling: best-effort per-entity with aggregated error at end.
- Cutover: stop the old app (not delete), refactor + cutover together.
- App settings: already set on new app by user out of band.

## Success Criteria

1. `functions/silver_worker.py` and `shared/azure_clients/queue_client.py` are deleted from the repository.
2. `functions/silver_nexudus.py` contains the inline sequential orchestrator.
3. `host.json` has `functionTimeout: 01:00:00` and no `extensions.queues` block.
4. `function_app.py` no longer imports or registers `silver_worker_bp`.
5. `CLAUDE.md` accurately reflects the new architecture with an updated `Last updated` date.
6. After cutover, `meta.sync_runs` shows all 7 silver entities succeeding nightly on `func-infinitspace-etl` with `triggered_by='timer'`.
7. `func-infinitspace-datawarehouse` is stopped (not deleted) and receives no invocations.
8. No regression in bronze, AVA, Xero, BambooHR, finance dashboard, or Reply.io.
