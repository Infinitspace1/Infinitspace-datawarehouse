"""Tests for the run-health verdict and the graded scrape-retry loop (no network).

The numbers in the fixtures are the real ones measured on 2026-08-17/18 Apify
runs, so a threshold change that would have let the collapsed London week pass
as healthy fails here.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from functions import location_scraper as ls
from shared.location_scraper import run_health
from shared.location_scraper.activities import run_health as run_health_act


# --- real log excerpts ------------------------------------------------------

def _log(attempted: int, lost: int) -> str:
    """memo23 run log with `attempted` candidates, `lost` of them dropped."""
    lines = []
    for i in range(attempted):
        if i < lost:
            lines.append(
                f"[internal] listing {i}: mobile API 403 and unblocker fallback unavailable/failed."
            )
        else:
            lines.append(
                f"[internal] listing {i}: mobile API 403 (App Check) -> recovered via website unblocker (full detail, charged)."
            )
    return "\n".join(lines)


class TestParseDetailHealth:
    def test_counts_lost_and_recovered(self):
        health = run_health.parse_detail_health(_log(48, 34))
        assert health["attempted"] == 48
        assert health["lost"] == 34
        assert health["recovered"] == 14
        assert health["loss_rate"] == pytest.approx(34 / 48)

    def test_no_markers_leaves_loss_rate_unknown(self):
        health = run_health.parse_detail_health("INFO  Done: 25 records pushed.")
        assert health["loss_rate"] is None
        assert health["attempted"] == 0

    def test_empty_log_is_safe(self):
        assert run_health.parse_detail_health(None)["loss_rate"] is None


class TestAssessRun:
    def test_healthy_run_passes(self):
        # London 2026-07-31 10:36 UTC: 395 candidates, 21 lost, 374 items.
        verdict = run_health.assess_run(
            raw_item_count=374,
            baseline_counts=[236, 134, 47],
            detail_health=run_health.parse_detail_health(_log(395, 21)),
            used_enumeration=True,
            enumerated_url_count=395,
        )
        assert verdict["ok"] is True
        assert verdict["status"] == "ok"
        assert verdict["reason"] == ""

    def test_collapsed_week_is_degraded(self):
        # London 2026-08-17 (weekly-london-2026-W34): enumeration empty, actor
        # dropped 34/48, 14 items against a 236-item baseline.
        verdict = run_health.assess_run(
            raw_item_count=14,
            baseline_counts=[236, 134, 47],
            detail_health=run_health.parse_detail_health(_log(48, 34)),
            used_enumeration=True,
            enumerated_url_count=0,
        )
        assert verdict["ok"] is False
        assert verdict["status"] == "degraded"
        assert "enumeration returned no URLs" in verdict["reason"]
        assert "71%" in verdict["reason"]
        assert "14 items vs a 236-item baseline" in verdict["reason"]

    def test_small_city_at_its_own_scale_passes(self):
        # cupertino returns 3 listings every week -- must not read as a collapse.
        verdict = run_health.assess_run(
            raw_item_count=3,
            baseline_counts=[3, 3, 3],
            detail_health=run_health.parse_detail_health(_log(3, 0)),
        )
        assert verdict["ok"] is True

    def test_detail_loss_alone_degrades(self):
        # New York 2026-08-17: 24% loss, no baseline recorded yet.
        verdict = run_health.assess_run(
            raw_item_count=28,
            baseline_counts=[],
            detail_health=run_health.parse_detail_health(_log(37, 9)),
        )
        assert verdict["ok"] is False
        assert "dropped 9/37" in verdict["reason"]

    def test_volume_alone_degrades(self):
        verdict = run_health.assess_run(
            raw_item_count=28,
            baseline_counts=[456, 458],
            detail_health={"loss_rate": None},
        )
        assert verdict["ok"] is False
        assert "458-item baseline" in verdict["reason"]

    def test_zero_items_degrades(self):
        verdict = run_health.assess_run(raw_item_count=0, baseline_counts=[])
        assert verdict["ok"] is False
        assert "no items returned" in verdict["reason"]

    def test_exhausted_provider_quota_is_not_worth_retrying(self):
        # 2026-08-18: after a 317-listing London run drained the actor's shared
        # unblocker budget, every city started afterwards logged this and
        # returned nothing — including the loopnet.com SEARCH stage.
        log = (
            "[unblocker] scrape.do returned 502 (transient route failure) — retrying once.\n"
            "[unblocker] scrapingbee is quota-throttled (HTTP 401) — dropping it for the rest of this run.\n"
            "[internal-handler] done. Total items saved: 0."
        )
        verdict = run_health.assess_run(
            raw_item_count=0,
            baseline_counts=[456],
            detail_health=run_health.parse_detail_health(log),
        )
        assert verdict["ok"] is False
        assert verdict["retry_useless"] is True
        assert "unblocker quota is exhausted" in verdict["reason"]

    def test_a_good_run_that_drains_the_quota_is_still_good(self):
        # London 2026-08-18 09:32 delivered 267 items / 180 buildings and hit
        # the 401 on its way out. Draining the budget is a retry signal, not a
        # quality one — grading this run degraded would be a false alarm.
        log = _log(317, 50) + "\n[unblocker] scrapingbee is quota-throttled (HTTP 401)"
        verdict = run_health.assess_run(
            raw_item_count=267,
            baseline_counts=[236, 134],
            detail_health=run_health.parse_detail_health(log),
            used_enumeration=True,
            enumerated_url_count=317,
        )
        assert verdict["ok"] is True
        assert verdict["reason"] == ""
        assert verdict["retry_useless"] is True

    def test_a_normal_degraded_run_is_still_worth_retrying(self):
        verdict = run_health.assess_run(
            raw_item_count=14,
            baseline_counts=[236],
            detail_health=run_health.parse_detail_health(_log(48, 34)),
        )
        assert verdict["ok"] is False
        assert verdict["retry_useless"] is False

    def test_no_signal_at_all_passes(self):
        # First ever run of a city, source with no detail markers.
        verdict = run_health.assess_run(raw_item_count=42, baseline_counts=[])
        assert verdict["ok"] is True

    def test_thresholds_are_env_tunable(self, monkeypatch):
        detail = run_health.parse_detail_health(_log(37, 9))  # 24% loss
        monkeypatch.setenv("LOCATION_SCRAPER_DETAIL_LOSS_THRESHOLD", "0.5")
        assert run_health.assess_run(raw_item_count=28, detail_health=detail)["ok"] is True
        monkeypatch.setenv("LOCATION_SCRAPER_DETAIL_LOSS_THRESHOLD", "0.1")
        assert run_health.assess_run(raw_item_count=28, detail_health=detail)["ok"] is False

    def test_baseline_uses_the_best_recent_run(self):
        # A degraded week must not lower the bar for the next one.
        verdict = run_health.assess_run(raw_item_count=20, baseline_counts=[14, 38, 236])
        assert verdict["baseline"] == 236
        assert verdict["ok"] is False


class TestAssessRunHealthActivity:
    def test_reads_the_apify_log_and_the_baseline(self):
        with patch.object(run_health_act.apify_client, "get_run_log", return_value=_log(48, 34)), \
             patch.object(run_health_act, "recent_raw_item_counts", return_value=[236, 134]):
            verdict = run_health_act.assess_run_health(
                {
                    "run_id": "weekly-london-2026-W34",
                    "city": "london",
                    "source": "loopnet",
                    "apify_run_id": "OCUqSw4swtHE420TG",
                    "raw_item_count": 14,
                    "used_enumeration": True,
                    "enumerated_url_count": 0,
                }
            )
        assert verdict["status"] == "degraded"
        assert verdict["detail_lost"] == 34

    def test_baseline_query_failure_is_not_fatal(self):
        class Boom:
            def execute_query(self, *_args, **_kwargs):
                raise RuntimeError("SQL down")

        with patch.object(run_health_act, "get_sql_client", return_value=Boom()):
            assert run_health_act.recent_raw_item_counts("london", "loopnet", "run") == []


# --- the retry loop ---------------------------------------------------------

class FakeContext:
    """Minimal DurableOrchestrationContext for driving _scrape_with_retries."""

    def __init__(self):
        self.is_replaying = False
        self.current_utc_datetime = datetime(2026, 8, 18, 8, 0, tzinfo=timezone.utc)
        self.timers: list = []

    def call_activity(self, name, payload=None):
        return ("activity", name, payload)

    def create_timer(self, deadline):
        return ("timer", deadline)


class Harness:
    """Drives the generator, answering each activity from a scripted queue."""

    def __init__(self, **queues):
        self.queues = {name: list(values) for name, values in queues.items()}
        self.calls: list[tuple[str, dict]] = []
        self.context = FakeContext()

    def _answer(self, name, payload):
        self.calls.append((name, payload))
        queue = self.queues.get(name)
        if not queue:
            raise AssertionError(f"unexpected call to {name}")
        return queue.pop(0) if len(queue) > 1 else queue[0]

    def run(self, **kwargs):
        gen = ls._scrape_with_retries(self.context, **kwargs)
        sent = None
        try:
            while True:
                item = gen.send(sent)
                if item[0] == "timer":
                    self.context.timers.append(item[1])
                    sent = None
                else:
                    sent = self._answer(item[1], item[2])
        except StopIteration as stop:
            return stop.value

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]

    def payloads(self, name) -> list[dict]:
        return [payload for called, payload in self.calls if called == name]


US_CONFIG = {"actor": "loopnet", "city": "new york", "country_code": "us", "start_url": "https://x"}
UK_CONFIG = {"actor": "loopnet", "city": "london", "country_code": "gb", "start_url": "https://x"}
FINISHED = {"finished": True, "succeeded": True, "status": "SUCCEEDED"}
OK = {"ok": True, "status": "ok", "reason": ""}
BAD = {"ok": False, "status": "degraded", "reason": "actor dropped most listings"}


class TestScrapeWithRetries:
    def test_healthy_run_scrapes_once(self):
        h = Harness(
            ls_start_apify_run=[{"run_id": "r1", "dataset_id": "d1"}],
            ls_check_apify_run=[FINISHED],
            ls_fetch_and_persist_raw=[{"item_count": 456}],
            ls_assess_run_health=[OK],
        )
        config, raw_count, verdict = h.run(
            city="new york", run_id="weekly-new-york-2026-W35", source_config=US_CONFIG
        )
        assert h.names().count("ls_start_apify_run") == 1
        assert h.context.timers == []
        assert raw_count == 456
        assert verdict["ok"] is True
        assert config is US_CONFIG

    def test_degraded_attempt_is_retried_after_a_delay(self):
        h = Harness(
            ls_start_apify_run=[{"run_id": "r1", "dataset_id": "d1"}, {"run_id": "r2", "dataset_id": "d2"}],
            ls_check_apify_run=[FINISHED],
            ls_fetch_and_persist_raw=[{"item_count": 14}, {"item_count": 400}],
            ls_assess_run_health=[BAD, OK],
        )
        _, raw_count, verdict = h.run(
            city="london", run_id="weekly-london-2026-W35", source_config=US_CONFIG
        )
        assert h.names().count("ls_start_apify_run") == 2
        assert len(h.context.timers) == 1  # the retry delay
        assert raw_count == 400
        assert verdict["ok"] is True

    def test_keeps_the_best_attempt_when_every_retry_is_worse(self):
        h = Harness(
            ls_start_apify_run=[
                {"run_id": "r1", "dataset_id": "d1"},
                {"run_id": "r2", "dataset_id": "d2"},
                {"run_id": "r3", "dataset_id": "d3"},
            ],
            ls_check_apify_run=[FINISHED],
            # attempt 1 is the best; the retries come back weaker
            ls_fetch_and_persist_raw=[{"item_count": 120}, {"item_count": 30}, {"item_count": 11}, {"item_count": 120}],
            ls_assess_run_health=[BAD],
        )
        _, raw_count, verdict = h.run(
            city="london", run_id="weekly-london-2026-W35", source_config=US_CONFIG
        )
        assert h.names().count("ls_start_apify_run") == 3  # LOCATION_SCRAPER_MAX_ATTEMPTS
        # the winning dataset is re-persisted so bronze holds the best attempt
        assert h.payloads("ls_fetch_and_persist_raw")[-1]["dataset_id"] == "d1"
        assert raw_count == 120
        assert verdict["ok"] is False  # still reported as degraded

    def test_failed_apify_run_is_retried(self):
        h = Harness(
            ls_start_apify_run=[{"run_id": "r1", "dataset_id": "d1"}, {"run_id": "r2", "dataset_id": "d2"}],
            ls_check_apify_run=[
                {"finished": True, "succeeded": False, "status": "TIMED-OUT"},
                FINISHED,
            ],
            ls_fetch_and_persist_raw=[{"item_count": 456}],
            ls_assess_run_health=[OK],
        )
        _, raw_count, _ = h.run(
            city="new york", run_id="weekly-new-york-2026-W35", source_config=US_CONFIG
        )
        assert h.names().count("ls_start_apify_run") == 2
        assert raw_count == 456

    def test_every_attempt_failing_raises(self):
        h = Harness(
            ls_start_apify_run=[{"run_id": "r1", "dataset_id": "d1"}],
            ls_check_apify_run=[{"finished": True, "succeeded": False, "status": "FAILED"}],
            ls_assess_run_health=[{"ok": False, "reason": "no items returned", "retry_useless": False}],
        )
        with pytest.raises(RuntimeError, match="Apify run failed"):
            h.run(city="new york", run_id="weekly-new-york-2026-W35", source_config=US_CONFIG)
        assert h.names().count("ls_start_apify_run") == 3

    def test_exhausted_quota_stops_after_one_failed_attempt(self):
        h = Harness(
            ls_start_apify_run=[{"run_id": "r1", "dataset_id": "d1"}],
            ls_check_apify_run=[{"finished": True, "succeeded": False, "status": "FAILED"}],
            ls_assess_run_health=[
                {"ok": False, "reason": "the actor's unblocker quota is exhausted (HTTP 401)", "retry_useless": True}
            ],
        )
        with pytest.raises(RuntimeError, match="quota is exhausted"):
            h.run(city="new york", run_id="weekly-new-york-2026-W35", source_config=US_CONFIG)
        # one attempt, not three — the next two would fail identically
        assert h.names().count("ls_start_apify_run") == 1

    def test_exhausted_quota_stops_a_succeeded_but_empty_run(self):
        h = Harness(
            ls_start_apify_run=[{"run_id": "r1", "dataset_id": "d1"}],
            ls_check_apify_run=[FINISHED],
            ls_fetch_and_persist_raw=[{"item_count": 0}],
            ls_assess_run_health=[{"ok": False, "reason": "quota", "retry_useless": True}],
        )
        _, raw_count, verdict = h.run(
            city="austin", run_id="weekly-austin-2026-W35", source_config=US_CONFIG
        )
        assert h.names().count("ls_start_apify_run") == 1
        assert h.context.timers == []
        assert raw_count == 0
        assert verdict["ok"] is False

    def test_empty_enumeration_is_retried_before_the_fallback(self, monkeypatch):
        # The enumeration path is off by default since 2026-08-19; it stays
        # supported behind the env override, so opt in to exercise it.
        monkeypatch.setenv("LOOPNET_ENUMERATION_COUNTRIES", "gb,ca")
        h = Harness(
            ls_enumerate_loopnet_urls=[
                {"listing_urls": [], "count": 0},
                {"listing_urls": ["https://loopnet/1", "https://loopnet/2"], "count": 2},
            ],
            ls_start_apify_run=[{"run_id": "r1", "dataset_id": "d1"}],
            ls_check_apify_run=[FINISHED],
            ls_fetch_and_persist_raw=[{"item_count": 390}],
            ls_assess_run_health=[OK],
        )
        h.run(city="london", run_id="weekly-london-2026-W35", source_config=UK_CONFIG)
        assert h.names().count("ls_enumerate_loopnet_urls") == 2
        assert len(h.context.timers) == 1  # the enumeration retry delay
        # the scrape runs on the enumerated URLs, not the broad search
        assert h.payloads("ls_start_apify_run")[0]["listing_urls"] == [
            "https://loopnet/1",
            "https://loopnet/2",
        ]

    def test_enumeration_that_never_returns_urls_falls_back_and_degrades(self, monkeypatch):
        monkeypatch.setenv("LOOPNET_ENUMERATION_COUNTRIES", "gb,ca")
        h = Harness(
            ls_enumerate_loopnet_urls=[{"listing_urls": [], "count": 0}],
            ls_start_apify_run=[{"run_id": "r1", "dataset_id": "d1"}],
            ls_check_apify_run=[FINISHED],
            ls_fetch_and_persist_raw=[{"item_count": 14}],
            ls_assess_run_health=[BAD],
        )
        h.run(city="london", run_id="weekly-london-2026-W35", source_config=UK_CONFIG)
        # 3 enumeration attempts per scrape attempt, 3 scrape attempts
        assert h.names().count("ls_enumerate_loopnet_urls") == 9
        # the broad search still runs -- an empty enumeration is not fatal
        assert "listing_urls" not in h.payloads("ls_start_apify_run")[0]
        health_payload = h.payloads("ls_assess_run_health")[0]
        assert health_payload["used_enumeration"] is True
        assert health_payload["enumerated_url_count"] == 0

    def test_uk_no_longer_enumerates_by_default(self):
        """The gb/ca enumeration is off by default (2026-08-19).

        It forced memo23's per-listing detail fetch — the 403-ing stage behind
        the throttled paid unblocker that left London at 11 buildings — while
        the paginated search reaches the same market through the free mobile
        API. See LOOPNET_ENUMERATION_COUNTRIES.
        """
        h = Harness(
            ls_start_apify_run=[{"run_id": "r1", "dataset_id": "d1"}],
            ls_check_apify_run=[FINISHED],
            ls_fetch_and_persist_raw=[{"item_count": 320}],
            ls_assess_run_health=[OK],
        )
        h.run(city="london", run_id="weekly-london-2026-W35", source_config=UK_CONFIG)
        assert "ls_enumerate_loopnet_urls" not in h.names()
        assert "listing_urls" not in h.payloads("ls_start_apify_run")[0]
        assert h.payloads("ls_assess_run_health")[0]["used_enumeration"] is False

    def test_us_cities_never_enumerate(self):
        h = Harness(
            ls_start_apify_run=[{"run_id": "r1", "dataset_id": "d1"}],
            ls_check_apify_run=[FINISHED],
            ls_fetch_and_persist_raw=[{"item_count": 456}],
            ls_assess_run_health=[OK],
        )
        h.run(city="new york", run_id="weekly-new-york-2026-W35", source_config=US_CONFIG)
        assert "ls_enumerate_loopnet_urls" not in h.names()
