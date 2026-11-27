#!/usr/bin/env python3
"""Offline unit tests for news-alpha-radar (stdlib unittest, mocked network, no secrets)."""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

REPO_DIR = Path(__file__).resolve().parent.parent
if str(REPO_DIR) not in sys.path:
    sys.path.insert(0, str(REPO_DIR))

import radar  # noqa: E402

KST = radar.KST

_APP_ENV_PREFIXES = (
    "GEMINI_", "TELEGRAM_", "DASHBOARD_", "DAEMON_", "MIN_CATALYST_",
    "MIN_TRADING_VALUE_", "MAX_ALERTS_", "MAX_DAILY_", "MAX_MARKET_", "MAX_OFFHOURS_",
    "MAX_BULLISH_PREPRICED_", "OFFHOURS_DIGEST_", "STOCK_DEDUP_",
    "THEME_COOLDOWN_", "DEDUP_MODE", "TRIGGER_MODE", "STAGE1_DAILY_",
    "INSTANT_FLASH_", "EXTRA_RSS_", "MARKET_LOOP_",
    "AUTOTRADE_", "KIS_", "GITHUB_REPORT_",
    "GITHUB_EVENT_SCHEDULE", "GITHUB_STEP_SUMMARY",
)
_SAVED_ENV: dict[str, str] = {}


def setUpModule() -> None:
    _SAVED_ENV.clear()
    for key in list(os.environ.keys()):
        if any(key == p or key.startswith(p) for p in _APP_ENV_PREFIXES):
            _SAVED_ENV[key] = os.environ.pop(key)


def tearDownModule() -> None:
    for key, val in _SAVED_ENV.items():
        os.environ[key] = val
    _SAVED_ENV.clear()


class _FakeResponse:
    def __init__(self, payload: dict, status: int = 200) -> None:
        self._bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.status = status

    def read(self) -> bytes:
        return self._bytes

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *args) -> None:
        return None


class TestTimeAndCronHelpers(unittest.TestCase):
    def test_market_watch_window_boundaries(self) -> None:
        self.assertFalse(radar.is_market_watch_window(datetime(2026, 11, 16, 7, 59, tzinfo=KST)))
        self.assertTrue(radar.is_market_watch_window(datetime(2026, 11, 16, 8, 0, tzinfo=KST)))
        self.assertTrue(radar.is_market_watch_window(datetime(2026, 11, 20, 15, 30, tzinfo=KST)))
        self.assertFalse(radar.is_market_watch_window(datetime(2026, 11, 20, 15, 31, tzinfo=KST)))
        self.assertFalse(radar.is_market_watch_window(datetime(2026, 11, 21, 10, 0, tzinfo=KST)))
        self.assertTrue(radar.is_market_watch_window(datetime(2026, 11, 19, 23, 0, tzinfo=timezone.utc)))
        self.assertFalse(radar.is_market_watch_window(datetime(2026, 11, 20, 6, 31, tzinfo=timezone.utc)))

    def test_compute_cron_jitter(self) -> None:
        # Fri 00:25:10 UTC == 09:25:10 KST, expected 00:22:00 UTC (09:22 KST) -> 190s
        sched, jitter = radar.compute_cron_jitter(
            "7,22,37,52 0-11 * * 1-5",
            datetime(2026, 11, 20, 9, 25, 10, tzinfo=KST),
        )
        self.assertEqual(sched, datetime(2026, 11, 20, 9, 22, 0, tzinfo=KST))
        self.assertEqual(jitter, 190)

        # Fri 21:41:09 KST (12:41:09 UTC), expected 21:13 KST -> 28m 09s = 1689s
        sched2, jitter2 = radar.compute_cron_jitter(
            "13 12-21 * * 1-5",
            datetime(2026, 11, 20, 21, 41, 9, tzinfo=KST),
        )
        self.assertEqual(sched2, datetime(2026, 11, 20, 21, 13, 0, tzinfo=KST))
        self.assertEqual(jitter2, 1689)

        # Manual run (empty schedule) -> (None, None)
        self.assertEqual(radar.compute_cron_jitter("", datetime(2026, 11, 20, 9, 0, tzinfo=KST)), (None, None))

    def test_parse_pub_date(self) -> None:
        dt_kst = radar.parse_pub_date("Fri, 20 Nov 2026 17:56:45 +0900")
        self.assertEqual(dt_kst, datetime(2026, 11, 20, 17, 56, 45, tzinfo=KST))
        dt_gmt = radar.parse_pub_date("Fri, 20 Nov 2026 08:56:45 GMT")
        self.assertEqual(dt_gmt, datetime(2026, 11, 20, 17, 56, 45, tzinfo=KST))
        dt_iso = radar.parse_pub_date("2026-11-20T08:56:45Z")
        self.assertEqual(dt_iso, datetime(2026, 11, 20, 17, 56, 45, tzinfo=KST))
        self.assertIsNone(radar.parse_pub_date("not-a-date"))
        self.assertIsNone(radar.parse_pub_date(""))


class TestAtomicSaveAndTraceRotation(unittest.TestCase):
    def test_atomic_state_save_preserves_existing_file_on_failure(self) -> None:
        now = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            store = radar.StateStore(state_path)
            store.mark_seen("https://ex.com/old", "기존 기사", now=now)
            store.save(now=now)
            original_text = state_path.read_text(encoding="utf-8")

            # Simulate crash right at os.replace during next save
            store.mark_seen("https://ex.com/new", "신규 기사", now=now)
            with mock.patch("os.replace", side_effect=OSError("simulated crash")):
                with self.assertRaises(OSError):
                    store.save(now=now)

            # Original file untouched & valid JSON, no leftover temp files
            self.assertEqual(state_path.read_text(encoding="utf-8"), original_text)
            reloaded = radar.StateStore(state_path)
            self.assertTrue(reloaded.is_seen("https://ex.com/old", "기존 기사"))
            self.assertFalse(reloaded.is_seen("https://ex.com/new", "신규 기사"))
            tmp_files = [p for p in Path(tmp).iterdir() if p.name != "state.json"]
            self.assertEqual(tmp_files, [])

    def test_state_backward_compat_without_deferred_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "state.json"
            p.write_text(json.dumps({"seen_urls": {"https://ex.com/1": "2026-11-20T09:00:00+09:00"}}), encoding="utf-8")
            store = radar.StateStore(p)
            self.assertEqual(store.deferred, [])
            self.assertIn("https://ex.com/1", store.seen_urls)

    def test_trace_rotation_by_age_and_bytes(self) -> None:
        now = datetime(2026, 11, 20, 12, 0, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "trace.jsonl"
            tl = radar.TraceLogger(trace_path, max_days=7, max_bytes=350)
            # 10 days old -> pruned by age
            tl.emit({"ts": (now - timedelta(days=10)).isoformat(), "type": "old", "pad": "a" * 80})
            # 5 recent entries -> oldest trimmed by byte cap (350 bytes)
            for idx in range(5):
                tl.emit({"ts": (now - timedelta(minutes=10 - idx)).isoformat(), "type": "recent", "i": idx, "pad": "x" * 70})
            tl.rotate(now=now)

            lines = trace_path.read_text(encoding="utf-8").splitlines()
            records = [json.loads(l) for l in lines]
            self.assertTrue(all(r["type"] == "recent" for r in records))
            self.assertLessEqual(trace_path.stat().st_size, 350)
            self.assertEqual(records[-1]["i"], 4)


class TestGeminiOptimization(unittest.TestCase):
    def test_generation_config_by_model_family(self) -> None:
        cfg_36 = radar.build_gemini_generation_config("gemini-3.6-flash", thinking_level="LOW", thinking_budget=1024)
        self.assertEqual(cfg_36, {"responseMimeType": "application/json", "thinkingConfig": {"thinkingLevel": "LOW"}})
        cfg_38 = radar.build_gemini_generation_config("gemini-3.8-flash", thinking_level="MINIMAL", thinking_budget=0)
        self.assertEqual(cfg_38["thinkingConfig"], {"thinkingLevel": "LOW"})
        cfg_25 = radar.build_gemini_generation_config("gemini-2.5-flash", thinking_level="LOW", thinking_budget=1024)
        self.assertEqual(cfg_25["thinkingConfig"], {"thinkingBudget": 1024})
        self.assertEqual(cfg_25["temperature"], 0.25)

    def test_timeout_failover_and_thought_filtering(self) -> None:
        calls: list[tuple[str, int]] = []

        def fake_urlopen(req, timeout=0):
            calls.append((req.full_url, timeout))
            if "gemini-3.6-flash" in req.full_url:
                raise TimeoutError("The read operation timed out")
            return _FakeResponse({
                "candidates": [{
                    "content": {
                        "parts": [
                            {"thought": True, "text": "internal {bad json"},
                            {"text": '{"title": "ok", "stocks": []}'},
                        ]
                    }
                }]
            })

        with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            res, model = radar.call_gemini(
                "key",
                ["gemini-3.6-flash", "gemini-3.5-flash-lite"],
                "sys",
                "user",
                thinking_level="LOW",
                timeout=25,
            )
        self.assertEqual(model, "gemini-3.5-flash-lite")
        self.assertEqual(res["title"], "ok")
        self.assertEqual(calls[0][1], 25)

    def test_auth_error_fails_fast(self) -> None:
        cnt = [0]

        def fake_urlopen(req, timeout=0):
            cnt[0] += 1
            raise urllib.error.HTTPError(
                req.full_url, 400, "Bad Request", {}, io.BytesIO(b'{"error":{"message":"API key not valid"}}')
            )

        with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            with self.assertRaises(radar.GeminiAuthError):
                radar.call_gemini("bad", ["gemini-3.6-flash", "gemini-3.5-flash-lite"], "sys", "user")
        self.assertEqual(cnt[0], 1)


class TestTelegramChunkResumeAndDeferred(unittest.TestCase):
    def test_telegram_partial_chunk_failure_resumes_without_duplicating_chunk_0(self) -> None:
        # Build a multi-chunk HTML message (> 3950 chars)
        long_lines = [f"• 항목 {i}: " + ("가" * 120) for i in range(45)]
        msg = "🚨 <b>[이슈 레이더] 장문 테스트</b>\n⭐ 촉매 점수: <b>8.5/10</b>\n" + "\n".join(long_lines)
        chunks = radar.split_telegram_chunks(msg)
        self.assertGreaterEqual(len(chunks), 2)

        sent_texts: list[str] = []
        attempt = [0]

        def fake_urlopen(req, timeout=0):
            body = json.loads(req.data.decode("utf-8"))
            attempt[0] += 1
            # Fail on 2nd chunk during first send
            if attempt[0] == 2:
                raise urllib.error.HTTPError(req.full_url, 502, "Bad Gateway", {}, io.BytesIO(b"gateway err"))
            sent_texts.append(body["text"])
            return _FakeResponse({"ok": True})

        with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            with self.assertRaises(radar.TelegramPartialSendError) as ctx:
                radar.send_telegram("tok", "chat", msg, start_chunk=0)
            self.assertEqual(ctx.exception.sent_chunks, 1)

            # Resume from chunk 1: chunk 0 must NOT be resent!
            total = radar.send_telegram("tok", "chat", msg, start_chunk=ctx.exception.sent_chunks)
            self.assertEqual(total, len(chunks))

        self.assertEqual(sent_texts, chunks)

    def test_stage2_failure_deferred_retried_next_cycle_with_fresh_quotes_and_delay_banner(self) -> None:
        t0 = datetime(2026, 11, 20, 9, 10, 0, tzinfo=KST)
        clock = [t0]

        def now_fn() -> datetime:
            return clock[0]

        item_a = radar.NewsItem(
            "대규모 통신망 해킹 사고 발생…사이버보안 긴급 점검",
            "https://ex.com/a",
            "연합뉴스_최신",
            "속보",
            published_at="Fri, 20 Nov 2026 09:05:00 +0900",
            first_seen=t0.isoformat(),
        )
        item_b = radar.NewsItem(
            "국가시험 AI 안경 부정행위 적발…전파탐지기 도입 추진",
            "https://ex.com/b",
            "매일경제_경제",
            "경제/산업",
            published_at="Fri, 20 Nov 2026 09:06:00 +0900",
            first_seen=t0.isoformat(),
        )

        # Cycle 1: Candidate A fails Stage 2 -> deferred(stage2_pending); Candidate B succeeds in same cycle!
        # Cycle 2 (+10 min): Candidate A retried from deferred -> refetches quotes -> succeeds with delay banner!
        stage2_attempts_a = [0]
        quote_prices_seen: list[int] = []
        sent_msgs: list[str] = []

        def fake_fetch_rss(state, max_per_feed=25, now=None):
            return [it for it in (item_a, item_b) if not state.is_seen(it.url, it.title)]

        def fake_fetch_quotes(codes):
            # Price moves from 14000 (cycle 1) to 15200 (cycle 2) to prove fresh quote refetch
            price = 14000 if clock[0] == t0 else 15200
            quote_prices_seen.append(price)
            return {
                c: radar.StockQuote(c, price, 200, 3.5, 100000, "OPEN", radar.classify_stage(3.5, 100000))
                for c in codes
            }

        def fake_call_gemini(api_key, models, sys_prompt, user_prompt, **kwargs):
            data = json.loads(user_prompt)
            if sys_prompt == radar.SCREENING_PROMPT:
                evts = []
                for cl in data["clusters"]:
                    if "해킹" in cl["headline"]:
                        evts.append({
                            "cluster_id": cl["cluster_id"],
                            "catalyst_score": 9.0,
                            "theme_key": "통신망_해킹",
                            "event_summary": "통신망 해킹 발생",
                            "second_order_keywords": ["사이버보안"],
                        })
                    elif "AI 안경" in cl["headline"]:
                        evts.append({
                            "cluster_id": cl["cluster_id"],
                            "catalyst_score": 8.2,
                            "theme_key": "AI안경_전파탐지기",
                            "event_summary": "전파탐지기 도입 추진",
                            "second_order_keywords": ["전파탐지기"],
                        })
                return {"selected_events": evts}, "gemini-3.5-flash-lite"

            headline = data["event_cluster"]["headline"]
            if "해킹" in headline:
                stage2_attempts_a[0] += 1
                if stage2_attempts_a[0] == 1:
                    raise RuntimeError("forced stage2 failure")
                # Verify fresh quote price was passed into Stage 2 on retry!
                cands = data["verified_krx_candidates"]
                self.assertTrue(any(c.get("quote", {}).get("price") == 15200 for c in cands))
                return {
                    "title": "통신망 해킹 → 사이버보안 수혜",
                    "event_summary": "통신망 해킹",
                    "stocks": [{"code": "263860", "name": "지니언스", "direction": "상승수혜"}],
                }, "gemini-3.6-flash"
            return {
                "title": "AI 안경 부정행위 → 전파탐지기 수혜",
                "event_summary": "전파탐지기 도입",
                "stocks": [{"code": "446840", "name": "지슨", "direction": "상승수혜"}],
            }, "gemini-3.6-flash"

        def fake_sleep(sec):
            clock[0] = clock[0] + timedelta(minutes=10)

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            summary_file = tmp_dir / "step_summary.md"
            env = {
                "GEMINI_API_KEY": "test-key",
                "TELEGRAM_BOT_TOKEN": "tok",
                "TELEGRAM_CHAT_ID": "chat",
                "MAX_ALERTS_PER_RUN": "2",
                "MAX_DAILY_ALERTS": "10",
                "GITHUB_EVENT_SCHEDULE": "7,22,37,52 0-11 * * 1-5",
                "GITHUB_STEP_SUMMARY": str(summary_file),
            }
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(radar, "fetch_rss_news", side_effect=fake_fetch_rss),
                mock.patch.object(radar, "call_gemini", side_effect=fake_call_gemini),
                mock.patch.object(radar, "load_krx_master", return_value={c["code"]: dict(c) for c in radar.SEED_COMPANIES}),
                mock.patch.object(radar, "fetch_naver_quotes", side_effect=fake_fetch_quotes),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_msgs.append(m) or 1),
                mock.patch.object(radar.time, "sleep", side_effect=fake_sleep),
            ):
                radar.run_live(dry_run=False, cycles=2, interval_sec=120, data_dir=tmp_dir, now_fn=now_fn)

            # Both alerts delivered: B in cycle 1, A in cycle 2 via deferred retry!
            self.assertEqual(len(sent_msgs), 2)
            self.assertIn("전파탐지기", sent_msgs[0])
            self.assertNotIn("지연 발송", sent_msgs[0])
            self.assertIn("사이버보안", sent_msgs[1])
            self.assertIn("⏱ 지연 발송 (최초 포착 09:10, 10분 경과)", sent_msgs[1])
            self.assertIn("15,200원", sent_msgs[1])

            # Deferred queue now empty & recent_alerts populated for PR-3 dedup
            reloaded = radar.StateStore(tmp_dir / "state.json")
            self.assertEqual(reloaded.deferred, [])
            self.assertEqual(len(reloaded.recent_alerts), 2)
            by_theme = {a["theme_key"]: a for a in reloaded.recent_alerts}
            self.assertIn("AI안경_전파탐지기", by_theme)
            self.assertIn("통신망_해킹", by_theme)
            self.assertEqual(by_theme["AI안경_전파탐지기"]["final_codes"], ["446840"])
            self.assertIn("446840", by_theme["AI안경_전파탐지기"]["cand_codes"])
            self.assertTrue(len(by_theme["AI안경_전파탐지기"]["tokens"]) >= 1)
            self.assertTrue(len(by_theme["AI안경_전파탐지기"]["keywords"]) >= 1)
            self.assertEqual(by_theme["통신망_해킹"]["final_codes"], ["263860"])
            self.assertGreaterEqual(by_theme["통신망_해킹"]["catalyst_score"], 8.5)

            # Trace file contains run_start (with jitter), deferred events, article decisions, cycle summaries
            trace_records = [json.loads(l) for l in (tmp_dir / "trace.jsonl").read_text(encoding="utf-8").splitlines()]
            types = {r["type"] for r in trace_records}
            self.assertTrue({"run_start", "deferred_event", "article_decision", "cycle_summary"}.issubset(types))
            run_start = [r for r in trace_records if r["type"] == "run_start"][0]
            self.assertEqual(run_start["jitter_sec"], 180)  # 09:10 vs 09:07 KST = 180s
            # Step summary markdown written
            summary_txt = summary_file.read_text(encoding="utf-8")
            self.assertIn("News Alpha Radar 실행 요약", summary_txt)
            self.assertIn("180s", summary_txt)

    def test_send_pending_skips_stage2_within_15m_and_downgrades_after_15m(self) -> None:
        base_t = datetime(2026, 11, 20, 10, 0, 0, tzinfo=KST)
        cluster = radar.EventCluster(
            "evt_x",
            "해킹 비상",
            "사회/사건",
            [radar.NewsItem("해킹 비상", "https://ex.com/x", "연합뉴스_최신", "사회/사건", first_seen=base_t.isoformat())],
        )
        stage1_cand = {
            "cluster_id": "evt_x",
            "catalyst_score": 8.5,
            "theme_key": "해킹_비상",
            "second_order_keywords": ["사이버보안"],
        }

        # Case A: 5 minutes later -> send_pending sends directly WITHOUT calling Stage 2
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            st = radar.StateStore(tmp_dir / "state.json")
            st.deferred = [{
                "kind": "send_pending",
                "cluster": cluster.to_dict(),
                "stage1": stage1_cand,
                "theme_key": "해킹_비상",
                "catalyst_score": 8.5,
                "first_seen": base_t.isoformat(),
                "created_at": base_t.isoformat(),
                "stage2_completed_at": base_t.isoformat(),
                "expires_at": (base_t + timedelta(minutes=60)).isoformat(),
                "retries": 0,
                "reason": "send_error",
                "message_html": "🚨 <b>[이슈 레이더] 해킹 비상</b>\n⭐ 촉매 점수: <b>8.5/10</b>\n본문",
                "deep_title": "해킹 비상",
                "sent_chunks": 0,
            }]
            st.save(now=base_t)

            sent = []
            with (
                mock.patch.dict(os.environ, {"GEMINI_API_KEY": "k", "TELEGRAM_BOT_TOKEN": "t", "TELEGRAM_CHAT_ID": "c"}, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=[]),
                mock.patch.object(radar, "call_gemini", side_effect=AssertionError("Stage 2 must not be called")),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent.append((m, start_chunk)) or 1),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: base_t + timedelta(minutes=5))
            self.assertEqual(len(sent), 1)
            self.assertIn("⏱ 지연 발송 (최초 포착 10:00, 5분 경과)", sent[0][0])

        # Case B: 20 minutes later (>15m, sent_chunks=0) -> downgrades to stage2_pending & re-runs Stage 2!
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            st = radar.StateStore(tmp_dir / "state.json")
            st.deferred = [{
                "kind": "send_pending",
                "cluster": cluster.to_dict(),
                "stage1": stage1_cand,
                "theme_key": "해킹_비상",
                "catalyst_score": 8.5,
                "first_seen": base_t.isoformat(),
                "created_at": base_t.isoformat(),
                "stage2_completed_at": base_t.isoformat(),
                "expires_at": (base_t + timedelta(minutes=60)).isoformat(),
                "retries": 0,
                "reason": "send_error",
                "message_html": "stale html",
                "deep_title": "해킹 비상",
                "sent_chunks": 0,
            }]
            st.save(now=base_t)

            s2_called = [0]
            sent2 = []

            def fake_s2(*args, **kwargs):
                s2_called[0] += 1
                return {"title": "재분석 해킹 비상", "event_summary": "요약", "stocks": [{"code": "263860", "name": "지니언스"}]}, "gemini-3.6-flash"

            with (
                mock.patch.dict(os.environ, {"GEMINI_API_KEY": "k", "TELEGRAM_BOT_TOKEN": "t", "TELEGRAM_CHAT_ID": "c"}, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=[]),
                mock.patch.object(radar, "call_gemini", side_effect=fake_s2),
                mock.patch.object(radar, "load_krx_master", return_value={c["code"]: dict(c) for c in radar.SEED_COMPANIES}),
                mock.patch.object(radar, "fetch_naver_quotes", return_value={}),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent2.append(m) or 1),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: base_t + timedelta(minutes=20))
            self.assertEqual(s2_called[0], 1)
            self.assertEqual(len(sent2), 1)
            self.assertIn("재분석 해킹 비상", sent2[0])
            self.assertIn("20분 경과", sent2[0])

    def test_deferred_ttl_expiration_and_rank_overflow_traced(self) -> None:
        t0 = datetime(2026, 11, 20, 10, 0, 0, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            st = radar.StateStore(tmp_dir / "state.json")
            # Already expired deferred entry
            st.deferred = [{
                "kind": "stage2_pending",
                "cluster": {"cluster_id": "evt_old", "headline": "old", "category": "속보", "items": []},
                "stage1": {"cluster_id": "evt_old", "catalyst_score": 8.0},
                "theme_key": "만료_테마",
                "catalyst_score": 8.0,
                "first_seen": (t0 - timedelta(minutes=90)).isoformat(),
                "created_at": (t0 - timedelta(minutes=90)).isoformat(),
                "expires_at": (t0 - timedelta(minutes=30)).isoformat(),
                "retries": 0,
            }]
            st.save(now=t0)

            # 30 truly distinct articles -> 30 clusters -> top 25 in stage 1, ranks 26..30 traced as cluster_rank_overflow
            articles = [
                radar.NewsItem(
                    f"알파기업{i} 베타기술{i} 감마수주{i}",
                    f"https://ex.com/{i}",
                    "매일경제_경제",
                    "경제/산업",
                    published_at="Fri, 20 Nov 2026 09:50:00 +0900",
                    first_seen=t0.isoformat(),
                )
                for i in range(30)
            ]
            with (
                mock.patch.dict(os.environ, {"GEMINI_API_KEY": "k"}, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=articles),
                mock.patch.object(radar, "call_gemini", return_value=({"selected_events": []}, "gemini-3.5-flash-lite")),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t0)

            reloaded = radar.StateStore(tmp_dir / "state.json")
            self.assertEqual(reloaded.deferred, [])

            records = [json.loads(l) for l in (tmp_dir / "trace.jsonl").read_text(encoding="utf-8").splitlines()]
            expired = [r for r in records if r["type"] == "deferred_event" and r["action"] == "expired"]
            self.assertEqual(len(expired), 1)
            art_recs = [r for r in records if r["type"] == "article_decision"]
            self.assertEqual(len(art_recs), 30)
            self.assertEqual(sum(1 for r in art_recs if r["included_in_stage1"]), 25)
            self.assertEqual(sum(1 for r in art_recs if r["deferred_reason"] == "cluster_rank_overflow"), 5)
            self.assertTrue(all(r["pub_lag_sec"] == 600 for r in art_recs))

    def test_e2e_telegram_second_chunk_fail_resumes_in_next_cycle(self) -> None:
        t0 = datetime(2026, 11, 20, 10, 0, 0, tzinfo=KST)
        clock = [t0]
        item = radar.NewsItem("초대형 사이버보안 해킹 사고", "https://ex.com/long", "연합뉴스_최신", "사회/사건", first_seen=t0.isoformat())
        # Produce a report long enough to split into 2 Telegram chunks
        long_reason = "세부연결근거 " * 140
        stage2_calls = [0]
        sent_chunks_log: list[str] = []
        tg_attempts = [0]

        def fake_fetch(state, max_per_feed=25, now=None):
            return [] if state.is_seen(item.url, item.title) else [item]

        def fake_gemini(api_key, models, sp, up, **kw):
            data = json.loads(up)
            if sp == radar.SCREENING_PROMPT:
                cid = data["clusters"][0]["cluster_id"]
                return {"selected_events": [{"cluster_id": cid, "catalyst_score": 8.9, "theme_key": "대형해킹", "second_order_keywords": ["사이버보안"]}]}, "gemini-3.5-flash-lite"
            stage2_calls[0] += 1
            return {
                "title": "초대형 사이버보안 해킹 사고 분석",
                "event_summary": "대규모 해킹 발생",
                "causal_chain": [f"{i}단계: " + ("상세인과설명" * 40) for i in range(1, 16)],
                "stocks": [{"code": "263860", "name": "지니언스", "direction": "상승수혜", "reason": long_reason}],
                "entry_strategy": "분할 관찰",
                "invalidation_rules": "무효화 조건",
                "key_risks": "리스크",
            }, "gemini-3.6-flash"

        def fake_urlopen(req, timeout=0):
            body = json.loads(req.data.decode("utf-8"))
            tg_attempts[0] += 1
            if tg_attempts[0] == 2:
                raise urllib.error.HTTPError(req.full_url, 502, "Bad Gateway", {}, io.BytesIO(b"fail chunk 2"))
            sent_chunks_log.append(body["text"])
            return _FakeResponse({"ok": True})

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            env = {"GEMINI_API_KEY": "k", "TELEGRAM_BOT_TOKEN": "tok", "TELEGRAM_CHAT_ID": "chat"}
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(radar, "fetch_rss_news", side_effect=fake_fetch),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini),
                mock.patch.object(radar, "load_krx_master", return_value={c["code"]: dict(c) for c in radar.SEED_COMPANIES}),
                mock.patch.object(radar, "fetch_naver_quotes", return_value={}),
                mock.patch("urllib.request.urlopen", side_effect=fake_urlopen),
                mock.patch.object(radar.time, "sleep", side_effect=lambda s: clock.__setitem__(0, clock[0] + timedelta(minutes=2))),
            ):
                radar.run_live(dry_run=False, cycles=2, interval_sec=120, data_dir=tmp_dir, now_fn=lambda: clock[0])

            # Stage 2 called only ONCE (cycle 1); cycle 2 resumed from chunk 1 without re-sending chunk 0!
            self.assertEqual(stage2_calls[0], 1)
            self.assertGreaterEqual(len(sent_chunks_log), 2)
            # Chunk 0 appears exactly once!
            self.assertEqual(sum(1 for c in sent_chunks_log if "초대형 사이버보안 해킹 사고 분석" in c), 1)
            reloaded = radar.StateStore(tmp_dir / "state.json")
            self.assertEqual(reloaded.deferred, [])
            self.assertEqual(reloaded.sent_today(now=clock[0]), 1)

    def test_deferred_capacity_limit_and_max_retries_drop(self) -> None:
        now = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            st = radar.StateStore(tmp_dir / "state.json")
            tl = radar.TraceLogger(tmp_dir / "trace.jsonl")
            for i in range(6):
                radar.enqueue_deferred(
                    st,
                    {
                        "kind": "stage2_pending",
                        "cluster": {"cluster_id": f"evt_{i}", "headline": f"h{i}", "category": "속보", "items": []},
                        "stage1": {"cluster_id": f"evt_{i}", "catalyst_score": 7.5 + i * 0.2},
                        "theme_key": f"테마_{i}",
                        "catalyst_score": 7.5 + i * 0.2,
                        "first_seen": now.isoformat(),
                        "created_at": now.isoformat(),
                        "expires_at": (now + timedelta(minutes=60)).isoformat(),
                        "retries": 0,
                        "reason": "stage2_error",
                    },
                    tl,
                    cycle=1,
                    now=now,
                )
            self.assertEqual(len(st.deferred), radar.MAX_DEFERRED_ITEMS)
            # Lowest score (evt_0, 7.5) was evicted
            kept_ids = [d["cluster"]["cluster_id"] for d in st.deferred]
            self.assertNotIn("evt_0", kept_ids)
            self.assertEqual(kept_ids[0], "evt_5")
            recs = [json.loads(l) for l in (tmp_dir / "trace.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertTrue(any(r["action"] == "dropped_capacity" and r["cluster_id"] == "evt_0" for r in recs))


class _FakeXmlResponse:
    def __init__(self, xml_bytes: bytes, status: int = 200, headers: dict[str, str] | None = None) -> None:
        self._bytes = xml_bytes
        self.status = status
        self.headers = headers or {}

    def read(self) -> bytes:
        return self._bytes

    def __enter__(self) -> "_FakeXmlResponse":
        return self

    def __exit__(self, *args) -> None:
        return None


class TestPR2QuotaPendingRssAndTokens(unittest.TestCase):
    def test_pacific_midnight_resets_ai_counter_not_alert_counter(self) -> None:
        # Winter PST = UTC-8, KST = UTC+9 -> 17:00 KST is PT midnight!
        before_pt_midnight = datetime(2026, 11, 20, 16, 59, tzinfo=KST)
        after_pt_midnight = datetime(2026, 11, 20, 17, 1, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            st = radar.StateStore(Path(tmp) / "state.json")
            st.record_ai_request("gemini-3.5-flash-lite", stage="stage1", now=before_pt_midnight)
            st.record_ai_request("gemini-3.6-flash", stage="stage2", now=before_pt_midnight)
            st.mark_model_exhausted("gemini-3.6-flash", now=before_pt_midnight)
            st.mark_alerted("테스트_테마", now=before_pt_midnight, tokens=["컬리"], final_codes=["263860"], catalyst_score=8.5)
            st.save(now=before_pt_midnight)

            self.assertEqual(st.ai_requests_today(now=before_pt_midnight), 2)
            self.assertTrue(st.is_model_exhausted("gemini-3.6-flash", now=before_pt_midnight))
            self.assertEqual(st.sent_today(now=before_pt_midnight), 1)

            # After PT midnight (17:01 KST): AI counter & exhausted reset, KST alert count preserved!
            self.assertEqual(st.ai_requests_today(now=after_pt_midnight), 0)
            self.assertFalse(st.is_model_exhausted("gemini-3.6-flash", now=after_pt_midnight))
            self.assertEqual(st.sent_today(now=after_pt_midnight), 1)

    def test_model_daily_quota_429_circuit_breaker_and_rpm_distinction(self) -> None:
        t1 = datetime(2026, 11, 20, 14, 0, tzinfo=KST)
        t_next_pt = datetime(2026, 11, 20, 17, 5, tzinfo=KST)
        called_models: list[str] = []
        fail_36_once = [True]

        def fake_urlopen(req, timeout=0):
            url = req.full_url
            called_models.append(url)
            if "gemini-3.6-flash" in url and fail_36_once[0]:
                fail_36_once[0] = False
                body = b'{"error":{"code":429,"message":"Quota exceeded for metric GenerateRequestsPerDayPerProjectPerModel"}}'
                raise urllib.error.HTTPError(url, 429, "Too Many Requests", {}, io.BytesIO(body))
            return _FakeResponse({"candidates": [{"content": {"parts": [{"text": '{"ok": true}'}]}}]})

        with tempfile.TemporaryDirectory() as tmp:
            st = radar.StateStore(Path(tmp) / "state.json")
            with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen), mock.patch.object(radar.time, "sleep"):
                # Call 1: 3.6-flash hits daily quota 429 -> immediately falls back to 3.5-flash-lite & marks exhausted
                res1, m1 = radar.call_gemini(
                    "k", ["gemini-3.6-flash", "gemini-3.5-flash-lite"], "sys", "u", state=st, stage="stage2", now=t1
                )
                self.assertEqual(m1, "gemini-3.5-flash-lite")
                self.assertEqual(res1, {"ok": True})
                self.assertTrue(st.is_model_exhausted("gemini-3.6-flash", now=t1))
                self.assertEqual(st.ai_requests_today("gemini-3.6-flash", now=t1), 1)
                self.assertEqual(st.ai_requests_today("gemini-3.5-flash-lite", now=t1), 1)

                # Call 2 (same PT day): 3.6-flash MUST NOT be called at all!
                called_models.clear()
                res2, m2 = radar.call_gemini(
                    "k", ["gemini-3.6-flash", "gemini-3.5-flash-lite"], "sys", "u", state=st, stage="stage2", now=t1 + timedelta(minutes=10)
                )
                self.assertEqual(m2, "gemini-3.5-flash-lite")
                self.assertFalse(any("gemini-3.6-flash" in u for u in called_models))

                # Call 3 (after PT midnight): 3.6-flash is back in rotation!
                called_models.clear()
                res3, m3 = radar.call_gemini(
                    "k", ["gemini-3.6-flash", "gemini-3.5-flash-lite"], "sys", "u", state=st, stage="stage2", now=t_next_pt
                )
                self.assertEqual(m3, "gemini-3.6-flash")
                self.assertTrue(any("gemini-3.6-flash" in u for u in called_models))

        # Verify RPM vs RPD detection
        self.assertTrue(radar.is_daily_quota_error("GenerateRequestsPerDay exceeded"))
        self.assertFalse(radar.is_daily_quota_error("RequestsPerMinute limit reached"))

    def test_seen_evaluated_separation_and_pending_ttl_for_40_clusters(self) -> None:
        t0 = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
        clock = [t0]
        articles = [
            radar.NewsItem(
                f"독립종목{i} 핵심기술{i} 설비투자{i}",
                f"https://ex.com/a{i}",
                "매일경제_경제",
                "경제/산업",
                published_at="Fri, 20 Nov 2026 09:55:00 +0900",
                first_seen=t0.isoformat(),
            )
            for i in range(40)
        ]

        s1_batch_sizes: list[int] = []

        def fake_fetch(state, max_per_feed=25, now=None):
            return [a for a in articles if not state.is_seen(a.url, a.title)]

        def fake_gemini(api_key, models, sp, up, **kw):
            s1_batch_sizes.append(len(json.loads(up)["clusters"]))
            return {"selected_events": []}, "gemini-3.5-flash-lite"

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            with (
                mock.patch.dict(os.environ, {"GEMINI_API_KEY": "k"}, clear=False),
                mock.patch.object(radar, "fetch_rss_news", side_effect=fake_fetch),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini),
                mock.patch.object(radar.time, "sleep", side_effect=lambda s: clock.__setitem__(0, clock[0] + timedelta(minutes=2))),
            ):
                # Cycle 1: 40 fresh -> 25 evaluated, 15 kept in pending!
                # Cycle 2: 0 new RSS -> remaining 15 in pending evaluated!
                radar.run_live(dry_run=False, cycles=2, interval_sec=120, data_dir=tmp_dir, now_fn=lambda: clock[0])

            self.assertEqual(s1_batch_sizes, [25, 15])
            reloaded = radar.StateStore(tmp_dir / "state.json")
            self.assertEqual(len(reloaded.pending), 0)
            self.assertEqual(len(reloaded.evaluated), 40)

        # Now test pending TTL expiry (95m > 90m market TTL -> dropped without AI call)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            st = radar.StateStore(tmp_dir / "state.json")
            old_item = radar.NewsItem("지연된 기사", "https://ex.com/stale", "연합뉴스_최신", "속보", first_seen=t0.isoformat())
            st.mark_seen(old_item.url, old_item.title, now=t0)
            st.add_pending(old_item, now=t0)
            st.save(now=t0)

            with (
                mock.patch.dict(os.environ, {"GEMINI_API_KEY": "k"}, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=[]),
                mock.patch.object(radar, "call_gemini", side_effect=AssertionError("AI must not be called for expired pending")),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t0 + timedelta(minutes=95))

            reloaded2 = radar.StateStore(tmp_dir / "state.json")
            self.assertEqual(len(reloaded2.pending), 0)
            recs = [json.loads(l) for l in (tmp_dir / "trace.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertTrue(any(r.get("deferred_reason") == "pending_ttl_expired" for r in recs))

    def test_parallel_rss_google_cb_conditional_304_and_host_concurrency(self) -> None:
        import threading as _th
        now = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
        xml_body = (
            b'<?xml version="1.0" encoding="UTF-8"?><rss><channel>'
            b'<item><title>AI \xeb\xb0\x98\xeb\x8f\x84\xec\xb2\xb4 \xec\x88\x98\xec\xa3\xbc \xed\x99\x95\xeb\x8c\x80</title>'
            b'<link>https://yna.example/1</link><pubDate>Fri, 20 Nov 2026 09:58:00 +0900</pubDate></item>'
            b'</channel></rss>'
        )
        feeds = [
            {"name": "구글뉴스_한국_주요", "category": "속보", "url": "https://news.google.com/rss?a=1", "priority": 1, "group": "google", "timeout": 5},
            {"name": "구글뉴스_한국_사회", "category": "사회/사건", "url": "https://news.google.com/rss?a=2", "priority": 1, "group": "google", "timeout": 5},
            {"name": "연합1", "category": "속보", "url": "https://www.yna.co.kr/rss/1.xml", "priority": 1},
            {"name": "연합2", "category": "사회/사건", "url": "https://www.yna.co.kr/rss/2.xml", "priority": 1},
            {"name": "연합3", "category": "경제/산업", "url": "https://www.yna.co.kr/rss/3.xml", "priority": 2},
            {"name": "연합4", "category": "경제/산업", "url": "https://www.yna.co.kr/rss/4.xml", "priority": 2},
        ]

        lock = _th.Lock()
        active_yna = [0]
        max_active_yna = [0]
        seen_urls: list[str] = []
        seen_if_none_match: list[str | None] = []

        def fake_urlopen(req, timeout=0):
            url = req.full_url
            with lock:
                seen_urls.append(url)
            if "news.google.com" in url:
                raise urllib.error.HTTPError(url, 503, "Service Unavailable", {}, io.BytesIO(b"503"))
            inm = req.get_header("If-none-match")
            with lock:
                seen_if_none_match.append(inm)
                active_yna[0] += 1
                max_active_yna[0] = max(max_active_yna[0], active_yna[0])
            try:
                radar.time.sleep(0.03)
                if inm == '"etag-v1"':
                    raise urllib.error.HTTPError(url, 304, "Not Modified", {}, io.BytesIO(b""))
                return _FakeXmlResponse(xml_body, status=200, headers={"ETag": '"etag-v1"', "Last-Modified": "Fri, 20 Nov 2026 09:58:00 GMT"})
            finally:
                with lock:
                    active_yna[0] -= 1

        with tempfile.TemporaryDirectory() as tmp:
            st = radar.StateStore(Path(tmp) / "state.json")
            with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
                items1 = radar.fetch_rss_news(st, now=now, feeds=feeds)
                # Google #2 MUST be skipped by circuit breaker!
                self.assertIn("https://news.google.com/rss?a=1", seen_urls)
                self.assertNotIn("https://news.google.com/rss?a=2", seen_urls)
                # Host concurrency strictly <= 2
                self.assertLessEqual(max_active_yna[0], 2)
                self.assertEqual(len(items1), 1)  # deduplicated across 4 identical feeds
                self.assertEqual(st.feed_stats["연합1"]["etag"], '"etag-v1"')

                # Second fetch sends If-None-Match -> receives 304 (not counted as failure!)
                seen_urls.clear()
                seen_if_none_match.clear()
                items2 = radar.fetch_rss_news(st, now=now + timedelta(minutes=2), feeds=feeds[2:])
                self.assertEqual(items2, [])
                self.assertTrue(all(h == '"etag-v1"' for h in seen_if_none_match))
                self.assertEqual(st.feed_stats["연합1"]["not_modified_304"], 1)
                self.assertEqual(st.feed_stats["연합1"]["fail"], 0)

    def test_probe_feeds_outputs_status_items_and_pubdate(self) -> None:
        now = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
        xml_body = (
            b'<?xml version="1.0" encoding="UTF-8"?><rss><channel>'
            b'<item><title>Test Headline</title><link>https://ex.com/1</link>'
            b'<pubDate>Fri, 20 Nov 2026 09:50:00 +0900</pubDate></item>'
            b'</channel></rss>'
        )
        feeds = [
            {"name": "정상피드", "category": "속보", "url": "https://ok.example/rss", "priority": 1},
            {"name": "실패피드", "category": "속보", "url": "https://err.example/rss", "priority": 2},
        ]

        def fake_urlopen(req, timeout=0):
            if "err.example" in req.full_url:
                raise urllib.error.HTTPError(req.full_url, 403, "Forbidden", {}, io.BytesIO(b""))
            return _FakeXmlResponse(xml_body)

        with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            rows = radar.probe_feeds(feeds=feeds, now=now)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["status"], 200)
        self.assertEqual(rows[0]["items"], 1)
        self.assertEqual(rows[0]["lag_min"], 10.0)
        self.assertEqual(rows[1]["status"], 403)
        self.assertIsNotNone(rows[1]["error"])

    def test_token_normalization_and_outlet_extraction(self) -> None:
        self.assertEqual(radar.normalize_token("컬리가"), "컬리")
        self.assertEqual(radar.normalize_token("컬리의"), "컬리")
        self.assertEqual(radar.normalize_token("데이터센터에서"), "데이터센터")
        self.assertEqual(radar.normalize_token("3370만명"), "")
        self.assertEqual(radar.normalize_token("2026년"), "")
        self.assertEqual(radar.normalize_token("발표"), "")
        toks = radar.extract_normalized_tokens("[단독] 컬리가 3370만명 개인정보를 유출 - MBC 뉴스")
        self.assertIn("컬리", toks)
        self.assertIn("개인정보", toks)
        self.assertIn("유출", toks)
        self.assertNotIn("mbc", toks)
        self.assertNotIn("3370만명", toks)

        g_item = radar.NewsItem("컬리 유출 사태 - MBC 뉴스", "https://news.google.com/1", "구글뉴스_한국_주요", "속보")
        y_item = radar.NewsItem("컬리 유출", "https://yna.co.kr/1", "연합뉴스_사회", "사회/사건")
        self.assertEqual(radar.extract_outlet(g_item), "MBC 뉴스")
        self.assertEqual(radar.extract_outlet(y_item), "연합뉴스")


class TestWorkflowConfig(unittest.TestCase):
    def test_radar_yml_prime_cron_and_trace_upload(self) -> None:
        yml = (REPO_DIR / ".github" / "workflows" / "radar.yml").read_text(encoding="utf-8")
        self.assertIn('cron: "7,22,37,52 22-23 * * 0-4"', yml)
        self.assertIn('cron: "7,22,37,52 0-11 * * 1-5"', yml)
        self.assertIn('cron: "13 12-21 * * 1-5"', yml)
        self.assertNotIn("*/15", yml)
        self.assertNotIn("Private도 월 2,000분 이내", yml)
        self.assertIn("GEMINI_DEEP_TIMEOUT: ${{ vars.GEMINI_DEEP_TIMEOUT || '25' }}", yml)
        self.assertIn("GITHUB_EVENT_SCHEDULE: ${{ github.event.schedule }}", yml)
        self.assertIn("actions/upload-artifact@v4", yml)
        self.assertIn("data/trace.jsonl", yml)
        self.assertIn("- probe", yml)
        self.assertIn("--probe-feeds", yml)


class TestPR3DedupTriggerDaemonAndDashboard(unittest.TestCase):
    def test_pr3_dedup_hub_exclusion_and_escalation_followup(self) -> None:
        now = datetime(2026, 11, 20, 11, 0, tzinfo=KST)
        # Hub stock '005930' appears in 3 distinct alerts within 7 days -> excluded from overlap!
        recent = [
            {"ts": (now - timedelta(days=3)).isoformat(), "theme_key": "이슈1", "tokens": ["alpha"], "keywords": ["kw1"], "cand_codes": ["005930"], "final_codes": ["005930"], "catalyst_score": 8.0},
            {"ts": (now - timedelta(days=2)).isoformat(), "theme_key": "이슈2", "tokens": ["beta"], "keywords": ["kw2"], "cand_codes": ["005930"], "final_codes": ["005930"], "catalyst_score": 8.0},
            {
                "ts": (now - timedelta(hours=2)).isoformat(),
                "theme_key": "컬리_개인정보유출",
                "tokens": ["컬리", "개인정보", "유출", "피해", "보상"],
                "keywords": ["사이버보안", "정보보안", "개인정보"],
                "cand_codes": ["005930", "263860", "053800"],
                "final_codes": ["005930", "263860", "053800"],
                "catalyst_score": 8.0,
            },
        ]
        hubs = radar.compute_hub_stocks(recent, now=now)
        self.assertIn("005930", hubs)
        self.assertNotIn("263860", hubs)

        # Unrelated event sharing only hub stock '005930' must NOT be marked duplicate
        unrelated = radar.evaluate_duplicate_candidate(
            theme_key="전혀다른_메모리_수주",
            tokens=["메모리", "신공장", "증설"],
            keywords=["반도체", "장비", "수주"],
            codes=["005930", "999999"],
            catalyst_score=8.0,
            recent_alerts=recent,
            cooldown_hours=12,
            now=now,
        )
        self.assertFalse(unrelated["is_dup"])

        # Same incident with DIFFERENT theme_key -> caught by (T, K, C) rules!
        same_diff_key = radar.evaluate_duplicate_candidate(
            theme_key="컬리_해킹_2보",
            tokens=["컬리", "개인정보", "유출", "추가", "조사"],
            keywords=["사이버보안", "정보보안"],
            codes=["263860", "053800"],
            catalyst_score=8.4,  # < 8.0 + 1.0 -> blocked
            recent_alerts=recent,
            cooldown_hours=12,
            now=now,
        )
        self.assertTrue(same_diff_key["is_dup"])
        self.assertFalse(same_diff_key["escalated"])

        # Same incident with +1.0 escalation (9.1 >= 8.0 + 1.0) -> allowed with escalated=True!
        escalated = radar.evaluate_duplicate_candidate(
            theme_key="컬리_해킹_전수조사",
            tokens=["컬리", "개인정보", "유출", "정부", "전수조사"],
            keywords=["사이버보안", "정보보안"],
            codes=["263860", "053800"],
            catalyst_score=9.1,
            recent_alerts=recent,
            cooldown_hours=12,
            now=now,
        )
        self.assertFalse(escalated["is_dup"])
        self.assertTrue(escalated["escalated"])

    def test_pr3_e2e_enforce_blocks_before_stage2_and_escalates_with_followup_prefix(self) -> None:
        t0 = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
        item_dup = radar.NewsItem("컬리 개인정보 유출 해킹 추가 피해 확인", "https://ex.com/dup", "연합뉴스_사회", "사회/사건", first_seen=t0.isoformat())
        item_esc = radar.NewsItem("컬리 개인정보 유출 해킹 정부 전수조사 착수", "https://ex.com/esc", "연합뉴스_사회", "사회/사건", first_seen=t0.isoformat())

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            st = radar.StateStore(tmp_dir / "state.json")
            st.mark_alerted(
                "컬리_최초보도",
                now=t0 - timedelta(hours=1),
                tokens=["컬리", "개인정보", "유출", "해킹", "피해"],
                keywords=["사이버보안", "정보보안"],
                cand_codes=["263860", "053800"],
                final_codes=["263860", "053800"],
                catalyst_score=8.0,
            )
            st.save(now=t0)

            stage2_calls = [0]
            sent_msgs: list[str] = []

            def fake_gemini(api_key, models, sp, up, **kw):
                if sp == radar.SCREENING_PROMPT:
                    cls = json.loads(up)["clusters"]
                    return {
                        "selected_events": [{
                            "cluster_id": cls[0]["cluster_id"],
                            "catalyst_score": 8.2,  # dup, not escalated
                            "theme_key": "컬리_다른테마키",
                            "first_order_keywords": ["정보보안"],
                            "second_order_keywords": ["사이버보안"],
                        }]
                    }, "gemini-3.5-flash-lite"
                stage2_calls[0] += 1
                return {"title": "심층분석", "event_summary": "요약", "stocks": [{"code": "263860", "name": "지니언스"}]}, "gemini-3.6-flash"

            env = {"GEMINI_API_KEY": "k", "TELEGRAM_BOT_TOKEN": "t", "TELEGRAM_CHAT_ID": "c", "DEDUP_MODE": "enforce", "TRIGGER_MODE": "shadow"}
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=[item_dup]),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini),
                mock.patch.object(radar, "load_krx_master", return_value={c["code"]: dict(c) for c in radar.SEED_COMPANIES}),
                mock.patch.object(radar, "fetch_naver_quotes", return_value={}),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_msgs.append(m) or 1),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t0)

            # Blocked BEFORE Stage 2 -> 0 Stage 2 calls, 0 alerts!
            self.assertEqual(stage2_calls[0], 0)
            self.assertEqual(len(sent_msgs), 0)

            # Now score 9.2 (>= 8.0 + 1.0) -> escalated pass-through with [후속] prefix!
            def fake_gemini_esc(api_key, models, sp, up, **kw):
                if sp == radar.SCREENING_PROMPT:
                    cls = json.loads(up)["clusters"]
                    return {
                        "selected_events": [{
                            "cluster_id": cls[0]["cluster_id"],
                            "catalyst_score": 9.2,
                            "theme_key": "컬리_최초보도",
                            "first_order_keywords": ["정보보안"],
                            "second_order_keywords": ["사이버보안"],
                        }]
                    }, "gemini-3.5-flash-lite"
                stage2_calls[0] += 1
                return {"title": "컬리 해킹 정부 전수조사 확대", "event_summary": "요약", "stocks": [{"code": "263860", "name": "지니언스"}]}, "gemini-3.6-flash"

            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=[item_esc]),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini_esc),
                mock.patch.object(radar, "load_krx_master", return_value={c["code"]: dict(c) for c in radar.SEED_COMPANIES}),
                mock.patch.object(radar, "fetch_naver_quotes", return_value={}),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_msgs.append(m) or 1),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t0 + timedelta(minutes=5))

            self.assertEqual(stage2_calls[0], 1)
            self.assertEqual(len(sent_msgs), 1)
            self.assertIn("[후속] 컬리 해킹 정부 전수조사 확대", sent_msgs[0])

    def test_pr3_stage1_trigger_hs_political_guard_and_rpd_tiers(self) -> None:
        now = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            st = radar.StateStore(Path(tmp) / "state.json")
            st.last_stage1_at = (now - timedelta(minutes=2)).isoformat()

            # 1) Plain single article -> waiting (should_run=False)
            plain = [radar.NewsItem("지역 축제 주말 개최 안내", "https://ex.com/1", "매일경제_사회", "사회/사건", first_seen=now.isoformat())]
            res_wait = radar.evaluate_stage1_trigger(plain, radar.cluster_news_ranked(plain, now=now), st, now=now, market_window=True)
            self.assertFalse(res_wait["should_run"])
            self.assertEqual(res_wait["reason"], "waiting_batch_or_trigger")

            # 2) Political multi-outlet story MUST NOT trigger multi_outlet!
            pol = [
                radar.NewsItem("민주당 여야 탄핵안 국회 본회의 공방", "https://ex.com/p1", "연합뉴스_최신", "속보", first_seen=now.isoformat()),
                radar.NewsItem("민주당 여야 탄핵안 국회 본회의 공방 격화", "https://ex.com/p2", "매일경제_사회", "사회/사건", first_seen=now.isoformat()),
            ]
            res_pol = radar.evaluate_stage1_trigger(pol, radar.cluster_news_ranked(pol, now=now), st, now=now, market_window=True)
            self.assertFalse(res_pol["should_run"])

            # 3) Non-political multi-outlet story triggers multi_outlet!
            multi = [
                radar.NewsItem("차세대 전고체 배터리 상용화 일정 앞당겨", "https://ex.com/m1", "연합뉴스_산업", "경제/산업", first_seen=now.isoformat()),
                radar.NewsItem("차세대 전고체 배터리 상용화 일정 가속", "https://ex.com/m2", "매일경제_경제", "경제/산업", first_seen=now.isoformat()),
            ]
            res_multi = radar.evaluate_stage1_trigger(multi, radar.cluster_news_ranked(multi, now=now), st, now=now, market_window=True)
            self.assertTrue(res_multi["should_run"])
            self.assertEqual(res_multi["reason"], "multi_outlet")

            # 4) RPD at 92% (460 / 500) -> h_only blocks multi_outlet & batch, but allows Hard keyword!
            pt_day = radar.pt_date_str(now)
            st.daily_ai_calls[pt_day] = {"gemini-3.5-flash-lite": {"requests": 460, "stage1": 460, "stage2": 0}}
            res_blocked = radar.evaluate_stage1_trigger(multi, radar.cluster_news_ranked(multi, now=now), st, now=now, market_window=True, daily_rpd_limit=500)
            self.assertFalse(res_blocked["should_run"])
            self.assertEqual(res_blocked["rpd_tier"], "h_only")
            self.assertEqual(res_blocked["reason"], "rpd_90_h_only_guard")

            hard_item = [radar.NewsItem("대형 통신사 랜섬웨어 해킹 사고 발생", "https://ex.com/h1", "연합뉴스_최신", "속보", first_seen=now.isoformat())]
            res_hard = radar.evaluate_stage1_trigger(hard_item, radar.cluster_news_ranked(hard_item, now=now), st, now=now, market_window=True, daily_rpd_limit=500)
            self.assertTrue(res_hard["should_run"])
            self.assertEqual(res_hard["reason"], "hard_keyword")

    def test_daemon_intervals_default_feeds_and_paused_heartbeat(self) -> None:
        # Adaptive intervals
        self.assertEqual(radar.resolve_daemon_interval_sec(datetime(2026, 11, 20, 9, 30, tzinfo=KST)), (60, "정규장/장전"))
        self.assertEqual(radar.resolve_daemon_interval_sec(datetime(2026, 11, 20, 16, 30, tzinfo=KST)), (180, "장전초/장후"))
        self.assertEqual(radar.resolve_daemon_interval_sec(datetime(2026, 11, 20, 23, 0, tzinfo=KST)), (600, "심야/주말"))
        self.assertEqual(radar.resolve_daemon_interval_sec(datetime(2026, 11, 21, 10, 0, tzinfo=KST)), (600, "심야/주말"))

        # Daemon default feeds include 전자신문_속보 & 뉴시스_속보 (12 feeds total)
        daemon_feeds = radar.resolve_active_feeds(None, daemon_mode=True)
        assert daemon_feeds is not None
        names = {f["name"] for f in daemon_feeds}
        self.assertIn("전자신문_속보", names)
        self.assertIn("뉴시스_속보", names)
        self.assertEqual(len(daemon_feeds), 12)

        # Paused runtime_config prevents run_live call and records status.json
        t0 = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            radar.save_runtime_config(tmp_dir, {"paused": True})
            with mock.patch.object(radar, "run_live", side_effect=AssertionError("paused daemon must not call run_live")):
                radar.run_daemon(dry_run=True, data_dir=tmp_dir, now_fn=lambda: t0, max_iterations=1)
            status = json.loads((tmp_dir / "status.json").read_text(encoding="utf-8"))
            self.assertEqual(status["state"], "stopped")  # exited cleanly after max_iterations=1

    def test_dashboard_http_api_auth_config_and_probe(self) -> None:
        import base64
        import threading as _th
        import dashboard

        now = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            st = radar.StateStore(tmp_dir / "state.json")
            st.mark_alerted("테스트_알림", now=now, final_codes=["446840"], catalyst_score=8.7, title="테스트 알림 제목")
            st.save(now=now)
            radar.write_status_snapshot(
                tmp_dir,
                {
                    "pid": 12345,
                    "mode": "daemon",
                    "state": "running",
                    "heartbeat_at": datetime.now(KST).isoformat(),
                    "poll_interval_sec": 60,
                    "window_label": "정규장/장전",
                },
            )

            srv = dashboard.create_dashboard_server("127.0.0.1", 0, data_dir=tmp_dir, password="secret")
            port = srv.server_address[1]
            t = _th.Thread(target=srv.serve_forever, daemon=True)
            t.start()
            auth_hdr = "Basic " + base64.b64encode(b"admin:secret").decode("ascii")
            base = f"http://127.0.0.1:{port}"
            try:
                # 1) Unauthenticated request -> 401
                with self.assertRaises(urllib.error.HTTPError) as ctx:
                    urllib.request.urlopen(f"{base}/api/overview", timeout=5)
                self.assertEqual(ctx.exception.code, 401)

                # 2) Authenticated HTML page -> 200
                req_html = urllib.request.Request(f"{base}/", headers={"Authorization": auth_hdr})
                with urllib.request.urlopen(req_html, timeout=5) as resp:
                    self.assertEqual(resp.status, 200)
                    self.assertIn("News Alpha Radar", resp.read().decode("utf-8"))

                # 3) Authenticated overview -> returns daemon alive + recent alert
                req_ov = urllib.request.Request(f"{base}/api/overview", headers={"Authorization": auth_hdr})
                with urllib.request.urlopen(req_ov, timeout=5) as resp:
                    ov = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(ov["daemon"]["alive"])
                self.assertEqual(len(ov["recent_alerts"]), 1)
                self.assertEqual(ov["recent_alerts"][0]["title"], "테스트 알림 제목")

                # 4) POST /api/config updates runtime_config.json
                body = json.dumps({"min_catalyst_score": 8.0, "dedup_mode": "shadow", "enabled_feeds": ["연합뉴스_최신", "뉴시스_속보"]}).encode("utf-8")
                req_cfg = urllib.request.Request(f"{base}/api/config", data=body, headers={"Authorization": auth_hdr, "Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req_cfg, timeout=5) as resp:
                    cfg_res = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(cfg_res["ok"])
                saved_cfg = radar.load_runtime_config(tmp_dir)
                self.assertEqual(saved_cfg["min_catalyst_score"], 8.0)
                self.assertEqual(saved_cfg["enabled_feeds"], ["연합뉴스_최신", "뉴시스_속보"])

                # 5) POST /api/actions/scan-now wakes running daemon via trigger_now file
                req_scan = urllib.request.Request(f"{base}/api/actions/scan-now", data=b"{}", headers={"Authorization": auth_hdr, "Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req_scan, timeout=5) as resp:
                    scan_res = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(scan_res["mode"], "woke_daemon")
                self.assertTrue((tmp_dir / "trigger_now").exists())

                # 6) POST /api/actions/probe-feeds returns mocked rows
                fake_rows = [{"name": "연합뉴스_최신", "status": 200, "items": 25, "elapsed": 0.4, "lag_min": 5.0, "error": None, "sample_title": "속보"}]
                with mock.patch.object(radar, "probe_feeds", return_value=fake_rows):
                    req_probe = urllib.request.Request(f"{base}/api/actions/probe-feeds", data=b"{}", headers={"Authorization": auth_hdr, "Content-Type": "application/json"}, method="POST")
                    with urllib.request.urlopen(req_probe, timeout=5) as resp:
                        probe_res = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(probe_res["ok"])
                self.assertEqual(len(probe_res["rows"]), 1)

                # 7) POST /api/actions/self-update returns structured result
                fake_upd = {"ok": True, "status": "UPDATED", "updated": True, "before": "50f0c0c", "after": "abc1234", "message": "업데이트 완료"}
                with mock.patch.object(dashboard, "run_self_update", return_value=fake_upd):
                    req_upd = urllib.request.Request(f"{base}/api/actions/self-update", data=b"{}", headers={"Authorization": auth_hdr, "Content-Type": "application/json"}, method="POST")
                    with urllib.request.urlopen(req_upd, timeout=5) as resp:
                        upd_res = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(upd_res["ok"])
                self.assertTrue(upd_res["updated"])
                self.assertEqual(upd_res["after"], "abc1234")
            finally:
                srv.shutdown()
                srv.server_close()


class TestRealtimeCallbackFlashAndSelfUpdate(unittest.TestCase):
    def test_telegram_listener_parsing_spam_filter_and_inbox_enqueue(self) -> None:
        import telegram_listener

        ch = telegram_listener.parse_channel_list("@Yonhap_News, https://t.me/fast_kr/123\nt.me/AlphaFeed?foo=1\nyonhap_news")
        self.assertEqual(ch, ["Yonhap_News", "fast_kr", "AlphaFeed"])

        now = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
        # Spam & too-short messages filtered
        self.assertIsNone(telegram_listener.extract_message_payload("[광고] 무료리딩방 급등주 추천인 코드", "스팸채널", 1, now=now))
        self.assertIsNone(telegram_listener.extract_message_payload("짧음", "속보", 2, now=now))

        # Valid breaking message with embedded URL
        ev = telegram_listener.extract_message_payload(
            "**[속보] 대형 통신사 네트워크 해킹 피해 확산**\n정부 긴급 조사 착수 https://yna.co.kr/view/123",
            "연합속보",
            42,
            channel_username="yonhap_news",
            now=now,
        )
        assert ev is not None
        self.assertEqual(ev["priority"], 0)
        self.assertEqual(ev["url"], "https://yna.co.kr/view/123")
        self.assertIn("대형 통신사 네트워크 해킹 피해 확산", ev["title"])
        self.assertEqual(ev["source"], "텔레그램_연합속보")

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            written = telegram_listener.enqueue_inbox_event(tmp_dir, ev)
            self.assertTrue(written.exists())
            self.assertEqual((tmp_dir / "trigger_now").read_text(encoding="utf-8"), "inbox")
            drained = radar.ingest_inbox_events(tmp_dir, now=now)
            self.assertEqual(len(drained), 1)
            self.assertEqual(drained[0].priority, 0)
            self.assertFalse(written.exists())  # consumed!

    def test_inbox_fastpath_bypasses_rss_and_sends_two_stage_flash_then_deep(self) -> None:
        import telegram_listener

        t0 = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            # Pre-seed last_stage1_at 1 minute ago so normal waiting batch would NOT fire
            st = radar.StateStore(tmp_dir / "state.json")
            st.last_stage1_at = (t0 - timedelta(minutes=1)).isoformat()
            st.save(now=t0)

            ev = telegram_listener.extract_message_payload(
                "신규 차세대 위성통신 국산화 프로젝트 정부 본사업 확정 https://ex.com/sat",
                "속보봇",
                77,
                now=t0,
            )
            assert ev is not None
            telegram_listener.enqueue_inbox_event(tmp_dir, ev)

            sent_msgs: list[str] = []

            def fake_gemini(api_key, models, sp, up, **kw):
                if sp == radar.SCREENING_PROMPT:
                    cls = json.loads(up)["clusters"]
                    return {
                        "selected_events": [{
                            "cluster_id": cls[0]["cluster_id"],
                            "catalyst_score": 8.6,
                            "theme_key": "위성통신_국산화",
                            "event_summary": "정부가 위성통신 국산화 본사업을 확정했습니다.",
                            "why_notable": "보안·전파탐지 및 위성 단말 장비 2차 수혜 기대",
                            "first_order_keywords": ["위성"],
                            "second_order_keywords": ["전파탐지"],
                        }]
                    }, "gemini-3.5-flash-lite"
                return {
                    "title": "위성통신 국산화 본사업 확정 → 전파·보안장비 2차 수혜",
                    "event_summary": "정부 본사업 확정",
                    "stocks": [{"code": "446840", "name": "지슨", "direction": "상승수혜"}],
                }, "gemini-3.6-flash"

            env = {"GEMINI_API_KEY": "k", "TELEGRAM_BOT_TOKEN": "t", "TELEGRAM_CHAT_ID": "c"}
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(radar, "fetch_rss_news", side_effect=AssertionError("inbox_only must skip RSS")),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini),
                mock.patch.object(radar, "load_krx_master", return_value={c["code"]: dict(c) for c in radar.SEED_COMPANIES}),
                mock.patch.object(radar, "fetch_naver_quotes", return_value={}),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_msgs.append(m) or 1),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t0, daemon_mode=True, inbox_only=True)

            # 2-stage push: 1st message is ⚡ [속보 1보], 2nd message is 🚨 [이슈 레이더] deep report!
            self.assertEqual(len(sent_msgs), 2)
            self.assertIn("⚡ <b>[속보 1보]", sent_msgs[0])
            self.assertIn("🚨 <b>[이슈 레이더]", sent_msgs[1])
            self.assertIn("지슨", sent_msgs[1])

            recs = [json.loads(l) for l in (tmp_dir / "trace.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertTrue(any(r["type"] == "trigger_decision" and r["reason"] == "realtime_callback_inbox" for r in recs))
            self.assertTrue(any(r["type"] == "flash_alert_sent" for r in recs))

    def test_self_update_propagates_python_bin_and_surfaces_rollback_error(self) -> None:
        import subprocess
        import dashboard

        # 1) Updater systemd unit pins PYTHON_BIN=__PYTHON__ and update.sh enforces Python 3.11+
        svc_txt = (REPO_DIR / "deploy" / "ubuntu" / "news-alpha-updater.service").read_text(encoding="utf-8")
        self.assertIn("Environment=PYTHON_BIN=__PYTHON__", svc_txt)
        upd_txt = (REPO_DIR / "deploy" / "ubuntu" / "update.sh").read_text(encoding="utf-8")
        self.assertIn("_resolve_python_bin", upd_txt)
        self.assertIn("sys.version_info >= (3, 11)", upd_txt)

        # 2) dashboard.run_self_update passes sys.executable via PYTHON_BIN and surfaces TEST_ERROR on ROLLBACK
        captured: dict[str, object] = {}

        def fake_run(cmd, **kwargs):
            captured["cmd"] = cmd
            captured["env"] = kwargs.get("env") or {}
            return subprocess.CompletedProcess(
                cmd,
                1,
                stdout="STATUS=ROLLBACK\nPYTHON_BIN=/opt/py312/python3\nTEST_ERROR=AssertionError: boom\n",
                stderr="",
            )

        with mock.patch.object(dashboard.subprocess, "run", side_effect=fake_run):
            res = dashboard.run_self_update(REPO_DIR)

        self.assertFalse(res["ok"])
        self.assertEqual(res["status"], "ROLLBACK")
        self.assertEqual(res["python_bin"], "/opt/py312/python3")
        self.assertIn("AssertionError: boom", res["message"])
        self.assertEqual(res["error"], res["message"])
        self.assertEqual(captured["env"]["PYTHON_BIN"], sys.executable)

        # 3) update.sh bash syntax & resolver picks installed systemd ExecStart even when PATH has fake old python3
        bash_chk = subprocess.run(["bash", "-n", str(REPO_DIR / "deploy" / "ubuntu" / "update.sh")], capture_output=True, text=True)
        self.assertEqual(bash_chk.returncode, 0, bash_chk.stderr)

        with tempfile.TemporaryDirectory() as tmp:
            fake_home = Path(tmp)
            fake_bin = fake_home / "fakebin"
            fake_bin.mkdir()
            # Fake old python3 (exits 1 on version check like Python 3.8!)
            fake_py = fake_bin / "python3"
            fake_py.write_text("#!/usr/bin/env bash\nexit 1\n", encoding="utf-8")
            fake_py.chmod(0o755)
            unit_dir = fake_home / ".config" / "systemd" / "user"
            unit_dir.mkdir(parents=True)
            (unit_dir / "news-alpha-dashboard.service").write_text(
                f"[Service]\nExecStart={sys.executable} -u /tmp/dashboard.py\n",
                encoding="utf-8",
            )
            probe_cmd = (
                f'HOME="{fake_home}" PATH="{fake_bin}:/usr/bin:/bin" '
                f'bash -c \'UNIT_DIR="$HOME/.config/systemd/user"; REPO_DIR="{REPO_DIR}"; '
                f'eval "$(sed -n "/^_is_py311_plus/,/^PYTHON_BIN=/p" "{REPO_DIR}/deploy/ubuntu/update.sh")"; '
                f'echo "$PYTHON_BIN"\''
            )
            res_py = subprocess.run(["bash", "-c", probe_cmd], capture_output=True, text=True)
            self.assertEqual(res_py.returncode, 0, res_py.stderr)
            self.assertEqual(res_py.stdout.strip(), sys.executable)


class TestBacktestFixesAndPerformanceTracking(unittest.TestCase):
    def test_split_alert_limits_resolution_and_legacy_max_daily_compat(self) -> None:
        # Pure defaults: market 8, offhours 3, daily 11
        lim_default = radar.resolve_alert_limits({})
        self.assertEqual(lim_default["max_market_alerts"], 8)
        self.assertEqual(lim_default["max_offhours_alerts"], 3)
        self.assertEqual(lim_default["max_daily_alerts"], 11)
        self.assertFalse(lim_default["offhours_digest_enabled"])
        self.assertEqual(lim_default["offhours_digest_time"], "07:50")

        # Legacy MAX_DAILY_ALERTS=10 only: offhours capped at 3 so market still gets >=7 (up to 8)
        with mock.patch.dict(os.environ, {"MAX_DAILY_ALERTS": "10"}, clear=False):
            lim_leg10 = radar.resolve_alert_limits({})
        self.assertEqual(lim_leg10["max_daily_alerts"], 10)
        self.assertEqual(lim_leg10["max_market_alerts"], 8)
        self.assertEqual(lim_leg10["max_offhours_alerts"], 3)

        # Legacy MAX_DAILY_ALERTS=5 only
        with mock.patch.dict(os.environ, {"MAX_DAILY_ALERTS": "5"}, clear=False):
            lim_leg5 = radar.resolve_alert_limits({})
        self.assertEqual(lim_leg5["max_daily_alerts"], 5)
        self.assertEqual(lim_leg5["max_market_alerts"], 5)
        self.assertEqual(lim_leg5["max_offhours_alerts"], 1)

        # Explicit split env vars
        with mock.patch.dict(
            os.environ,
            {"MAX_MARKET_ALERTS": "6", "MAX_OFFHOURS_ALERTS": "2", "OFFHOURS_DIGEST_ENABLED": "true", "OFFHOURS_DIGEST_TIME": "07:45"},
            clear=False,
        ):
            lim_split = radar.resolve_alert_limits({})
        self.assertEqual(lim_split["max_market_alerts"], 6)
        self.assertEqual(lim_split["max_offhours_alerts"], 2)
        self.assertEqual(lim_split["max_daily_alerts"], 8)
        self.assertTrue(lim_split["offhours_digest_enabled"])
        self.assertEqual(lim_split["offhours_digest_time"], "07:45")

        # runtime_config split overrides stale legacy env MAX_DAILY_ALERTS=10
        with mock.patch.dict(os.environ, {"MAX_DAILY_ALERTS": "10"}, clear=False):
            lim_rc = radar.resolve_alert_limits({"max_market_alerts": 10, "max_offhours_alerts": 4})
        self.assertEqual(lim_rc["max_market_alerts"], 10)
        self.assertEqual(lim_rc["max_offhours_alerts"], 4)
        self.assertEqual(lim_rc["max_daily_alerts"], 14)

    def test_offhours_limit_preserves_market_window_budget_e2e(self) -> None:
        t_night1 = datetime(2026, 11, 20, 1, 0, tzinfo=KST)   # Fri 01:00 (offhours)
        t_night2 = datetime(2026, 11, 20, 2, 0, tzinfo=KST)   # Fri 02:00 (offhours)
        t_market = datetime(2026, 11, 20, 9, 15, tzinfo=KST)  # Fri 09:15 (market window)

        comps = {c["code"]: dict(c) for c in radar.SEED_COMPANIES}
        sent_msgs: list[str] = []

        night_items = [
            radar.NewsItem("초고압 변압기 대규모 정전 비상 대책", "https://ex.com/n1", "연합뉴스_최신", "속보", first_seen=t_night1.isoformat()),
            radar.NewsItem("가축전염병 전국 방역 비상 진단키트 긴급 투입", "https://ex.com/n2", "연합뉴스_사회", "사회/사건", first_seen=t_night1.isoformat()),
            radar.NewsItem("주요 해협 봉쇄 우려 해상운송 운임 급등", "https://ex.com/n3", "매일경제_경제", "경제/산업", first_seen=t_night1.isoformat()),
        ]
        market_item = radar.NewsItem(
            "협동로봇 국산화 대규모 수주 계약 체결", "https://ex.com/m1", "연합뉴스_산업", "경제/산업", first_seen=t_market.isoformat()
        )

        def fake_gemini(api_key, models, sp, up, **kw):
            data = json.loads(up)
            if sp == radar.SCREENING_PROMPT:
                evts = []
                for cl in data["clusters"]:
                    h = cl["headline"]
                    if "정전" in h:
                        evts.append({"cluster_id": cl["cluster_id"], "catalyst_score": 8.9, "theme_key": "정전_변압기", "second_order_keywords": ["전력기기"]})
                    elif "가축전염병" in h:
                        evts.append({"cluster_id": cl["cluster_id"], "catalyst_score": 8.6, "theme_key": "방역_진단", "second_order_keywords": ["감염병"]})
                    elif "해협" in h:
                        evts.append({"cluster_id": cl["cluster_id"], "catalyst_score": 8.2, "theme_key": "해협_해운", "second_order_keywords": ["조선해운"]})
                    elif "협동로봇" in h:
                        evts.append({"cluster_id": cl["cluster_id"], "catalyst_score": 8.7, "theme_key": "로봇_수주", "second_order_keywords": ["로봇"]})
                return {"selected_events": evts}, "gemini-3.5-flash-lite"

            h = data["event_cluster"]["headline"]
            if "정전" in h:
                return {"title": "정전 → 변압기 수혜", "event_summary": "정전", "stocks": [{"code": "298040", "name": "효성중공업", "direction": "상승수혜"}]}, "gemini-3.6-flash"
            if "가축전염병" in h:
                return {"title": "방역 → 진단키트 수혜", "event_summary": "방역", "stocks": [{"code": "096530", "name": "씨젠", "direction": "상승수혜"}]}, "gemini-3.6-flash"
            if "해협" in h:
                return {"title": "해협 봉쇄 → 해운 수혜", "event_summary": "해협", "stocks": [{"code": "011200", "name": "HMM", "direction": "상승수혜"}]}, "gemini-3.6-flash"
            return {"title": "로봇 수주 → 협동로봇 수혜", "event_summary": "로봇", "stocks": [{"code": "277810", "name": "레인보우로보틱스", "direction": "상승수혜"}]}, "gemini-3.6-flash"

        env = {
            "GEMINI_API_KEY": "k",
            "TELEGRAM_BOT_TOKEN": "t",
            "TELEGRAM_CHAT_ID": "c",
            "MAX_MARKET_ALERTS": "8",
            "MAX_OFFHOURS_ALERTS": "2",
            "OFFHOURS_DIGEST_ENABLED": "false",
            "MAX_ALERTS_PER_RUN": "5",
            "INSTANT_FLASH_ALERT": "false",
            "TRIGGER_MODE": "shadow",
            "DEDUP_MODE": "enforce",
        }
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini),
                mock.patch.object(radar, "load_krx_master", return_value=comps),
                mock.patch.object(radar, "fetch_naver_quotes", return_value={}),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_msgs.append(m) or 1),
            ):
                # 1) 01:00 KST (offhours): 3 events arrive -> capped at MAX_OFFHOURS_ALERTS=2!
                with mock.patch.object(radar, "fetch_rss_news", return_value=night_items):
                    radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t_night1, daemon_mode=True)
                self.assertEqual(len(sent_msgs), 2)
                st1 = radar.StateStore(tmp_dir / "state.json")
                self.assertEqual(st1.sent_offhours_today(now=t_night1), 2)
                self.assertEqual(st1.sent_market_today(now=t_night1), 0)

                # 2) 02:00 KST (still offhours): offhours limit reached -> skips AI calls completely!
                with mock.patch.object(radar, "fetch_rss_news", side_effect=AssertionError("offhours limit must skip RSS")):
                    radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t_night2, daemon_mode=True)
                self.assertEqual(len(sent_msgs), 2)

                # 3) 09:15 KST (market window, SAME KST day!): market budget is 0/8 -> sends immediately!
                # Also clears deferred (if not expired) or handles fresh market news
                st_pre_mkt = radar.StateStore(tmp_dir / "state.json")
                st_pre_mkt.deferred = []
                st_pre_mkt.save(now=t_market)
                with mock.patch.object(radar, "fetch_rss_news", return_value=[market_item]):
                    radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t_market, daemon_mode=True)
                self.assertEqual(len(sent_msgs), 3)
                self.assertIn("레인보우로보틱스", sent_msgs[2])
                st2 = radar.StateStore(tmp_dir / "state.json")
                self.assertEqual(st2.sent_market_today(now=t_market), 1)
                self.assertEqual(st2.sent_offhours_today(now=t_market), 2)

    def test_offhours_digest_queues_overnight_and_sends_single_0750_summary(self) -> None:
        t_thu_night = datetime(2026, 11, 19, 22, 30, tzinfo=KST)
        t_fri_early = datetime(2026, 11, 20, 2, 15, tzinfo=KST)
        t_fri_0750 = datetime(2026, 11, 20, 7, 50, tzinfo=KST)
        t_fri_0755 = datetime(2026, 11, 20, 7, 55, tzinfo=KST)

        comps = {c["code"]: dict(c) for c in radar.SEED_COMPANIES}
        sent_msgs: list[str] = []

        item1 = radar.NewsItem("대규모 정전 확산 초고압 변압기 긴급 발주", "https://ex.com/d1", "연합뉴스_최신", "속보", first_seen=t_thu_night.isoformat())
        item2 = radar.NewsItem("방산 위성 발사체 대규모 수주 확정", "https://ex.com/d2", "매일경제_경제", "경제/산업", first_seen=t_fri_early.isoformat())
        item3 = radar.NewsItem("상하수도 수자원 정수 플랜트 정부 대책", "https://ex.com/d3", "매일경제_사회", "사회/사건", first_seen=t_fri_early.isoformat())

        def fake_gemini(api_key, models, sp, up, **kw):
            data = json.loads(up)
            if sp == radar.SCREENING_PROMPT:
                evts = []
                for cl in data["clusters"]:
                    h = cl["headline"]
                    if "정전" in h:
                        evts.append({"cluster_id": cl["cluster_id"], "catalyst_score": 9.1, "theme_key": "정전_전력망", "second_order_keywords": ["전력기기"]})
                    elif "방산" in h:
                        evts.append({"cluster_id": cl["cluster_id"], "catalyst_score": 8.7, "theme_key": "방산_위성", "second_order_keywords": ["우주항공"]})
                    elif "수자원" in h:
                        evts.append({"cluster_id": cl["cluster_id"], "catalyst_score": 7.8, "theme_key": "수자원_플랜트", "second_order_keywords": ["수자원"]})
                return {"selected_events": evts}, "gemini-3.5-flash-lite"

            h = data["event_cluster"]["headline"]
            if "정전" in h:
                return {"title": "정전 비상 → 초고압 변압기 수혜", "event_summary": "변압기 발주", "stocks": [{"code": "298040", "name": "효성중공업", "direction": "상승수혜"}]}, "gemini-3.6-flash"
            if "방산" in h:
                return {"title": "방산 위성 수주 → 우주항공 수혜", "event_summary": "위성 수주", "stocks": [{"code": "012450", "name": "한화에어로스페이스", "direction": "상승수혜"}]}, "gemini-3.6-flash"
            return {"title": "수자원 플랜트 수혜", "event_summary": "수자원", "stocks": [{"code": "002150", "name": "도화엔지니어링", "direction": "상승수혜"}]}, "gemini-3.6-flash"

        def fake_quotes(codes):
            return {
                c: radar.StockQuote(c, 50000, 0, 0.0, 0, "CLOSE", radar.classify_stage(0.0, 0))
                for c in codes
            }

        env = {
            "GEMINI_API_KEY": "k",
            "TELEGRAM_BOT_TOKEN": "t",
            "TELEGRAM_CHAT_ID": "c",
            "MAX_MARKET_ALERTS": "8",
            "MAX_OFFHOURS_ALERTS": "2",
            "OFFHOURS_DIGEST_ENABLED": "true",
            "OFFHOURS_DIGEST_TIME": "07:50",
            "INSTANT_FLASH_ALERT": "true",
            "TRIGGER_MODE": "shadow",
        }
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini),
                mock.patch.object(radar, "load_krx_master", return_value=comps),
                mock.patch.object(radar, "fetch_naver_quotes", side_effect=fake_quotes),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_msgs.append(m) or 1),
            ):
                # Thu 22:30 -> queued, 0 sends (no flash alert either!)
                with mock.patch.object(radar, "fetch_rss_news", return_value=[item1]):
                    radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t_thu_night, daemon_mode=True)
                self.assertEqual(len(sent_msgs), 0)

                # Fri 02:15 -> 2 more queued (total 3 in overnight_queue), still 0 sends!
                with mock.patch.object(radar, "fetch_rss_news", return_value=[item2, item3]):
                    radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t_fri_early, daemon_mode=True)
                self.assertEqual(len(sent_msgs), 0)
                st_q = radar.StateStore(tmp_dir / "state.json")
                self.assertEqual(len(st_q.overnight_queue), 3)

                # Fri 07:50 -> ONE bundled pre-market digest containing top 2 (scores 9.1 & 8.7)!
                with mock.patch.object(radar, "fetch_rss_news", return_value=[]):
                    radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t_fri_0750, daemon_mode=True)
                self.assertEqual(len(sent_msgs), 1)
                self.assertIn("[장전 요약]", sent_msgs[0])
                self.assertIn("효성중공업", sent_msgs[0])
                self.assertIn("한화에어로스페이스", sent_msgs[0])
                self.assertNotIn("도화엔지니어링", sent_msgs[0])  # 3rd lowest score excluded by MAX_OFFHOURS_ALERTS=2

                st_done = radar.StateStore(tmp_dir / "state.json")
                self.assertEqual(st_done.overnight_queue, [])
                self.assertEqual(st_done.last_digest_date, "2026-11-20")
                self.assertEqual(st_done.sent_offhours_today(now=t_fri_0750), 1)
                self.assertEqual(st_done.sent_market_today(now=t_fri_0750), 0)

                # Performance records created for both stocks in the digest!
                perf_rows = radar.PerformanceTracker(tmp_dir / "performance.jsonl").load_records()
                self.assertEqual({r["code"] for r in perf_rows}, {"298040", "012450"})

                # Fri 07:55 -> must NOT re-send digest
                with mock.patch.object(radar, "fetch_rss_news", return_value=[]):
                    radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t_fri_0755, daemon_mode=True)
                self.assertEqual(len(sent_msgs), 1)

    def test_stock_code_24h_dedup_excludes_repeats_blocks_empty_and_allows_escalation(self) -> None:
        now = datetime(2026, 11, 20, 11, 0, tzinfo=KST)
        recent = [
            {
                "ts": (now - timedelta(hours=5)).isoformat(),
                "theme_key": "이전_보안이슈",
                "final_codes": ["263860"],
                "catalyst_score": 8.0,
            },
            {
                "ts": (now - timedelta(hours=26)).isoformat(),  # > 24h ago -> expired!
                "theme_key": "하루전_이슈",
                "final_codes": ["053800"],
                "catalyst_score": 8.0,
            },
        ]

        # 1) Partial overlap: 263860 excluded, 053800 (>24h) kept
        res_partial = radar.filter_recently_alerted_stocks(
            [{"code": "263860", "name": "지니언스"}, {"code": "053800", "name": "안랩"}],
            catalyst_score=8.4,
            recent_alerts=recent,
            now=now,
            window_hours=24,
        )
        self.assertEqual([s["code"] for s in res_partial["kept_stocks"]], ["053800"])
        self.assertEqual(res_partial["excluded_codes"], ["263860"])
        self.assertFalse(res_partial["all_filtered"])
        self.assertFalse(res_partial["escalated"])

        # 2) All stocks recently alerted and score < 8.0 + 1.0 -> all_filtered=True (do not send!)
        res_empty = radar.filter_recently_alerted_stocks(
            [{"code": "263860", "name": "지니언스"}],
            catalyst_score=8.5,
            recent_alerts=recent,
            now=now,
            window_hours=24,
        )
        self.assertEqual(res_empty["kept_stocks"], [])
        self.assertTrue(res_empty["all_filtered"])

        # 3) Escalation (+1.0 delta: 9.1 >= 8.0 + 1.0) -> allowed with escalated=True!
        res_esc = radar.filter_recently_alerted_stocks(
            [{"code": "263860", "name": "지니언스"}],
            catalyst_score=9.1,
            recent_alerts=recent,
            now=now,
            window_hours=24,
        )
        self.assertEqual([s["code"] for s in res_esc["kept_stocks"]], ["263860"])
        self.assertTrue(res_esc["escalated"])
        self.assertFalse(res_esc["all_filtered"])

        # 4) E2E check: completely different theme_key/tokens recommending only recently-alerted 263860 -> blocked!
        comps = {c["code"]: dict(c) for c in radar.SEED_COMPANIES}
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            st = radar.StateStore(tmp_dir / "state.json")
            st.mark_alerted("전혀다른_이전알림", now=now - timedelta(hours=3), final_codes=["263860"], catalyst_score=8.0, sector="기타")
            st.save(now=now)

            item_same_stock = radar.NewsItem("기업 클라우드 네트워크 관제 계약 확대", "https://ex.com/s1", "지디넷코리아_IT", "IT/신기술", first_seen=now.isoformat())
            sent_e2e: list[str] = []

            def fake_gemini_only_263860(api_key, models, sp, up, **kw):
                if sp == radar.SCREENING_PROMPT:
                    cid = json.loads(up)["clusters"][0]["cluster_id"]
                    return {"selected_events": [{"cluster_id": cid, "catalyst_score": 8.3, "theme_key": "신규_네트워크_관제", "second_order_keywords": ["사이버보안"]}]}, "gemini-3.5-flash-lite"
                return {"title": "네트워크 관제 확대", "event_summary": "관제", "stocks": [{"code": "263860", "name": "지니언스", "direction": "상승수혜"}]}, "gemini-3.6-flash"

            env = {"GEMINI_API_KEY": "k", "TELEGRAM_BOT_TOKEN": "t", "TELEGRAM_CHAT_ID": "c", "INSTANT_FLASH_ALERT": "false", "TRIGGER_MODE": "shadow"}
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=[item_same_stock]),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini_only_263860),
                mock.patch.object(radar, "load_krx_master", return_value=comps),
                mock.patch.object(radar, "fetch_naver_quotes", return_value={}),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_e2e.append(m) or 1),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: now, daemon_mode=True)

            # Stage 2 picked only 263860 which was alerted 3h ago -> remaining stocks == 0 -> NO alert sent!
            self.assertEqual(len(sent_e2e), 0)

            # Now score 9.2 (>= 8.0 + 1.0) -> allowed with [후속] prefix!
            item_esc_stock = radar.NewsItem("정부 클라우드 보안 의무화 법안 통과", "https://ex.com/s2", "지디넷코리아_IT", "IT/신기술", first_seen=now.isoformat())

            def fake_gemini_esc_263860(api_key, models, sp, up, **kw):
                if sp == radar.SCREENING_PROMPT:
                    cid = json.loads(up)["clusters"][0]["cluster_id"]
                    return {"selected_events": [{"cluster_id": cid, "catalyst_score": 9.2, "theme_key": "클라우드_보안_의무화", "second_order_keywords": ["사이버보안"]}]}, "gemini-3.5-flash-lite"
                return {"title": "클라우드 보안 의무화 통과", "event_summary": "법안 통과", "stocks": [{"code": "263860", "name": "지니언스", "direction": "상승수혜"}]}, "gemini-3.6-flash"

            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=[item_esc_stock]),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini_esc_263860),
                mock.patch.object(radar, "load_krx_master", return_value=comps),
                mock.patch.object(radar, "fetch_naver_quotes", return_value={}),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_e2e.append(m) or 1),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: now + timedelta(minutes=5), daemon_mode=True)

            self.assertEqual(len(sent_e2e), 1)
            self.assertIn("[후속] 클라우드 보안 의무화 통과", sent_e2e[0])

    def test_sector_daily_limit_caps_same_sector_and_allows_other_sectors(self) -> None:
        # Canonical sector normalization across differently worded cyber themes
        self.assertEqual(radar.classify_alert_sector(theme_key="개인정보유출_대응", second_keywords=["정보보안"]), "사이버보안")
        self.assertEqual(radar.classify_alert_sector(theme_key="랜섬웨어_긴급보안", second_keywords=["네트워크보안"]), "사이버보안")
        self.assertEqual(radar.classify_alert_sector(theme_key="초고압변압기_수주", second_keywords=["전력기기"]), "전력·에너지")

        t0 = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
        comps = {
            "263860": {"code": "263860", "name": "지니언스", "market": "KOSDAQ", "sector": "SW", "products": "정보보안, 사이버보안, NAC"},
            "053800": {"code": "053800", "name": "안랩", "market": "KOSDAQ", "sector": "SW", "products": "정보보안, 보안관제, 방화벽"},
            "300001": {"code": "300001", "name": "시큐테스트", "market": "KOSDAQ", "sector": "SW", "products": "정보보안, 클라우드보안, EDR"},
            "298040": {"code": "298040", "name": "효성중공업", "market": "KOSPI", "sector": "전력", "products": "초고압 변압기, 전력기기, ESS"},
        }
        items = [
            radar.NewsItem("A은행 대규모 해킹 개인정보 유출", "https://ex.com/c1", "연합뉴스_사회", "사회/사건", first_seen=t0.isoformat()),
            radar.NewsItem("B병원 랜섬웨어 감염 전산 마비", "https://ex.com/c2", "매일경제_사회", "사회/사건", first_seen=t0.isoformat()),
            radar.NewsItem("C쇼핑몰 고객정보유출 디도스 공격", "https://ex.com/c3", "지디넷코리아_IT", "IT/신기술", first_seen=t0.isoformat()),
            radar.NewsItem("수도권 데이터센터 대규모 정전 비상", "https://ex.com/p1", "연합뉴스_산업", "경제/산업", first_seen=t0.isoformat()),
        ]
        sent_msgs: list[str] = []
        s2_headlines: list[str] = []

        def fake_gemini(api_key, models, sp, up, **kw):
            data = json.loads(up)
            if sp == radar.SCREENING_PROMPT:
                evts = []
                for cl in data["clusters"]:
                    h = cl["headline"]
                    if "A은행" in h:
                        evts.append({"cluster_id": cl["cluster_id"], "catalyst_score": 9.0, "theme_key": "A은행_해킹", "second_order_keywords": ["사이버보안"]})
                    elif "B병원" in h:
                        evts.append({"cluster_id": cl["cluster_id"], "catalyst_score": 8.8, "theme_key": "B병원_랜섬웨어", "second_order_keywords": ["정보보안"]})
                    elif "C쇼핑몰" in h:
                        evts.append({"cluster_id": cl["cluster_id"], "catalyst_score": 8.5, "theme_key": "C쇼핑몰_디도스", "second_order_keywords": ["클라우드보안"]})
                    elif "정전" in h:
                        evts.append({"cluster_id": cl["cluster_id"], "catalyst_score": 8.4, "theme_key": "데이터센터_정전", "second_order_keywords": ["전력기기"]})
                return {"selected_events": evts}, "gemini-3.5-flash-lite"

            h = data["event_cluster"]["headline"]
            s2_headlines.append(h)
            if "A은행" in h:
                return {"title": "A은행 해킹 보안 수혜", "event_summary": "A은행", "stocks": [{"code": "263860", "name": "지니언스", "direction": "상승수혜"}]}, "gemini-3.6-flash"
            if "B병원" in h:
                return {"title": "B병원 랜섬웨어 보안 수혜", "event_summary": "B병원", "stocks": [{"code": "053800", "name": "안랩", "direction": "상승수혜"}]}, "gemini-3.6-flash"
            if "C쇼핑몰" in h:
                return {"title": "C쇼핑몰 디도스 보안 수혜", "event_summary": "C쇼핑몰", "stocks": [{"code": "300001", "name": "시큐테스트", "direction": "상승수혜"}]}, "gemini-3.6-flash"
            return {"title": "데이터센터 정전 전력기기 수혜", "event_summary": "정전", "stocks": [{"code": "298040", "name": "효성중공업", "direction": "상승수혜"}]}, "gemini-3.6-flash"

        env = {
            "GEMINI_API_KEY": "k",
            "TELEGRAM_BOT_TOKEN": "t",
            "TELEGRAM_CHAT_ID": "c",
            "MAX_ALERTS_PER_RUN": "5",
            "MAX_DAILY_PER_SECTOR": "2",
            "INSTANT_FLASH_ALERT": "false",
            "TRIGGER_MODE": "shadow",
            "DEDUP_MODE": "shadow",
        }
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=items),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini),
                mock.patch.object(radar, "load_krx_master", return_value=comps),
                mock.patch.object(radar, "fetch_naver_quotes", return_value={}),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_msgs.append(m) or 1),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t0, daemon_mode=True)

            # 2 cybersecurity alerts + 1 power alert sent; 3rd cybersecurity alert blocked BEFORE Stage 2!
            self.assertEqual(len(sent_msgs), 3)
            self.assertFalse(any("C쇼핑몰" in m for m in sent_msgs))
            self.assertFalse(any("C쇼핑몰" in h for h in s2_headlines))
            self.assertTrue(any("효성중공업" in m for m in sent_msgs))

            st = radar.StateStore(tmp_dir / "state.json")
            self.assertEqual(st.sector_count_today("사이버보안", now=t0), 2)
            self.assertEqual(st.sector_count_today("전력·에너지", now=t0), 1)

    def test_performance_tracker_horizons_and_dashboard_direction_stats(self) -> None:
        import threading as _th
        import dashboard

        t_alert = datetime(2026, 11, 19, 10, 0, tzinfo=KST)      # Thu 10:00
        t_5m = datetime(2026, 11, 19, 10, 5, tzinfo=KST)         # Thu 10:05
        t_30m = datetime(2026, 11, 19, 10, 30, tzinfo=KST)       # Thu 10:30
        t_close = datetime(2026, 11, 19, 15, 30, tzinfo=KST)     # Thu 15:30
        t_next_close = datetime(2026, 11, 20, 15, 30, tzinfo=KST)  # Fri 15:30

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            tracker = radar.PerformanceTracker(tmp_dir / "performance.jsonl")
            init_quotes = {
                "298040": radar.StockQuote("298040", 10000, 100, 1.0, 50000, "OPEN", ""),
                "011200": radar.StockQuote("011200", 20000, -100, -0.5, 40000, "OPEN", ""),
                "042700": radar.StockQuote("042700", 15000, 300, 2.0, 60000, "OPEN", ""),
            }
            tracker.record_alert(
                alert_ts=t_alert,
                theme_key="테스트_성과",
                sector="전력·에너지",
                title="테스트 성과 알림",
                catalyst_score=8.5,
                window="market",
                stocks=[
                    {"code": "298040", "name": "효성중공업", "direction": "상승수혜"},
                    {"code": "011200", "name": "HMM", "direction": "하락주의"},
                    {"code": "042700", "name": "한미반도체", "direction": "옥석가리기주의"},
                ],
                quotes=init_quotes,
            )

            # +5m: 298040 +2.0%, 011200 -1.0%, 042700 -2.0%
            u5 = tracker.update_pending(
                now=t_5m,
                fetch_quotes_fn=lambda codes: {
                    "298040": radar.StockQuote("298040", 10200, 0, 0.0, 0, "OPEN", ""),
                    "011200": radar.StockQuote("011200", 19800, 0, 0.0, 0, "OPEN", ""),
                    "042700": radar.StockQuote("042700", 14700, 0, 0.0, 0, "OPEN", ""),
                },
            )
            self.assertEqual(u5, 3)

            # +30m: 298040 +5.0%, 011200 -2.5%, 042700 -4.0%
            u30 = tracker.update_pending(
                now=t_30m,
                fetch_quotes_fn=lambda codes: {
                    "298040": radar.StockQuote("298040", 10500, 0, 0.0, 0, "OPEN", ""),
                    "011200": radar.StockQuote("011200", 19500, 0, 0.0, 0, "OPEN", ""),
                    "042700": radar.StockQuote("042700", 14400, 0, 0.0, 0, "OPEN", ""),
                },
            )
            self.assertEqual(u30, 3)

            # Same-day close: 298040 +4.0%, 011200 -3.0%, 042700 -6.0%
            uc = tracker.update_pending(
                now=t_close,
                fetch_quotes_fn=lambda codes: {
                    "298040": radar.StockQuote("298040", 10400, 0, 0.0, 0, "CLOSE", ""),
                    "011200": radar.StockQuote("011200", 19400, 0, 0.0, 0, "CLOSE", ""),
                    "042700": radar.StockQuote("042700", 14100, 0, 0.0, 0, "CLOSE", ""),
                },
            )
            self.assertEqual(uc, 3)

            # Next-day close: 298040 +8.0%, 011200 -5.0%, 042700 -8.0%
            un = tracker.update_pending(
                now=t_next_close,
                fetch_quotes_fn=lambda codes: {
                    "298040": radar.StockQuote("298040", 10800, 0, 0.0, 0, "CLOSE", ""),
                    "011200": radar.StockQuote("011200", 19000, 0, 0.0, 0, "CLOSE", ""),
                    "042700": radar.StockQuote("042700", 13800, 0, 0.0, 0, "CLOSE", ""),
                },
            )
            self.assertEqual(un, 3)

            rows = tracker.load_records()
            self.assertEqual(len(rows), 3)
            self.assertTrue(all(r["complete"] for r in rows))
            by_code = {r["code"]: r for r in rows}
            self.assertEqual(by_code["298040"]["ret_5m"], 2.0)
            self.assertEqual(by_code["298040"]["ret_30m"], 5.0)
            self.assertEqual(by_code["298040"]["ret_close"], 4.0)
            self.assertEqual(by_code["298040"]["ret_next_close"], 8.0)
            self.assertEqual(by_code["011200"]["ret_next_close"], -5.0)
            self.assertEqual(by_code["042700"]["ret_next_close"], -8.0)

            # Overnight alert schedule skips weekend: Fri 22:00 -> close Mon 11-23, next_close Tue 11-24, 5m Mon 09:05
            fri_night = datetime(2026, 11, 20, 22, 0, tzinfo=KST)
            sched_wk = radar.PerformanceTracker._compute_schedule(fri_night)
            self.assertEqual(sched_wk["close_date"], "2026-11-23")
            self.assertEqual(sched_wk["next_close_date"], "2026-11-24")
            self.assertEqual(sched_wk["due_5m"], datetime(2026, 11, 23, 9, 5, tzinfo=KST))

            # Verify Dashboard /api/overview, /api/performance, and new config fields via POST /api/config
            srv = dashboard.create_dashboard_server("127.0.0.1", 0, data_dir=tmp_dir, password="")
            port = srv.server_address[1]
            t = _th.Thread(target=srv.serve_forever, daemon=True)
            t.start()
            base = f"http://127.0.0.1:{port}"
            try:
                with urllib.request.urlopen(f"{base}/api/performance", timeout=5) as resp:
                    perf = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(perf["total_records"], 3)
                self.assertEqual(perf["completed_records"], 3)
                self.assertEqual(perf["round_trip_cost_pct"], radar.ROUND_TRIP_COST_PCT)
                self.assertEqual(perf["by_direction"]["상승수혜"]["ret_next_close"]["avg_pct"], 8.0)
                self.assertEqual(perf["by_direction"]["상승수혜"]["ret_next_close"]["net_avg_pct"], 7.77)
                self.assertEqual(perf["by_direction"]["상승수혜"]["ret_next_close"]["win_rate_pct"], 100.0)
                self.assertEqual(perf["by_direction"]["상승수혜"]["ret_next_close"]["net_win_rate_pct"], 100.0)
                self.assertEqual(perf["by_direction"]["하락주의"]["ret_next_close"]["avg_pct"], -5.0)
                self.assertIsNone(perf["by_direction"]["하락주의"]["ret_next_close"]["net_avg_pct"])
                self.assertEqual(perf["by_direction"]["하락주의"]["ret_next_close"]["win_rate_pct"], 100.0)
                self.assertEqual(perf["by_direction"]["옥석가리기주의"]["ret_next_close"]["avg_pct"], -8.0)
                self.assertIsNone(perf["by_direction"]["옥석가리기주의"]["ret_next_close"]["net_avg_pct"])
                self.assertEqual(perf["by_direction"]["옥석가리기주의"]["ret_next_close"]["win_rate_pct"], 100.0)

                cfg_patch = json.dumps({
                    "max_market_alerts": 7,
                    "max_offhours_alerts": 2,
                    "offhours_digest_enabled": True,
                    "offhours_digest_time": "07:50",
                    "stock_dedup_hours": 24,
                    "max_daily_per_sector": 2,
                    "max_bullish_prepriced_pct": 12.5,
                }).encode("utf-8")
                req_cfg = urllib.request.Request(
                    f"{base}/api/config",
                    data=cfg_patch,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req_cfg, timeout=5) as resp:
                    res_cfg = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(res_cfg["ok"])
                self.assertEqual(res_cfg["config"]["max_market_alerts"], 7)
                self.assertEqual(res_cfg["config"]["max_offhours_alerts"], 2)
                self.assertEqual(res_cfg["config"]["max_daily_alerts"], 9)
                self.assertTrue(res_cfg["config"]["offhours_digest_enabled"])
                self.assertEqual(res_cfg["config"]["stock_dedup_hours"], 24)
                self.assertEqual(res_cfg["config"]["max_daily_per_sector"], 2)
                self.assertEqual(res_cfg["config"]["max_bullish_prepriced_pct"], 12.5)

                with urllib.request.urlopen(f"{base}/api/overview", timeout=5) as resp:
                    ov = json.loads(resp.read().decode("utf-8"))
                self.assertIn("performance", ov)
                self.assertEqual(ov["performance"]["by_direction"]["상승수혜"]["ret_close"]["avg_pct"], 4.0)
                self.assertEqual(ov["performance"]["by_direction"]["상승수혜"]["ret_close"]["net_avg_pct"], 3.77)
                self.assertEqual(ov["config"]["max_bullish_prepriced_pct"], 12.5)
            finally:
                srv.shutdown()
                srv.server_close()

    def test_prompt_bias_neutralized_and_krx_master_does_not_merge_seed_bias(self) -> None:
        # 1) SCREENING_PROMPT must not hardcode the backtest-leaking AI-glasses/RF-detector example
        self.assertNotIn("AI 안경", radar.SCREENING_PROMPT)
        self.assertNotIn("전파탐지기", radar.SCREENING_PROMPT)
        self.assertIn("관성적 뉴스", radar.SCREENING_PROMPT)
        self.assertIn("전 산업군", radar.SCREENING_PROMPT)
        self.assertIn("옥석가리기주의", radar.DEEP_PROMPT)

        # 2) SEED_COMPANIES must be diversified across industries without keyword-stuffed promo labels
        distinct_sectors = {c["sector"] for c in radar.SEED_COMPANIES}
        self.assertGreaterEqual(len(distinct_sectors), 6)
        all_seed_prods = " ".join(c["products"] for c in radar.SEED_COMPANIES)
        self.assertNotIn("시험장 보안 장비", all_seed_prods)
        self.assertNotIn("알파-P", all_seed_prods)

        # 3) load_krx_master with a valid cache must NOT merge SEED_COMPANIES descriptions on top of real KRX data
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "krx_master.json"
            cache_path.write_text(
                json.dumps(
                    {
                        "updated_at": datetime.now(KST).isoformat(),
                        "companies": [
                            {"code": "005930", "name": "삼성전자", "market": "KOSPI", "sector": "반도체", "products": "메모리 반도체"},
                            {"code": "446840", "name": "지슨", "market": "KOSDAQ", "sector": "통신장비", "products": "무선도청 탐지"},
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            loaded = radar.load_krx_master(cache_path, allow_network=False)
            self.assertEqual(set(loaded.keys()), {"005930", "446840"})
            self.assertEqual(loaded["446840"]["products"], "무선도청 탐지")

            # Offline fallback (no cache, allow_network=False) falls back to SEED_COMPANIES
            fallback = radar.load_krx_master(Path(tmp) / "missing.json", allow_network=False)
            self.assertEqual(len(fallback), len(radar.SEED_COMPANIES))

    def test_alpha_safety_guards_deep_models_triggers_session_hub_and_weak_leftovers(self) -> None:
        # 1) Stage 2 deep model order puts full flash models BEFORE flash-lite fallbacks across 3.x & 2.5 RPD buckets
        deep_order = [m.strip() for m in radar.DEFAULT_DEEP_MODELS.split(",") if m.strip()]
        self.assertEqual(
            deep_order,
            [
                "gemini-3.6-flash",
                "gemini-3.8-flash",
                "gemini-2.5-flash",
                "gemini-3.5-flash-lite",
                "gemini-2.5-flash-lite",
            ],
        )
        screen_order = [m.strip() for m in radar.DEFAULT_SCREEN_MODELS.split(",") if m.strip()]
        self.assertEqual(
            screen_order,
            [
                "gemini-3.5-flash-lite",
                "gemini-2.5-flash-lite",
                "gemini-2.5-flash",
                "gemini-3.6-flash",
                "gemini-3.8-flash",
            ],
        )

        # 2) Routine security spam moved from HARD to SOFT triggers
        for kw in ("해킹", "개인정보 유출", "정보유출", "전파탐지", "불법촬영"):
            self.assertNotIn(kw, radar.HARD_TRIGGER_KEYWORDS)
            self.assertIn(kw, radar.SOFT_TRIGGER_KEYWORDS)
        for kw in ("대규모 해킹", "랜섬웨어", "전산 마비"):
            self.assertIn(kw, radar.HARD_TRIGGER_KEYWORDS)

        now = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            st = radar.StateStore(Path(tmp) / "state.json")
            st.last_stage1_at = (now - timedelta(minutes=1)).isoformat()
            # Single-outlet routine privacy leak must NOT hard-trigger Stage 1 immediately
            routine = [radar.NewsItem("소규모 온라인몰 개인정보 유출 신고 접수", "https://ex.com/r1", "연합뉴스_사회", "사회/사건", first_seen=now.isoformat())]
            res_r = radar.evaluate_stage1_trigger(routine, radar.cluster_news_ranked(routine, now=now), st, now=now, market_window=True)
            self.assertFalse(res_r["should_run"])
            # Major ransomware / mass hack still hard-triggers immediately
            major = [radar.NewsItem("주요 은행 전산 마비 대규모 해킹 확인", "https://ex.com/r2", "연합뉴스_사회", "사회/사건", first_seen=now.isoformat())]
            res_m = radar.evaluate_stage1_trigger(major, radar.cluster_news_ranked(major, now=now), st, now=now, market_window=True)
            self.assertTrue(res_m["should_run"])
            self.assertEqual(res_m["reason"], "hard_keyword")

            # 3) Daemon mode defaults offhours_digest_enabled=True, non-daemon defaults False
            self.assertTrue(radar.resolve_alert_limits({}, daemon_mode=True)["offhours_digest_enabled"])
            self.assertFalse(radar.resolve_alert_limits({}, daemon_mode=False)["offhours_digest_enabled"])
            self.assertFalse(radar.resolve_alert_limits({"offhours_digest_enabled": False}, daemon_mode=True)["offhours_digest_enabled"])

            # 4) Cross-midnight offhours session key unifies Mon 22:00 + Tue 01:00 into ONE session
            t_mon_22 = datetime(2026, 11, 16, 22, 0, tzinfo=KST)
            t_tue_01 = datetime(2026, 11, 17, 1, 0, tzinfo=KST)
            t_tue_07 = datetime(2026, 11, 17, 7, 30, tzinfo=KST)
            t_tue_16 = datetime(2026, 11, 17, 16, 0, tzinfo=KST)
            self.assertEqual(radar.offhours_session_key(t_mon_22), "2026-11-17")
            self.assertEqual(radar.offhours_session_key(t_tue_01), "2026-11-17")
            self.assertEqual(radar.offhours_session_key(t_tue_07), "2026-11-17")
            self.assertEqual(radar.offhours_session_key(t_tue_16), "2026-11-18")
            st.mark_alerted("월요일밤1", now=t_mon_22, final_codes=["298040"], catalyst_score=8.5)
            st.mark_alerted("화요일새벽1", now=t_tue_01, final_codes=["012450"], catalyst_score=8.5)
            self.assertEqual(st.sent_offhours_today(now=t_tue_01), 2)
            self.assertEqual(st.sent_offhours_today(now=t_tue_07), 2)
            self.assertEqual(st.sent_offhours_today(now=t_tue_16), 0)

        # 5) compute_hub_stocks must NOT exempt a single-sector repeater (e.g. 카티스 4x in 사이버보안)
        recent_hub = [
            {"ts": (now - timedelta(days=3)).isoformat(), "theme_key": "보안1", "sector": "사이버보안", "final_codes": ["140430", "005930"]},
            {"ts": (now - timedelta(days=2)).isoformat(), "theme_key": "보안2", "sector": "사이버보안", "final_codes": ["140430"]},
            {"ts": (now - timedelta(days=1)).isoformat(), "theme_key": "보안3", "sector": "사이버보안", "final_codes": ["140430"]},
            {"ts": (now - timedelta(days=2)).isoformat(), "theme_key": "반도체1", "sector": "반도체·AI", "final_codes": ["005930"]},
            {"ts": (now - timedelta(days=1)).isoformat(), "theme_key": "전력1", "sector": "전력·에너지", "final_codes": ["005930"]},
        ]
        hubs = radar.compute_hub_stocks(recent_hub, now=now)
        self.assertIn("005930", hubs)       # 3 appearances across 3 distinct sectors -> true hub!
        self.assertNotIn("140430", hubs)    # 3 appearances in ONLY 1 sector -> NOT exempted!

        # 6) filter_recently_alerted_stocks weak-leftover guard
        recent_leader = [{"ts": (now - timedelta(hours=3)).isoformat(), "theme_key": "대장주_기알림", "final_codes": ["298040"], "catalyst_score": 8.4}]
        # Leader excluded leaving ONLY 단기테마성 상승수혜 -> blocked!
        sf_weak = radar.filter_recently_alerted_stocks(
            [
                {"code": "298040", "name": "효성중공업", "direction": "상승수혜", "revenue_link": "본업직결"},
                {"code": "999001", "name": "단기테마주", "direction": "상승수혜", "revenue_link": "단기테마성"},
            ],
            catalyst_score=8.5,
            recent_alerts=recent_leader,
            now=now,
            window_hours=24,
        )
        self.assertTrue(sf_weak["only_weak_leftovers"])
        self.assertTrue(sf_weak["all_filtered"])
        self.assertEqual(sf_weak["kept_stocks"], [])

        # Leader excluded leaving 부분연관 상승수혜 -> kept!
        sf_solid = radar.filter_recently_alerted_stocks(
            [
                {"code": "298040", "name": "효성중공업", "direction": "상승수혜", "revenue_link": "본업직결"},
                {"code": "267260", "name": "HD현대일렉트릭", "direction": "상승수혜", "revenue_link": "부분연관"},
            ],
            catalyst_score=8.5,
            recent_alerts=recent_leader,
            now=now,
            window_hours=24,
        )
        self.assertFalse(sf_solid["only_weak_leftovers"])
        self.assertFalse(sf_solid["all_filtered"])
        self.assertEqual([s["code"] for s in sf_solid["kept_stocks"]], ["267260"])

        # Leader excluded leaving 하락주의 warning -> warning preserved!
        sf_warn = radar.filter_recently_alerted_stocks(
            [
                {"code": "298040", "name": "효성중공업", "direction": "상승수혜", "revenue_link": "본업직결"},
                {"code": "011200", "name": "HMM", "direction": "하락주의", "revenue_link": "단기테마성"},
            ],
            catalyst_score=8.5,
            recent_alerts=recent_leader,
            now=now,
            window_hours=24,
        )
        self.assertFalse(sf_warn["only_weak_leftovers"])
        self.assertFalse(sf_warn["all_filtered"])
        self.assertEqual([s["code"] for s in sf_warn["kept_stocks"]], ["011200"])

    def test_intraday_prepriced_chasing_guard_unit_and_e2e(self) -> None:
        q_spiked = radar.StockQuote("298040", 500000, 75000, 17.6, 300000, "OPEN", radar.classify_stage(17.6, 300000))
        q_fresh = radar.StockQuote("267260", 300000, 6000, 2.0, 200000, "OPEN", radar.classify_stage(2.0, 200000))
        q_closed = radar.StockQuote("298040", 500000, 75000, 17.6, 300000, "CLOSE", radar.classify_stage(17.6, 300000))

        # All bullish picks >= +15% during OPEN -> blocked
        hit, det = radar.is_all_bullish_prepriced(
            [{"code": "298040", "name": "효성중공업", "direction": "상승수혜"}],
            {"298040": q_spiked},
            max_pct=15.0,
        )
        self.assertTrue(hit)
        self.assertEqual(len(det), 1)

        # Partial (one +17.6%, one +2.0%) -> allowed
        hit_part, _ = radar.is_all_bullish_prepriced(
            [
                {"code": "298040", "name": "효성중공업", "direction": "상승수혜"},
                {"code": "267260", "name": "HD현대일렉트릭", "direction": "상승수혜"},
            ],
            {"298040": q_spiked, "267260": q_fresh},
            max_pct=15.0,
        )
        self.assertFalse(hit_part)

        # Pure 하락주의 / 옥석가리기주의 warning alert -> never blocked
        hit_warn, _ = radar.is_all_bullish_prepriced(
            [{"code": "298040", "name": "효성중공업", "direction": "옥석가리기주의"}],
            {"298040": q_spiked},
            max_pct=15.0,
        )
        self.assertFalse(hit_warn)

        # Market status CLOSE (e.g. holiday) -> never blocked by stale previous-day surge
        hit_closed, _ = radar.is_all_bullish_prepriced(
            [{"code": "298040", "name": "효성중공업", "direction": "상승수혜"}],
            {"298040": q_closed},
            max_pct=15.0,
        )
        self.assertFalse(hit_closed)

        # Trading hours boundary: 08:15 pre-market False, 09:00~15:30 True
        self.assertFalse(radar.is_regular_trading_hours(datetime(2026, 11, 20, 8, 15, tzinfo=KST)))
        self.assertTrue(radar.is_regular_trading_hours(datetime(2026, 11, 20, 9, 0, tzinfo=KST)))
        self.assertTrue(radar.is_regular_trading_hours(datetime(2026, 11, 20, 15, 30, tzinfo=KST)))
        self.assertFalse(radar.is_regular_trading_hours(datetime(2026, 11, 20, 15, 31, tzinfo=KST)))

        # E2E: at 10:15 KST regular trading hours, spiked +18.2% bullish pick is blocked with prepriced_skip trace,
        # while at 08:15 KST pre-market it is NOT blocked!
        comps = {c["code"]: dict(c) for c in radar.SEED_COMPANIES}
        t_open = datetime(2026, 11, 20, 10, 15, tzinfo=KST)
        t_pre = datetime(2026, 11, 20, 8, 15, tzinfo=KST)
        item_open = radar.NewsItem("초고압 변압기 긴급 대규모 수주 계약", "https://ex.com/p_open", "연합뉴스_산업", "경제/산업", first_seen=t_open.isoformat())
        item_pre = radar.NewsItem("초고압 변압기 긴급 대규모 수주 계약", "https://ex.com/p_pre", "연합뉴스_산업", "경제/산업", first_seen=t_pre.isoformat())

        def fake_gemini_power(api_key, models, sp, up, **kw):
            if sp == radar.SCREENING_PROMPT:
                cid = json.loads(up)["clusters"][0]["cluster_id"]
                return {"selected_events": [{"cluster_id": cid, "catalyst_score": 8.8, "theme_key": "변압기_수주", "second_order_keywords": ["전력기기"]}]}, "gemini-3.5-flash-lite"
            return {"title": "변압기 수주 급등", "event_summary": "수주", "stocks": [{"code": "298040", "name": "효성중공업", "direction": "상승수혜"}]}, "gemini-3.6-flash"

        env = {"GEMINI_API_KEY": "k", "TELEGRAM_BOT_TOKEN": "t", "TELEGRAM_CHAT_ID": "c", "INSTANT_FLASH_ALERT": "false", "TRIGGER_MODE": "shadow"}
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            sent_open: list[str] = []
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=[item_open]),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini_power),
                mock.patch.object(radar, "load_krx_master", return_value=comps),
                mock.patch.object(radar, "fetch_naver_quotes", return_value={"298040": q_spiked}),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_open.append(m) or 1),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t_open, daemon_mode=True)
            self.assertEqual(len(sent_open), 0)
            traces = [json.loads(l) for l in (tmp_dir / "trace.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertTrue(any(r["type"] == "prepriced_skip" for r in traces))

        with tempfile.TemporaryDirectory() as tmp2:
            tmp_dir2 = Path(tmp2)
            sent_pre: list[str] = []
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=[item_pre]),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini_power),
                mock.patch.object(radar, "load_krx_master", return_value=comps),
                mock.patch.object(radar, "fetch_naver_quotes", return_value={"298040": q_spiked}),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_pre.append(m) or 1),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir2, now_fn=lambda: t_pre, daemon_mode=True)
            self.assertEqual(len(sent_pre), 1)


class TestRemoteHttpsTunnelAndCookieAuth(unittest.TestCase):
    def test_tunnel_url_extraction_password_generation_and_telegram_notice(self) -> None:
        import tunnel_manager

        # 1) URL extraction accepts real trycloudflare tunnel URLs and rejects api.trycloudflare.com
        self.assertEqual(
            tunnel_manager.extract_trycloudflare_url(
                "2026-11-21T14:00:02Z INF |  https://bright-alpha-radar-news.trycloudflare.com  |"
            ),
            "https://bright-alpha-radar-news.trycloudflare.com",
        )
        self.assertIsNone(tunnel_manager.extract_trycloudflare_url("https://api.trycloudflare.com/tunnel"))
        self.assertIsNone(tunnel_manager.extract_trycloudflare_url("Requesting new quick Tunnel on trycloudflare.com..."))

        # 2) Architecture detection
        self.assertEqual(tunnel_manager.detect_cloudflared_asset("x86_64"), "cloudflared-linux-amd64")
        self.assertEqual(tunnel_manager.detect_cloudflared_asset("aarch64"), "cloudflared-linux-arm64")
        self.assertEqual(tunnel_manager.detect_cloudflared_asset("armv7l"), "cloudflared-linux-arm")

        # 3) Auto-generate strong DASHBOARD_PASSWORD when empty, preserve comments, and send once via Telegram
        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DASHBOARD_PASSWORD", None)
            tmp_dir = Path(tmp)
            env_path = tmp_dir / ".env"
            data_dir = tmp_dir / "data"
            env_path.write_text(
                "# 주석 유지 테스트\nTELEGRAM_BOT_TOKEN=tok123\nTELEGRAM_CHAT_ID=999\nDASHBOARD_PASSWORD=\n",
                encoding="utf-8",
            )

            pw1, created1 = tunnel_manager.ensure_dashboard_password(env_path, data_dir=data_dir)
            self.assertTrue(created1)
            self.assertEqual(len(pw1), 12)
            self.assertIn("# 주석 유지 테스트", env_path.read_text(encoding="utf-8"))
            self.assertEqual(tunnel_manager.read_dotenv_value(env_path, "DASHBOARD_PASSWORD"), pw1)

            # Second call preserves existing password
            pw2, created2 = tunnel_manager.ensure_dashboard_password(env_path, data_dir=data_dir)
            self.assertFalse(created2)
            self.assertEqual(pw2, pw1)

            now = datetime.now(KST)
            tunnel_manager.write_tunnel_status(
                data_dir,
                {
                    "pid": 111,
                    "state": "running",
                    "url": "https://quick-alpha-radar.trycloudflare.com",
                    "heartbeat_at": now.isoformat(),
                },
            )
            st = tunnel_manager.read_tunnel_status(data_dir, now=now)
            self.assertTrue(st["alive"])
            self.assertEqual(st["url"], "https://quick-alpha-radar.trycloudflare.com")

            # Stale heartbeat (> 90s) marks tunnel offline
            st_stale = tunnel_manager.read_tunnel_status(data_dir, now=now + timedelta(minutes=5))
            self.assertFalse(st_stale["alive"])
            self.assertEqual(st_stale["state"], "offline")

            sent_msgs: list[str] = []
            with mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_msgs.append(m) or 1):
                ok1 = tunnel_manager.send_tunnel_link_telegram(
                    data_dir,
                    tmp_dir,
                    include_generated_password=True,
                )
                self.assertTrue(ok1)
                self.assertEqual(len(sent_msgs), 1)
                self.assertIn("https://quick-alpha-radar.trycloudflare.com", sent_msgs[0])
                self.assertIn(pw1, sent_msgs[0])  # First send includes newly generated password

                # Subsequent notification does NOT leak raw password again
                ok2 = tunnel_manager.send_tunnel_link_telegram(
                    data_dir,
                    tmp_dir,
                    include_generated_password=True,
                )
                self.assertTrue(ok2)
                self.assertEqual(len(sent_msgs), 2)
                self.assertNotIn(pw1, sent_msgs[1])

    def test_tunnel_health_probe_and_telegram_self_heal_reconnect(self) -> None:
        import threading as _th
        import tunnel_manager

        # 1) Public URL probe distinguishes healthy 200/401 from dead Cloudflare 530 (Error 1033) / 502 / NXDOMAIN
        with mock.patch("urllib.request.urlopen", return_value=_FakeResponse({"ok": True}, status=200)):
            self.assertEqual(
                tunnel_manager.probe_tunnel_public_url("https://live-radar.trycloudflare.com"),
                (True, "HTTP 200"),
            )

        err_401 = urllib.error.HTTPError("https://live-radar.trycloudflare.com/", 401, "Unauthorized", {}, io.BytesIO(b""))
        with mock.patch("urllib.request.urlopen", side_effect=err_401):
            self.assertEqual(
                tunnel_manager.probe_tunnel_public_url("https://live-radar.trycloudflare.com"),
                (True, "HTTP 401"),
            )

        err_530 = urllib.error.HTTPError("https://dead-radar.trycloudflare.com/", 530, "Error 1033", {}, io.BytesIO(b"1033"))
        with mock.patch("urllib.request.urlopen", side_effect=err_530):
            self.assertEqual(
                tunnel_manager.probe_tunnel_public_url("https://dead-radar.trycloudflare.com"),
                (False, "HTTP 530"),
            )

        with mock.patch("urllib.request.urlopen", side_effect=urllib.error.URLError("Name or service not known")):
            ok_dns, detail_dns = tunnel_manager.probe_tunnel_public_url("https://nx-radar.trycloudflare.com")
            self.assertFalse(ok_dns)
            self.assertIn("Name or service not known", detail_dns)

        # 2) When user types '대시보드' and current URL returns HTTP 530 (expired tunnel), responder triggers reconnect_evt!
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            data_dir = tmp_dir / "data"
            (tmp_dir / ".env").write_text("TELEGRAM_BOT_TOKEN=tok1\nTELEGRAM_CHAT_ID=777\n", encoding="utf-8")
            tunnel_manager.write_tunnel_status(
                data_dir,
                {
                    "pid": 333,
                    "state": "running",
                    "url": "https://expired-radar.trycloudflare.com",
                    "heartbeat_at": datetime.now(KST).isoformat(),
                },
            )
            stop_evt = _th.Event()
            reconnect_evt = _th.Event()
            sent_replies: list[str] = []
            calls = {"n": 0}

            def fake_tg_updates(req, timeout=0):
                calls["n"] += 1
                if calls["n"] == 1:
                    return _FakeResponse({"ok": True, "result": []})
                if calls["n"] == 2:
                    stop_evt.set()
                    return _FakeResponse(
                        {
                            "ok": True,
                            "result": [
                                {
                                    "update_id": 10,
                                    "message": {"chat": {"id": 777, "type": "private"}, "text": "대시보드"},
                                }
                            ],
                        }
                    )
                return _FakeResponse({"ok": True, "result": []})

            with (
                mock.patch("urllib.request.urlopen", side_effect=fake_tg_updates),
                mock.patch.object(tunnel_manager, "probe_tunnel_public_url", return_value=(False, "HTTP 530")),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_replies.append(m) or 1),
            ):
                tunnel_manager._bot_command_responder_loop(stop_evt, data_dir, tmp_dir, reconnect_evt=reconnect_evt)

            self.assertTrue(reconnect_evt.is_set())
            self.assertEqual(len(sent_replies), 1)
            self.assertIn("새 보안 링크를 발급", sent_replies[0])

    def test_dashboard_cookie_login_logout_and_tunnel_actions(self) -> None:
        import threading as _th
        import dashboard
        import tunnel_manager

        now = datetime.now(KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            tunnel_manager.write_tunnel_status(
                tmp_dir,
                {
                    "pid": 222,
                    "state": "running",
                    "url": "https://mobile-radar-link.trycloudflare.com",
                    "target": "http://127.0.0.1:8787",
                    "heartbeat_at": now.isoformat(),
                },
            )
            srv = dashboard.create_dashboard_server("127.0.0.1", 0, data_dir=tmp_dir, password="my-mobile-pw")
            port = srv.server_address[1]
            t = _th.Thread(target=srv.serve_forever, daemon=True)
            t.start()
            base = f"http://127.0.0.1:{port}"
            try:
                # 1) Unauthenticated browser GET / serves mobile-friendly LOGIN_HTML (200) instead of raw JSON
                with urllib.request.urlopen(f"{base}/", timeout=5) as resp:
                    self.assertEqual(resp.status, 200)
                    login_page = resp.read().decode("utf-8")
                self.assertIn('id="login-form"', login_page)
                self.assertNotIn('id="kpi-alerts"', login_page)

                # 2) Unauthenticated API request still returns 401
                with self.assertRaises(urllib.error.HTTPError) as ctx_unauth:
                    urllib.request.urlopen(f"{base}/api/overview", timeout=5)
                self.assertEqual(ctx_unauth.exception.code, 401)

                # 3) Wrong password on POST /api/auth/login -> 401
                bad_req = urllib.request.Request(
                    f"{base}/api/auth/login",
                    data=json.dumps({"password": "wrong"}).encode("utf-8"),
                    headers={"Content-Type": "application/json", "X-Requested-With": "fetch"},
                    method="POST",
                )
                with self.assertRaises(urllib.error.HTTPError) as ctx_bad:
                    urllib.request.urlopen(bad_req, timeout=5)
                self.assertEqual(ctx_bad.exception.code, 401)

                # 4) Valid password over HTTPS forwarded header -> 200 + Set-Cookie with HttpOnly & Secure
                ok_req = urllib.request.Request(
                    f"{base}/api/auth/login",
                    data=json.dumps({"password": "my-mobile-pw"}).encode("utf-8"),
                    headers={
                        "Content-Type": "application/json",
                        "X-Requested-With": "fetch",
                        "X-Forwarded-Proto": "https",
                    },
                    method="POST",
                )
                with urllib.request.urlopen(ok_req, timeout=5) as resp:
                    self.assertEqual(resp.status, 200)
                    set_cookie = resp.headers.get("Set-Cookie") or ""
                self.assertIn("nar_session=", set_cookie)
                self.assertIn("HttpOnly", set_cookie)
                self.assertIn("Secure", set_cookie)
                cookie_pair = set_cookie.split(";", 1)[0]

                # 5) Cookie-authenticated GET / and GET /api/overview return full dashboard & tunnel URL!
                dash_req = urllib.request.Request(f"{base}/", headers={"Cookie": cookie_pair})
                with urllib.request.urlopen(dash_req, timeout=5) as resp:
                    self.assertIn('id="kpi-alerts"', resp.read().decode("utf-8"))

                ov_req = urllib.request.Request(f"{base}/api/overview", headers={"Cookie": cookie_pair})
                with urllib.request.urlopen(ov_req, timeout=5) as resp:
                    ov = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(ov["auth_enabled"])
                self.assertTrue(ov["tunnel"]["alive"])
                self.assertEqual(ov["tunnel"]["url"], "https://mobile-radar-link.trycloudflare.com")

                # 6) POST /api/actions/send-tunnel-link triggers Telegram send
                with mock.patch.object(tunnel_manager, "send_tunnel_link_telegram", return_value=True) as m_send:
                    link_req = urllib.request.Request(
                        f"{base}/api/actions/send-tunnel-link",
                        data=b"{}",
                        headers={"Cookie": cookie_pair, "Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(link_req, timeout=5) as resp:
                        self.assertTrue(json.loads(resp.read().decode("utf-8"))["ok"])
                    m_send.assert_called_once()

                # 7) POST /api/config toggles tunnel_enabled=False
                cfg_req = urllib.request.Request(
                    f"{base}/api/config",
                    data=json.dumps({"tunnel_enabled": False}).encode("utf-8"),
                    headers={"Cookie": cookie_pair, "Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(cfg_req, timeout=5) as resp:
                    cfg_out = json.loads(resp.read().decode("utf-8"))
                self.assertFalse(cfg_out["config"]["tunnel_enabled"])

                # 8) POST /api/auth/logout clears cookie
                out_req = urllib.request.Request(
                    f"{base}/api/auth/logout",
                    data=b"{}",
                    headers={"Cookie": cookie_pair, "Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(out_req, timeout=5) as resp:
                    self.assertIn("Max-Age=0", resp.headers.get("Set-Cookie") or "")
            finally:
                srv.shutdown()
                srv.server_close()


class TestQuotaConservationDigestResilienceAndLiquidity(unittest.TestCase):
    def test_gemini_multi_key_rotation_and_overnight_digest_trigger_batching(self) -> None:
        # 0) Legacy 3-model .env strings auto-upgrade to 5-model defaults without manual .env edits!
        with mock.patch.dict(
            os.environ,
            {
                "GEMINI_SCREEN_MODELS": "gemini-3.5-flash-lite,gemini-3.6-flash,gemini-3.8-flash",
                "GEMINI_DEEP_MODELS": "gemini-3.6-flash,gemini-3.8-flash,gemini-3.5-flash-lite",
            },
            clear=False,
        ):
            self.assertEqual(
                radar.resolve_gemini_models("GEMINI_SCREEN_MODELS", radar.DEFAULT_SCREEN_MODELS, radar.LEGACY_SCREEN_MODELS),
                ["gemini-3.5-flash-lite", "gemini-2.5-flash-lite", "gemini-2.5-flash", "gemini-3.6-flash", "gemini-3.8-flash"],
            )
            self.assertEqual(
                radar.resolve_gemini_models("GEMINI_DEEP_MODELS", radar.DEFAULT_DEEP_MODELS, radar.LEGACY_DEEP_MODELS),
                ["gemini-3.6-flash", "gemini-3.8-flash", "gemini-2.5-flash", "gemini-3.5-flash-lite", "gemini-2.5-flash-lite"],
            )
        # Custom user-chosen model list is still respected!
        with mock.patch.dict(os.environ, {"GEMINI_DEEP_MODELS": "gemini-2.5-flash"}, clear=False):
            self.assertEqual(
                radar.resolve_gemini_models("GEMINI_DEEP_MODELS", radar.DEFAULT_DEEP_MODELS, radar.LEGACY_DEEP_MODELS),
                ["gemini-2.5-flash"],
            )

        # 1) Multi-project key parsing (GEMINI_API_KEYS + GEMINI_API_KEY deduplicated in order)
        with mock.patch.dict(os.environ, {"GEMINI_API_KEYS": "proj1_key, proj2_key", "GEMINI_API_KEY": "proj1_key"}, clear=False):
            self.assertEqual(radar.resolve_gemini_api_keys(), ["proj1_key", "proj2_key"])

        # 2) Daily RPD exhaustion on proj1_key rotates to proj2_key on the SAME top model!
        now = datetime(2026, 11, 20, 10, 30, tzinfo=KST)
        seen_keys: list[tuple[str, str]] = []

        def fake_urlopen(req, timeout=0):
            hdr_key = req.headers.get("X-goog-api-key") or req.headers.get("x-goog-api-key") or ""
            model_used = "gemini-3.6-flash" if "gemini-3.6-flash" in req.full_url else "other"
            seen_keys.append((model_used, hdr_key))
            if hdr_key == "proj1_key":
                err_body = io.BytesIO(b'{"error": {"code": 429, "message": "Quota exceeded for metric: GenerateRequestsPerDayPerProjectPerModel"}}')
                raise urllib.error.HTTPError(req.full_url, 429, "Too Many Requests", {}, err_body)
            return _FakeResponse({"candidates": [{"content": {"parts": [{"text": '{"ok": true}'}]}}]})

        with tempfile.TemporaryDirectory() as tmp:
            st = radar.StateStore(Path(tmp) / "state.json")
            with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
                out, used_model = radar.call_gemini(
                    "proj1_key,proj2_key",
                    ["gemini-3.6-flash", "gemini-2.5-flash"],
                    "sys",
                    "usr",
                    state=st,
                    stage="stage2",
                    now=now,
                )
            self.assertEqual(out, {"ok": True})
            self.assertEqual(used_model, "gemini-3.6-flash")
            self.assertEqual(seen_keys, [("gemini-3.6-flash", "proj1_key"), ("gemini-3.6-flash", "proj2_key")])
            self.assertTrue(st.is_model_exhausted("gemini-3.6-flash#k1", now=now))
            self.assertFalse(st.is_model_exhausted("gemini-3.6-flash", now=now))

            # Next call skips exhausted proj1_key directly and uses proj2_key immediately!
            seen_keys.clear()
            with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
                radar.call_gemini("proj1_key,proj2_key", ["gemini-3.6-flash"], "sys", "usr", state=st, stage="stage2", now=now)
            self.assertEqual(seen_keys, [("gemini-3.6-flash", "proj2_key")])

            # 3) Overnight digest_mode=True batches quietly for 90 min even when multi-outlet / callback items arrive
            t_night = datetime(2026, 11, 19, 23, 30, tzinfo=KST)
            st.last_stage1_at = (t_night - timedelta(minutes=25)).isoformat()
            night_multi = [
                radar.NewsItem("반도체 패키징 증설 대규모 투자 추진", "https://ex.com/nm1", "연합뉴스_산업", "경제/산업", priority=0, first_seen=t_night.isoformat()),
                radar.NewsItem("반도체 패키징 증설 대규모 투자 가속", "https://ex.com/nm2", "매일경제_경제", "경제/산업", first_seen=t_night.isoformat()),
            ]
            ranked_nm = radar.cluster_news_ranked(night_multi, now=t_night)
            trig_wait = radar.evaluate_stage1_trigger(
                night_multi, ranked_nm, st, now=t_night, market_window=False, digest_mode=True, digest_time="07:50"
            )
            self.assertFalse(trig_wait["should_run"])
            self.assertEqual(trig_wait["reason"], "overnight_digest_batching")

            # After 95 minutes -> runs overnight_digest_interval
            st.last_stage1_at = (t_night - timedelta(minutes=95)).isoformat()
            trig_interval = radar.evaluate_stage1_trigger(
                night_multi, ranked_nm, st, now=t_night, market_window=False, digest_mode=True, digest_time="07:50"
            )
            self.assertTrue(trig_interval["should_run"])
            self.assertEqual(trig_interval["reason"], "overnight_digest_interval")

            # Weekday 07:20 KST (30 min before 07:50 digest, last S1 35 min ago) -> premarket_digest_prep flush!
            t_prep = datetime(2026, 11, 20, 7, 20, tzinfo=KST)
            st.last_stage1_at = (t_prep - timedelta(minutes=35)).isoformat()
            trig_prep = radar.evaluate_stage1_trigger(
                night_multi[:1], radar.cluster_news_ranked(night_multi[:1], now=t_prep), st,
                now=t_prep, market_window=False, digest_mode=True, digest_time="07:50",
            )
            self.assertTrue(trig_prep["should_run"])
            self.assertEqual(trig_prep["reason"], "premarket_digest_prep")

            # Market 60s poll soft cooldown: elapsed 1m waits, elapsed 2m runs, hard keyword runs at 30s
            t_mkt = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
            st.last_stage1_at = (t_mkt - timedelta(minutes=1)).isoformat()
            soft2 = [radar.NewsItem("반도체 특허 승인 공급계약 추진", "https://ex.com/s2", "연합뉴스_산업", "경제/산업", first_seen=t_mkt.isoformat())]
            self.assertFalse(
                radar.evaluate_stage1_trigger(soft2, radar.cluster_news_ranked(soft2, now=t_mkt), st, now=t_mkt, market_window=True)["should_run"]
            )
            st.last_stage1_at = (t_mkt - timedelta(minutes=2)).isoformat()
            self.assertTrue(
                radar.evaluate_stage1_trigger(soft2, radar.cluster_news_ranked(soft2, now=t_mkt), st, now=t_mkt, market_window=True)["should_run"]
            )

    def test_premarket_digest_0730_prewarm_and_0750_stage2_outage_lossless_fallback(self) -> None:
        comps = {c["code"]: dict(c) for c in radar.SEED_COMPANIES}

        def fake_quotes(codes):
            return {
                c: radar.StockQuote(
                    c, 50000, 500, 1.0, 20000, "CLOSE",
                    radar.classify_stage(1.0, 20000),
                    trading_value=0, prev_trading_value=1_500_000_000,
                )
                for c in codes
            }

        # Scenario A: 07:35 & 07:40 staggered pre-warming computes 1 item per cycle -> 07:50 needs ZERO Gemini calls!
        with tempfile.TemporaryDirectory() as tmp_a:
            dir_a = Path(tmp_a)
            t_0735 = datetime(2026, 11, 20, 7, 35, tzinfo=KST)
            t_0740 = datetime(2026, 11, 20, 7, 40, tzinfo=KST)
            t_0750 = datetime(2026, 11, 20, 7, 50, tzinfo=KST)

            st_a = radar.StateStore(dir_a / "state.json")
            for idx, (tk, kw, hl, sc) in enumerate([
                ("정전_변압기", "전력기기", "대규모 정전 초고압 변압기 긴급 발주", 9.2),
                ("위성_발사체", "우주항공", "방산 위성 발사체 대규모 수주 확정", 8.8),
            ]):
                cl = radar.EventCluster(
                    f"pw{idx}", hl, "속보",
                    [radar.NewsItem(hl, f"https://ex.com/pw{idx}", "연합뉴스_최신", "속보", first_seen=t_0735.isoformat())],
                )
                st_a.enqueue_overnight(
                    {
                        "cluster": cl.to_dict(),
                        "stage1": {"cluster_id": cl.cluster_id, "catalyst_score": sc, "theme_key": tk, "event_summary": hl, "second_order_keywords": [kw]},
                        "theme_key": tk,
                        "sector": "전력·에너지" if idx == 0 else "방산·우주항공",
                        "catalyst_score": sc,
                        "first_seen": t_0735.isoformat(),
                        "created_at": t_0735.isoformat(),
                    },
                    now=t_0735,
                )
            st_a.save(now=t_0735)

            s2_prewarm_calls = [0]
            sent_a: list[str] = []

            def fake_gemini_prewarm(api_key, models, sp, up, **kw):
                s2_prewarm_calls[0] += 1
                h = json.loads(up)["event_cluster"]["headline"]
                if "정전" in h:
                    return {"title": "사전분석 변압기 수혜", "event_summary": h, "stocks": [{"code": "298040", "name": "효성중공업", "direction": "상승수혜"}]}, "gemini-3.6-flash"
                return {"title": "사전분석 우주항공 수혜", "event_summary": h, "stocks": [{"code": "012450", "name": "한화에어로스페이스", "direction": "상승수혜"}]}, "gemini-3.6-flash"

            env_a = {"GEMINI_API_KEY": "k", "TELEGRAM_BOT_TOKEN": "t", "TELEGRAM_CHAT_ID": "c", "MAX_OFFHOURS_ALERTS": "2", "OFFHOURS_DIGEST_ENABLED": "true"}
            with (
                mock.patch.dict(os.environ, env_a, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=[]),
                mock.patch.object(radar, "load_krx_master", return_value=comps),
                mock.patch.object(radar, "fetch_naver_quotes", side_effect=fake_quotes),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_a.append(m) or 1),
            ):
                with mock.patch.object(radar, "call_gemini", side_effect=fake_gemini_prewarm):
                    radar.run_live(dry_run=False, cycles=1, data_dir=dir_a, now_fn=lambda: t_0735, daemon_mode=True)
                    self.assertEqual(s2_prewarm_calls[0], 1)  # Staggered: only 1 item per cycle!
                    radar.run_live(dry_run=False, cycles=1, data_dir=dir_a, now_fn=lambda: t_0740, daemon_mode=True)
                    self.assertEqual(s2_prewarm_calls[0], 2)

                # At 07:50, even if Gemini is completely unreachable, precomputed_s2 serves the full digest with 0 live calls!
                with mock.patch.object(radar, "call_gemini", side_effect=AssertionError("07:50 must not call Gemini when prewarmed")):
                    radar.run_live(dry_run=False, cycles=1, data_dir=dir_a, now_fn=lambda: t_0750, daemon_mode=True)

            self.assertEqual(len(sent_a), 1)
            self.assertIn("사전분석 변압기 수혜", sent_a[0])
            self.assertIn("사전분석 우주항공 수혜", sent_a[0])
            self.assertIn("전일 거래대금 15.0억", sent_a[0])

        # Scenario B: 12 un-warmed overnight items queued + 100% Stage 2 503/429 outage at 07:50
        # -> Circuit breaker stops after 2 Stage 2 errors AND Stage 1+KRX lossless fallback STILL sends digest!
        with tempfile.TemporaryDirectory() as tmp_b:
            dir_b = Path(tmp_b)
            t_0750 = datetime(2026, 11, 20, 7, 50, tzinfo=KST)
            st_b = radar.StateStore(dir_b / "state.json")
            kw_cycle = ["전력기기", "우주항공", "감염병", "조선해운", "로봇", "수자원"]
            for i in range(12):
                kw = kw_cycle[i % len(kw_cycle)]
                hl = f"야간 대형 촉매 이슈 {i + 1}호 ({kw})"
                cl = radar.EventCluster(
                    f"outage_{i}", hl, "속보",
                    [radar.NewsItem(hl, f"https://ex.com/out{i}", "연합뉴스_최신", "속보", first_seen=(t_0750 - timedelta(hours=3)).isoformat())],
                )
                st_b.enqueue_overnight(
                    {
                        "cluster": cl.to_dict(),
                        "stage1": {
                            "cluster_id": cl.cluster_id,
                            "catalyst_score": round(9.5 - i * 0.1, 1),
                            "theme_key": f"야간이슈_{i}_{kw}",
                            "event_summary": f"{hl} 요약",
                            "why_notable": f"{kw} 밸류체인 2차 수혜",
                            "second_order_keywords": [kw],
                        },
                        "theme_key": f"야간이슈_{i}_{kw}",
                        "sector": f"섹터_{i}",
                        "catalyst_score": round(9.5 - i * 0.1, 1),
                        "first_seen": (t_0750 - timedelta(hours=3)).isoformat(),
                        "created_at": (t_0750 - timedelta(hours=3)).isoformat(),
                    },
                    now=t_0750 - timedelta(hours=1),
                )
            st_b.save(now=t_0750 - timedelta(hours=1))
            self.assertEqual(len(st_b.overnight_queue), 12)

            s2_fail_calls = [0]
            sent_b: list[str] = []

            def always_503(*args, **kwargs):
                s2_fail_calls[0] += 1
                raise RuntimeError("HTTP 503 Service Unavailable / 429 Resource Exhausted")

            env_b = {
                "GEMINI_API_KEY": "k",
                "TELEGRAM_BOT_TOKEN": "t",
                "TELEGRAM_CHAT_ID": "c",
                "MAX_OFFHOURS_ALERTS": "3",
                "MAX_DAILY_PER_SECTOR": "0",
                "OFFHOURS_DIGEST_ENABLED": "true",
            }
            with (
                mock.patch.dict(os.environ, env_b, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=[]),
                mock.patch.object(radar, "load_krx_master", return_value=comps),
                mock.patch.object(radar, "fetch_naver_quotes", side_effect=fake_quotes),
                mock.patch.object(radar, "call_gemini", side_effect=always_503),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_b.append(m) or 1),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=dir_b, now_fn=lambda: t_0750, daemon_mode=True)

            # Circuit breaker tripped at 2 errors (NOT 12!), and digest WAS sent via stage1-fallback!
            self.assertEqual(s2_fail_calls[0], 2)
            self.assertEqual(len(sent_b), 1)
            self.assertIn("[장전 요약] 간밤 핵심 2차 파급 이슈 (3건)", sent_b[0])
            self.assertIn("stage1-fallback", sent_b[0])
            self.assertIn("효성중공업", sent_b[0])

            traces_b = [json.loads(l) for l in (dir_b / "trace.jsonl").read_text(encoding="utf-8").splitlines()]
            digest_ev = next(r for r in traces_b if r["type"] == "premarket_digest_sent")
            self.assertEqual(digest_ev["count"], 3)
            self.assertEqual(digest_ev["fallback_count"], 3)

    def test_hybrid_liquidity_filter_naver_fchart_and_buyable_performance_split(self) -> None:
        import threading as _th
        import dashboard

        # 1) fetch_naver_quotes parses accumulatedTradingValue ("1,200백만"), upper-limit/halt caution, and fchart prior-day turnover
        radar._PREV_TRADING_VALUE_CACHE.clear()
        t_open = datetime(2026, 11, 20, 9, 20, tzinfo=KST)

        def fake_naver_http(req, timeout=0):
            url = req.full_url
            if "polling.finance.naver.com" in url:
                return _FakeResponse({
                    "datas": [
                        {
                            "itemCode": "446840",
                            "closePrice": "13,000",
                            "compareToPreviousClosePrice": "3,000",
                            "fluctuationsRatio": "29.95",
                            "compareToPreviousPrice": {"code": "1"},
                            "accumulatedTradingVolume": "92,300",
                            "accumulatedTradingValue": "1,200백만",
                            "marketStatus": "OPEN",
                        },
                        {
                            "itemCode": "140430",
                            "closePrice": "2,500",
                            "compareToPreviousClosePrice": "0",
                            "fluctuationsRatio": "0.00",
                            "accumulatedTradingVolume": "0",
                            "marketStatus": "HALT",
                        },
                    ]
                })
            if "fchart.stock.naver.com" in url and "symbol=446840" in url:
                xml_bytes = (
                    b'<?xml version="1.0" encoding="EUC-KR" ?><protocol><chartdata>'
                    b'<item data="20261119|10000|10200|9800|10000|85000" />'
                    b'<item data="20261120|13000|13000|13000|13000|92300" />'
                    b'</chartdata></protocol>'
                )

                class _RawResp:
                    def read(self):
                        return xml_bytes
                    def __enter__(self):
                        return self
                    def __exit__(self, *a):
                        return None

                return _RawResp()
            raise urllib.error.URLError("no mock")

        with mock.patch("urllib.request.urlopen", side_effect=fake_naver_http):
            qmap = radar.fetch_naver_quotes(["446840", "140430"], now=t_open)

        q_446840 = qmap["446840"]
        self.assertEqual(q_446840.trading_value, 1_200_000_000)       # 12.0억
        self.assertEqual(q_446840.prev_trading_value, 850_000_000)    # 8.5억 (from 20261119 close*vol)
        self.assertEqual(q_446840.caution_status, "상한가잠김")
        self.assertEqual(radar.assess_quote_executability(q_446840), (False, "상한가잠김"))
        self.assertEqual(radar.recommend_position_cap_krw(q_446840), 12_000_000)  # 1% of max(8.5억, 12.0억) = 1,200만원
        note = radar.format_liquidity_note(q_446840)
        self.assertIn("전일 8.5억 · 당일 12.0억", note)
        self.assertIn("1% 적정진입 ~1,200만원", note)
        self.assertIn("현재 매수 어려움(상한가 잠김)", note)

        q_140430 = qmap["140430"]
        self.assertEqual(radar.assess_quote_executability(q_140430), (False, "거래정지"))

        # 2) Hybrid liquidity filter: <5억 micro-cap & halted excluded, today-surging small-cap (>=5억) & warnings & unknown quotes kept!
        quotes_mix = {
            "000001": radar.StockQuote("000001", 2000, 50, 2.5, 20000, "OPEN", "", trading_value=80_000_000, prev_trading_value=50_000_000),      # max 0.8억 -> drop
            "000002": radar.StockQuote("000002", 8000, 400, 5.2, 80000, "OPEN", "", trading_value=640_000_000, prev_trading_value=170_000_000),   # prev 1.7억, today 6.4억 -> KEEP!
            "140430": q_140430,                                                                                                                   # halted -> drop
            "000003": radar.StockQuote("000003", 3000, -100, -3.2, 10000, "OPEN", "", trading_value=30_000_000, prev_trading_value=40_000_000),   # 하락주의 warning -> KEEP!
            "000004": radar.StockQuote("000004", 10000, 0, 0.0, 0, "CLOSE", ""),                                                                  # unknown liquidity -> KEEP!
        }
        lf = radar.filter_illiquid_bullish_stocks(
            [
                {"code": "000001", "name": "극소형주", "direction": "상승수혜"},
                {"code": "000002", "name": "수급폭발소형주", "direction": "상승수혜"},
                {"code": "140430", "name": "정지종목", "direction": "상승수혜"},
                {"code": "000003", "name": "소형피해주", "direction": "하락주의"},
                {"code": "000004", "name": "시세미확인주", "direction": "상승수혜"},
            ],
            quotes_mix,
            min_trading_value_eok=5.0,
        )
        self.assertEqual([s["code"] for s in lf["kept_stocks"]], ["000002", "000003", "000004"])
        self.assertEqual(lf["excluded_codes"], ["000001", "140430"])
        self.assertFalse(lf["all_filtered"])

        # 3) E2E run_live blocks alert when all bullish candidates fail 5억 liquidity filter (emits illiquid_skip)
        comps = {c["code"]: dict(c) for c in radar.SEED_COMPANIES}
        item_ill = radar.NewsItem("AI 안경 부정행위 적발 전파탐지 의무화", "https://ex.com/ill1", "연합뉴스_사회", "사회/사건", first_seen=t_open.isoformat())

        def fake_gemini_ill(api_key, models, sp, up, **kw):
            if sp == radar.SCREENING_PROMPT:
                cid = json.loads(up)["clusters"][0]["cluster_id"]
                return {"selected_events": [{"cluster_id": cid, "catalyst_score": 8.6, "theme_key": "전파탐지_극소형", "second_order_keywords": ["전파탐지"]}]}, "gemini-3.5-flash-lite"
            return {"title": "전파탐지 테마", "event_summary": "요약", "stocks": [{"code": "446840", "name": "지슨", "direction": "상승수혜"}]}, "gemini-3.6-flash"

        q_tiny = {"446840": radar.StockQuote("446840", 5000, 100, 2.0, 10000, "OPEN", "🟢 미반영/초입 구간", trading_value=50_000_000, prev_trading_value=170_000_000)}
        with tempfile.TemporaryDirectory() as tmp_e2e:
            dir_e2e = Path(tmp_e2e)
            sent_ill: list[str] = []
            env_ill = {
                "GEMINI_API_KEY": "k",
                "TELEGRAM_BOT_TOKEN": "t",
                "TELEGRAM_CHAT_ID": "c",
                "MIN_TRADING_VALUE_EOK": "5.0",
                "INSTANT_FLASH_ALERT": "false",
                "TRIGGER_MODE": "shadow",
            }
            with (
                mock.patch.dict(os.environ, env_ill, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=[item_ill]),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini_ill),
                mock.patch.object(radar, "load_krx_master", return_value=comps),
                mock.patch.object(radar, "fetch_naver_quotes", return_value=q_tiny),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_ill.append(m) or 1),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=dir_e2e, now_fn=lambda: t_open, daemon_mode=True)

            self.assertEqual(len(sent_ill), 0)
            tr_ill = [json.loads(l) for l in (dir_e2e / "trace.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertTrue(any(r["type"] == "illiquid_skip" and r["blocked"] for r in tr_ill))

            # 4) PerformanceTracker separates unbuyable upper-limit lock from realistic buyable_net_avg_pct + Dashboard config API
            pt = radar.PerformanceTracker(dir_e2e / "performance.jsonl")
            pt.record_alert(
                alert_ts=datetime(2026, 11, 20, 8, 20, tzinfo=KST),
                theme_key="보안_이슈",
                sector="전파·물리보안",
                title="보안 수혜",
                catalyst_score=8.9,
                window="market",
                stocks=[
                    {"code": "446840", "name": "지슨", "direction": "상승수혜"},
                    {"code": "263860", "name": "지니언스", "direction": "상승수혜"},
                ],
                quotes={
                    "446840": q_446840,  # +29.95% OPEN -> buyable=False (상한가잠김), price=13000
                    "263860": radar.StockQuote("263860", 10000, 200, 2.0, 150000, "OPEN", "", trading_value=1_500_000_000, prev_trading_value=2_000_000_000),
                },
            )
            pt.update_pending(
                now=datetime(2026, 11, 20, 15, 35, tzinfo=KST),
                fetch_quotes_fn=lambda codes: {
                    "446840": radar.StockQuote("446840", 18200, 0, 0.0, 100000, "CLOSE", ""),   # +40.00% (unbuyable mirage!)
                    "263860": radar.StockQuote("263860", 10623, 0, 0.0, 200000, "CLOSE", ""),   # +6.23% (realistically buyable!)
                },
            )
            summary = radar.summarize_performance(pt.load_records())
            bull = summary["by_direction"]["상승수혜"]
            self.assertEqual(bull["total"], 2)
            self.assertEqual(bull["unbuyable_total"], 1)
            self.assertAlmostEqual(bull["ret_close"]["net_avg_pct"], 22.89, places=1)        # ((40 + 6.23)/2) - 0.23
            self.assertEqual(bull["ret_close"]["buyable_count"], 1)
            self.assertAlmostEqual(bull["ret_close"]["buyable_net_avg_pct"], 6.00, places=2)  # 6.23 - 0.23!

            # Verify Dashboard /api/config round-trips min_trading_value_eok
            srv = dashboard.create_dashboard_server("127.0.0.1", 0, data_dir=dir_e2e, password="")
            port = srv.server_address[1]
            t = _th.Thread(target=srv.serve_forever, daemon=True)
            t.start()
            try:
                req_cfg = urllib.request.Request(
                    f"http://127.0.0.1:{port}/api/config",
                    data=json.dumps({"min_trading_value_eok": 7.5}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req_cfg, timeout=5) as resp:
                    saved_cfg = json.loads(resp.read().decode("utf-8"))["config"]
                self.assertEqual(saved_cfg["min_trading_value_eok"], 7.5)
            finally:
                srv.shutdown()
                srv.server_close()


class TestPaperAutoTraderAndKISVTS(unittest.TestCase):
    def test_second_order_entry_filter_slippage_and_liquidity_cap(self) -> None:
        import paper_trader

        # 1) Candidate filter blocks 1차 직접, 단기테마성, and 하락주의 while allowing 2차 파급 + 본업직결/부분연관
        self.assertFalse(
            paper_trader.is_second_order_candidate(
                {"code": "005930", "name": "삼성전자", "direction": "상승수혜", "order_tier": "1차 직접", "revenue_link": "본업직결"}
            )
        )
        self.assertFalse(
            paper_trader.is_second_order_candidate(
                {"code": "999999", "name": "테마주", "direction": "상승수혜", "order_tier": "2차 파급", "revenue_link": "단기테마성"}
            )
        )
        self.assertFalse(
            paper_trader.is_second_order_candidate(
                {"code": "011200", "name": "HMM", "direction": "하락주의", "order_tier": "2차 파급", "revenue_link": "본업직결"}
            )
        )
        self.assertTrue(
            paper_trader.is_second_order_candidate(
                {"code": "263860", "name": "지니언스", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}
            )
        )

        # 2) Intraday buy applies KRX tick slippage, 0.015% buy fee, and 1% liquidity cap
        t_buy = datetime(2026, 11, 20, 10, 5, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            trader = paper_trader.PaperAutoTrader(tmp_dir, tmp_dir)
            sent_tg: list[str] = []

            # Daily turnover 6억 -> 1% position cap = 6,000,000원, order budget 1,000,000원 -> ~1,000,000원 used.
            # Now test smaller turnover 8억 with order_amount=10,000,000원 -> capped to 8,000,000원 (1% of 8억)!
            quotes = {
                "005930": radar.StockQuote("005930", 70000, 1000, 1.5, 500000, "OPEN", "", trading_value=50_000_000_000),
                "263860": radar.StockQuote(
                    "263860",
                    10000,
                    150,
                    1.5,
                    80000,
                    "OPEN",
                    "🟢 미반영/초입 구간",
                    trading_value=800_000_000,
                    prev_trading_value=600_000_000,
                ),
            }
            res = trader.on_alert_candidates(
                alert_ts=t_buy,
                theme_key="통신망_해킹",
                sector="사이버보안",
                title="통신망 해킹 → 보안솔루션 2차 수혜",
                catalyst_score=8.8,
                stocks=[
                    {"code": "005930", "name": "삼성전자", "direction": "상승수혜", "order_tier": "1차 직접", "revenue_link": "본업직결"},
                    {"code": "263860", "name": "지니언스", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"},
                ],
                quotes=quotes,
                runtime_cfg={"autotrade_enabled": True, "autotrade_order_amount_krw": 10_000_000},
                daemon_mode=True,
                send_fn=sent_tg.append,
            )
            self.assertIsNotNone(res)
            assert res is not None
            self.assertEqual(res["action"], "bought")
            pos = res["position"]
            self.assertEqual(pos["code"], "263860")  # 1차 직접(005930) skipped, 2차 파급(263860) bought!
            # 10,000원 in <15억 liquidity -> +3 ticks * 10원 = 10,030원 fill price
            self.assertEqual(pos["avg_buy_price"], 10030)
            # Capped by 1% of 8억 = 8,000,000원 (not 9,800,000원)
            self.assertLessEqual(pos["invested_krw"], 8_010_000)
            self.assertGreater(pos["invested_krw"], 7_900_000)
            self.assertEqual(len(sent_tg), 1)
            self.assertIn("[모의 자동매수 체결]", sent_tg[0])

            # Same code on same day is NOT bought a second time
            dup_res = trader.on_alert_candidates(
                alert_ts=t_buy + timedelta(minutes=10),
                theme_key="통신망_해킹_후속",
                sector="사이버보안",
                title="후속 기사",
                catalyst_score=9.1,
                stocks=[{"code": "263860", "name": "지니언스", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}],
                quotes=quotes,
                daemon_mode=True,
            )
            self.assertIsNone(dup_res)

    def test_premarket_queue_buys_calm_open_and_cancels_gap_up(self) -> None:
        import paper_trader

        t_digest = datetime(2026, 11, 20, 7, 50, tzinfo=KST)
        t_whipsaw = datetime(2026, 11, 20, 9, 2, tzinfo=KST)
        t_open = datetime(2026, 11, 20, 9, 9, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            trader = paper_trader.PaperAutoTrader(tmp_dir, tmp_dir)
            sent_tg: list[str] = []

            # Queue 2 premarket candidates at 07:50
            q1 = trader.on_alert_candidates(
                alert_ts=t_digest,
                theme_key="변압기_수주",
                sector="전력기기·전선",
                title="북미 변압기 공급 부족",
                catalyst_score=8.7,
                stocks=[{"code": "267260", "name": "HD현대일렉트릭", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}],
                quotes={},
                daemon_mode=True,
                send_fn=sent_tg.append,
            )
            q2 = trader.on_alert_candidates(
                alert_ts=t_digest,
                theme_key="전파탐지_이슈",
                sector="전파·물리보안",
                title="시험장 전파차단 의무화",
                catalyst_score=8.4,
                stocks=[{"code": "446840", "name": "지슨", "direction": "상승수혜", "order_tier": "2차 파급", "revenue_link": "본업직결"}],
                quotes={},
                daemon_mode=True,
                send_fn=sent_tg.append,
            )
            self.assertEqual(q1["action"], "queued_for_open")  # type: ignore[index]
            self.assertEqual(q2["action"], "queued_for_open")  # type: ignore[index]
            self.assertEqual(len(sent_tg), 0)

            open_quotes = {
                "267260": radar.StockQuote("267260", 50000, 900, 1.8, 100000, "OPEN", "", trading_value=5_000_000_000),
                "446840": radar.StockQuote("446840", 11250, 1250, 12.5, 100000, "OPEN", "", trading_value=1_500_000_000),
            }
            # At 09:02 (opening-bell chaos before 09:08): waits in queue without premature execution!
            early_res = trader.tick_positions(
                now=t_whipsaw,
                daemon_mode=True,
                fetch_quotes_fn=lambda codes: {c: open_quotes[c] for c in codes if c in open_quotes},
                send_fn=sent_tg.append,
            )
            self.assertEqual(early_res, {"bought": 0, "sold": 0})
            self.assertEqual(len(trader.build_overview(now=t_whipsaw)["pending_open_entries"]), 2)

            # At 09:09 (>= 09:08 stabilized): 267260 (+1.8%, 50억) -> BOUGHT; 446840 (+12.5%) -> CANCELLED!
            tick_res = trader.tick_positions(
                now=t_open,
                daemon_mode=True,
                fetch_quotes_fn=lambda codes: {c: open_quotes[c] for c in codes if c in open_quotes},
                send_fn=sent_tg.append,
            )
            self.assertEqual(tick_res, {"bought": 1, "sold": 0})
            ov = trader.build_overview(now=t_open)
            self.assertEqual([p["code"] for p in ov["positions"]], ["267260"])
            self.assertEqual(ov["pending_open_entries"], [])
            self.assertEqual(len(sent_tg), 1)
            self.assertIn("HD현대일렉트릭(267260)", sent_tg[0])

    def test_exit_engine_trailing_stop_stoploss_and_eod_liquidation(self) -> None:
        import paper_trader

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            trader = paper_trader.PaperAutoTrader(tmp_dir, tmp_dir)
            trace = radar.TraceLogger(tmp_dir / "trace.jsonl")
            sent_tg: list[str] = []

            # Buy 3 positions at 09:30 (50억+ turnover -> 1 tick slippage)
            t0 = datetime(2026, 11, 20, 9, 30, tzinfo=KST)
            for idx, (code, name) in enumerate([("100001", "트레일링주"), ("100002", "손절주"), ("100003", "장마감주")]):
                trader.on_alert_candidates(
                    alert_ts=t0 + timedelta(minutes=idx),
                    theme_key=f"테마_{idx}",
                    sector="사이버보안",
                    title=f"이슈 {idx}",
                    catalyst_score=8.5,
                    stocks=[{"code": code, "name": name, "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}],
                    quotes={code: radar.StockQuote(code, 10000, 100, 1.0, 600000, "OPEN", "", trading_value=6_000_000_000)},
                    daemon_mode=True,
                    send_fn=sent_tg.append,
                    trace=trace,
                )
            self.assertEqual(len(trader.load_account(t0)["positions"]), 3)

            # 1) 10:00 -> 100001 rises to 10,600 (+5.9% vs 10,010 buy) -> arms trailing stop, does NOT sell yet!
            #    100002 drops to 9,650 (-3.6% vs 10,010 buy) -> triggers -3.0% hard stop-loss!
            t1 = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
            r1 = trader.tick_positions(
                now=t1,
                daemon_mode=True,
                fetch_quotes_fn=lambda codes: {
                    "100001": radar.StockQuote("100001", 10600, 700, 7.0, 700000, "OPEN", "", trading_value=7_000_000_000),
                    "100002": radar.StockQuote("100002", 9650, -250, -2.5, 650000, "OPEN", "", trading_value=6_500_000_000),
                    "100003": radar.StockQuote("100003", 10150, 250, 2.5, 600000, "OPEN", "", trading_value=6_000_000_000),
                },
                send_fn=sent_tg.append,
                trace=trace,
            )
            self.assertEqual(r1, {"bought": 0, "sold": 1})
            acc1 = trader.load_account(t1)
            self.assertTrue(acc1["positions"]["100001"]["trailing_armed"])
            self.assertNotIn("100002", acc1["positions"])

            # 2) 10:20 -> 100001 surges to 11,100 (+10.89% peak), still rising -> held!
            t2 = datetime(2026, 11, 20, 10, 20, tzinfo=KST)
            r2 = trader.tick_positions(
                now=t2,
                daemon_mode=True,
                fetch_quotes_fn=lambda codes: {
                    "100001": radar.StockQuote("100001", 11100, 1200, 12.1, 800000, "OPEN", "", trading_value=8_000_000_000),
                    "100003": radar.StockQuote("100003", 10200, 300, 3.0, 620000, "OPEN", "", trading_value=6_200_000_000),
                },
                send_fn=sent_tg.append,
                trace=trace,
            )
            self.assertEqual(r2, {"bought": 0, "sold": 0})
            self.assertEqual(trader.load_account(t2)["positions"]["100001"]["highest_price"], 11100)

            # 3) 10:35 -> 100001 pulls back from 11,100 to 10,820 (-2.52% pullback >= 2.0% trailing) -> trailing profit exit!
            t3 = datetime(2026, 11, 20, 10, 35, tzinfo=KST)
            r3 = trader.tick_positions(
                now=t3,
                daemon_mode=True,
                fetch_quotes_fn=lambda codes: {
                    "100001": radar.StockQuote("100001", 10820, 920, 9.3, 850000, "OPEN", "", trading_value=8_500_000_000),
                    "100003": radar.StockQuote("100003", 10200, 300, 3.0, 620000, "OPEN", "", trading_value=6_200_000_000),
                },
                send_fn=sent_tg.append,
                trace=trace,
            )
            self.assertEqual(r3, {"bought": 0, "sold": 1})

            # 4) 15:15 -> 100003 liquidated by EOD rule!
            t4 = datetime(2026, 11, 20, 15, 15, tzinfo=KST)
            r4 = trader.tick_positions(
                now=t4,
                daemon_mode=True,
                fetch_quotes_fn=lambda codes: {
                    "100003": radar.StockQuote("100003", 10180, 280, 2.8, 700000, "OPEN", "", trading_value=7_000_000_000),
                },
                send_fn=sent_tg.append,
                trace=trace,
            )
            self.assertEqual(r4, {"bought": 0, "sold": 1})

            ov = trader.build_overview(now=t4)
            self.assertEqual(ov["positions"], [])
            self.assertEqual(ov["trade_count"], 3)
            self.assertEqual(ov["win_count"], 2)
            self.assertEqual(ov["loss_count"], 1)
            reasons = [tr["exit_reason"] for tr in ov["recent_trades"]]
            self.assertTrue(any("장마감 시간청산" in r for r in reasons))
            self.assertTrue(any("트레일링 익절" in r for r in reasons))
            self.assertTrue(any("손절" in r for r in reasons))

    def test_kis_vts_client_mocked_token_cache_and_vts_orders(self) -> None:
        import paper_trader

        now = datetime(2026, 11, 20, 10, 0, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            (tmp_dir / ".env").write_text(
                "KIS_APP_KEY=PS_MOCK_KEY_12345\nKIS_APP_SECRET= MOCK_SECRET_67890 \nKIS_CANO=50123456-01\n",
                encoding="utf-8",
            )
            creds = paper_trader.resolve_kis_credentials(tmp_dir)
            self.assertEqual(creds["configured"], "true")
            self.assertEqual(creds["cano"], "50123456")
            self.assertEqual(creds["acnt_prdt_cd"], "01")
            self.assertEqual(creds["cano_masked"], "5012****-01")

            called_reqs: list[tuple[str, str, dict[str, str]]] = []

            def fake_kis_http(req, timeout=0):
                url = req.full_url
                hdrs = {k.lower(): v for k, v in req.header_items()}
                called_reqs.append((req.get_method(), url, hdrs))
                # Hard safety check: EVERY request must target openapivts.koreainvestment.com:29443!
                self.assertTrue(url.startswith("https://openapivts.koreainvestment.com:29443/"))
                if url.endswith("/oauth2/tokenP"):
                    return _FakeResponse({"access_token": "vts_tok_abc", "expires_in": 86400})
                if "/uapi/domestic-stock/v1/trading/order-cash" in url:
                    return _FakeResponse({"rt_cd": "0", "msg1": "모의주문 접수 완료", "output": {"ODNO": "0009876"}})
                if "/uapi/domestic-stock/v1/trading/inquire-balance" in url:
                    return _FakeResponse(
                        {
                            "rt_cd": "0",
                            "output1": [{"pdno": "263860", "hldg_qty": "10"}],
                            "output2": [{"dnca_tot_amt": "9500000", "tot_evlu_amt": "10120000", "evlu_pfls_smtl_amt": "120000"}],
                        }
                    )
                raise AssertionError(f"Unexpected URL: {url}")

            client = paper_trader.KISPaperClient(creds, tmp_dir)
            with mock.patch("urllib.request.urlopen", side_effect=fake_kis_http):
                ok_buy, odno_buy, _ = client.place_market_order(side="buy", code="263860", qty=10, now=now)
                ok_sell, odno_sell, _ = client.place_market_order(side="sell", code="263860", qty=10, now=now + timedelta(minutes=5))
                bal = client.inquire_balance(now=now + timedelta(minutes=6))

            self.assertTrue(ok_buy)
            self.assertEqual(odno_buy, "0009876")
            self.assertTrue(ok_sell)
            self.assertEqual(odno_sell, "0009876")
            self.assertEqual(bal["total_equity_krw"], 10120000)

            # Token issued ONCE and reused from kis_token_vts.json for all 3 API calls!
            token_calls = [u for _, u, _ in called_reqs if u.endswith("/oauth2/tokenP")]
            self.assertEqual(len(token_calls), 1)
            order_tr_ids = [h["tr_id"] for _, u, h in called_reqs if "order-cash" in u]
            self.assertEqual(order_tr_ids, ["VTTC0802U", "VTTC0801U"])

    def test_e2e_run_live_autotrader_and_dashboard_autotrade_endpoints(self) -> None:
        import threading as _th
        import dashboard

        t_live = datetime(2026, 11, 20, 10, 12, tzinfo=KST)
        comps = {c["code"]: dict(c) for c in radar.SEED_COMPANIES}
        item = radar.NewsItem(
            "대형 통신사 유심 서버 해킹 발생… 보안 관제 긴급 도입",
            "https://ex.com/hack_auto_1",
            "연합뉴스_IT",
            "IT/과학",
            first_seen=t_live.isoformat(),
        )

        def fake_gemini(api_key, models, sp, up, **kw):
            if sp == radar.SCREENING_PROMPT:
                cid = json.loads(up)["clusters"][0]["cluster_id"]
                return {
                    "selected_events": [
                        {
                            "cluster_id": cid,
                            "catalyst_score": 8.9,
                            "theme_key": "통신해킹_보안수혜",
                            "second_order_keywords": ["네트워크 접근제어", "EDR"],
                        }
                    ]
                }, "gemini-2.5-flash-lite"
            return {
                "title": "통신사 유심 해킹 → EDR·접근제어 2차 수혜",
                "event_summary": "해킹 사고로 보안 솔루션 긴급 발주",
                "stocks": [
                    {"code": "005930", "name": "삼성전자", "direction": "상승수혜", "order_tier": "1차 직접", "revenue_link": "본업직결"},
                    {"code": "263860", "name": "지니언스", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"},
                ],
            }, "gemini-2.5-flash"

        q_live = {
            "005930": radar.StockQuote("005930", 70000, 700, 1.0, 500000, "OPEN", "", trading_value=30_000_000_000),
            "263860": radar.StockQuote("263860", 12000, 180, 1.5, 250000, "OPEN", "🟢 미반영/초입 구간", trading_value=2_500_000_000),
        }
        q_higher = {
            "263860": radar.StockQuote("263860", 12650, 830, 7.0, 320000, "OPEN", "", trading_value=3_500_000_000),
        }

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            sent_msgs: list[str] = []
            env = {
                "GEMINI_API_KEY": "k",
                "TELEGRAM_BOT_TOKEN": "t",
                "TELEGRAM_CHAT_ID": "c",
                "INSTANT_FLASH_ALERT": "false",
                "TRIGGER_MODE": "shadow",
            }
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=[item]),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini),
                mock.patch.object(radar, "load_krx_master", return_value=comps),
                mock.patch.object(radar, "fetch_naver_quotes", return_value=q_live),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_msgs.append(m) or 1),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t_live, daemon_mode=True)

            # Message 0 = Stage 2 Radar report, Message 1 = Paper Auto-Buy execution alert!
            self.assertEqual(len(sent_msgs), 2)
            self.assertIn("[모의 자동매수 체결]", sent_msgs[1])
            self.assertIn("지니언스(263860)", sent_msgs[1])

            srv = dashboard.create_dashboard_server("127.0.0.1", 0, data_dir=tmp_dir, password="")
            port = srv.server_address[1]
            th = _th.Thread(target=srv.serve_forever, daemon=True)
            th.start()
            base = f"http://127.0.0.1:{port}"
            try:
                # 1) GET /api/overview exposes open position in autotrade
                with urllib.request.urlopen(f"{base}/api/overview", timeout=5) as resp:
                    ov = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(len(ov["autotrade"]["positions"]), 1)
                self.assertEqual(ov["autotrade"]["positions"][0]["code"], "263860")

                # 2) POST /api/config updates autotrade rules cleanly
                req_cfg = urllib.request.Request(
                    f"{base}/api/config",
                    data=json.dumps({"autotrade_take_profit_pct": 6.5, "autotrade_order_amount_krw": 1_500_000}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req_cfg, timeout=5) as resp:
                    cfg_saved = json.loads(resp.read().decode("utf-8"))["config"]
                self.assertEqual(cfg_saved["autotrade_take_profit_pct"], 6.5)
                self.assertEqual(cfg_saved["autotrade_order_amount_krw"], 1_500_000)

                # 3) POST /api/actions/autotrade/close liquidates position at q_higher (+5% profit)
                with mock.patch.object(radar, "fetch_naver_quotes", return_value=q_higher):
                    req_close = urllib.request.Request(
                        f"{base}/api/actions/autotrade/close",
                        data=json.dumps({"code": "ALL"}).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req_close, timeout=5) as resp:
                        close_res = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(close_res["ok"])
                self.assertEqual(close_res["result"]["closed_count"], 1)
                self.assertEqual(close_res["autotrade"]["positions"], [])
                self.assertGreater(close_res["autotrade"]["realized_pnl_krw"], 0)

                # 4) POST /api/actions/autotrade/reset restores 10,000,000원 clean slate
                req_reset = urllib.request.Request(
                    f"{base}/api/actions/autotrade/reset",
                    data=b"{}",
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req_reset, timeout=5) as resp:
                    reset_res = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(reset_res["autotrade"]["cash_krw"], 10_000_000)
                self.assertEqual(reset_res["autotrade"]["trade_count"], 0)
            finally:
                srv.shutdown()
                srv.server_close()


class TestReportSyncAndFullSetFeedbackLoop(unittest.TestCase):
    def test_analytics_bundle_traded_vs_skipped_commit_stats_and_zero_secret_leak(self) -> None:
        import paper_trader
        import report_sync

        t_entry = datetime(2026, 11, 23, 10, 5, tzinfo=KST)
        t_peak = datetime(2026, 11, 23, 10, 40, tzinfo=KST)
        t_exit = datetime(2026, 11, 23, 11, 10, tzinfo=KST)
        t_close = datetime(2026, 11, 23, 15, 40, tzinfo=KST)

        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(os.environ, {}, clear=False):
            tmp_dir = Path(tmp)
            # Populate fake secrets in .env to prove 0% secret leakage in exported bundle & markdown
            (tmp_dir / ".env").write_text(
                "GEMINI_API_KEY=SECRET_GEMINI_999\n"
                "TELEGRAM_BOT_TOKEN=SECRET_TG_888\n"
                "DASHBOARD_PASSWORD=SECRET_PW_777\n"
                "GITHUB_REPORT_TOKEN=ghp_SECRET_GH_666\n",
                encoding="utf-8",
            )
            trace = radar.TraceLogger(tmp_dir / "trace.jsonl")
            trace.emit({"ts": t_entry.isoformat(), "type": "illiquid_skip", "blocked": True, "theme_key": "소형테마", "details": ["초소형(1.2억<5.0억)"]})
            trace.emit({"ts": t_entry.isoformat(), "type": "prepriced_skip", "theme_key": "급등테마", "details": ["급등주(+19.0%)"]})

            pt = radar.PerformanceTracker(tmp_dir / "performance.jsonl")
            pt.record_alert(
                alert_ts=t_entry,
                theme_key="통신망_해킹",
                sector="사이버보안",
                title="통신사 해킹 보안 수혜",
                catalyst_score=8.9,
                window="market",
                stocks=[
                    {"code": "263860", "name": "지니언스", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"},
                    {"code": "005930", "name": "삼성전자", "direction": "상승수혜", "order_tier": "1차 직접", "revenue_link": "본업직결"},
                    {"code": "298040", "name": "효성중공업", "direction": "상승수혜", "order_tier": "2차 파급", "revenue_link": "본업직결"},
                ],
                quotes={
                    "263860": radar.StockQuote("263860", 10000, 150, 1.5, 500000, "OPEN", "", trading_value=6_000_000_000),
                    "005930": radar.StockQuote("005930", 70000, 700, 1.0, 500000, "OPEN", "", trading_value=50_000_000_000),
                    "298040": radar.StockQuote("298040", 400000, 28000, 7.5, 100000, "OPEN", "", trading_value=15_000_000_000),
                },
            )
            pt.update_pending(
                now=t_close,
                fetch_quotes_fn=lambda codes: {
                    "263860": radar.StockQuote("263860", 10800, 0, 0.0, 600000, "CLOSE", ""),  # +8.0%
                    "005930": radar.StockQuote("005930", 68600, 0, 0.0, 600000, "CLOSE", ""),  # -2.0%
                    "298040": radar.StockQuote("298040", 448000, 0, 0.0, 150000, "CLOSE", ""), # +12.0%
                },
            )

            trader = paper_trader.PaperAutoTrader(tmp_dir, tmp_dir)
            with mock.patch.object(report_sync, "try_background_github_sync"):
                trader.on_alert_candidates(
                    alert_ts=t_entry,
                    theme_key="통신망_해킹",
                    sector="사이버보안",
                    title="통신사 해킹 보안 수혜",
                    catalyst_score=8.9,
                    stocks=[
                        {"code": "005930", "name": "삼성전자", "direction": "상승수혜", "order_tier": "1차 직접", "revenue_link": "본업직결"},
                        {"code": "263860", "name": "지니언스", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"},
                        {"code": "298040", "name": "효성중공업", "direction": "상승수혜", "order_tier": "2차 파급", "revenue_link": "본업직결"},
                    ],
                    quotes={
                        "005930": radar.StockQuote("005930", 70000, 700, 1.0, 500000, "OPEN", "", trading_value=50_000_000_000),
                        "263860": radar.StockQuote("263860", 10000, 150, 1.5, 500000, "OPEN", "", trading_value=6_000_000_000),
                        "298040": radar.StockQuote("298040", 400000, 28000, 7.5, 100000, "OPEN", "", trading_value=15_000_000_000),
                    },
                    daemon_mode=True,
                    trace=trace,
                )
                # Peak at 10,950 (+9.39%), then trailing pullback to 10,700 -> trailing exit!
                trader.tick_positions(
                    now=t_peak,
                    daemon_mode=True,
                    fetch_quotes_fn=lambda c: {"263860": radar.StockQuote("263860", 10950, 1100, 11.0, 600000, "OPEN", "", trading_value=7_000_000_000)},
                    trace=trace,
                )
                trader.tick_positions(
                    now=t_exit,
                    daemon_mode=True,
                    fetch_quotes_fn=lambda c: {"263860": radar.StockQuote("263860", 10700, 850, 8.5, 650000, "OPEN", "", trading_value=7_500_000_000)},
                    trace=trace,
                )

            bundle = report_sync.build_analytics_bundle(tmp_dir, tmp_dir, target_date="2026-11-23", now=t_close)
            md = report_sync.format_ai_feedback_markdown(bundle)
            raw_json = json.dumps(bundle, ensure_ascii=False)

            # Verify zero secret leakage
            for secret in ("SECRET_GEMINI_999", "SECRET_TG_888", "SECRET_PW_777", "ghp_SECRET_GH_666"):
                self.assertNotIn(secret, raw_json)
                self.assertNotIn(secret, md)

            # Verify trade + peak giveback + commit tracking + traded vs skipped matrix
            self.assertEqual(bundle["today_trade_summary"]["closed_count"], 1)
            self.assertEqual(bundle["today_trade_summary"]["wins"], 1)
            self.assertGreater(bundle["today_closed_trades"][0]["missed_from_peak_pct"], 2.0)
            self.assertTrue(len(bundle["by_commit"]) >= 1)

            by_code = {r["code"]: r for r in bundle["today_alert_vs_trade_matrix"]}
            self.assertTrue(by_code["263860"]["was_traded"])
            self.assertFalse(by_code["005930"]["was_traded"])
            self.assertIn("1차/단기테마 제외", by_code["005930"]["skip_reason_hint"])
            self.assertFalse(by_code["298040"]["was_traded"])
            self.assertIn("진입상한 초과", by_code["298040"]["skip_reason_hint"])
            self.assertEqual(bundle["today_funnel"]["illiquid_blocked"], 1)
            self.assertEqual(bundle["today_funnel"]["prepriced_blocked"], 1)

    def test_github_contents_put_dedup_eod_telegram_and_dashboard_sync_routes(self) -> None:
        import base64 as _b64
        import threading as _th
        import dashboard
        import report_sync

        t_eod = datetime(2026, 11, 23, 15, 40, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GITHUB_REPORT_TOKEN", None)
            os.environ.pop("GITHUB_REPORT_REPO", None)
            tmp_dir = Path(tmp)
            (tmp_dir / ".env").write_text("# 기존 주석 유지\nGEMINI_API_KEY=k\n", encoding="utf-8")

            # Seed 1 alert so EOD summary sees real activity
            radar.PerformanceTracker(tmp_dir / "performance.jsonl").record_alert(
                alert_ts=datetime(2026, 11, 23, 10, 0, tzinfo=KST),
                theme_key="변압기_수주",
                sector="전력기기·전선",
                title="변압기 수혜",
                catalyst_score=8.7,
                window="market",
                stocks=[{"code": "267260", "name": "HD현대일렉트릭", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}],
                quotes={"267260": radar.StockQuote("267260", 50000, 500, 1.0, 100000, "OPEN", "", trading_value=5_000_000_000)},
            )

            gh_store: dict[str, bytes] = {}
            put_calls: list[str] = []

            def fake_github_api(req, timeout=0):
                url = req.full_url
                self.assertTrue(url.startswith("https://api.github.com/repos/jjangna0817-hue/news-alpha-radar-logs/contents/"))
                rel = urllib.parse.unquote(url.split("/contents/", 1)[1])
                method = req.get_method()
                if method == "GET":
                    if rel not in gh_store:
                        raise urllib.error.HTTPError(url, 404, "Not Found", {}, io.BytesIO(b"{}"))
                    return _FakeResponse(
                        {
                            "sha": "sha123",
                            "content": _b64.b64encode(gh_store[rel]).decode("ascii"),
                        }
                    )
                if method == "PUT":
                    body = json.loads(req.data.decode("utf-8"))
                    gh_store[rel] = _b64.b64decode(body["content"])
                    put_calls.append(rel)
                    return _FakeResponse({"content": {"path": rel, "sha": "sha456"}}, status=201)
                raise AssertionError(f"Unexpected GitHub call: {method} {url}")

            srv = dashboard.create_dashboard_server("127.0.0.1", 0, data_dir=tmp_dir, password="")
            port = srv.server_address[1]
            th = _th.Thread(target=srv.serve_forever, daemon=True)
            th.start()
            base = f"http://127.0.0.1:{port}"
            try:
                # 1) GET /api/autotrade/report returns ready-to-paste markdown & bundle
                with urllib.request.urlopen(f"{base}/api/autotrade/report?date=2026-11-23", timeout=5) as resp:
                    rep_res = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(rep_res["ok"])
                self.assertIn("News Alpha Radar 일일 통합 성적표", rep_res["markdown"])

                # 2) POST /api/actions/autotrade/github-config saves token to .env and uploads reports/*.{json,md}
                real_urlopen = urllib.request.urlopen

                def selective_urlopen(req_or_url, *a, **kw):
                    u = req_or_url.full_url if hasattr(req_or_url, "full_url") else str(req_or_url)
                    if u.startswith("https://api.github.com/"):
                        return fake_github_api(req_or_url, *a, **kw)
                    return real_urlopen(req_or_url, *a, **kw)

                with mock.patch("urllib.request.urlopen", side_effect=selective_urlopen):
                    req_gh = urllib.request.Request(
                        f"{base}/api/actions/autotrade/github-config",
                        data=json.dumps(
                            {
                                "repo": "jjangna0817-hue/news-alpha-radar-logs",
                                "token": "github_pat_TESTTOKEN123456",
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with real_urlopen(req_gh, timeout=5) as resp:
                        gh_res = json.loads(resp.read().decode("utf-8"))
                    self.assertTrue(gh_res["ok"])
                    self.assertIn("reports/latest.json", gh_store)
                    self.assertIn("# 기존 주석 유지", (tmp_dir / ".env").read_text(encoding="utf-8"))

                    # Second sync at exact same timestamp with identical content skips redundant PUTs!
                    put_calls.clear()
                    s_same1 = report_sync.sync_report_to_github(tmp_dir, tmp_dir, target_date="2026-11-23", include_prev_trading_day=False, now=t_eod)
                    put_calls.clear()
                    s_same2 = report_sync.sync_report_to_github(tmp_dir, tmp_dir, target_date="2026-11-23", include_prev_trading_day=False, now=t_eod)
                    self.assertTrue(s_same1["ok"] and s_same2["ok"])
                    self.assertEqual(put_calls, [])

                    # 3) maybe_run_daily_report_and_sync sends 15:35 EOD Telegram summary ONCE per day
                    eod_tg: list[str] = []
                    r_first = report_sync.maybe_run_daily_report_and_sync(
                        tmp_dir,
                        tmp_dir,
                        now=t_eod,
                        daemon_mode=True,
                        send_fn=eod_tg.append,
                    )
                    r_second = report_sync.maybe_run_daily_report_and_sync(
                        tmp_dir,
                        tmp_dir,
                        now=t_eod + timedelta(minutes=10),
                        daemon_mode=True,
                        send_fn=eod_tg.append,
                    )
                    self.assertTrue(r_first["telegram_sent"])
                    self.assertFalse(r_second["telegram_sent"])
                    self.assertEqual(len(eod_tg), 1)
                    self.assertIn("[일일 자동매매 &amp; 알림 성적표 · 2026-11-23]", eod_tg[0])
            finally:
                srv.shutdown()
                srv.server_close()


class TestStructuralGuardsNov23Fixes(unittest.TestCase):
    def test_blocks_substitute_diffusion_laggard_when_core_overheated_but_allows_second_order_vs_first_order(self) -> None:
        import paper_trader
        import report_sync

        t_mid = datetime(2026, 11, 24, 10, 45, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            trace = radar.TraceLogger(tmp_dir / "trace.jsonl")
            trader = paper_trader.PaperAutoTrader(tmp_dir, tmp_dir)

            # 1) Exact reproduction of 2026-11-23 한한령_해제_수혜:
            #    4 core 2nd-order stocks already surged +6% ~ +10%, peripheral 5th stock (확산) is -1.30%.
            #    Must NOT buy the peripheral substitute laggard!
            stocks_hanhan = [
                {"code": "123690", "name": "한국화장품", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"},
                {"code": "253450", "name": "스튜디오드래곤", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"},
                {"code": "104620", "name": "노랑풍선", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"},
                {"code": "035760", "name": "CJ ENM", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"},
                {"code": "003350", "name": "한국화장품제조", "direction": "상승수혜", "order_tier": "2차 파급(확산)", "revenue_link": "본업직결"},
            ]
            quotes_hanhan = {
                "123690": radar.StockQuote("123690", 11000, 990, 9.91, 800000, "OPEN", "", trading_value=12_000_000_000),
                "253450": radar.StockQuote("253450", 48000, 4400, 10.05, 500000, "OPEN", "", trading_value=25_000_000_000),
                "104620": radar.StockQuote("104620", 6500, 380, 6.11, 700000, "OPEN", "", trading_value=9_000_000_000),
                "035760": radar.StockQuote("035760", 62000, 3900, 6.75, 400000, "OPEN", "", trading_value=18_000_000_000),
                "003350": radar.StockQuote("003350", 54200, -710, -1.30, 120000, "OPEN", "", trading_value=6_500_000_000),
            }
            with mock.patch.object(report_sync, "try_background_github_sync"):
                res_blocked = trader.on_alert_candidates(
                    alert_ts=t_mid,
                    theme_key="한한령_해제_수혜",
                    sector="화장품·소비재",
                    title="한한령 해제 기대감 → K-뷰티·콘텐츠 2차 수혜",
                    catalyst_score=8.7,
                    stocks=stocks_hanhan,
                    quotes=quotes_hanhan,
                    daemon_mode=True,
                    trace=trace,
                )
            self.assertIsNone(res_blocked)
            self.assertEqual(trader.load_account(t_mid)["positions"], {})

            funnel = report_sync._summarize_traces_for_date(tmp_dir, "2026-11-24")
            self.assertEqual(funnel["substitute_buy_blocked"], 1)

            # 2) Conversely, when 1차 직접 (삼성SDI +12%) surges while 2차 파급(핵심) (원익IPS +1.2%) is still calm,
            #    buying the 2차 파급(핵심) supplier MUST succeed!
            with mock.patch.object(report_sync, "try_background_github_sync"):
                res_ok = trader.on_alert_candidates(
                    alert_ts=t_mid + timedelta(minutes=5),
                    theme_key="반도체_대규모투자",
                    sector="반도체·소부장",
                    title="대형 칩메이커 라인 증설 → 전공정 장비 수혜",
                    catalyst_score=8.8,
                    stocks=[
                        {"code": "006400", "name": "삼성SDI", "direction": "상승수혜", "order_tier": "1차 직접", "revenue_link": "본업직결"},
                        {"code": "240810", "name": "원익IPS", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"},
                    ],
                    quotes={
                        "006400": radar.StockQuote("006400", 300000, 32000, 12.0, 500000, "OPEN", "", trading_value=80_000_000_000),
                        "240810": radar.StockQuote("240810", 30000, 350, 1.2, 300000, "OPEN", "", trading_value=9_000_000_000),
                    },
                    daemon_mode=True,
                    trace=trace,
                )
            self.assertIsNotNone(res_ok)
            assert res_ok is not None
            self.assertEqual(res_ok["position"]["code"], "240810")

    def test_entry_window_0908_to_1400_queues_early_and_skips_afternoon_while_keeping_exits(self) -> None:
        import paper_trader
        import report_sync

        t_903 = datetime(2026, 11, 24, 9, 3, tzinfo=KST)
        t_1015 = datetime(2026, 11, 24, 10, 15, tzinfo=KST)
        t_1417 = datetime(2026, 11, 24, 14, 17, tzinfo=KST)
        t_1515 = datetime(2026, 11, 24, 15, 15, tzinfo=KST)

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            trace = radar.TraceLogger(tmp_dir / "trace.jsonl")
            trader = paper_trader.PaperAutoTrader(tmp_dir, tmp_dir)

            with mock.patch.object(report_sync, "try_background_github_sync"):
                # 1) Alert at 09:03 (< 09:08) queues for 09:08 stabilization instead of buying opening whipsaw
                r_early = trader.on_alert_candidates(
                    alert_ts=t_903,
                    theme_key="보안_이슈",
                    sector="사이버보안",
                    title="개장 직후 보안 속보",
                    catalyst_score=8.5,
                    stocks=[{"code": "263860", "name": "지니언스", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}],
                    quotes={"263860": radar.StockQuote("263860", 18000, 180, 1.0, 100000, "OPEN", "", trading_value=3_000_000_000)},
                    daemon_mode=True,
                    trace=trace,
                )
                self.assertIsNotNone(r_early)
                assert r_early is not None
                self.assertEqual(r_early["action"], "queued_for_open")

                # 2) Alert at 10:15 (inside 09:08 ~ 14:00) buys immediately
                r_mid = trader.on_alert_candidates(
                    alert_ts=t_1015,
                    theme_key="로봇_자동화",
                    sector="로봇·자동화",
                    title="공장 자동화 도입 확대",
                    catalyst_score=8.6,
                    stocks=[{"code": "348340", "name": "뉴로메카", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}],
                    quotes={"348340": radar.StockQuote("348340", 25000, 400, 1.6, 200000, "OPEN", "", trading_value=6_000_000_000)},
                    daemon_mode=True,
                    trace=trace,
                )
                self.assertIsNotNone(r_mid)
                assert r_mid is not None
                self.assertEqual(r_mid["action"], "bought")

                # 3) Alert at 14:17 (> 14:00 cutoff) skips new buy (reproducing 아이티센글로벌 late-entry fix)
                r_late = trader.on_alert_candidates(
                    alert_ts=t_1417,
                    theme_key="스테이블코인_이슈",
                    sector="금융·핀테크",
                    title="오후 늦은 스테이블코인 기사",
                    catalyst_score=8.4,
                    stocks=[{"code": "124500", "name": "아이티센글로벌", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}],
                    quotes={"124500": radar.StockQuote("124500", 10000, 150, 1.5, 300000, "OPEN", "", trading_value=5_000_000_000)},
                    daemon_mode=True,
                    trace=trace,
                )
                self.assertIsNone(r_late)
                self.assertNotIn("124500", trader.load_account(t_1417)["positions"])

                # 4) Exit monitoring remains active after 14:00 -> liquidates 348340 at 15:15 EOD!
                r_eod = trader.tick_positions(
                    now=t_1515,
                    daemon_mode=True,
                    fetch_quotes_fn=lambda codes: {"348340": radar.StockQuote("348340", 25600, 1000, 4.0, 300000, "OPEN", "", trading_value=8_000_000_000)},
                    trace=trace,
                )
                self.assertEqual(r_eod, {"bought": 0, "sold": 1})

            funnel = report_sync._summarize_traces_for_date(tmp_dir, "2026-11-24")
            self.assertEqual(funnel["cutoff_buy_blocked"], 1)

    def test_caution_only_noise_alert_blocked_while_bearish_risk_warning_sent(self) -> None:
        import report_sync

        # Unit check on helper
        self.assertEqual(
            radar.is_caution_only_alert(
                [{"code": "042700", "name": "한미반도체", "direction": "옥석가리기주의"}]
            )[0],
            True,
        )
        self.assertEqual(
            radar.is_caution_only_alert(
                [{"code": "005930", "name": "삼성전자", "direction": "하락주의"}]
            )[0],
            False,
        )

        # E2E run_live check: caution-only alert is dropped, bearish risk alert IS delivered!
        t_live = datetime(2026, 11, 24, 10, 20, tzinfo=KST)
        comps = {c["code"]: dict(c) for c in radar.SEED_COMPANIES}
        items = [
            radar.NewsItem(
                "코스닥 액티브 ETF 순자산 1조 돌파… 자금 유입",
                "https://ex.com/etf_noise_1",
                "한국경제_증권",
                "증권",
                first_seen=t_live.isoformat(),
            ),
            radar.NewsItem(
                "미 반도체 관세 전격 인상 충격… 메모리 수출 비상",
                "https://ex.com/semi_crash_1",
                "연합뉴스_경제",
                "경제",
                first_seen=t_live.isoformat(),
            ),
        ]

        def fake_gemini(api_key, models, sp, up, **kw):
            payload = json.loads(up)
            if sp == radar.SCREENING_PROMPT:
                evs = []
                for c in payload["clusters"]:
                    if "ETF" in c["headline"]:
                        evs.append({
                            "cluster_id": c["cluster_id"],
                            "catalyst_score": 8.3,
                            "theme_key": "액티브ETF_노이즈",
                            "second_order_keywords": ["사이버보안"],
                        })
                    else:
                        evs.append({
                            "cluster_id": c["cluster_id"],
                            "catalyst_score": 8.8,
                            "theme_key": "반도체관세_충격",
                            "negative_keywords": ["반도체"],
                        })
                return {"selected_events": evs}, "gemini-3.5-flash-lite"

            if "ETF" in json.dumps(payload, ensure_ascii=False):
                return {
                    "title": "코스닥 액티브 ETF 자금 유입 점검",
                    "event_summary": "단순 지수 자금 유입으로 개별 기업 직접 수혜 미미",
                    "stocks": [
                        {"code": "263860", "name": "지니언스", "direction": "옥석가리기주의", "order_tier": "단순연관", "revenue_link": "단기테마성"},
                    ],
                }, "gemini-3.6-flash"
            return {
                "title": "미 반도체 관세 전격 인상 → 대형 메모리 단기 하방 주의",
                "event_summary": "대미 반도체 관세 인상으로 수출 채산성 악화 우려",
                "stocks": [
                    {"code": "005930", "name": "삼성전자", "direction": "하락주의", "order_tier": "1차 직접(역풍)", "revenue_link": "본업직결"},
                ],
            }, "gemini-3.6-flash"

        q_map = {
            "005930": radar.StockQuote("005930", 68000, -1400, -2.0, 900000, "OPEN", "", trading_value=90_000_000_000),
            "263860": radar.StockQuote("263860", 12000, 100, 0.8, 150000, "OPEN", "", trading_value=2_000_000_000),
        }

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            sent_msgs: list[str] = []
            env = {
                "GEMINI_API_KEY": "k",
                "TELEGRAM_BOT_TOKEN": "t",
                "TELEGRAM_CHAT_ID": "c",
                "INSTANT_FLASH_ALERT": "false",
                "AUTOTRADE_ENABLED": "false",
                "TRIGGER_MODE": "shadow",
            }
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(radar, "fetch_rss_news", return_value=items),
                mock.patch.object(radar, "call_gemini", side_effect=fake_gemini),
                mock.patch.object(radar, "load_krx_master", return_value=comps),
                mock.patch.object(radar, "fetch_naver_quotes", return_value=q_map),
                mock.patch.object(radar, "send_telegram", side_effect=lambda t, c, m, start_chunk=0: sent_msgs.append(m) or 1),
            ):
                radar.run_live(dry_run=False, cycles=1, data_dir=tmp_dir, now_fn=lambda: t_live, daemon_mode=True)

            # Only the genuine 하락주의 warning is sent; the 옥석가리기주의-only ETF noise alert was blocked!
            self.assertEqual(len(sent_msgs), 1)
            self.assertIn("미 반도체 관세 전격 인상", sent_msgs[0])
            self.assertNotIn("액티브 ETF", sent_msgs[0])

            funnel = report_sync._summarize_traces_for_date(tmp_dir, "2026-11-24")
            self.assertEqual(funnel["caution_only_blocked"], 1)


class TestPatch20261125Improvements(unittest.TestCase):
    def test_prev_day_backfill_preserves_historical_snapshot_and_funnel(self) -> None:
        import base64 as _b64
        import report_sync

        t_day1_alert = datetime(2026, 11, 24, 10, 0, tzinfo=KST)
        t_day1_eod = datetime(2026, 11, 24, 15, 37, tzinfo=KST)
        t_day2_eod = datetime(2026, 11, 25, 15, 37, tzinfo=KST)

        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(os.environ, {}, clear=False):
            tmp_dir = Path(tmp)
            (tmp_dir / ".env").write_text(
                "GITHUB_REPORT_REPO=jjangna0817-hue/news-alpha-radar-logs\n"
                "GITHUB_REPORT_TOKEN=github_pat_TEST_BACKFILL_123\n",
                encoding="utf-8",
            )
            trace = radar.TraceLogger(tmp_dir / "trace.jsonl")
            trace.emit({"ts": t_day1_alert.isoformat(), "type": "cycle_summary", "fresh_count": 2779, "cluster_count": 4657, "alerts_sent": 35})
            trace.emit({"ts": t_day1_alert.isoformat(), "type": "caution_only_skip", "theme_key": "노이즈테마", "details": ["X"]})

            pt = radar.PerformanceTracker(tmp_dir / "performance.jsonl")
            pt.record_alert(
                alert_ts=t_day1_alert,
                theme_key="보안_이슈",
                sector="사이버보안",
                title="보안 수혜",
                catalyst_score=8.5,
                window="market",
                stocks=[{"code": "263860", "name": "지니언스", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}],
                quotes={"263860": radar.StockQuote("263860", 10000, 150, 1.5, 200000, "OPEN", "", trading_value=6_000_000_000)},
            )
            pt.update_pending(
                now=t_day1_eod,
                fetch_quotes_fn=lambda codes: {"263860": radar.StockQuote("263860", 10300, 0, 0.0, 300000, "CLOSE", "")},
            )

            gh_store: dict[str, bytes] = {}

            def fake_gh(req, timeout=0):
                url = req.full_url
                rel = urllib.parse.unquote(url.split("/contents/", 1)[1])
                if req.get_method() == "GET":
                    if rel not in gh_store:
                        raise urllib.error.HTTPError(url, 404, "Not Found", {}, io.BytesIO(b"{}"))
                    return _FakeResponse({"sha": "s1", "content": _b64.b64encode(gh_store[rel]).decode("ascii")})
                body = json.loads(req.data.decode("utf-8"))
                gh_store[rel] = _b64.b64decode(body["content"])
                return _FakeResponse({"content": {"path": rel, "sha": "s2"}}, status=201)

            with mock.patch("urllib.request.urlopen", side_effect=fake_gh):
                # Day 1 EOD sync: 0 open positions, EOD exit True, funnel has 35 alerts
                res1 = report_sync.sync_report_to_github(tmp_dir, tmp_dir, target_date="2026-11-24", include_prev_trading_day=False, now=t_day1_eod)
                self.assertTrue(res1["ok"])

                # Simulate Day 2 changes: trace rotated (Day 1 gone), config changed (EOD False),
                # new Day 2 open position added, and Day 1 alert gets next-day close (+16.5%)
                (tmp_dir / "trace.jsonl").write_text("", encoding="utf-8")
                radar.save_runtime_config(tmp_dir, {"autotrade_eod_exit_enabled": False})
                import paper_trader
                trader = paper_trader.PaperAutoTrader(tmp_dir, tmp_dir)
                with mock.patch.object(report_sync, "try_background_github_sync"):
                    trader.on_alert_candidates(
                        alert_ts=datetime(2026, 11, 25, 9, 15, tzinfo=KST),
                        theme_key="해운_이슈",
                        sector="조선·해운·물류",
                        title="해운 수혜",
                        catalyst_score=8.5,
                        stocks=[{"code": "011200", "name": "HMM", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}],
                        quotes={"011200": radar.StockQuote("011200", 22000, -100, -0.45, 500000, "OPEN", "", trading_value=15_000_000_000)},
                        runtime_cfg={"autotrade_eod_exit_enabled": False},
                        daemon_mode=True,
                    )
                pt.update_pending(
                    now=t_day2_eod,
                    fetch_quotes_fn=lambda codes: {"263860": radar.StockQuote("263860", 11650, 0, 0.0, 500000, "CLOSE", "")},
                )
                # Also remove local cache to verify GitHub remote fallback works cleanly
                cache_day1 = tmp_dir / "daily_reports" / "2026-11-24.json"
                if cache_day1.exists():
                    cache_day1.unlink()

                res2 = report_sync.sync_report_to_github(tmp_dir, tmp_dir, target_date="2026-11-25", include_prev_trading_day=True, now=t_day2_eod)
                self.assertTrue(res2["ok"])

            day1_after = json.loads(gh_store["reports/2026-11-24.json"].decode("utf-8"))
            # ret_next_close was backfilled!
            self.assertAlmostEqual(day1_after["today_alert_vs_trade_matrix"][0]["ret_next_close"], 16.5, places=1)
            self.assertEqual(day1_after["backfilled_at"], t_day2_eod.isoformat())
            # Historical Day 1 snapshot MUST NOT be clobbered by Day 2's state!
            self.assertEqual(day1_after["open_positions"], [])
            self.assertTrue(day1_after["config_snapshot"]["autotrade_eod_exit_enabled"])
            self.assertEqual(day1_after["today_funnel"]["alerts_sent_in_cycles"], 35)
            self.assertEqual(day1_after["today_funnel"]["fresh_articles"], 2779)

    def test_premarket_queue_limit_12_prevents_valid_candidate_eviction(self) -> None:
        import paper_trader
        import report_sync

        t_pre = datetime(2026, 11, 25, 8, 15, tzinfo=KST)
        t_open = datetime(2026, 11, 25, 9, 8, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            trader = paper_trader.PaperAutoTrader(tmp_dir, tmp_dir)

            # Queue 6 premarket alerts (scores 8.8 down to 8.0); previously the 6th (알테오젠 8.0) was evicted by [:5]
            cands = [
                ("163730", "핑거", 8.8),
                ("079550", "LIG", 8.8),
                ("011200", "HMM", 8.5),
                ("060250", "NHN KCP", 8.5),
                ("420770", "기가비스", 8.5),
                ("196170", "알테오젠", 8.0),
            ]
            for idx, (code, name, score) in enumerate(cands):
                trader.on_alert_candidates(
                    alert_ts=t_pre + timedelta(minutes=idx),
                    theme_key=f"테마_{idx}",
                    sector="바이오",
                    title=f"장전 이슈 {idx}",
                    catalyst_score=score,
                    stocks=[{"code": code, "name": name, "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}],
                    quotes={},
                    daemon_mode=True,
                )
            self.assertEqual(len(trader.load_account(t_pre)["pending_open_entries"]), 6)

            # At 09:08: 핑거(-4.6%) and 기가비스(-6.1%) fail min_change_pct -> 6th candidate 알테오젠(+0.5%) MUST be bought!
            open_quotes = {
                "163730": radar.StockQuote("163730", 10000, -480, -4.6, 200000, "OPEN", "", trading_value=2_000_000_000),
                "079550": radar.StockQuote("079550", 365000, -1500, -0.4, 50000, "OPEN", "", trading_value=18_000_000_000),
                "011200": radar.StockQuote("011200", 22000, -300, -1.3, 400000, "OPEN", "", trading_value=12_000_000_000),
                "060250": radar.StockQuote("060250", 10800, -50, -0.5, 250000, "OPEN", "", trading_value=3_000_000_000),
                "420770": radar.StockQuote("420770", 30000, -1950, -6.1, 100000, "OPEN", "", trading_value=3_000_000_000),
                "196170": radar.StockQuote("196170", 300000, 1500, 0.5, 50000, "OPEN", "", trading_value=25_000_000_000),
            }
            with mock.patch.object(report_sync, "try_background_github_sync"):
                res = trader.tick_positions(
                    now=t_open,
                    daemon_mode=True,
                    fetch_quotes_fn=lambda codes: {c: open_quotes[c] for c in codes if c in open_quotes},
                )
            self.assertEqual(res["bought"], 4)
            bought_codes = set(trader.load_account(t_open)["positions"].keys())
            self.assertIn("196170", bought_codes)
            self.assertNotIn("163730", bought_codes)
            self.assertNotIn("420770", bought_codes)

    def test_premarket_crash_guard_blocks_severe_drop_and_allows_healthy_core_peer(self) -> None:
        import paper_trader
        import report_sync

        t_digest = datetime(2026, 11, 24, 7, 51, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            trace = radar.TraceLogger(tmp_dir / "trace.jsonl")
            trader = paper_trader.PaperAutoTrader(tmp_dir, tmp_dir)

            # 1) Exact reproduction of 하나에어로(-18.6%) vs 한국카본(-5.06%):
            #    하나에어로 is blocked (<= -10%), so healthy 2nd core peer 한국카본 is queued instead!
            res1 = trader.on_alert_candidates(
                alert_ts=t_digest,
                theme_key="항공우주방산투자",
                sector="방산·우주항공",
                title="KAI 경남 투자협약",
                catalyst_score=8.5,
                stocks=[
                    {"code": "271160", "name": "하나에어로", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"},
                    {"code": "017960", "name": "한국카본", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"},
                ],
                quotes={
                    "271160": radar.StockQuote("271160", 5070, -1160, -18.6, 800000, "CLOSE", "", trading_value=36_900_000_000),
                    "017960": radar.StockQuote("017960", 12000, -640, -5.06, 400000, "CLOSE", "", trading_value=12_000_000_000),
                },
                daemon_mode=True,
                trace=trace,
            )
            self.assertIsNotNone(res1)
            assert res1 is not None
            self.assertEqual(res1["action"], "queued_for_open")
            self.assertEqual(res1["entry"]["code"], "017960")

            # 2) If core crashed (-15%) and only a diffusion peer remains, do NOT substitute diffusion!
            res2 = trader.on_alert_candidates(
                alert_ts=t_digest + timedelta(minutes=1),
                theme_key="급락테마",
                sector="방산·우주항공",
                title="급락 핵심주와 확산주",
                catalyst_score=8.4,
                stocks=[
                    {"code": "999001", "name": "급락핵심주", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"},
                    {"code": "999002", "name": "대리확산주", "direction": "상승수혜", "order_tier": "2차 파급(확산)", "revenue_link": "부분연관"},
                ],
                quotes={
                    "999001": radar.StockQuote("999001", 8500, -1500, -15.0, 500000, "CLOSE", "", trading_value=10_000_000_000),
                    "999002": radar.StockQuote("999002", 10000, 100, 1.0, 300000, "CLOSE", "", trading_value=5_000_000_000),
                },
                daemon_mode=True,
                trace=trace,
            )
            self.assertIsNone(res2)

            funnel = report_sync._summarize_traces_for_date(tmp_dir, "2026-11-24")
            self.assertEqual(funnel["premarket_crash_blocked"], 2)

            # 3) Setting autotrade_premarket_max_drop_pct=0 disables the guard for instant rollback
            res_off = trader.on_alert_candidates(
                alert_ts=t_digest + timedelta(minutes=2),
                theme_key="가드해제테스트",
                sector="방산·우주항공",
                title="가드 비활성 확인",
                catalyst_score=8.5,
                stocks=[{"code": "271160", "name": "하나에어로", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "검토필요"}],
                quotes={"271160": radar.StockQuote("271160", 5070, -1160, -18.6, 800000, "CLOSE", "", trading_value=36_900_000_000)},
                runtime_cfg={"autotrade_premarket_max_drop_pct": 0.0},
                daemon_mode=True,
            )
            self.assertIsNotNone(res_off)
            assert res_off is not None
            self.assertEqual(res_off["entry"]["code"], "271160")

    def test_default_entry_max_change_2pct_blocks_chasing_and_is_overridable(self) -> None:
        import paper_trader
        import report_sync

        t_mid = datetime(2026, 11, 25, 10, 0, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            trader = paper_trader.PaperAutoTrader(tmp_dir, tmp_dir)
            self.assertEqual(paper_trader.resolve_autotrade_config()["autotrade_max_change_pct"], 2.0)

            stock = [{"code": "214430", "name": "아이쓰리시스템", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}]
            q_chase = {"214430": radar.StockQuote("214430", 54200, 1600, 3.05, 50000, "OPEN", "", trading_value=3_000_000_000)}

            with mock.patch.object(report_sync, "try_background_github_sync"):
                # Default +2.0% cap blocks +3.05% chasing entry
                r_blocked = trader.on_alert_candidates(
                    alert_ts=t_mid,
                    theme_key="추격차단테스트",
                    sector="화재·재난안전",
                    title="이미 +3% 상승 종목",
                    catalyst_score=8.5,
                    stocks=stock,
                    quotes=q_chase,
                    daemon_mode=True,
                )
                self.assertIsNone(r_blocked)

                # Explicit runtime_cfg override to 4.0% allows instant rollback without code revert
                r_override = trader.on_alert_candidates(
                    alert_ts=t_mid + timedelta(minutes=1),
                    theme_key="롤백오버라이드테스트",
                    sector="화재·재난안전",
                    title="설정 오버라이드 허용",
                    catalyst_score=8.5,
                    stocks=stock,
                    quotes=q_chase,
                    runtime_cfg={"autotrade_max_change_pct": 4.0},
                    daemon_mode=True,
                )
                self.assertIsNotNone(r_override)
                assert r_override is not None
                self.assertEqual(r_override["action"], "bought")

    def test_breakeven_stop_protects_mid_rally_and_allows_trailing_run(self) -> None:
        import paper_trader
        import report_sync

        t0 = datetime(2026, 11, 25, 9, 20, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            trader = paper_trader.PaperAutoTrader(tmp_dir, tmp_dir)

            with mock.patch.object(report_sync, "try_background_github_sync"):
                # Buy 2 stocks at 10,000 (+1 tick = 10,010 fill)
                for idx, (code, name) in enumerate([("105740", "디케이락형"), ("079550", "대장주형")]):
                    trader.on_alert_candidates(
                        alert_ts=t0 + timedelta(minutes=idx),
                        theme_key=f"본전테스트_{idx}",
                        sector="방산·우주항공",
                        title=f"테스트 {idx}",
                        catalyst_score=8.5,
                        stocks=[{"code": code, "name": name, "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}],
                        quotes={code: radar.StockQuote(code, 10000, 50, 0.5, 500000, "OPEN", "", trading_value=8_000_000_000)},
                        daemon_mode=True,
                    )

                # Step 1: Both reach +3.6% (10,370 vs 10,010) -> breakeven armed, NOT trailing armed, NOT sold!
                r1 = trader.tick_positions(
                    now=t0 + timedelta(minutes=15),
                    daemon_mode=True,
                    fetch_quotes_fn=lambda codes: {
                        "105740": radar.StockQuote("105740", 10370, 420, 4.2, 600000, "OPEN", "", trading_value=9_000_000_000),
                        "079550": radar.StockQuote("079550", 10370, 420, 4.2, 600000, "OPEN", "", trading_value=9_000_000_000),
                    },
                )
                self.assertEqual(r1["sold"], 0)
                acc1 = trader.load_account(t0 + timedelta(minutes=15))
                self.assertTrue(acc1["positions"]["105740"]["breakeven_armed"])
                self.assertFalse(acc1["positions"]["105740"]["trailing_armed"])

                # Step 2: Both pull back to +1.5% (10,160) -> still ABOVE +0.3% floor, so neither is shaken out!
                r2 = trader.tick_positions(
                    now=t0 + timedelta(minutes=25),
                    daemon_mode=True,
                    fetch_quotes_fn=lambda codes: {
                        "105740": radar.StockQuote("105740", 10160, 210, 2.1, 620000, "OPEN", "", trading_value=9_000_000_000),
                        "079550": radar.StockQuote("079550", 10160, 210, 2.1, 620000, "OPEN", "", trading_value=9_000_000_000),
                    },
                )
                self.assertEqual(r2["sold"], 0)

                # Step 3: 105740 fades down to +0.2% (10,030 <= +0.3%) -> breakeven exit protects capital!
                #         079550 resumes rally to +6.5% (10,660) -> trailing armed and held!
                r3 = trader.tick_positions(
                    now=t0 + timedelta(minutes=40),
                    daemon_mode=True,
                    fetch_quotes_fn=lambda codes: {
                        "105740": radar.StockQuote("105740", 10030, 80, 0.8, 650000, "OPEN", "", trading_value=9_000_000_000),
                        "079550": radar.StockQuote("079550", 10660, 710, 7.1, 700000, "OPEN", "", trading_value=10_000_000_000),
                    },
                )
                self.assertEqual(r3["sold"], 1)
                self.assertNotIn("105740", trader.load_account(t0 + timedelta(minutes=40))["positions"])
                self.assertTrue(trader.load_account(t0 + timedelta(minutes=40))["positions"]["079550"]["trailing_armed"])

                # Step 4: 079550 pulls back from 10,660 to 10,400 (-2.44% pullback) -> exits via trailing TP!
                r4 = trader.tick_positions(
                    now=t0 + timedelta(minutes=55),
                    daemon_mode=True,
                    fetch_quotes_fn=lambda codes: {
                        "079550": radar.StockQuote("079550", 10400, 450, 4.5, 750000, "OPEN", "", trading_value=10_000_000_000),
                    },
                )
                self.assertEqual(r4["sold"], 1)

                trades = {tr["code"]: tr for tr in trader.load_trades(limit=10)}
                self.assertIn("본전보호 청산", trades["105740"]["exit_reason"])
                self.assertIn("트레일링 익절", trades["079550"]["exit_reason"])

                # Step 5: Setting autotrade_breakeven_trigger_pct=0 disables breakeven for instant rollback
                trader.on_alert_candidates(
                    alert_ts=t0 + timedelta(minutes=60),
                    theme_key="본전비활성테스트",
                    sector="방산·우주항공",
                    title="비활성 확인",
                    catalyst_score=8.5,
                    stocks=[{"code": "394800", "name": "비활성주", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}],
                    quotes={"394800": radar.StockQuote("394800", 10000, 0, 0.0, 500000, "OPEN", "", trading_value=8_000_000_000)},
                    runtime_cfg={"autotrade_breakeven_trigger_pct": 0.0},
                    daemon_mode=True,
                )
                trader.tick_positions(
                    now=t0 + timedelta(minutes=70),
                    runtime_cfg={"autotrade_breakeven_trigger_pct": 0.0},
                    daemon_mode=True,
                    fetch_quotes_fn=lambda c: {"394800": radar.StockQuote("394800", 10370, 370, 3.7, 600000, "OPEN", "", trading_value=9_000_000_000)},
                )
                r_off = trader.tick_positions(
                    now=t0 + timedelta(minutes=80),
                    runtime_cfg={"autotrade_breakeven_trigger_pct": 0.0},
                    daemon_mode=True,
                    fetch_quotes_fn=lambda c: {"394800": radar.StockQuote("394800", 10020, 20, 0.2, 620000, "OPEN", "", trading_value=9_000_000_000)},
                )
                self.assertEqual(r_off["sold"], 0)
                self.assertIn("394800", trader.load_account(t0 + timedelta(minutes=80))["positions"])

    def test_block_standalone_diffusion_intraday_premarket_and_toggle(self) -> None:
        import paper_trader
        import report_sync

        t_pre = datetime(2026, 11, 27, 9, 6, tzinfo=KST)
        t_mid = datetime(2026, 11, 27, 9, 33, tzinfo=KST)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            trace = radar.TraceLogger(tmp_dir / "trace.jsonl")
            trader = paper_trader.PaperAutoTrader(tmp_dir, tmp_dir)

            self.assertTrue(paper_trader.resolve_autotrade_config()["autotrade_block_diffusion"])

            # 1) Premarket diffusion-only alert (삼성전기/팬오션 case): blocked by default!
            r_pre_blocked = trader.on_alert_candidates(
                alert_ts=t_pre,
                theme_key="자율주행_로봇_5G통신",
                sector="전기전자",
                title="삼성전기 확산 단독",
                catalyst_score=8.2,
                stocks=[{"code": "009150", "name": "삼성전기", "direction": "상승수혜", "order_tier": "2차 파급(확산)", "revenue_link": "부분연관"}],
                quotes={"009150": radar.StockQuote("009150", 787000, 2000, 0.25, 200000, "OPEN", "", trading_value=600_000_000_000)},
                daemon_mode=True,
                trace=trace,
            )
            self.assertIsNone(r_pre_blocked)
            self.assertEqual(trader.load_account(t_pre)["pending_open_entries"], [])

            # 2) Intraday diffusion-only alert (현대로템/BNK금융 case): blocked by default!
            with mock.patch.object(report_sync, "try_background_github_sync"):
                r_mid_blocked = trader.on_alert_candidates(
                    alert_ts=t_mid,
                    theme_key="해외철도수주",
                    sector="철도/인프라",
                    title="현대로템 확산 단독",
                    catalyst_score=8.5,
                    stocks=[{"code": "064350", "name": "현대로템", "direction": "상승수혜", "order_tier": "2차 파급(확산)", "revenue_link": "부분연관"}],
                    quotes={"064350": radar.StockQuote("064350", 59400, 400, 0.68, 300000, "OPEN", "", trading_value=20_000_000_000)},
                    daemon_mode=True,
                    trace=trace,
                )
                self.assertIsNone(r_mid_blocked)

                # 3) Core 2nd-order stock buys normally!
                r_core_ok = trader.on_alert_candidates(
                    alert_ts=t_mid + timedelta(minutes=1),
                    theme_key="철도노조_대체교통",
                    sector="조선·해운·물류",
                    title="동양고속 핵심 수혜",
                    catalyst_score=8.0,
                    stocks=[{"code": "084670", "name": "동양고속", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결"}],
                    quotes={"084670": radar.StockQuote("084670", 25900, 200, 0.78, 120000, "OPEN", "", trading_value=6_000_000_000)},
                    daemon_mode=True,
                    trace=trace,
                )
                self.assertIsNotNone(r_core_ok)
                assert r_core_ok is not None
                self.assertEqual(r_core_ok["action"], "bought")

                # 4) Instant rollback via runtime_cfg={"autotrade_block_diffusion": False} re-allows standalone diffusion
                r_mid_override = trader.on_alert_candidates(
                    alert_ts=t_mid + timedelta(minutes=2),
                    theme_key="해외철도수주_재허용",
                    sector="철도/인프라",
                    title="확산 허용 토글 테스트",
                    catalyst_score=8.5,
                    stocks=[{"code": "064350", "name": "현대로템", "direction": "상승수혜", "order_tier": "2차 파급(확산)", "revenue_link": "부분연관"}],
                    quotes={"064350": radar.StockQuote("064350", 59400, 400, 0.68, 300000, "OPEN", "", trading_value=20_000_000_000)},
                    runtime_cfg={"autotrade_block_diffusion": False},
                    daemon_mode=True,
                    trace=trace,
                )
                self.assertIsNotNone(r_mid_override)
                assert r_mid_override is not None
                self.assertEqual(r_mid_override["action"], "bought")

            funnel = report_sync._summarize_traces_for_date(tmp_dir, "2026-11-27")
            self.assertEqual(funnel["substitute_buy_blocked"], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)



