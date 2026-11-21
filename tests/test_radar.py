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
    "MAX_ALERTS_", "MAX_DAILY_", "MAX_MARKET_", "MAX_OFFHOURS_",
    "MAX_BULLISH_PREPRICED_", "OFFHOURS_DIGEST_", "STOCK_DEDUP_",
    "THEME_COOLDOWN_", "DEDUP_MODE", "TRIGGER_MODE", "STAGE1_DAILY_",
    "INSTANT_FLASH_", "EXTRA_RSS_", "MARKET_LOOP_",
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
        # 1) Stage 2 deep model order puts gemini-3.8-flash BEFORE gemini-3.5-flash-lite
        deep_order = [m.strip() for m in radar.DEFAULT_DEEP_MODELS.split(",") if m.strip()]
        self.assertEqual(deep_order, ["gemini-3.6-flash", "gemini-3.8-flash", "gemini-3.5-flash-lite"])

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
