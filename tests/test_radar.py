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
            finally:
                srv.shutdown()
                srv.server_close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
