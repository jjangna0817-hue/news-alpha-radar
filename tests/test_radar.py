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

            # Deferred queue now empty
            reloaded = radar.StateStore(tmp_dir / "state.json")
            self.assertEqual(reloaded.deferred, [])

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
