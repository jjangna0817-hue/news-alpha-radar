#!/usr/bin/env python3
"""News Alpha Radar — 단일 파일 24/7 국내주식 일상뉴스 2차 파급효과 레이더 (추가 비용 0원).

외부 패키지 설치 불필요 (Python 3.11+ 표준 라이브러리 전용).
- 뉴스 수집: 무료 한국 RSS 11개 피드 (사회·사건사고·IT·경제·국제) + 시황 노이즈 제거 + 클러스터링
- 종목 검증: KRX KIND 전체 상장사(코스피·코스닥 약 2,600개) 실시간 로드 & 2차 키워드 매칭
- 시세 체크: 네이버 금융 공개 JSON 배치 조회 (미반영 초입 vs 과열 급등 자동 판정)
- AI 추론: Google AI Studio 무료 티어 2단계 (Flash-Lite 스크리닝 → Flash 심층 매매 가이드)
- 알림 발송: 텔레그램 Bot API 무료 실시간 푸시 (평일 장전·장중 08:00~15:30 KST 2분 간격 6회 연속 감시)
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("news_alpha_radar")

KST = timezone(timedelta(hours=9))
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)

DEFAULT_SCREEN_MODELS = "gemini-3.5-flash-lite,gemini-3.6-flash,gemini-3.8-flash"
DEFAULT_DEEP_MODELS = "gemini-3.6-flash,gemini-3.5-flash-lite,gemini-3.8-flash"
DEFAULT_SCREEN_THINKING_LEVEL = "MINIMAL"
DEFAULT_DEEP_THINKING_LEVEL = "LOW"
DEFAULT_THINKING_BUDGET = 1024
DEFAULT_SCREEN_TIMEOUT = 25
DEFAULT_DEEP_TIMEOUT = 30
DEFAULT_MARKET_LOOP_COUNT = 6
DEFAULT_MARKET_LOOP_INTERVAL_SEC = 120

RSS_FEEDS: list[dict[str, Any]] = [
    {"name": "구글뉴스_한국_주요", "category": "속보", "url": "https://news.google.com/rss?hl=ko&gl=KR&ceid=KR:ko", "priority": 1},
    {"name": "구글뉴스_한국_사회", "category": "사회/사건", "url": "https://news.google.com/rss/headlines/section/topic/NATION?hl=ko&gl=KR&ceid=KR:ko", "priority": 1},
    {"name": "구글뉴스_한국_과학기술", "category": "IT/신기술", "url": "https://news.google.com/rss/headlines/section/topic/TECHNOLOGY?hl=ko&gl=KR&ceid=KR:ko", "priority": 1},
    {"name": "구글뉴스_한국_경제", "category": "경제/산업", "url": "https://news.google.com/rss/headlines/section/topic/BUSINESS?hl=ko&gl=KR&ceid=KR:ko", "priority": 2},
    {"name": "구글뉴스_한국_세계", "category": "국제/지정학", "url": "https://news.google.com/rss/headlines/section/topic/WORLD?hl=ko&gl=KR&ceid=KR:ko", "priority": 2},
    {"name": "연합뉴스_최신", "category": "속보", "url": "https://www.yna.co.kr/rss/news.xml", "priority": 1},
    {"name": "연합뉴스_사회", "category": "사회/사건", "url": "https://www.yna.co.kr/rss/society.xml", "priority": 1},
    {"name": "연합뉴스_산업", "category": "산업", "url": "https://www.yna.co.kr/rss/industry.xml", "priority": 2},
    {"name": "지디넷코리아_IT", "category": "IT/신기술", "url": "https://feeds.feedburner.com/zdkorea", "priority": 2},
    {"name": "매일경제_경제", "category": "경제/산업", "url": "https://www.mk.co.kr/rss/30100041/", "priority": 2},
    {"name": "매일경제_사회", "category": "사회/사건", "url": "https://www.mk.co.kr/rss/50400012/", "priority": 2},
]

NOISE_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"^\[?마감시황\]?",
        r"^\[?장중시황\]?",
        r"^\[?코스피\s*마감",
        r"^\[?코스닥\s*마감",
        r"^\[?뉴욕증시\s*마감",
        r"^\[?오늘의\s*운세",
        r"^\[?(부고|부음)\]?",
        r"^\[?인사\]?",
        r"^\[?동정\]?",
        r"^\[?포토\]?",
        r"^\[?사진\]?",
        r"목표가\s*(상향|하향|유지)",
        r"투자의견\s*(매수|중립|유지)",
        r"외국인\s*순매수\s*상위",
        r"기관\s*순매수\s*상위",
    ]
]

KEYWORD_EXPANSIONS: dict[str, list[str]] = {
    "전파탐지기": ["무선도청", "불법촬영", "전파", "보안", "탐지", "rf", "감시"],
    "전파탐지": ["무선도청", "불법촬영", "전파", "보안", "탐지", "rf"],
    "몰카탐지": ["불법촬영", "무선도청", "보안", "탐지", "열화상"],
    "도청방지": ["무선도청", "전파", "보안", "탐지"],
    "열화상카메라": ["적외선", "열화상", "센서", "카메라", "보안", "감시"],
    "사이버보안": ["정보보안", "네트워크보안", "악성코드", "edr", "nac", "방화벽", "암호"],
    "스마트안경": ["ar", "마이크로led", "광학", "렌즈", "카메라모듈", "디스플레이"],
    "안티드론": ["드론", "레이더", "전파", "방산", "감시", "무인기"],
    "전력기기": ["변압기", "차단기", "배전반", "전선", "케이블", "송전", "ess"],
    "화재안전": ["소화", "화재", "방재", "열폭주", "감지기", "스프링클러"],
    "감염병": ["진단키트", "백신", "항바이러스", "마스크", "방역", "진단시약"],
    "수자원": ["상하수도", "정수", "수처리", "담수화", "펌프"],
    "해저케이블": ["전선", "광케이블", "해저", "통신케이블"],
    "희토류": ["영구자석", "희소금속", "자원", "제련", "소재"],
    "원격의료": ["비대면", "의료정보", "emr", "헬스케어", "디지털헬스"],
    "로봇": ["협동로봇", "감속기", "액추에이터", "자동화", "무인"],
    "우주항공": ["위성", "발사체", "항공기", "우주", "방산"],
}

SEED_COMPANIES: list[dict[str, str]] = [
    {"code": "446840", "name": "지슨", "market": "KOSDAQ", "sector": "통신 및 방송 장비 제조업", "products": "무선도청 탐지 시스템, 이동형 전파탐지기(알파-P), 불법촬영 탐지(알파-C), 무선백도어 해킹 탐지(알파-H), 시험장 보안 장비"},
    {"code": "140430", "name": "카티스", "market": "KOSDAQ", "sector": "소프트웨어 개발 및 공급업", "products": "공간보안 플랫폼, 물리보안 시스템, 침입감지·전파탐지 보안 솔루션"},
    {"code": "263860", "name": "지니언스", "market": "KOSDAQ", "sector": "소프트웨어 개발 및 공급업", "products": "네트워크 접근제어(NAC), 단말 위협 탐지 및 대응(EDR), 사이버보안 솔루션"},
    {"code": "053800", "name": "안랩", "market": "KOSDAQ", "sector": "소프트웨어 개발 및 공급업", "products": "정보보안 소프트웨어(V3), 네트워크 보안, 보안관제, 위협탐지(XDR)"},
    {"code": "452430", "name": "사피엔반도체", "market": "KOSDAQ", "sector": "반도체 제조업", "products": "AR/AI 스마트안경용 마이크로LED 디스플레이 구동칩(DDIC) 팹리스"},
    {"code": "053450", "name": "세코닉스", "market": "KOSDAQ", "sector": "사진장비 및 광학기기 제조업", "products": "모바일·차량용 카메라 렌즈, AR 글래스 초소형 광학 부품, 광학필름"},
]

KRX_KIND_URLS = {
    "KOSPI": "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt",
    "KOSDAQ": "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt",
}

SCREENING_PROMPT = """당신은 한국 주식시장(코스피·코스닥)의 '일상 뉴스·사건사고 기반 2차 파급효과' 전문 애널리스트입니다.

[임무]
입력된 여러 뉴스 이벤트 클러스터 중에서, 국내 증시의 특정 섹터·테마·종목에 강한 자금 유입 또는 하락 충격을 줄 수 있는 '신규 촉매(Catalyst)' 사건만 선별하세요.

[핵심 추론 렌즈 — 2차 연쇄 파급효과를 최우선으로 찾으세요]
1. 부작용·역기능 해결 수혜 (가장 강력):
   - 신기술·사회 사건이 유발한 문제/불안을 막거나 해결하는 장비·솔루션·규제 수혜 분야
   - 예: "AI 안경 시험 부정행위/몰카 논란" → 1차: 스마트안경 부품 / 2차(핵심): 전파탐지기·불법촬영탐지·열화상카메라·보안장비
   - 예: "전기차 지하주차장 화재" → 2차(핵심): 열폭주방지·소화설비·배터리진단·화재감지기
   - 예: "대형 해킹·통신망 마비" → 2차(핵심): 사이버보안·네트워크접근제어·관제
2. 정책·예산·의무화 촉발:
   - 사건 직후 정부·국회·대기업이 전수조사, 예산 증액, 장비 의무 도입, 반입 금지를 검토/발표할 가능성
3. 공급 차질·규제에 따른 반사이익·대체재:
   - 수출 통제, 공장 화재, 항로 봉쇄, 전염병 확산 시 국내 대체재/국산화/방역/운임 수혜

[반드시 제외할 노이즈]
- 이미 끝난 코스피/코스닥 시황, 외국인·기관 수급 요약, 단순 목표가/리포트
- 산업·정책 파급이 없는 단순 연예·스포츠·단발성 개인 범죄

[출력 JSON 스키마 — 반드시 단일 JSON 객체만 반환]
{
  "selected_events": [
    {
      "cluster_id": "입력된 cluster_id 그대로",
      "catalyst_score": 8.5,
      "theme_key": "중복방지용_짧은_한글_테마키",
      "event_summary": "무슨 일이 발생했는지 1~2문장 요약",
      "why_notable": "왜 이 뉴스가 주식시장(특히 2차 파급)을 움직일지 1문장",
      "first_order_keywords": ["KRX 상장사 주요제품/업종 검색용 1차 키워드 2~4개"],
      "second_order_keywords": ["KRX 상장사 주요제품/업종 검색용 2차 연쇄 수혜 키워드 3~6개"],
      "negative_keywords": ["악재/피해가 예상되는 업종·제품 키워드 0~3개"]
    }
  ]
}
해당되는 고촉매 사건이 없으면 {"selected_events": []} 를 반환하세요."""

DEEP_PROMPT = """당신은 한국 주식시장(코스피·코스닥)의 냉철한 이벤트 드리븐(Event-Driven) 전략가입니다.

[절대 원칙]
1. 할루시네이션 금지: 종목은 반드시 입력된 `verified_krx_candidates` 목록에 있는 실제 6자리 종목코드와 종목명만 선택하세요. 목록에 없는 종목코드를 절대 지어내지 마세요.
2. 옥석 가리기:
   - 부정적 사회 사건(부정행위, 화재, 해킹 등)에서는 '문제 해결·차단·정부 예산 수혜(2차)'가 진짜 주도주가 되고, '원인 기기 부품(1차)'은 단기 급등 후 급락하는 경우가 많습니다. 이를 명확히 구분하세요.
   - 각 종목의 실제 `products`(주요제품)를 보고 `본업직결` / `부분연관` / `단기테마성`을 냉정하게 판정하세요.
3. 선반영 반영:
   - 입력된 `quote.change_pct`와 `quote.reaction_stage`를 반드시 확인해, 이미 +15% 이상 급등한 종목은 절대 추격 매수를 권하지 말고 눌림목·리스크 경고를 적으세요.

[출력 JSON 스키마 — 반드시 단일 JSON 객체만 반환]
{
  "title": "한눈에 들어오는 알림 제목",
  "event_summary": "핵심 사건 요약 2문장 이내",
  "causal_chain": [
    "1단계(사건): ...",
    "2단계(사회/정책 반응): ...",
    "3단계(2차 핵심 수혜): ...",
    "4단계(주의/역풍): ..."
  ],
  "beneficiary_sectors": ["수혜 섹터/테마 1~3개"],
  "negative_sectors": ["피해/주의 섹터 0~2개"],
  "stocks": [
    {
      "code": "6자리 종목코드 (verified_krx_candidates에 존재하는 것만)",
      "name": "종목명",
      "direction": "상승수혜 | 하락주의 | 옥석가리기주의",
      "order_tier": "2차 파급(핵심) | 2차 파급(확산) | 1차 직접 | 피해/역풍",
      "revenue_link": "본업직결 | 부분연관 | 단기테마성",
      "reason": "주요제품(products)과 사건의 구체적 연결 근거 1문장"
    }
  ],
  "entry_strategy": "현재 주가 반응(선반영 여부)을 고려한 구체적 진입/분할/관찰 전략",
  "invalidation_rules": "이 시나리오가 틀렸을 때 즉시 손절·철수할 명확한 조건",
  "key_risks": "재료 소멸, 테마 과열, 실제 매출 규모 한계 등 핵심 리스크"
}"""


@dataclass(slots=True)
class NewsItem:
    title: str
    url: str
    source: str
    category: str
    published_at: str = ""
    summary: str = ""
    priority: int = 2


@dataclass(slots=True)
class EventCluster:
    cluster_id: str
    headline: str
    category: str
    items: list[NewsItem] = field(default_factory=list)

    def to_prompt_dict(self) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "category": self.category,
            "article_count": len(self.items),
            "headline": self.headline,
            "related_titles": [i.title for i in self.items[:5]],
            "summary": self.items[0].summary[:280] if self.items else "",
            "published_at": self.items[0].published_at if self.items else "",
        }


@dataclass(slots=True)
class StockQuote:
    code: str
    price: int
    change: int
    change_pct: float
    volume: int
    market_status: str
    reaction_stage: str


def strip_html(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html.unescape(text or ""))).strip()


def norm_key(text: str) -> str:
    cleaned = re.sub(r"\[[^\]]*\]|\([^\)]*\)", " ", text)
    cleaned = re.sub(r"-\s*[^-]+$", " ", cleaned)
    return re.sub(r"[^0-9a-zA-Z가-힣]", "", cleaned).lower()


def title_hash(title: str) -> str:
    return hashlib.sha1(norm_key(title).encode("utf-8")).hexdigest()[:16]


def extract_json(raw_text: str) -> dict[str, Any]:
    text = raw_text.strip()
    if text.startswith("`" * 3):
        text = re.sub(r"^`{3}(?:json)?\s*", "", text)
        text = re.sub(r"\s*`{3}$", "", text)
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end > start:
        parsed = json.loads(text[start : end + 1])
        if isinstance(parsed, dict):
            return parsed
    raise ValueError(f"JSON 파싱 실패: {text[:180]}")


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.seen_urls: dict[str, str] = {}
        self.seen_titles: dict[str, str] = {}
        self.alerted_themes: dict[str, str] = {}
        self.daily_counts: dict[str, int] = {}
        if path.exists():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                self.seen_urls = dict(raw.get("seen_urls", {}))
                self.seen_titles = dict(raw.get("seen_titles", {}))
                self.alerted_themes = dict(raw.get("alerted_themes", {}))
                self.daily_counts = {str(k): int(v) for k, v in raw.get("daily_counts", {}).items()}
            except Exception:
                pass

    def is_seen(self, url: str, title: str) -> bool:
        return url in self.seen_urls or title_hash(title) in self.seen_titles

    def mark_seen(self, url: str, title: str) -> None:
        now_iso = datetime.now(KST).isoformat()
        if url:
            self.seen_urls[url] = now_iso
        self.seen_titles[title_hash(title)] = now_iso

    def in_cooldown(self, theme_key: str, hours: int) -> bool:
        k = norm_key(theme_key)
        if not k or k not in self.alerted_themes:
            return False
        try:
            ts = datetime.fromisoformat(self.alerted_themes[k])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=KST)
            return (datetime.now(KST) - ts) < timedelta(hours=hours)
        except ValueError:
            return False

    def mark_alerted(self, theme_key: str) -> None:
        now = datetime.now(KST)
        k = norm_key(theme_key)
        if k:
            self.alerted_themes[k] = now.isoformat()
        day = now.strftime("%Y-%m-%d")
        self.daily_counts[day] = self.daily_counts.get(day, 0) + 1

    def sent_today(self) -> int:
        return self.daily_counts.get(datetime.now(KST).strftime("%Y-%m-%d"), 0)

    def save(self) -> None:
        cutoff = datetime.now(KST) - timedelta(days=7)

        def _prune(d: dict[str, str]) -> dict[str, str]:
            out: dict[str, str] = {}
            for k, v in d.items():
                try:
                    ts = datetime.fromisoformat(v)
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=KST)
                    if ts >= cutoff:
                        out[k] = v
                except ValueError:
                    pass
            return out

        self.seen_urls = _prune(self.seen_urls)
        self.seen_titles = _prune(self.seen_titles)
        self.alerted_themes = _prune(self.alerted_themes)
        day_cut = (datetime.now(KST) - timedelta(days=14)).strftime("%Y-%m-%d")
        self.daily_counts = {k: v for k, v in self.daily_counts.items() if k >= day_cut}

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(
                {
                    "seen_urls": self.seen_urls,
                    "seen_titles": self.seen_titles,
                    "alerted_themes": self.alerted_themes,
                    "daily_counts": self.daily_counts,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )


def fetch_rss_news(state: StateStore, max_per_feed: int = 25) -> list[NewsItem]:
    collected: list[NewsItem] = []
    for feed in RSS_FEEDS:
        req = urllib.request.Request(
            feed["url"],
            headers={"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/xml, text/xml"},
        )
        try:
            with urllib.request.urlopen(req, timeout=12) as resp:
                root = ET.fromstring(resp.read())
            for elem in root.findall(".//item")[:max_per_feed]:
                title = strip_html(elem.findtext("title") or "")
                link = (elem.findtext("link") or "").strip()
                if title and link:
                    collected.append(
                        NewsItem(
                            title=title,
                            url=link,
                            source=feed["name"],
                            category=feed["category"],
                            published_at=(elem.findtext("pubDate") or "").strip(),
                            summary=strip_html(elem.findtext("description") or "")[:360],
                            priority=int(feed["priority"]),
                        )
                    )
        except Exception as exc:
            logger.warning("RSS 수집 스킵 (%s): %s", feed["name"], exc)

    fresh: list[NewsItem] = []
    batch_titles: set[str] = set()
    for item in collected:
        if any(p.search(item.title) for p in NOISE_PATTERNS):
            continue
        if state.is_seen(item.url, item.title):
            continue
        nk = norm_key(item.title)
        if not nk or nk in batch_titles:
            continue
        batch_titles.add(nk)
        fresh.append(item)
    return fresh


def cluster_news(items: list[NewsItem], max_clusters: int = 25) -> list[EventCluster]:
    stops = {"속보", "단독", "종합", "오늘", "내일", "올해", "내년", "관련", "대해", "통해", "위해", "뉴스"}

    def _tokens(t: str) -> set[str]:
        c = re.sub(r"\[[^\]]*\]|\([^\)]*\)|-\s*[^-]+$", " ", t)
        return {w for w in re.findall(r"[가-힣a-zA-Z0-9]{2,}", c.lower()) if w not in stops}

    clusters: list[tuple[set[str], EventCluster]] = []
    for item in sorted(items, key=lambda x: x.priority):
        toks = _tokens(item.title)
        best_i, best_sim = -1, 0.0
        for idx, (ctoks, _) in enumerate(clusters):
            union = toks | ctoks
            sim = (len(toks & ctoks) / len(union)) if union else 0.0
            if sim >= 0.34 and sim > best_sim:
                best_sim, best_i = sim, idx
        if best_i >= 0:
            clusters[best_i][0].update(toks)
            clusters[best_i][1].items.append(item)
        else:
            cid = "evt_" + hashlib.sha1(item.title.encode("utf-8")).hexdigest()[:10]
            clusters.append((set(toks), EventCluster(cid, item.title, item.category, [item])))

    ranked = [c for _, c in clusters]
    ranked.sort(key=lambda c: (-len(c.items), min(i.priority for i in c.items)))
    return ranked[:max_clusters]


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self._in_cell = False
        self._row: list[str] = []
        self._cell: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        if t == "tr":
            self._row = []
        elif t in ("td", "th"):
            self._in_cell, self._cell = True, []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t in ("td", "th") and self._in_cell:
            self._row.append(re.sub(r"\s+", " ", "".join(self._cell)).strip())
            self._in_cell = False
        elif t == "tr" and self._row:
            self.rows.append(self._row)
            self._row = []

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell.append(data)


def load_krx_master(cache_path: Path, allow_network: bool = True) -> dict[str, dict[str, str]]:
    companies: dict[str, dict[str, str]] = {c["code"]: dict(c) for c in SEED_COMPANIES}

    def _merge(rows: list[dict[str, str]]) -> None:
        for r in rows:
            code = r["code"]
            if code not in companies:
                companies[code] = dict(r)
            else:
                prod = companies[code]["products"]
                if r.get("products") and r["products"] not in prod:
                    companies[code]["products"] = f"{r['products']}, {prod}".strip(", ")

    if cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            ts = datetime.fromisoformat(cached["updated_at"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=KST)
            if (datetime.now(KST) - ts) < timedelta(days=7):
                _merge(cached.get("companies", []))
                return companies
        except Exception:
            pass

    if allow_network:
        fetched: list[dict[str, str]] = []
        for market, url in KRX_KIND_URLS.items():
            try:
                req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    raw = resp.read()
                text = ""
                for enc in ("euc-kr", "cp949", "utf-8"):
                    try:
                        text = raw.decode(enc)
                        break
                    except UnicodeDecodeError:
                        continue
                parser = _TableParser()
                parser.feed(text)
                if len(parser.rows) < 2:
                    continue
                hdr = {name: i for i, name in enumerate(parser.rows[0])}
                ni, ci, si, pi = hdr.get("회사명", 0), hdr.get("종목코드", 1), hdr.get("업종", 2), hdr.get("주요제품", 3)
                for row in parser.rows[1:]:
                    if len(row) <= max(ni, ci):
                        continue
                    code = re.sub(r"[^0-9A-Za-z]", "", row[ci])
                    code = code.zfill(6) if code.isdigit() else code
                    if len(code) == 6 and row[ni].strip():
                        fetched.append({
                            "code": code,
                            "name": row[ni].strip(),
                            "market": market,
                            "sector": row[si].strip() if len(row) > si else "",
                            "products": row[pi].strip() if len(row) > pi else "",
                        })
            except Exception as exc:
                logger.warning("KRX %s 로드 실패: %s", market, exc)

        if fetched:
            _merge(fetched)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(
                json.dumps({"updated_at": datetime.now(KST).isoformat(), "companies": fetched}, ensure_ascii=False),
                encoding="utf-8",
            )
    return companies


def match_krx_stocks(
    companies: dict[str, dict[str, str]],
    second_kw: list[str],
    first_kw: list[str],
    neg_kw: list[str],
) -> list[dict[str, Any]]:
    def _expand(kws: list[str]) -> list[tuple[str, float]]:
        w: dict[str, float] = {}
        for k in kws:
            kl = k.strip().lower()
            if len(kl) < 2:
                continue
            w[kl] = max(w.get(kl, 0.0), 1.0)
            for sk, exps in KEYWORD_EXPANSIONS.items():
                if kl == sk or kl in sk or sk in kl:
                    for e in exps:
                        w[e] = max(w.get(e, 0.0), 0.6)
        return sorted(w.items(), key=lambda x: -x[1])

    used: set[str] = set()
    matched: list[dict[str, Any]] = []

    for kws, order_type, lim in [(second_kw, "2차_수혜", 6), (first_kw, "1차_수혜", 4), (neg_kw, "피해_우려", 3)]:
        exp = _expand(kws)
        if not exp:
            continue
        scored: list[dict[str, Any]] = []
        for comp in companies.values():
            if comp["code"] in used:
                continue
            nl, pl, sl = comp["name"].lower(), comp["products"].lower(), comp["sector"].lower()
            score, hits = 0.0, []
            for term, wt in exp:
                ts = (2.5 * wt if term in nl else 0.0) + (2.0 * wt if term in pl else 0.0) + (0.8 * wt if term in sl else 0.0)
                if ts > 0:
                    score += ts
                    hits.append(term)
            if score >= 1.2:
                scored.append({**comp, "order_type": order_type, "matched_keywords": hits[:6], "_score": score})
        scored.sort(key=lambda x: (-x["_score"], x["name"]))
        for s in scored[:lim]:
            used.add(s["code"])
            matched.append(s)
    return matched


def classify_stage(pct: float, vol: int) -> str:
    if pct >= 24.0:
        return "🔴 상한가 근접(추격금지)"
    if pct >= 14.0:
        return "🟠 이미 급등(과열주의)"
    if pct >= 5.0:
        return "🟡 반응 시작(수급유입)"
    if pct >= -2.5:
        return "⚪ 장전/장마감(시초가 주목)" if vol == 0 else "🟢 미반영/초입 구간"
    if pct <= -8.0:
        return "🔵 급락/악재반영 중"
    return "🔵 약세/조정 구간"


def fetch_naver_quotes(codes: list[str]) -> dict[str, StockQuote]:
    uniq = list(dict.fromkeys(c.zfill(6) for c in codes if c))
    if not uniq:
        return {}

    def _int(v: Any) -> int:
        try:
            return int(re.sub(r"[^0-9\-]", "", str(v or "0")))
        except ValueError:
            return 0

    def _flt(v: Any) -> float:
        try:
            return float(re.sub(r"[^0-9\.\-]", "", str(v or "0")))
        except ValueError:
            return 0.0

    def _parse(row: dict[str, Any]) -> StockQuote | None:
        code = str(row.get("itemCode") or row.get("stockCode") or "").strip()
        if not code:
            return None
        price = _int(row.get("closePrice"))
        chg = _int(row.get("compareToPreviousClosePrice"))
        pct = _flt(row.get("fluctuationsRatio"))
        dcode = str((row.get("compareToPreviousPrice") or {}).get("code", ""))
        if dcode in ("4", "5"):
            pct = -abs(pct)
            chg = -abs(chg)
        vol = _int(row.get("accumulatedTradingVolume"))
        return StockQuote(
            code=code.zfill(6) if code.isdigit() else code,
            price=price,
            change=chg,
            change_pct=round(pct, 2),
            volume=vol,
            market_status=str(row.get("marketStatus") or "UNKNOWN"),
            reaction_stage=classify_stage(pct, vol),
        )

    quotes: dict[str, StockQuote] = {}
    try:
        url = "https://polling.finance.naver.com/api/realtime/domestic/stock/" + ",".join(uniq)
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        for row in data.get("datas", []):
            q = _parse(row)
            if q:
                quotes[q.code] = q
    except Exception as exc:
        logger.warning("네이버 배치 시세 조회 실패: %s", exc)

    for code in uniq:
        if code in quotes:
            continue
        try:
            url = f"https://m.stock.naver.com/api/stock/{code}/basic"
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                row = json.loads(resp.read().decode("utf-8"))
            row.setdefault("itemCode", code)
            q = _parse(row)
            if q:
                quotes[q.code] = q
        except Exception:
            pass
    return quotes


class GeminiAuthError(RuntimeError):
    """GEMINI_API_KEY 누락 또는 유효하지 않은 키 오류."""


def _env_str(name: str, default: str) -> str:
    val = (os.getenv(name) or "").strip()
    return val if val else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env_str(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env_str(name, str(default)))
    except ValueError:
        return default


def build_gemini_generation_config(
    model: str,
    thinking_level: str | None = None,
    thinking_budget: int | None = None,
) -> dict[str, Any]:
    """모델 세대에 맞는 안전한 generationConfig/thinkingConfig 구성.

    - Gemini 3.x / 4.x: thinkingLevel 사용 (thinkingBudget 및 낮은 temperature 미전송).
      gemini-3.8-flash / 3.7 / Pro / 4.x는 MINIMAL 미지원이므로 LOW로 자동 보정.
    - Gemini 2.5: thinkingBudget으로 추론 토큰 상한 제한.
    """
    cfg: dict[str, Any] = {"responseMimeType": "application/json"}
    ml = model.strip().lower()

    if "gemini-2.5" in ml:
        cfg["temperature"] = 0.25
        if thinking_budget is not None:
            cfg["thinkingConfig"] = {"thinkingBudget": max(0, int(thinking_budget))}
    elif re.search(r"gemini-[34](?:[\.\-]|$)", ml):
        if thinking_level:
            level = thinking_level.strip().upper()
            no_minimal = any(k in ml for k in ("gemini-3.8", "gemini-3.7", "gemini-4", "-pro"))
            if level == "MINIMAL" and no_minimal:
                level = "LOW"
            cfg["thinkingConfig"] = {"thinkingLevel": level}
    else:
        cfg["temperature"] = 0.25

    return cfg


def call_gemini(
    api_key: str,
    models: list[str],
    system_prompt: str,
    user_prompt: str,
    *,
    thinking_level: str | None = None,
    thinking_budget: int | None = None,
    timeout: int = DEFAULT_DEEP_TIMEOUT,
) -> tuple[dict[str, Any], str]:
    if not api_key:
        raise GeminiAuthError("GEMINI_API_KEY가 비어 있습니다. GitHub Secrets에 등록하세요.")
    if not models:
        raise RuntimeError("호출할 Gemini 모델 목록이 비어 있습니다.")

    last_err: Exception | None = None
    for model in models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        gen_cfg = build_gemini_generation_config(
            model,
            thinking_level=thinking_level,
            thinking_budget=thinking_budget,
        )
        for attempt in range(2):
            t0 = time.monotonic()
            body = json.dumps({
                "systemInstruction": {"parts": [{"text": system_prompt}]},
                "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
                "generationConfig": gen_cfg,
            }).encode("utf-8")
            try:
                req = urllib.request.Request(
                    url,
                    data=body,
                    headers={"Content-Type": "application/json", "x-goog-api-key": api_key},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    res = json.loads(resp.read().decode("utf-8"))
                parts = (res.get("candidates") or [{}])[0].get("content", {}).get("parts", [])
                text = "".join(p.get("text", "") for p in parts if not p.get("thought"))
                if not text:
                    text = "".join(p.get("text", "") for p in parts)
                elapsed = time.monotonic() - t0
                thoughts = (res.get("usageMetadata") or {}).get("thoughtsTokenCount")
                if thoughts is not None:
                    logger.info("Gemini %s 응답 완료 (%.1fs, thoughts=%s)", model, elapsed, thoughts)
                else:
                    logger.info("Gemini %s 응답 완료 (%.1fs)", model, elapsed)
                return extract_json(text), model
            except urllib.error.HTTPError as exc:
                err_txt = exc.read().decode("utf-8", errors="ignore")[:260]
                logger.warning(
                    "Gemini %s HTTP %d (시도 %d, %.1fs): %s",
                    model, exc.code, attempt + 1, time.monotonic() - t0, err_txt,
                )
                last_err = exc
                if exc.code == 400 and "api key" in err_txt.lower():
                    raise GeminiAuthError(f"Gemini API 키 오류 (HTTP 400): {err_txt}") from exc
                if exc.code == 400 and "thinking" in err_txt.lower() and "thinkingConfig" in gen_cfg and attempt == 0:
                    logger.info("Gemini %s thinkingConfig 미지원 감지 — 기본 설정으로 재시도합니다.", model)
                    gen_cfg = {k: v for k, v in gen_cfg.items() if k != "thinkingConfig"}
                    continue
                if exc.code in (429, 503) and attempt == 0:
                    time.sleep(3)
                    continue
                break
            except Exception as exc:
                logger.warning("Gemini %s 오류 (%.1fs): %s", model, time.monotonic() - t0, exc)
                last_err = exc
                break
    raise RuntimeError(f"모든 Gemini 모델 호출 실패: {last_err}")


def format_telegram_html(report: dict[str, Any], quotes: dict[str, StockQuote], articles: list[NewsItem], model_used: str) -> str:
    esc = lambda s: html.escape(str(s or ""), quote=False)
    esc_a = lambda s: html.escape(str(s or ""), quote=True)
    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")

    lines = [
        f"🚨 <b>[이슈 레이더] {esc(report['title'])}</b>",
        f"⭐ 촉매 점수: <b>{float(report['catalyst_score']):.1f}/10</b>  |  🕒 {now_str}",
        "",
        "📰 <b>사건 요약</b>",
        esc(report["event_summary"]),
    ]
    if report.get("causal_chain"):
        lines += ["", "🔗 <b>1·2차 파급 인과 체인</b>"] + [f"• {esc(s)}" for s in report["causal_chain"]]

    ben, neg = report.get("beneficiary_sectors") or [], report.get("negative_sectors") or []
    if ben or neg:
        lines += ["", "🧭 <b>영향 섹터</b>"]
        if ben:
            lines.append(f"• 수혜: {esc(', '.join(ben))}")
        if neg:
            lines.append(f"• 주의/역풍: {esc(', '.join(neg))}")

    if report.get("stocks"):
        lines += ["", "🎯 <b>KRX 검증 관련주 &amp; 선반영 체크</b>"]
        for s in report["stocks"]:
            code = s["code"]
            icon = {"상승수혜": "📈", "하락주의": "📉", "옥석가리기주의": "⚠️"}.get(s.get("direction", ""), "•")
            q = quotes.get(code)
            if q and q.price > 0:
                sign = "+" if q.change_pct > 0 else ""
                qtxt = f"{q.price:,}원 ({sign}{q.change_pct:.2f}%) · {q.reaction_stage}"
            else:
                qtxt = "시세 확인중"
            naver = f"https://m.stock.naver.com/domestic/stock/{code}/total"
            lines.append(
                f"{icon} <a href=\"{naver}\"><b>{esc(s['name'])}({code})</b></a> "
                f"[{esc(s.get('order_tier', '2차 파급'))} / {esc(s.get('revenue_link', '부분연관'))}]\n"
                f"   ├ 현재 반응: {esc(qtxt)}\n"
                f"   └ 연결 근거: {esc(s.get('reason', ''))}"
            )

    lines += [
        "",
        "📘 <b>실전 매매 가이드</b>",
        f"• <b>진입/관찰 전략</b>: {esc(report.get('entry_strategy', ''))}",
        f"• <b>무효화(손절) 조건</b>: {esc(report.get('invalidation_rules', ''))}",
        f"• <b>핵심 리스크</b>: {esc(report.get('key_risks', ''))}",
    ]
    if articles:
        lines += ["", "📎 <b>근거 기사</b>"] + [
            f"• <a href=\"{esc_a(a.url)}\">{esc(a.title[:70])}</a> ({esc(a.source)})"
            for a in articles[:3]
        ]
    lines += [
        "",
        f"<i>🤖 분석모델: {esc(model_used)} | 투자 참고용 정보이며 최종 매매 판단과 책임은 본인에게 있습니다.</i>",
    ]
    return "\n".join(lines)


def send_telegram(token: str, chat_id: str, html_text: str) -> None:
    if not (token and chat_id):
        raise RuntimeError("TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID가 비어 있습니다.")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    chunks, cur, cur_len = [], [], 0
    for line in html_text.splitlines():
        if cur and cur_len + len(line) + 1 > 3950:
            chunks.append("\n".join(cur))
            cur, cur_len = [line], len(line)
        else:
            cur.append(line)
            cur_len += len(line) + 1
    if cur:
        chunks.append("\n".join(cur))

    for chunk in chunks:
        payload = json.dumps({
            "chat_id": chat_id,
            "text": chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }).encode("utf-8")
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=12) as resp:
                res = json.loads(resp.read().decode("utf-8"))
                if not res.get("ok"):
                    raise RuntimeError(f"텔레그램 발송 실패: {res}")
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"텔레그램 HTTP {exc.code} 오류: {err_body}") from exc


def run_demo(dry_run: bool, token: str, chat_id: str) -> None:
    report = {
        "title": "[테스트] AI 안경 시험 부정행위 비상 → 전파탐지기·보안장비 2차 수혜",
        "catalyst_score": 8.8,
        "event_summary": "세무사·수능 등 국가시험에서 AI 스마트안경 부정행위가 적발되며 정부·국회에서 전파탐지기 도입 및 사업장 보안 강화 대책이 추진 중입니다.",
        "causal_chain": [
            "1단계(사건): 국가시험·채용시험에서 AI 안경 악용 적발 및 기술 유출 우려 확산",
            "2단계(정부 대응): 교육부·국회 전파탐지기 도입 예산 검토 + 대기업 사업장 반입 통제",
            "3단계(2차 핵심 수혜): 전파탐지기·불법촬영/무선도청 탐지·보안장비 수요 확대 기대",
            "4단계(옥석 가리기): 1차 스마트안경 부품주는 정책 직접 수혜가 아니므로 급등 추격 주의",
        ],
        "beneficiary_sectors": ["전파탐지기·무선도청 탐지(2차 핵심)", "물리·네트워크 보안(2차 확산)"],
        "negative_sectors": ["AI 안경 단순 테마 편승 부품주"],
        "stocks": [
            {"code": "446840", "name": "지슨", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "본업직결", "reason": "이동형 전파탐지기(알파-P)·무선도청/불법촬영 탐지 장비가 본업으로 정부 도입 논의와 직결"},
            {"code": "140430", "name": "카티스", "direction": "상승수혜", "order_tier": "2차 파급(핵심)", "revenue_link": "부분연관", "reason": "공간·물리보안 플랫폼 및 탐지 솔루션 보유로 보안 강화 수혜 기대"},
            {"code": "263860", "name": "지니언스", "direction": "상승수혜", "order_tier": "2차 파급(확산)", "revenue_link": "부분연관", "reason": "비인가 단말 접근제어(NAC)·EDR 전문 기업으로 기업 단말 통제 강화 수혜"},
            {"code": "452430", "name": "사피엔반도체", "direction": "옥석가리기주의", "order_tier": "1차 직접", "revenue_link": "단기테마성", "reason": "AR/AI 안경용 마이크로LED 구동칩 기업이나 부정행위 규제 이슈이므로 단기 추격 주의"},
        ],
        "entry_strategy": "대장주가 +15% 이상 급등했다면 당일 추격 매수 금지, 눌림목·조정 구간 분할 관찰. 1차 부품주보다 2차 보안·탐지 본업주 중심으로 압축.",
        "invalidation_rules": "정부 대책이 전파탐지기 구매 없이 인력 감독으로 축소되거나, 대장주가 대량 거래량과 함께 시초가를 이탈하면 즉시 손절.",
        "key_risks": "실제 예산 규모 대비 시가총액 단기 과열 및 시험 종료 직후 재료 소멸 급락 위험.",
    }
    quotes = {
        "446840": StockQuote("446840", 3820, 185, 5.09, 412000, "OPEN", classify_stage(5.09, 412000)),
        "140430": StockQuote("140430", 6150, 210, 3.53, 185000, "OPEN", classify_stage(3.53, 185000)),
        "263860": StockQuote("263860", 14200, 250, 1.79, 98000, "OPEN", classify_stage(1.79, 98000)),
        "452430": StockQuote("452430", 25400, 3600, 16.51, 920000, "OPEN", classify_stage(16.51, 920000)),
    }
    articles = [NewsItem("수능 앞두고 'AI 안경' 시험 부정행위 비상…정부 전파탐지기 도입 추진", "https://news.google.com", "샘플테스트", "사회/사건")]
    msg = format_telegram_html(report, quotes, articles, "demo-test")
    if dry_run or not (token and chat_id):
        print(msg)
    else:
        send_telegram(token, chat_id, msg)
        logger.info("데모 테스트 알림을 텔레그램으로 성공적으로 발송했습니다!")


def is_market_watch_window(now: datetime | None = None) -> bool:
    """평일 장전·장중(월~금 08:00~15:30 KST) 연속 감시 구간 여부 반환."""
    if now is None:
        current = datetime.now(KST)
    elif now.tzinfo is None:
        current = now.replace(tzinfo=KST)
    else:
        current = now.astimezone(KST)
    if current.weekday() >= 5:
        return False
    return (8, 0) <= (current.hour, current.minute) <= (15, 30)


def resolve_loop_schedule(
    cycles_arg: int | None = None,
    interval_arg: int | None = None,
    now: datetime | None = None,
) -> tuple[int, int, bool]:
    """실행 횟수와 대기 간격(초)을 결정.

    평일 08:00~15:30 KST에는 기본 2분(120초) 간격 6회 연속 감시 워커 루프를 수행하고,
    그 외 시간대에는 1회 단발 감시를 수행한다.
    """
    market_window = is_market_watch_window(now)
    default_cycles = max(1, _env_int("MARKET_LOOP_COUNT", DEFAULT_MARKET_LOOP_COUNT))
    default_interval = max(0, _env_int("MARKET_LOOP_INTERVAL_SEC", DEFAULT_MARKET_LOOP_INTERVAL_SEC))

    if cycles_arg is not None:
        total_cycles = max(1, cycles_arg)
    else:
        total_cycles = default_cycles if market_window else 1

    loop_interval = max(0, interval_arg) if interval_arg is not None else default_interval
    return total_cycles, loop_interval, market_window


def run_live(
    dry_run: bool,
    cycles: int | None = None,
    interval_sec: int | None = None,
    data_dir: Path | None = None,
) -> None:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    tg_chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    min_score = _env_float("MIN_CATALYST_SCORE", 7.5)
    max_per_run = max(1, _env_int("MAX_ALERTS_PER_RUN", 2))
    max_daily = max(1, _env_int("MAX_DAILY_ALERTS", 10))
    cooldown_h = max(1, _env_int("THEME_COOLDOWN_HOURS", 12))

    screen_models = [m.strip() for m in _env_str("GEMINI_SCREEN_MODELS", DEFAULT_SCREEN_MODELS).split(",") if m.strip()]
    deep_models = [m.strip() for m in _env_str("GEMINI_DEEP_MODELS", DEFAULT_DEEP_MODELS).split(",") if m.strip()]
    screen_thinking_level = _env_str("GEMINI_SCREEN_THINKING_LEVEL", DEFAULT_SCREEN_THINKING_LEVEL)
    deep_thinking_level = _env_str("GEMINI_DEEP_THINKING_LEVEL", DEFAULT_DEEP_THINKING_LEVEL)
    deep_thinking_budget = max(0, _env_int("GEMINI_THINKING_BUDGET", DEFAULT_THINKING_BUDGET))
    screen_timeout = max(5, _env_int("GEMINI_SCREEN_TIMEOUT", DEFAULT_SCREEN_TIMEOUT))
    deep_timeout = max(5, _env_int("GEMINI_DEEP_TIMEOUT", DEFAULT_DEEP_TIMEOUT))

    now_kst = datetime.now(KST)
    total_cycles, loop_interval, market_window = resolve_loop_schedule(cycles, interval_sec, now=now_kst)
    if total_cycles > 1:
        logger.info(
            "장전·장중 워커 루프 시작 (총 %d회, %d초 간격, 장중구간=%s, 시작=%s)",
            total_cycles, loop_interval, market_window, now_kst.strftime("%Y-%m-%d %H:%M KST"),
        )
    else:
        logger.info(
            "단일 감시 사이클 시작 (장중구간=%s, 시작=%s)",
            market_window, now_kst.strftime("%Y-%m-%d %H:%M KST"),
        )

    base_dir = data_dir if data_dir is not None else Path("data")
    state = StateStore(base_dir / "state.json")
    companies: dict[str, dict[str, str]] | None = None
    batch_themes: set[str] = set()
    sent_this_run = 0
    ai_calls = 0
    skipped_cycles = 0
    successful_cycles = 0
    failed_cycles = 0
    last_cycle_err: Exception | None = None

    try:
        for cycle in range(1, total_cycles + 1):
            cycle_start = time.monotonic()
            tag = f"[사이클 {cycle}/{total_cycles}]"

            if state.sent_today() >= max_daily:
                logger.info("%s 오늘 최대 알림(%d건) 도달 — 무료 할당량 보호를 위해 종료합니다.", tag, max_daily)
                break
            if sent_this_run >= max_per_run:
                logger.info("%s 이번 실행 최대 알림(%d건) 도달 — 워커 루프를 종료합니다.", tag, max_per_run)
                break

            try:
                fresh = fetch_rss_news(state)
                logger.info("%s 신규 수집 기사(노이즈 제거 후): %d건", tag, len(fresh))
                if not fresh:
                    skipped_cycles += 1
                    successful_cycles += 1
                    logger.info("%s 새 기사 없음 — AI 호출 스킵 (Gemini 무료 한도 보호)", tag)
                else:
                    clusters = cluster_news(fresh)
                    logger.info("%s 클러스터링 완료: %d개 이벤트", tag, len(clusters))

                    screen_raw, screen_model = call_gemini(
                        api_key,
                        screen_models,
                        SCREENING_PROMPT,
                        json.dumps({"clusters": [c.to_prompt_dict() for c in clusters], "min_score_hint": min_score}, ensure_ascii=False),
                        thinking_level=screen_thinking_level,
                        thinking_budget=0,
                        timeout=screen_timeout,
                    )
                    ai_calls += 1
                    logger.info("%s Stage 1 스크리닝 완료 (%s)", tag, screen_model)

                    for item in fresh:
                        state.mark_seen(item.url, item.title)
                    if not dry_run:
                        state.save()

                    by_id = {c.cluster_id: c for c in clusters}
                    candidates = [
                        e for e in screen_raw.get("selected_events", [])
                        if e.get("cluster_id") in by_id and float(e.get("catalyst_score", 0) or 0) >= min_score
                    ]
                    candidates.sort(key=lambda e: -float(e.get("catalyst_score", 0)))

                    if not candidates:
                        logger.info("%s 촉매 점수 %.1f 이상 핵심 이슈 없음 — 다음 주기에 재탐색합니다.", tag, min_score)
                    else:
                        if companies is None:
                            companies = load_krx_master(base_dir / "krx_master.json", allow_network=True)
                            logger.info("%s KRX 상장사 DB 로드 완료: %d개 기업", tag, len(companies))

                        limit = min(max_per_run - sent_this_run, max(0, max_daily - state.sent_today()))
                        sent_in_cycle = 0

                        for cand in candidates:
                            if sent_in_cycle >= limit:
                                break
                            tkey = str(cand.get("theme_key") or cand["cluster_id"])
                            tnorm = norm_key(tkey)
                            if tnorm in batch_themes or state.in_cooldown(tkey, cooldown_h):
                                logger.info("%s 중복/쿨다운 테마 스킵: %s", tag, tkey)
                                continue
                            batch_themes.add(tnorm)

                            cluster = by_id[cand["cluster_id"]]
                            matched = match_krx_stocks(
                                companies,
                                cand.get("second_order_keywords") or [],
                                cand.get("first_order_keywords") or [],
                                cand.get("negative_keywords") or [],
                            )
                            quotes = fetch_naver_quotes([m["code"] for m in matched])
                            enriched = []
                            for m in matched:
                                row = {k: v for k, v in m.items() if k != "_score"}
                                q = quotes.get(m["code"])
                                if q:
                                    row["quote"] = {
                                        "price": q.price,
                                        "change_pct": q.change_pct,
                                        "volume": q.volume,
                                        "reaction_stage": q.reaction_stage,
                                    }
                                enriched.append(row)

                            deep_raw, deep_model = call_gemini(
                                api_key,
                                deep_models,
                                DEEP_PROMPT,
                                json.dumps(
                                    {
                                        "event_cluster": cluster.to_prompt_dict(),
                                        "stage1_screening": cand,
                                        "verified_krx_candidates": enriched,
                                    },
                                    ensure_ascii=False,
                                ),
                                thinking_level=deep_thinking_level,
                                thinking_budget=deep_thinking_budget,
                                timeout=deep_timeout,
                            )
                            ai_calls += 1
                            logger.info("%s Stage 2 심층 분석 완료 (%s)", tag, deep_model)

                            # 할루시네이션 가드: 실제 KRX 매칭 후보에 있는 종목코드만 통과
                            valid_by_code = {m["code"]: m for m in matched}
                            clean_stocks = []
                            for s in deep_raw.get("stocks", []):
                                code = str(s.get("code", "")).strip().zfill(6)
                                if code in valid_by_code:
                                    s["code"] = code
                                    s["name"] = valid_by_code[code]["name"]
                                    clean_stocks.append(s)
                            if not clean_stocks and matched:
                                clean_stocks = [
                                    {
                                        "code": m["code"],
                                        "name": m["name"],
                                        "direction": "하락주의" if m["order_type"] == "피해_우려" else "상승수혜",
                                        "order_tier": "2차 파급(핵심)" if m["order_type"] == "2차_수혜" else "1차 직접",
                                        "revenue_link": "검토필요",
                                        "reason": f"주요제품: {m['products'][:90]}",
                                    }
                                    for m in matched[:4]
                                ]

                            deep_raw["stocks"] = clean_stocks[:6]
                            deep_raw["catalyst_score"] = float(cand.get("catalyst_score", min_score))
                            deep_raw.setdefault("title", cluster.headline)
                            deep_raw.setdefault("event_summary", cand.get("event_summary", cluster.headline))

                            msg = format_telegram_html(deep_raw, quotes, cluster.items, deep_model)
                            if dry_run:
                                print(msg)
                            else:
                                send_telegram(tg_token, tg_chat, msg)
                                state.mark_alerted(tkey)
                                state.save()
                                logger.info("%s 텔레그램 알림 발송 완료: %s", tag, deep_raw["title"])
                            sent_in_cycle += 1
                            sent_this_run += 1

                    successful_cycles += 1
            except GeminiAuthError:
                raise
            except Exception as exc:
                if total_cycles == 1:
                    raise
                failed_cycles += 1
                last_cycle_err = exc
                logger.warning("%s 일시 오류 발생 — 다음 사이클에서 재시도합니다: %s", tag, exc)

            if cycle < total_cycles:
                if state.sent_today() >= max_daily or sent_this_run >= max_per_run:
                    logger.info("%s 알림 한도 도달 — 남은 워커 사이클을 조기 종료합니다.", tag)
                    break
                elapsed = time.monotonic() - cycle_start
                wait_sec = max(0.0, loop_interval - elapsed)
                logger.info("%s 완료 (소요 %.1fs) — %.0f초 후 다음 감시 시작", tag, elapsed, wait_sec)
                if wait_sec > 0:
                    time.sleep(wait_sec)
    finally:
        if not dry_run:
            state.save()

    logger.info(
        "감시 종료 — 성공 %d/%d 사이클 (AI 스킵 %d회, AI 호출 %d회, 알림 %d건)",
        successful_cycles, total_cycles, skipped_cycles, ai_calls, sent_this_run,
    )
    if successful_cycles == 0 and failed_cycles > 0:
        raise RuntimeError(f"모든 감시 사이클 실패: {last_cycle_err}")


def main() -> int:
    parser = argparse.ArgumentParser(description="News Alpha Radar (24/7 Free)")
    parser.add_argument("--mode", choices=["live", "demo"], default="live")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cycles", type=int, default=None, help="연속 감시 횟수 강제 지정 (미지정 시 장중 6회 / 그 외 1회)")
    parser.add_argument("--interval", type=int, default=None, help="연속 감시 간격(초) 강제 지정 (기본 120초)")
    args = parser.parse_args()
    try:
        if args.mode == "demo":
            run_demo(args.dry_run, os.getenv("TELEGRAM_BOT_TOKEN", "").strip(), os.getenv("TELEGRAM_CHAT_ID", "").strip())
        else:
            run_live(args.dry_run, cycles=args.cycles, interval_sec=args.interval)
        return 0
    except Exception as exc:
        logger.exception("실행 오류: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
