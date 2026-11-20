#!/usr/bin/env python3
"""News Alpha Radar — 단일 파일 24/7 국내주식 일상뉴스 2차 파급효과 레이더 (추가 비용 0원).

외부 패키지 설치 불필요 (Python 3.11+ 표준 라이브러리 전용).
- 뉴스 수집: 무료 한국 RSS 10개 피드 (구글뉴스 2개 + 국내 직통 8개, 병렬 수집·조건부 GET) + 시황 노이즈 제거 + 클러스터링
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
import signal
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("news_alpha_radar")

KST = timezone(timedelta(hours=9))
try:
    PT = ZoneInfo("America/Los_Angeles")
except Exception:
    PT = timezone(timedelta(hours=-8))

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
DEFAULT_DEEP_TIMEOUT = 25
DEFAULT_MARKET_LOOP_COUNT = 6
DEFAULT_MARKET_LOOP_INTERVAL_SEC = 120

MAX_DEFERRED_ITEMS = 5
MAX_DEFERRED_RETRIES = 2
DEFERRED_TTL_MARKET_MIN = 60
DEFERRED_TTL_OFFHOURS_MIN = 180
SEND_PENDING_MAX_AGE_MIN = 15
PENDING_TTL_MARKET_MIN = 90
PENDING_TTL_OFFHOURS_MIN = 180
MAX_PUB_AGE_HOURS = 12
RSS_DIRECT_TIMEOUT = 8
RSS_GOOGLE_TIMEOUT = 5
RSS_MAX_PER_HOST = 2
TRACE_MAX_DAYS = 7
TRACE_MAX_BYTES = 5 * 1024 * 1024

DEFAULT_DEDUP_MODE = "shadow"
DEFAULT_TRIGGER_MODE = "shadow"
DEFAULT_STAGE1_DAILY_RPD_LIMIT = 500
HUB_STOCK_MIN_APPEARANCES = 3
ESCALATION_SCORE_DELTA = 1.0
DEFAULT_DAEMON_INTERVAL_MARKET_SEC = 60
DEFAULT_DAEMON_INTERVAL_EXTENDED_SEC = 180
DEFAULT_DAEMON_INTERVAL_OFFHOURS_SEC = 600
DEFAULT_DAEMON_BURST_WINDOW_MIN = 15

RSS_FEEDS: list[dict[str, Any]] = [
    {"name": "구글뉴스_한국_주요", "category": "속보", "url": "https://news.google.com/rss?hl=ko&gl=KR&ceid=KR:ko", "priority": 1, "group": "google", "timeout": RSS_GOOGLE_TIMEOUT},
    {"name": "구글뉴스_한국_사회", "category": "사회/사건", "url": "https://news.google.com/rss/headlines/section/topic/NATION?hl=ko&gl=KR&ceid=KR:ko", "priority": 1, "group": "google", "timeout": RSS_GOOGLE_TIMEOUT},
    {"name": "연합뉴스_최신", "category": "속보", "url": "https://www.yna.co.kr/rss/news.xml", "priority": 1},
    {"name": "연합뉴스_사회", "category": "사회/사건", "url": "https://www.yna.co.kr/rss/society.xml", "priority": 1},
    {"name": "연합뉴스_산업", "category": "경제/산업", "url": "https://www.yna.co.kr/rss/industry.xml", "priority": 2},
    {"name": "연합뉴스_경제", "category": "경제/산업", "url": "https://www.yna.co.kr/rss/economy.xml", "priority": 2},
    {"name": "지디넷코리아_IT", "category": "IT/신기술", "url": "https://feeds.feedburner.com/zdkorea", "priority": 2},
    {"name": "매일경제_경제", "category": "경제/산업", "url": "https://www.mk.co.kr/rss/30100041/", "priority": 2},
    {"name": "매일경제_기업경영", "category": "경제/산업", "url": "https://www.mk.co.kr/rss/50100032/", "priority": 2},
    {"name": "매일경제_사회", "category": "사회/사건", "url": "https://www.mk.co.kr/rss/50400012/", "priority": 2},
]

# Actions 러너 IP에서 --probe-feeds로 검증한 뒤 채택할 후보 피드 목록 (우분투 상시 서버에서는 전자신문_속보·뉴시스_속보 기본 활성)
PROBE_CANDIDATE_FEEDS: list[dict[str, Any]] = [
    {"name": "전자신문_AI·SW", "category": "IT/신기술", "url": "https://rss.etnews.com/04.xml", "priority": 2},
    {"name": "전자신문_속보", "category": "IT/신기술", "url": "https://rss.etnews.com/Section902.xml", "priority": 2},
    {"name": "뉴시스_속보", "category": "속보", "url": "https://www.newsis.com/RSS/sokbo.xml", "priority": 2},
    {"name": "조선비즈", "category": "경제/산업", "url": "https://biz.chosun.com/arc/outboundfeeds/rss/?outputType=xml", "priority": 2},
]

ALL_FEEDS: list[dict[str, Any]] = RSS_FEEDS + PROBE_CANDIDATE_FEEDS
DAEMON_DEFAULT_EXTRA_FEEDS: tuple[str, ...] = ("전자신문_속보", "뉴시스_속보")

# PR-3 Stage 1 트리거 2계층 키워드 (단독/속보 및 단순 정치 용어 제외)
HARD_TRIGGER_KEYWORDS: tuple[str, ...] = (
    "해킹", "개인정보 유출", "정보유출", "랜섬웨어", "리콜", "전량 회수", "영업정지", "판매중지",
    "생산중단", "가동중단", "가동 중단", "공장 화재", "화재 발생", "폭발 사고", "관세 부과",
    "수출통제", "수출규제", "전파탐지", "불법촬영", "콜레라", "전염병", "감염병", "방역 비상",
    "인수합병", "경영권 분쟁", "공개매수", "대규모 수주", "임상 성공", "임상 중단", "품목허가",
    "과징금", "시정명령", "압수수색", "부도", "회생절차", "상장폐지", "대규모 정전", "통신장애",
    "통신 마비", "공급망 마비", "해협 봉쇄", "항로 봉쇄",
)

SOFT_TRIGGER_KEYWORDS: tuple[str, ...] = (
    "수주", "공급계약", "계약 체결", "국회 통과", "법안", "의무화", "전수조사", "규제 강화",
    "특허", "승인", "대규모 투자", "증설", "합작", "제재", "파업", "파산", "감산",
    "가격 인상", "품귀", "공급 부족", "결함", "조사 착수", "검찰 기소", "정부 대책",
    "예산 증액", "반입 금지", "보안 강화", "부정행위",
)

POLITICAL_NOISE_RE = re.compile(
    r"대통령|국회의원|민주당|국민의힘|조국혁신당|개혁신당|탄핵|여야|정당|공천|대선|총선|지방선거|국정감사|필리버스터|대법원장|특검|국무총리|정치권"
)

NOISE_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"^\[?마감시황\]?",
        r"^\[?장중시황\]?",
        r"^\[?코스피\s*마감",
        r"^\[?코스닥\s*마감",
        r"^\[?뉴욕증시\s*마감",
        r"^\[?오늘의\s*(운세|날씨)",
        r"^\[?(부고|부음)\]?",
        r"^\[?인사\]?",
        r"^\[?동정\]?",
        r"^\[?포토\]?",
        r"^\[?사진\]?",
        r"^\[?(칼럼|사설|기고|인터뷰|게시판|알림)\]?",
        r"목표가\s*(상향|하향|유지)",
        r"투자의견\s*(매수|중립|유지)",
        r"외국인\s*순매수\s*상위",
        r"기관\s*순매수\s*상위",
    ]
]

TOKEN_STOPWORDS: frozenset[str] = frozenset({
    "속보", "단독", "종합", "오늘", "내일", "올해", "내년", "관련", "대해", "통해", "위해", "뉴스",
    "정부", "발표", "기업", "국내", "업계", "관계자", "전망", "우려", "증가", "감소", "확대", "추진",
    "예정", "계획", "지난", "이번", "오는", "최대", "최초", "사실상", "논란", "가능성", "영향", "대상",
    "진행", "기자", "보도", "확인", "최근", "가운데", "본격", "기존", "신규", "기준", "수준", "현재",
    "이후", "이전", "경우", "때문", "따른", "위주", "중심", "관련주", "시장", "한국",
})

KOREAN_POSTPOSITIONS: tuple[str, ...] = (
    "에서", "으로", "까지", "부터", "은", "는", "이", "가", "을", "를", "의", "에", "로", "와", "과", "도", "만",
)

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
    first_seen: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "url": self.url,
            "source": self.source,
            "category": self.category,
            "published_at": self.published_at,
            "summary": self.summary,
            "priority": self.priority,
            "first_seen": self.first_seen,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "NewsItem":
        return cls(
            title=str(data.get("title", "")),
            url=str(data.get("url", "")),
            source=str(data.get("source", "")),
            category=str(data.get("category", "")),
            published_at=str(data.get("published_at", "")),
            summary=str(data.get("summary", "")),
            priority=int(data.get("priority", 2)),
            first_seen=str(data.get("first_seen", "")),
        )


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

    def to_dict(self) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "headline": self.headline,
            "category": self.category,
            "items": [i.to_dict() for i in self.items[:5]],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EventCluster":
        return cls(
            cluster_id=str(data.get("cluster_id", "")),
            headline=str(data.get("headline", "")),
            category=str(data.get("category", "")),
            items=[NewsItem.from_dict(x) for x in (data.get("items") or []) if isinstance(x, dict)],
        )


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


def url_hash(url: str) -> str:
    return hashlib.sha1((url or "").encode("utf-8")).hexdigest()[:16]


def atomic_write_text(path: Path, content: str) -> None:
    """임시 파일에 기록·fsync 후 os.replace로 원자적 교체."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as fp:
            tmp_path = Path(fp.name)
            fp.write(content)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp_path, path)
        tmp_path = None
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass


def parse_iso_kst(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        return dt.replace(tzinfo=KST) if dt.tzinfo is None else dt.astimezone(KST)
    except ValueError:
        return None


def parse_pub_date(raw: str | None) -> datetime | None:
    """RSS pubDate(RFC 822 / ISO 8601)를 KST datetime으로 파싱. 실패 시 None."""
    text = (raw or "").strip()
    if not text:
        return None
    try:
        dt = parsedate_to_datetime(text)
        if dt is not None:
            return dt.replace(tzinfo=KST) if dt.tzinfo is None else dt.astimezone(KST)
    except Exception:
        pass
    return parse_iso_kst(text)


def _parse_cron_field(expr: str, min_val: int, max_val: int, *, is_dow: bool = False) -> set[int]:
    out: set[int] = set()
    for part in expr.split(","):
        part = part.strip()
        if not part:
            continue
        step = 1
        if "/" in part:
            base, step_s = part.split("/", 1)
            step = max(1, int(step_s))
        else:
            base = part
        if base == "*":
            start, end = min_val, max_val
        elif "-" in base:
            a, b = base.split("-", 1)
            start, end = int(a), int(b)
        else:
            start = end = int(base)
        for v in range(start, end + 1, step):
            if is_dow and v == 7:
                out.add(0)
            elif min_val <= v <= max_val:
                out.add(v)
    return out


def compute_cron_jitter(schedule_expr: str | None, started_at: datetime) -> tuple[datetime | None, int | None]:
    """GitHub Actions UTC cron 문자열 기준으로 가장 최근 예정 시각(KST)과 지연(초) 계산."""
    expr = (schedule_expr or "").strip()
    if not expr:
        return None, None
    fields = expr.split()
    if len(fields) != 5:
        return None, None
    try:
        mins = _parse_cron_field(fields[0], 0, 59)
        hours = _parse_cron_field(fields[1], 0, 23)
        doms = _parse_cron_field(fields[2], 1, 31)
        months = _parse_cron_field(fields[3], 1, 12)
        dows = _parse_cron_field(fields[4], 0, 7, is_dow=True)
    except ValueError:
        return None, None
    if not (mins and hours and doms and months and dows):
        return None, None

    started_utc = (
        started_at.replace(tzinfo=KST).astimezone(timezone.utc)
        if started_at.tzinfo is None
        else started_at.astimezone(timezone.utc)
    )
    cand = started_utc.replace(second=0, microsecond=0)
    # 최대 7일(10,080분) 역추적
    for _ in range(7 * 24 * 60 + 1):
        cron_dow = (cand.weekday() + 1) % 7
        if (
            cand.month in months
            and cand.day in doms
            and cron_dow in dows
            and cand.hour in hours
            and cand.minute in mins
        ):
            jitter = max(0, int((started_utc - cand).total_seconds()))
            return cand.astimezone(KST), jitter
        cand -= timedelta(minutes=1)
    return None, None


class TraceLogger:
    """기사 상태 전이·판정·실행 지연을 data/trace.jsonl에 기록하고 7일/5MB 기준 로테이션."""

    def __init__(self, path: Path, max_days: int = TRACE_MAX_DAYS, max_bytes: int = TRACE_MAX_BYTES) -> None:
        self.path = path
        self.max_days = max_days
        self.max_bytes = max_bytes

    def emit(self, record: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, ensure_ascii=False) + "\n"
        with self.path.open("a", encoding="utf-8") as fp:
            fp.write(line)

    def rotate(self, now: datetime | None = None) -> None:
        if not self.path.exists():
            return
        ref = now.astimezone(KST) if (now and now.tzinfo) else (now.replace(tzinfo=KST) if now else datetime.now(KST))
        cutoff = ref - timedelta(days=self.max_days)
        kept: list[str] = []
        for raw_line in self.path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                ts = parse_iso_kst(obj.get("ts"))
                if ts is None or ts >= cutoff:
                    kept.append(line)
            except Exception:
                continue

        # 5MB 초과 시 최신 레코드 우선 보존
        if self.max_bytes > 0:
            total = 0
            trimmed: list[str] = []
            for line in reversed(kept):
                sz = len(line.encode("utf-8")) + 1
                if total + sz > self.max_bytes and trimmed:
                    break
                trimmed.append(line)
                total += sz
            kept = list(reversed(trimmed))

        content = ("\n".join(kept) + "\n") if kept else ""
        atomic_write_text(self.path, content)


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


def item_dedup_key(url: str, title: str) -> str:
    th = title_hash(title)
    return f"t:{th}" if norm_key(title) else f"u:{url_hash(url)}"


def extract_outlet(item: NewsItem) -> str:
    """기사 원 언론사명 추출 (구글뉴스는 제목 끝 '- 언론사명', 직통 피드는 소스 접두사)."""
    if item.source.startswith("구글뉴스"):
        m = re.search(r"\s+-\s+([^-]{1,40})$", (item.title or "").strip())
        if m:
            return m.group(1).strip()
        return "구글뉴스"
    return item.source.split("_", 1)[0]


def normalize_token(word: str) -> str:
    """한국어 조사 제거·숫자 토큰 제외·불용어 필터링."""
    w = (word or "").strip().lower()
    if len(w) < 2 or w[0].isdigit():
        return ""
    if len(w) >= 3:
        for suf in KOREAN_POSTPOSITIONS:
            if w.endswith(suf) and len(w) - len(suf) >= 2:
                w = w[: -len(suf)]
                break
    if len(w) < 2 or w[0].isdigit() or w in TOKEN_STOPWORDS:
        return ""
    return w


def extract_normalized_tokens(text: str) -> list[str]:
    """제목/텍스트에서 괄호·언론사 접미사를 제거하고 정규화된 토큰 목록 반환."""
    cleaned = re.sub(r"\s+-\s+[^-]+$", " ", text or "")
    cleaned = re.sub(r"\[[^\]]*\]|\([^\)]*\)", " ", cleaned)
    out: list[str] = []
    for raw in re.findall(r"[가-힣a-zA-Z0-9]{2,}", cleaned):
        tok = normalize_token(raw)
        if tok:
            out.append(tok)
    return out


def top_cluster_tokens(cluster: EventCluster, limit: int = 8) -> list[str]:
    """클러스터 내 기사 제목들의 출현 빈도순 상위 정규화 토큰 추출."""
    counts: Counter[str] = Counter()
    texts = [item.title for item in cluster.items] if cluster.items else [cluster.headline]
    for txt in texts:
        counts.update(set(extract_normalized_tokens(txt)))
    return [tok for tok, _ in counts.most_common(limit)]


def pt_date_str(now: datetime | None = None) -> str:
    """Gemini API RPD 리셋 기준(태평양 시간 자정) 날짜 문자열 YYYY-MM-DD."""
    if now is None:
        dt = datetime.now(PT)
    elif now.tzinfo is None:
        dt = now.replace(tzinfo=KST).astimezone(PT)
    else:
        dt = now.astimezone(PT)
    return dt.strftime("%Y-%m-%d")


def load_dotenv_file(path: Path | None = None) -> None:
    """로컬/우분투 상시 서버 실행 시 .env 파일을 표준 라이브러리로 로드(기존 환경변수 우선)."""
    target = path if path is not None else (Path(__file__).resolve().parent / ".env")
    if not target.exists():
        return
    try:
        for raw in target.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            if key and key not in os.environ:
                os.environ[key] = val
    except Exception as exc:
        logger.warning(".env 로드 스킵: %s", exc)


def load_runtime_config(data_dir: Path) -> dict[str, Any]:
    """웹 대시보드에서 실시간 조정하는 data/runtime_config.json 로드."""
    cfg_path = data_dir / "runtime_config.json"
    if not cfg_path.exists():
        return {}
    try:
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
        return dict(raw) if isinstance(raw, dict) else {}
    except Exception:
        return {}


def save_runtime_config(data_dir: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    """data/runtime_config.json 원자적 저장."""
    clean = dict(cfg)
    clean["updated_at"] = datetime.now(KST).isoformat()
    atomic_write_text(
        data_dir / "runtime_config.json",
        json.dumps(clean, ensure_ascii=False, indent=2),
    )
    return clean


def resolve_active_feeds(
    runtime_cfg: dict[str, Any] | None = None,
    *,
    daemon_mode: bool = False,
) -> list[dict[str, Any]] | None:
    """활성 RSS 피드 목록 결정.

    기본 Actions 모드(오버라이드 없음)에서는 None을 반환해 fetch_rss_news 기본값을 유지하고,
    우분투 데몬 모드에서는 전자신문_속보·뉴시스_속보를 기본 추가하며,
    runtime_config.json의 enabled_feeds나 EXTRA_RSS_FEEDS 설정을 반영한다.
    """
    cfg = runtime_cfg or {}
    enabled_names = cfg.get("enabled_feeds")
    if isinstance(enabled_names, list) and enabled_names:
        wanted = {str(n).strip() for n in enabled_names if str(n).strip()}
        selected = [f for f in ALL_FEEDS if f["name"] in wanted]
        if selected:
            return selected

    extra_env = [x.strip() for x in (os.getenv("EXTRA_RSS_FEEDS") or "").split(",") if x.strip()]
    if daemon_mode or extra_env:
        base_names = {f["name"] for f in RSS_FEEDS}
        if daemon_mode:
            base_names.update(DAEMON_DEFAULT_EXTRA_FEEDS)
        base_names.update(extra_env)
        return [f for f in ALL_FEEDS if f["name"] in base_names]

    return None


def write_status_snapshot(data_dir: Path, payload: dict[str, Any]) -> None:
    """웹 대시보드 모니터링용 data/status.json 하트비트 원자적 기록."""
    try:
        atomic_write_text(
            data_dir / "status.json",
            json.dumps(payload, ensure_ascii=False, indent=2),
        )
    except Exception as exc:
        logger.warning("status.json 기록 실패: %s", exc)


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.seen_urls: dict[str, str] = {}
        self.seen_titles: dict[str, str] = {}
        self.pending: dict[str, dict[str, Any]] = {}
        self.evaluated: dict[str, str] = {}
        self.deferred: list[dict[str, Any]] = []
        self.alerted_themes: dict[str, str] = {}
        self.recent_alerts: list[dict[str, Any]] = []
        self.daily_counts: dict[str, int] = {}
        self.daily_ai_calls: dict[str, dict[str, dict[str, int]]] = {}
        self.exhausted_models: dict[str, str] = {}
        self.last_stage1_at: str | None = None
        self.feed_stats: dict[str, dict[str, Any]] = {}
        if path.exists():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                self.seen_urls = dict(raw.get("seen_urls", {}))
                self.seen_titles = dict(raw.get("seen_titles", {}))
                self.pending = {
                    str(k): dict(v)
                    for k, v in (raw.get("pending") or {}).items()
                    if isinstance(v, dict)
                }
                self.evaluated = {str(k): str(v) for k, v in (raw.get("evaluated") or {}).items()}
                self.deferred = [dict(d) for d in (raw.get("deferred") or []) if isinstance(d, dict)]
                self.alerted_themes = dict(raw.get("alerted_themes", {}))
                self.recent_alerts = [dict(a) for a in (raw.get("recent_alerts") or []) if isinstance(a, dict)]
                self.daily_counts = {str(k): int(v) for k, v in raw.get("daily_counts", {}).items()}
                raw_ai = raw.get("daily_ai_calls") or {}
                if isinstance(raw_ai, dict):
                    for day_k, models_v in raw_ai.items():
                        if isinstance(models_v, dict):
                            self.daily_ai_calls[str(day_k)] = {
                                str(m): {
                                    "requests": int(c.get("requests", 0)),
                                    "stage1": int(c.get("stage1", 0)),
                                    "stage2": int(c.get("stage2", 0)),
                                }
                                for m, c in models_v.items()
                                if isinstance(c, dict)
                            }
                self.exhausted_models = {str(k): str(v) for k, v in (raw.get("exhausted_models") or {}).items()}
                self.last_stage1_at = str(raw["last_stage1_at"]) if raw.get("last_stage1_at") else None
                self.feed_stats = {
                    str(k): dict(v)
                    for k, v in (raw.get("feed_stats") or {}).items()
                    if isinstance(v, dict)
                }
            except Exception:
                pass

    @staticmethod
    def _now_kst(now: datetime | None = None) -> datetime:
        if now is None:
            return datetime.now(KST)
        return now.replace(tzinfo=KST) if now.tzinfo is None else now.astimezone(KST)

    def is_seen(self, url: str, title: str) -> bool:
        return url in self.seen_urls or title_hash(title) in self.seen_titles

    def mark_seen(self, url: str, title: str, now: datetime | None = None) -> None:
        now_iso = self._now_kst(now).isoformat()
        if url:
            self.seen_urls[url] = now_iso
        self.seen_titles[title_hash(title)] = now_iso

    def is_evaluated(self, url: str, title: str) -> bool:
        return item_dedup_key(url, title) in self.evaluated

    def mark_evaluated(self, url: str, title: str, now: datetime | None = None) -> None:
        key = item_dedup_key(url, title)
        self.evaluated[key] = self._now_kst(now).isoformat()
        self.pending.pop(key, None)

    def add_pending(self, item: NewsItem, now: datetime | None = None) -> str:
        ref = self._now_kst(now)
        key = item_dedup_key(item.url, item.title)
        if key not in self.pending:
            data = item.to_dict()
            if not data.get("first_seen"):
                data["first_seen"] = ref.isoformat()
            pub_dt = parse_pub_date(item.published_at)
            data["pub_date"] = pub_dt.isoformat() if pub_dt else None
            self.pending[key] = data
        return key

    def get_pending_items(self) -> list[NewsItem]:
        return [NewsItem.from_dict(v) for v in self.pending.values()]

    def prune_pending_ttl(self, ttl_minutes: int, now: datetime | None = None) -> list[NewsItem]:
        ref = self._now_kst(now)
        cutoff = ref - timedelta(minutes=ttl_minutes)
        expired: list[NewsItem] = []
        kept: dict[str, dict[str, Any]] = {}
        for key, val in self.pending.items():
            fs = parse_iso_kst(val.get("first_seen"))
            if fs is not None and fs < cutoff:
                expired.append(NewsItem.from_dict(val))
            else:
                kept[key] = val
        self.pending = kept
        return expired

    def record_ai_request(self, model: str, stage: str = "other", now: datetime | None = None) -> None:
        pt_day = pt_date_str(self._now_kst(now))
        day_map = self.daily_ai_calls.setdefault(pt_day, {})
        entry = day_map.setdefault(model, {"requests": 0, "stage1": 0, "stage2": 0})
        entry["requests"] = int(entry.get("requests", 0)) + 1
        if stage in ("stage1", "stage2"):
            entry[stage] = int(entry.get(stage, 0)) + 1

    def ai_requests_today(self, model: str | None = None, now: datetime | None = None) -> int:
        pt_day = pt_date_str(self._now_kst(now))
        day_map = self.daily_ai_calls.get(pt_day) or {}
        if model is not None:
            return int((day_map.get(model) or {}).get("requests", 0))
        return sum(int(v.get("requests", 0)) for v in day_map.values())

    def mark_model_exhausted(self, model: str, now: datetime | None = None) -> None:
        self.exhausted_models[model] = pt_date_str(self._now_kst(now))

    def is_model_exhausted(self, model: str, now: datetime | None = None) -> bool:
        return self.exhausted_models.get(model) == pt_date_str(self._now_kst(now))

    def in_cooldown(self, theme_key: str, hours: int, now: datetime | None = None) -> bool:
        k = norm_key(theme_key)
        if not k or k not in self.alerted_themes:
            return False
        ts = parse_iso_kst(self.alerted_themes[k])
        if ts is None:
            return False
        return (self._now_kst(now) - ts) < timedelta(hours=hours)

    def alerts_in_rolling_window(self, minutes: int, now: datetime | None = None) -> int:
        ref = self._now_kst(now)
        cutoff = ref - timedelta(minutes=minutes)
        count = 0
        for rec in self.recent_alerts:
            ts = parse_iso_kst(rec.get("ts"))
            if ts is not None and ts >= cutoff:
                count += 1
        return count

    def mark_alerted(
        self,
        theme_key: str,
        now: datetime | None = None,
        *,
        tokens: list[str] | None = None,
        keywords: list[str] | None = None,
        cand_codes: list[str] | None = None,
        final_codes: list[str] | None = None,
        catalyst_score: float | None = None,
        title: str | None = None,
        stocks: list[dict[str, str]] | None = None,
    ) -> None:
        ref = self._now_kst(now)
        k = norm_key(theme_key)
        if k:
            self.alerted_themes[k] = ref.isoformat()
        day = ref.strftime("%Y-%m-%d")
        self.daily_counts[day] = self.daily_counts.get(day, 0) + 1
        rec: dict[str, Any] = {
            "ts": ref.isoformat(),
            "theme_key": theme_key,
            "tokens": list(tokens or []),
            "keywords": list(keywords or []),
            "cand_codes": list(cand_codes or []),
            "final_codes": list(final_codes or []),
            "catalyst_score": float(catalyst_score) if catalyst_score is not None else 0.0,
        }
        if title:
            rec["title"] = str(title)
        if stocks:
            rec["stocks"] = list(stocks)
        self.recent_alerts.append(rec)

    def sent_today(self, now: datetime | None = None) -> int:
        return self.daily_counts.get(self._now_kst(now).strftime("%Y-%m-%d"), 0)

    def save(self, now: datetime | None = None) -> None:
        ref = self._now_kst(now)
        cutoff = ref - timedelta(days=7)

        def _prune(d: dict[str, str]) -> dict[str, str]:
            out: dict[str, str] = {}
            for k, v in d.items():
                ts = parse_iso_kst(v)
                if ts is not None and ts >= cutoff:
                    out[k] = v
            return out

        self.seen_urls = _prune(self.seen_urls)
        self.seen_titles = _prune(self.seen_titles)
        self.evaluated = _prune(self.evaluated)
        self.alerted_themes = _prune(self.alerted_themes)
        self.recent_alerts = [
            a for a in self.recent_alerts
            if (ts := parse_iso_kst(a.get("ts"))) is not None and ts >= cutoff
        ]
        day_cut = (ref - timedelta(days=14)).strftime("%Y-%m-%d")
        self.daily_counts = {k: v for k, v in self.daily_counts.items() if k >= day_cut}
        pt_today = pt_date_str(ref)
        pt_cut = (ref.astimezone(PT) - timedelta(days=14)).strftime("%Y-%m-%d")
        self.daily_ai_calls = {k: v for k, v in self.daily_ai_calls.items() if k >= pt_cut}
        self.exhausted_models = {m: d for m, d in self.exhausted_models.items() if d == pt_today}

        payload = json.dumps(
            {
                "seen_urls": self.seen_urls,
                "seen_titles": self.seen_titles,
                "pending": self.pending,
                "evaluated": self.evaluated,
                "deferred": self.deferred,
                "alerted_themes": self.alerted_themes,
                "recent_alerts": self.recent_alerts,
                "daily_counts": self.daily_counts,
                "daily_ai_calls": self.daily_ai_calls,
                "exhausted_models": self.exhausted_models,
                "last_stage1_at": self.last_stage1_at,
                "feed_stats": self.feed_stats,
            },
            ensure_ascii=False,
            indent=2,
        )
        atomic_write_text(self.path, payload)


def compute_hub_stocks(
    recent_alerts: list[dict[str, Any]],
    now: datetime | None = None,
    window_days: int = 7,
) -> set[str]:
    """최근 7일 알림 중 3건 이상 반복 등장한 허브 종목코드를 데이터 기반으로 추출."""
    ref = StateStore._now_kst(now)
    cutoff = ref - timedelta(days=window_days)
    counts: Counter[str] = Counter()
    for rec in recent_alerts:
        ts = parse_iso_kst(rec.get("ts"))
        if ts is not None and ts < cutoff:
            continue
        codes = rec.get("final_codes") or rec.get("cand_codes") or []
        counts.update({str(c).strip() for c in codes if str(c).strip()})
    return {code for code, cnt in counts.items() if cnt >= HUB_STOCK_MIN_APPEARANCES}


def codes_overlap(codes_a: set[str], codes_b: set[str], hub_stocks: set[str]) -> bool:
    """허브 종목 제외 후 |A∩B| >= 2 이고 |A∩B| / min(|A|,|B|) >= 0.5 여부 판정."""
    a = {str(c).strip() for c in codes_a if str(c).strip()} - hub_stocks
    b = {str(c).strip() for c in codes_b if str(c).strip()} - hub_stocks
    if not a or not b:
        return False
    inter = len(a & b)
    return inter >= 2 and (inter / min(len(a), len(b))) >= 0.5


def evaluate_duplicate_candidate(
    *,
    theme_key: str,
    tokens: list[str],
    keywords: list[str],
    codes: list[str],
    catalyst_score: float,
    recent_alerts: list[dict[str, Any]],
    cooldown_hours: int,
    now: datetime | None = None,
    code_field: str = "cand_codes",
) -> dict[str, Any]:
    """PR-3 Stage 2 이전(및 발송 직전) (T, K, C) 다중 신호 중복 판정 + +1.0 에스컬레이션 예외."""
    ref = StateStore._now_kst(now)
    cutoff = ref - timedelta(hours=cooldown_hours)
    hubs = compute_hub_stocks(recent_alerts, now=ref)

    tnorm = norm_key(theme_key)
    t_set = {str(x).lower() for x in tokens if x}
    k_set = {str(x).lower() for x in keywords if x}
    c_set = {str(x).strip() for x in codes if x}

    best_match: dict[str, Any] | None = None
    for rec in recent_alerts:
        ts = parse_iso_kst(rec.get("ts"))
        if ts is None or ts < cutoff:
            continue
        prev_theme = str(rec.get("theme_key") or "")
        prev_tnorm = norm_key(prev_theme)
        prev_t = {str(x).lower() for x in (rec.get("tokens") or []) if x}
        prev_k = {str(x).lower() for x in (rec.get("keywords") or []) if x}
        prev_codes_raw = rec.get(code_field) or rec.get("final_codes") or rec.get("cand_codes") or []
        prev_c = {str(x).strip() for x in prev_codes_raw if x}

        tok_inter = len(t_set & prev_t)
        kw_inter = len(k_set & prev_k)
        c_ov = codes_overlap(c_set, prev_c, hubs)

        rule: str | None = None
        if tnorm and prev_tnorm and tnorm == prev_tnorm:
            rule = "theme_key_exact"
        elif tok_inter >= 3 and (kw_inter >= 2 or c_ov):
            rule = "tokens_and_kw_or_codes"
        elif kw_inter >= 3 and c_ov:
            rule = "keywords_and_codes"

        if rule is None:
            continue

        prev_score = float(rec.get("catalyst_score", 0.0) or 0.0)
        if best_match is None or prev_score > best_match["prev_score"]:
            best_match = {
                "rule": rule,
                "matched_theme": prev_theme,
                "prev_score": prev_score,
                "token_overlap": tok_inter,
                "kw_overlap": kw_inter,
                "code_overlap": c_ov,
            }

    if best_match is None:
        return {
            "is_dup": False,
            "escalated": False,
            "reason": "unique",
            "matched_theme": None,
            "prev_score": None,
            "token_overlap": 0,
            "kw_overlap": 0,
            "code_overlap": False,
            "hub_stocks": sorted(hubs),
        }

    if catalyst_score >= best_match["prev_score"] + ESCALATION_SCORE_DELTA:
        return {
            "is_dup": False,
            "escalated": True,
            "reason": f"escalation_pass:{best_match['rule']}",
            "matched_theme": best_match["matched_theme"],
            "prev_score": best_match["prev_score"],
            "token_overlap": best_match["token_overlap"],
            "kw_overlap": best_match["kw_overlap"],
            "code_overlap": best_match["code_overlap"],
            "hub_stocks": sorted(hubs),
        }

    return {
        "is_dup": True,
        "escalated": False,
        "reason": best_match["rule"],
        "matched_theme": best_match["matched_theme"],
        "prev_score": best_match["prev_score"],
        "token_overlap": best_match["token_overlap"],
        "kw_overlap": best_match["kw_overlap"],
        "code_overlap": best_match["code_overlap"],
        "hub_stocks": sorted(hubs),
    }


def evaluate_stage1_trigger(
    pending_items: list[NewsItem],
    ranked_clusters: list[EventCluster],
    state: StateStore,
    *,
    now: datetime,
    market_window: bool,
    daily_rpd_limit: int = DEFAULT_STAGE1_DAILY_RPD_LIMIT,
    screen_model: str = "gemini-3.5-flash-lite",
) -> dict[str, Any]:
    """PR-3 2계층(H/S) Stage 1 트리거 + PT 일일 RPD 70%/80%/90% 보호 가드."""
    limit = max(10, int(daily_rpd_limit))
    used = state.ai_requests_today(screen_model, now=now)
    ratio = used / limit

    if ratio >= 0.90:
        rpd_tier = "h_only"
        batch_th = 999999
        elapsed_th = 999999.0
    elif ratio >= 0.80:
        rpd_tier = "slowdown"
        batch_th = 12
        elapsed_th = 15.0 if market_window else 30.0
    elif ratio >= 0.70:
        rpd_tier = "caution"
        batch_th = 8
        elapsed_th = 10.0 if market_window else 20.0
    else:
        rpd_tier = "normal"
        batch_th = 6
        elapsed_th = 6.0 if market_window else 15.0

    last_dt = parse_iso_kst(state.last_stage1_at)
    elapsed_min = max(0.0, (now - last_dt).total_seconds() / 60.0) if last_dt is not None else None

    hard_hits: list[str] = []
    soft_hits_all: list[str] = []
    best_soft_in_cluster = 0
    has_soft_plus_multi_outlet = False
    max_non_pol_outlets = 0

    for cl in ranked_clusters[:25]:
        titles = [it.title for it in cl.items] if cl.items else [cl.headline]
        joined = " ".join(titles)
        cl_hard = [kw for kw in HARD_TRIGGER_KEYWORDS if kw in joined]
        for kw in cl_hard:
            if kw not in hard_hits:
                hard_hits.append(kw)

        is_political = any(POLITICAL_NOISE_RE.search(t) for t in titles)
        outlets = {extract_outlet(it) for it in cl.items} if cl.items else set()
        if not is_political:
            max_non_pol_outlets = max(max_non_pol_outlets, len(outlets))
            cl_soft = [kw for kw in SOFT_TRIGGER_KEYWORDS if kw in joined]
            for kw in cl_soft:
                if kw not in soft_hits_all:
                    soft_hits_all.append(kw)
            best_soft_in_cluster = max(best_soft_in_cluster, len(cl_soft))
            if cl_soft and len(outlets) >= 2:
                has_soft_plus_multi_outlet = True

    has_realtime_inbox = any(it.priority == 0 for it in pending_items)

    should_run = False
    reason = "waiting_batch_or_trigger"

    if hard_hits:
        should_run = True
        reason = "hard_keyword"
    elif rpd_tier == "h_only":
        should_run = False
        reason = "rpd_90_h_only_guard"
    elif has_realtime_inbox:
        should_run = True
        reason = "realtime_callback_inbox"
    elif best_soft_in_cluster >= 2 and rpd_tier in ("normal", "caution"):
        should_run = True
        reason = "soft_keywords_2plus"
    elif has_soft_plus_multi_outlet:
        should_run = True
        reason = "soft_plus_multi_outlet"
    elif max_non_pol_outlets >= 2 and rpd_tier in ("normal", "caution"):
        should_run = True
        reason = "multi_outlet"
    elif len(pending_items) >= batch_th:
        should_run = True
        reason = "pending_batch"
    elif elapsed_min is None or elapsed_min >= elapsed_th:
        should_run = True
        reason = "elapsed_time"

    return {
        "should_run": should_run,
        "reason": reason,
        "rpd_tier": rpd_tier,
        "rpd_used": used,
        "rpd_limit": limit,
        "rpd_ratio": round(ratio, 3),
        "hard_hits": hard_hits[:5],
        "soft_hits": soft_hits_all[:5],
        "max_outlets": max_non_pol_outlets,
        "has_realtime_inbox": has_realtime_inbox,
        "pending_count": len(pending_items),
        "elapsed_min": round(elapsed_min, 1) if elapsed_min is not None else None,
        "batch_threshold": batch_th,
        "elapsed_threshold_min": elapsed_th,
    }


def resolve_daemon_interval_sec(
    now: datetime | None = None,
    runtime_cfg: dict[str, Any] | None = None,
) -> tuple[int, str]:
    """우분투 상시 데몬의 KST 시간대별 적응형 감시 주기(초)와 구간명 반환."""
    ref = StateStore._now_kst(now)
    cfg = runtime_cfg or {}
    market_sec = max(15, int(cfg.get("daemon_interval_market_sec") or _env_int("DAEMON_INTERVAL_MARKET_SEC", DEFAULT_DAEMON_INTERVAL_MARKET_SEC)))
    ext_sec = max(30, int(cfg.get("daemon_interval_extended_sec") or _env_int("DAEMON_INTERVAL_EXTENDED_SEC", DEFAULT_DAEMON_INTERVAL_EXTENDED_SEC)))
    off_sec = max(60, int(cfg.get("daemon_interval_offhours_sec") or _env_int("DAEMON_INTERVAL_OFFHOURS_SEC", DEFAULT_DAEMON_INTERVAL_OFFHOURS_SEC)))

    if ref.weekday() < 5:
        hm = (ref.hour, ref.minute)
        if (8, 0) <= hm <= (15, 30):
            return market_sec, "정규장/장전"
        if (7, 0) <= hm < (8, 0) or (15, 30) < hm < (21, 0):
            return ext_sec, "장전초/장후"
    return off_sec, "심야/주말"


def ingest_inbox_events(data_dir: Path, now: datetime | None = None) -> list[NewsItem]:
    """텔레그램 실시간 콜백 리스너 등이 data/inbox/*.json 에 적재한 속보 이벤트를 안전하게 수거."""
    inbox_dir = data_dir / "inbox"
    if not inbox_dir.exists():
        return []
    ref = StateStore._now_kst(now)
    items: list[NewsItem] = []
    for path in sorted(inbox_dir.glob("*.json")):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                title = strip_html(str(raw.get("title") or ""))
                url = str(raw.get("url") or "").strip()
                if title and url:
                    items.append(
                        NewsItem(
                            title=title,
                            url=url,
                            source=str(raw.get("source") or "텔레그램_속보"),
                            category=str(raw.get("category") or "속보"),
                            published_at=str(raw.get("published_at") or ref.isoformat()),
                            summary=strip_html(str(raw.get("summary") or ""))[:360],
                            priority=int(raw.get("priority", 0)),
                            first_seen=str(raw.get("first_seen") or ref.isoformat()),
                        )
                    )
        except Exception as exc:
            logger.warning("인박스 이벤트 파싱 스킵 (%s): %s", path.name, exc)
        finally:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
    return items


def _fetch_single_feed(
    feed: dict[str, Any],
    *,
    prev_stat: dict[str, Any] | None,
    host_sem: threading.Semaphore,
    max_per_feed: int,
    seen_ts: str,
    use_conditional: bool = True,
) -> dict[str, Any]:
    name = str(feed["name"])
    url = str(feed["url"])
    is_google = feed.get("group") == "google" or "news.google.com" in url
    timeout = int(feed.get("timeout") or (RSS_GOOGLE_TIMEOUT if is_google else RSS_DIRECT_TIMEOUT))

    headers = {"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/xml, text/xml"}
    if use_conditional and prev_stat:
        if prev_stat.get("etag"):
            headers["If-None-Match"] = str(prev_stat["etag"])
        if prev_stat.get("last_modified"):
            headers["If-Modified-Since"] = str(prev_stat["last_modified"])

    t0 = time.monotonic()
    req = urllib.request.Request(url, headers=headers)
    try:
        with host_sem:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw_bytes = resp.read()
                status = int(getattr(resp, "status", 200) or 200)
                resp_headers = getattr(resp, "headers", None) or {}
                etag = resp_headers.get("ETag") or resp_headers.get("etag")
                last_mod = resp_headers.get("Last-Modified") or resp_headers.get("last-modified")
        root = ET.fromstring(raw_bytes)
        items: list[NewsItem] = []
        for elem in root.findall(".//item")[:max_per_feed]:
            title = strip_html(elem.findtext("title") or "")
            link = (elem.findtext("link") or "").strip()
            if title and link:
                items.append(
                    NewsItem(
                        title=title,
                        url=link,
                        source=name,
                        category=str(feed["category"]),
                        published_at=(elem.findtext("pubDate") or "").strip(),
                        summary=strip_html(elem.findtext("description") or "")[:360],
                        priority=int(feed["priority"]),
                        first_seen=seen_ts,
                    )
                )
        return {
            "name": name,
            "status": status,
            "items": items,
            "etag": etag,
            "last_modified": last_mod,
            "error": None,
            "elapsed": round(time.monotonic() - t0, 2),
            "trip_google_cb": False,
        }
    except urllib.error.HTTPError as exc:
        if exc.code == 304:
            return {
                "name": name,
                "status": 304,
                "items": [],
                "etag": (prev_stat or {}).get("etag"),
                "last_modified": (prev_stat or {}).get("last_modified"),
                "error": None,
                "elapsed": round(time.monotonic() - t0, 2),
                "trip_google_cb": False,
            }
        return {
            "name": name,
            "status": exc.code,
            "items": [],
            "etag": (prev_stat or {}).get("etag"),
            "last_modified": (prev_stat or {}).get("last_modified"),
            "error": f"HTTP {exc.code}: {exc.reason}",
            "elapsed": round(time.monotonic() - t0, 2),
            "trip_google_cb": is_google and exc.code in (429, 503),
        }
    except Exception as exc:
        return {
            "name": name,
            "status": 0,
            "items": [],
            "etag": (prev_stat or {}).get("etag"),
            "last_modified": (prev_stat or {}).get("last_modified"),
            "error": str(exc),
            "elapsed": round(time.monotonic() - t0, 2),
            "trip_google_cb": is_google,
        }


def fetch_rss_news(
    state: StateStore,
    max_per_feed: int = 25,
    now: datetime | None = None,
    feeds: list[dict[str, Any]] | None = None,
) -> list[NewsItem]:
    ref = (
        now.replace(tzinfo=KST) if (now and now.tzinfo is None)
        else (now.astimezone(KST) if now else datetime.now(KST))
    )
    seen_ts = ref.isoformat()
    target_feeds = list(feeds) if feeds is not None else RSS_FEEDS

    google_feeds = [f for f in target_feeds if f.get("group") == "google" or "news.google.com" in str(f.get("url", ""))]
    direct_feeds = [f for f in target_feeds if f not in google_feeds]

    host_sems: dict[str, threading.Semaphore] = {}
    for f in target_feeds:
        host = urllib.parse.urlparse(str(f["url"])).netloc or str(f["name"])
        if host not in host_sems:
            host_sems[host] = threading.Semaphore(RSS_MAX_PER_HOST)

    stats_snapshot = {name: dict(st) for name, st in state.feed_stats.items()}

    def _run_google_sequential() -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for idx, gf in enumerate(google_feeds):
            host = urllib.parse.urlparse(str(gf["url"])).netloc or str(gf["name"])
            res = _fetch_single_feed(
                gf,
                prev_stat=stats_snapshot.get(str(gf["name"])),
                host_sem=host_sems[host],
                max_per_feed=max_per_feed,
                seen_ts=seen_ts,
                use_conditional=True,
            )
            out.append(res)
            if res.get("trip_google_cb") and idx + 1 < len(google_feeds):
                logger.warning(
                    "구글뉴스 서킷 브레이커 발동 (%s: %s) — 남은 구글 피드 %d개 스킵",
                    gf["name"], res.get("error"), len(google_feeds) - idx - 1,
                )
                break
        return out

    results: list[dict[str, Any]] = []
    max_workers = max(1, min(8, len(direct_feeds) + (1 if google_feeds else 0)))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        direct_futs = [
            pool.submit(
                _fetch_single_feed,
                df,
                prev_stat=stats_snapshot.get(str(df["name"])),
                host_sem=host_sems[urllib.parse.urlparse(str(df["url"])).netloc or str(df["name"])],
                max_per_feed=max_per_feed,
                seen_ts=seen_ts,
                use_conditional=True,
            )
            for df in direct_feeds
        ]
        google_fut = pool.submit(_run_google_sequential) if google_feeds else None

        for fut in direct_futs:
            results.append(fut.result())
        if google_fut is not None:
            results.extend(google_fut.result())

    collected: list[NewsItem] = []
    for res in results:
        name = str(res["name"])
        st = state.feed_stats.setdefault(name, {"ok": 0, "fail": 0, "not_modified_304": 0})
        st["last_status"] = int(res.get("status", 0))
        st["last_fetched_at"] = seen_ts
        if res.get("etag"):
            st["etag"] = res["etag"]
        if res.get("last_modified"):
            st["last_modified"] = res["last_modified"]

        if res.get("status") == 304:
            st["not_modified_304"] = int(st.get("not_modified_304", 0)) + 1
            st["last_error"] = None
            continue
        if res.get("error"):
            st["fail"] = int(st.get("fail", 0)) + 1
            st["last_error"] = str(res["error"])[:160]
            logger.warning("RSS 수집 스킵 (%s): %s", name, res["error"])
            continue

        st["ok"] = int(st.get("ok", 0)) + 1
        st["last_error"] = None
        feed_items: list[NewsItem] = res.get("items") or []
        lags = []
        for it in feed_items:
            pdt = parse_pub_date(it.published_at)
            if pdt is not None:
                lags.append(max(0, int((ref - pdt).total_seconds())))
        if lags:
            st["last_min_pub_lag_sec"] = min(lags)
        collected.extend(feed_items)

    pub_cutoff = ref - timedelta(hours=MAX_PUB_AGE_HOURS)
    fresh: list[NewsItem] = []
    batch_titles: set[str] = set()
    for item in collected:
        if any(p.search(item.title) for p in NOISE_PATTERNS):
            continue
        # 느슨한 안전 상한(12시간): 파싱 성공 시에만 오래된 기사 제외 (파싱 실패 시 보존)
        pub_dt = parse_pub_date(item.published_at)
        if pub_dt is not None and pub_dt < pub_cutoff:
            continue
        if state.is_seen(item.url, item.title) or state.is_evaluated(item.url, item.title):
            continue
        nk = norm_key(item.title)
        if not nk or nk in batch_titles:
            continue
        batch_titles.add(nk)
        fresh.append(item)
    return fresh


def probe_feeds(feeds: list[dict[str, Any]] | None = None, now: datetime | None = None) -> list[dict[str, Any]]:
    """현재 실행 환경(러너 IP 포함)에서 RSS 피드별 상태코드·항목 수·최신 pubDate 점검."""
    ref = (
        now.replace(tzinfo=KST) if (now and now.tzinfo is None)
        else (now.astimezone(KST) if now else datetime.now(KST))
    )
    target_feeds = list(feeds) if feeds is not None else (RSS_FEEDS + PROBE_CANDIDATE_FEEDS)
    seen_ts = ref.isoformat()
    host_sems: dict[str, threading.Semaphore] = {}
    for f in target_feeds:
        host = urllib.parse.urlparse(str(f["url"])).netloc or str(f["name"])
        host_sems.setdefault(host, threading.Semaphore(RSS_MAX_PER_HOST))

    rows: list[dict[str, Any]] = []
    for f in target_feeds:
        host = urllib.parse.urlparse(str(f["url"])).netloc or str(f["name"])
        res = _fetch_single_feed(
            f,
            prev_stat=None,
            host_sem=host_sems[host],
            max_per_feed=25,
            seen_ts=seen_ts,
            use_conditional=False,
        )
        items: list[NewsItem] = res.get("items") or []
        latest_raw = items[0].published_at if items else ""
        latest_dt = parse_pub_date(latest_raw)
        lag_min = round((ref - latest_dt).total_seconds() / 60.0, 1) if latest_dt else None
        row = {
            "name": res["name"],
            "url": f["url"],
            "status": res["status"],
            "items": len(items),
            "elapsed": res["elapsed"],
            "latest_pub_date": latest_dt.isoformat() if latest_dt else latest_raw,
            "lag_min": lag_min,
            "error": res["error"],
            "sample_title": items[0].title[:50] if items else "",
        }
        rows.append(row)
        lag_txt = f"{row['lag_min']}m" if row["lag_min"] is not None else "N/A"
        print(
            f"{'OK ' if not row['error'] else 'ERR'} {row['name']:18s} | HTTP {row['status']:3d} | "
            f"items={row['items']:2d} | {row['elapsed']:4.2f}s | lag={lag_txt:>6s} | "
            f"pub={str(row['latest_pub_date'])[:25]} | {row['error'] or row['sample_title']}"
        )
    return rows


def cluster_news_ranked(items: list[NewsItem], now: datetime | None = None) -> list[EventCluster]:
    ref = (
        now.replace(tzinfo=KST) if (now and now.tzinfo is None)
        else (now.astimezone(KST) if now else datetime.now(KST))
    )
    clusters: list[tuple[set[str], EventCluster]] = []
    for item in sorted(items, key=lambda x: x.priority):
        toks = set(extract_normalized_tokens(item.title))
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

    def _cluster_sort_key(c: EventCluster) -> tuple[float, float]:
        newest: datetime | None = None
        for it in c.items:
            ts = parse_iso_kst(it.first_seen) or parse_pub_date(it.published_at)
            if ts is not None and (newest is None or ts > newest):
                newest = ts
        age_min = max(0.0, (ref - newest).total_seconds() / 60.0) if newest is not None else 60.0
        recency_bonus = max(0.0, 2.0 - (age_min / 30.0))
        min_prio = min((i.priority for i in c.items), default=2)
        priority_bonus = 3.0 if min_prio == 0 else (0.6 if min_prio == 1 else 0.0)
        score = len(c.items) * 1.5 + recency_bonus + priority_bonus
        newest_epoch = newest.timestamp() if newest is not None else 0.0
        return (-round(score, 4), -newest_epoch)

    ranked = [c for _, c in clusters]
    ranked.sort(key=_cluster_sort_key)
    return ranked


def cluster_news(items: list[NewsItem], max_clusters: int = 25, now: datetime | None = None) -> list[EventCluster]:
    return cluster_news_ranked(items, now=now)[:max_clusters]


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
            atomic_write_text(
                cache_path,
                json.dumps({"updated_at": datetime.now(KST).isoformat(), "companies": fetched}, ensure_ascii=False),
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


def is_daily_quota_error(err_text: str) -> bool:
    """HTTP 429 응답 본문이 분당 제한(RPM)이 아닌 일일 할당량(RPD) 소진인지 판별."""
    low = (err_text or "").lower()
    if any(k in low for k in ("perminute", "per minute", "requests_per_minute", "tokens_per_minute", "rpm")):
        return False
    if any(k in low for k in ("perday", "per day", "per_day", "daily", "rpd")):
        return True
    return "quota" in low and "second" not in low


def call_gemini(
    api_key: str,
    models: list[str],
    system_prompt: str,
    user_prompt: str,
    *,
    thinking_level: str | None = None,
    thinking_budget: int | None = None,
    timeout: int = DEFAULT_DEEP_TIMEOUT,
    state: StateStore | None = None,
    stage: str = "other",
    now: datetime | None = None,
) -> tuple[dict[str, Any], str]:
    if not api_key:
        raise GeminiAuthError("GEMINI_API_KEY가 비어 있습니다. GitHub Secrets에 등록하세요.")
    if not models:
        raise RuntimeError("호출할 Gemini 모델 목록이 비어 있습니다.")

    last_err: Exception | None = None
    attempted_any = False
    for model in models:
        if state is not None and state.is_model_exhausted(model, now=now):
            logger.info("Gemini %s 일일 할당량 소진 서킷 브레이커 활성 — 폴백 모델로 스킵합니다.", model)
            continue
        attempted_any = True
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        gen_cfg = build_gemini_generation_config(
            model,
            thinking_level=thinking_level,
            thinking_budget=thinking_budget,
        )
        for attempt in range(2):
            t0 = time.monotonic()
            if state is not None:
                state.record_ai_request(model, stage=stage, now=now)
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
                if exc.code == 429 and is_daily_quota_error(err_txt):
                    if state is not None:
                        state.mark_model_exhausted(model, now=now)
                    logger.warning("Gemini %s 일일 할당량 소진 감지 — 태평양 자정까지 폴백 체인에서 제외합니다.", model)
                    break
                if exc.code in (429, 503) and attempt == 0:
                    time.sleep(3)
                    continue
                break
            except Exception as exc:
                logger.warning("Gemini %s 오류 (%.1fs): %s", model, time.monotonic() - t0, exc)
                last_err = exc
                break
    if not attempted_any:
        raise RuntimeError("모든 Gemini 모델이 오늘(태평양 시간) 일일 할당량 소진 상태입니다.")
    raise RuntimeError(f"모든 Gemini 모델 호출 실패: {last_err}")


def format_telegram_html(
    report: dict[str, Any],
    quotes: dict[str, StockQuote],
    articles: list[NewsItem],
    model_used: str,
    *,
    delay_note: str | None = None,
    now: datetime | None = None,
) -> str:
    esc = lambda s: html.escape(str(s or ""), quote=False)
    esc_a = lambda s: html.escape(str(s or ""), quote=True)
    ref = now.astimezone(KST) if (now and now.tzinfo) else (now.replace(tzinfo=KST) if now else datetime.now(KST))
    now_str = ref.strftime("%Y-%m-%d %H:%M KST")

    lines = [
        f"🚨 <b>[이슈 레이더] {esc(report['title'])}</b>",
        f"⭐ 촉매 점수: <b>{float(report['catalyst_score']):.1f}/10</b>  |  🕒 {now_str}",
    ]
    if delay_note:
        lines.append(f"<i>{esc(delay_note)}</i>")
    lines += [
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


def format_flash_alert_html(
    cluster: EventCluster,
    cand: dict[str, Any],
    *,
    escalated: bool = False,
    now: datetime | None = None,
) -> str:
    """Stage 1 통과 즉시 3초 내 1차 속보 푸시 HTML 메시지 생성 (2보 심층 리포트 직전 발송)."""
    esc = lambda s: html.escape(str(s or ""), quote=False)
    esc_a = lambda s: html.escape(str(s or ""), quote=True)
    ref = StateStore._now_kst(now)
    score = float(cand.get("catalyst_score", 0.0) or 0.0)
    prefix = "[후속 속보 1보]" if escalated else "[속보 1보]"
    summary = str(cand.get("event_summary") or cluster.headline)
    why = str(cand.get("why_notable") or "").strip()
    kws = [str(k) for k in (cand.get("second_order_keywords") or [])[:5] if str(k).strip()]

    lines = [
        f"⚡ <b>{prefix} {esc(cluster.headline)}</b>",
        f"⭐ 촉매 점수: <b>{score:.1f}/10</b>  |  🕒 {ref.strftime('%H:%M:%S KST')}",
        "",
        f"📰 {esc(summary)}",
    ]
    if why:
        lines.append(f"💡 <b>2차 파급 포인트</b>: {esc(why)}")
    if kws:
        lines.append(f"🎯 <b>추적 키워드</b>: {esc(', '.join(kws))}")
    if cluster.items:
        first = cluster.items[0]
        lines.append(f"📎 <a href=\"{esc_a(first.url)}\">원문 보기</a> ({esc(first.source)})")
    lines += [
        "",
        "<i>🔎 KRX 관련주 검증 및 실시간 시세 심층 리포트를 10~15초 내 이어서 발송합니다...</i>",
    ]
    return "\n".join(lines)


class TelegramPartialSendError(RuntimeError):
    """텔레그램 청크 발송 중 실패 시 이미 전송된 청크 수를 함께 전달."""

    def __init__(self, message: str, sent_chunks: int = 0) -> None:
        super().__init__(message)
        self.sent_chunks = sent_chunks


def split_telegram_chunks(html_text: str, max_len: int = 3950) -> list[str]:
    chunks: list[str] = []
    cur: list[str] = []
    cur_len = 0
    for line in html_text.splitlines():
        if cur and cur_len + len(line) + 1 > max_len:
            chunks.append("\n".join(cur))
            cur, cur_len = [line], len(line)
        else:
            cur.append(line)
            cur_len += len(line) + 1
    if cur:
        chunks.append("\n".join(cur))
    return chunks


def send_telegram(token: str, chat_id: str, html_text: str, *, start_chunk: int = 0) -> int:
    if not (token and chat_id):
        raise TelegramPartialSendError("TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID가 비어 있습니다.", sent_chunks=start_chunk)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    chunks = split_telegram_chunks(html_text)
    if start_chunk >= len(chunks):
        return len(chunks)

    sent = start_chunk
    for chunk in chunks[start_chunk:]:
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
                    raise TelegramPartialSendError(f"텔레그램 발송 실패: {res}", sent_chunks=sent)
            sent += 1
        except TelegramPartialSendError:
            raise
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="ignore")
            raise TelegramPartialSendError(f"텔레그램 HTTP {exc.code} 오류: {err_body}", sent_chunks=sent) from exc
        except Exception as exc:
            raise TelegramPartialSendError(f"텔레그램 전송 오류: {exc}", sent_chunks=sent) from exc
    return sent


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


def _earliest_first_seen(cluster: EventCluster, fallback_now: datetime) -> str:
    best: datetime | None = None
    for item in cluster.items:
        ts = parse_iso_kst(item.first_seen)
        if ts is not None and (best is None or ts < best):
            best = ts
    return (best or fallback_now).isoformat()


def _format_delay_note(first_seen_iso: str, now: datetime) -> str:
    fs = parse_iso_kst(first_seen_iso) or now
    elapsed_min = max(1, int((now - fs).total_seconds() // 60))
    return f"⏱ 지연 발송 (최초 포착 {fs.strftime('%H:%M')}, {elapsed_min}분 경과)"


def _inject_delay_note_into_html(html_text: str, delay_note: str) -> str:
    tag_line = f"<i>{html.escape(delay_note, quote=False)}</i>"
    lines = html_text.splitlines()
    for idx, line in enumerate(lines):
        if "⏱ 지연 발송" in line:
            lines[idx] = tag_line
            return "\n".join(lines)
    for idx, line in enumerate(lines):
        if line.startswith("⭐ 촉매 점수:"):
            lines.insert(idx + 1, tag_line)
            return "\n".join(lines)
    return tag_line + "\n" + html_text


def _deferred_ttl_minutes(market_window: bool) -> int:
    return DEFERRED_TTL_MARKET_MIN if market_window else DEFERRED_TTL_OFFHOURS_MIN


def enqueue_deferred(
    state: StateStore,
    entry: dict[str, Any],
    trace: TraceLogger,
    cycle: int,
    now: datetime,
) -> None:
    cid = str(entry.get("cluster", {}).get("cluster_id") or "")
    tnorm = norm_key(str(entry.get("theme_key") or cid))

    kept: list[dict[str, Any]] = []
    for existing in state.deferred:
        ecid = str(existing.get("cluster", {}).get("cluster_id") or "")
        etnorm = norm_key(str(existing.get("theme_key") or ecid))
        if (cid and ecid == cid) or (tnorm and etnorm == tnorm):
            continue
        kept.append(existing)
    kept.append(entry)

    kept.sort(key=lambda d: (-float(d.get("catalyst_score", 0.0)), str(d.get("created_at", ""))))
    dropped = kept[MAX_DEFERRED_ITEMS:]
    state.deferred = kept[:MAX_DEFERRED_ITEMS]

    trace.emit({
        "ts": now.isoformat(),
        "type": "deferred_event",
        "cycle": cycle,
        "action": "enqueued",
        "kind": entry.get("kind"),
        "cluster_id": cid,
        "theme_key": entry.get("theme_key"),
        "catalyst_score": float(entry.get("catalyst_score", 0.0)),
        "retries": int(entry.get("retries", 0)),
        "reason": entry.get("reason"),
    })
    for d in dropped:
        trace.emit({
            "ts": now.isoformat(),
            "type": "deferred_event",
            "cycle": cycle,
            "action": "dropped_capacity",
            "kind": d.get("kind"),
            "cluster_id": d.get("cluster", {}).get("cluster_id"),
            "theme_key": d.get("theme_key"),
            "catalyst_score": float(d.get("catalyst_score", 0.0)),
            "retries": int(d.get("retries", 0)),
            "reason": "capacity_limit",
        })


def _build_stage2_message(
    *,
    api_key: str,
    deep_models: list[str],
    deep_thinking_level: str,
    deep_thinking_budget: int,
    deep_timeout: int,
    min_score: float,
    companies: dict[str, dict[str, str]],
    cluster: EventCluster,
    cand: dict[str, Any],
    delay_note: str | None,
    now: datetime,
    state: StateStore | None = None,
    matched_override: list[dict[str, Any]] | None = None,
    title_prefix: str = "",
) -> tuple[str, str, str, dict[str, Any]]:
    matched = (
        list(matched_override)
        if matched_override is not None
        else match_krx_stocks(
            companies,
            cand.get("second_order_keywords") or [],
            cand.get("first_order_keywords") or [],
            cand.get("negative_keywords") or [],
        )
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
        state=state,
        stage="stage2",
        now=now,
    )

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
    if title_prefix and not str(deep_raw["title"]).startswith(title_prefix):
        deep_raw["title"] = f"{title_prefix}{deep_raw['title']}"
    deep_raw.setdefault("event_summary", cand.get("event_summary", cluster.headline))

    raw_kws = list(cand.get("first_order_keywords") or []) + list(cand.get("second_order_keywords") or [])
    norm_kws = list(dict.fromkeys(tok for kw in raw_kws for tok in extract_normalized_tokens(str(kw))))
    alert_meta = {
        "tokens": top_cluster_tokens(cluster, limit=8),
        "keywords": norm_kws,
        "cand_codes": [m["code"] for m in matched],
        "final_codes": [s["code"] for s in deep_raw["stocks"]],
        "catalyst_score": float(deep_raw["catalyst_score"]),
        "title": str(deep_raw["title"]),
        "stocks": [
            {"code": s["code"], "name": s["name"], "direction": str(s.get("direction") or "")}
            for s in deep_raw["stocks"]
        ],
    }

    msg = format_telegram_html(deep_raw, quotes, cluster.items, deep_model, delay_note=delay_note, now=now)
    return msg, str(deep_raw["title"]), deep_model, alert_meta


def write_step_summary(
    *,
    started_at: datetime,
    schedule_expr: str,
    scheduled_at: datetime | None,
    jitter_sec: int | None,
    total_cycles: int,
    successful_cycles: int,
    skipped_cycles: int,
    ai_calls: int,
    sent_this_run: int,
    deferred_remaining: int,
    cycle_rows: list[dict[str, Any]],
    pending_remaining: int = 0,
    ai_requests_pt_today: int | None = None,
    exhausted_models: list[str] | None = None,
) -> None:
    summary_env = (os.getenv("GITHUB_STEP_SUMMARY") or "").strip()
    if not summary_env:
        return
    try:
        jitter_txt = (
            f"{jitter_sec}s (예정 {scheduled_at.strftime('%H:%M KST')}, cron `{schedule_expr}`)"
            if jitter_sec is not None and scheduled_at is not None
            else "수동 실행 / 해당 없음"
        )
        pt_quota_txt = f"{ai_requests_pt_today}회" if ai_requests_pt_today is not None else "-"
        exh_txt = ", ".join(exhausted_models) if exhausted_models else "없음"
        lines = [
            "## News Alpha Radar 실행 요약",
            "",
            f"- **시작 시각**: {started_at.strftime('%Y-%m-%d %H:%M:%S KST')}",
            f"- **Cron 지연**: {jitter_txt}",
            f"- **사이클**: {successful_cycles}/{total_cycles} 성공 (AI 스킵 {skipped_cycles}회, AI 호출 {ai_calls}회)",
            f"- **발송 알림 / Pending / Deferred 잔여**: {sent_this_run}건 / {pending_remaining}건 / {deferred_remaining}건",
            f"- **금일 PT 누적 AI 요청 / 소진 모델**: {pt_quota_txt} / {exh_txt}",
            "",
            "| 사이클 | 신규기사 | 클러스터 | Stage1 | Deferred 재시도 | 발송 | 비고 |",
            "|---:|---:|---:|---|---:|---:|---|",
        ]
        for r in cycle_rows:
            s1 = f"{r['stage1_model']} ({r['stage1_sec']:.1f}s)" if r.get("stage1_model") else ("스킵" if r.get("skipped_ai") else "-")
            lines.append(
                f"| {r['cycle']}/{total_cycles} | {r['fresh_count']} | {r['cluster_count']} | "
                f"{s1} | {r['deferred_retried']} | {r['alerts_sent']} | {r.get('note', '')} |"
            )
        lines.append("")
        summary_path = Path(summary_env)
        with summary_path.open("a", encoding="utf-8") as fp:
            fp.write("\n".join(lines) + "\n")
    except Exception as exc:
        logger.warning("GITHUB_STEP_SUMMARY 기록 실패: %s", exc)


def run_live(
    dry_run: bool,
    cycles: int | None = None,
    interval_sec: int | None = None,
    data_dir: Path | None = None,
    now_fn: Callable[[], datetime] | None = None,
    daemon_mode: bool = False,
    inbox_only: bool = False,
) -> None:
    def _now() -> datetime:
        dt = now_fn() if now_fn is not None else datetime.now(KST)
        return dt.replace(tzinfo=KST) if dt.tzinfo is None else dt.astimezone(KST)

    base_dir = data_dir if data_dir is not None else Path("data")
    runtime_cfg = load_runtime_config(base_dir)

    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    tg_chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    min_score = float(
        runtime_cfg["min_catalyst_score"]
        if runtime_cfg.get("min_catalyst_score") is not None
        else _env_float("MIN_CATALYST_SCORE", 7.5)
    )
    max_per_run = max(
        1,
        int(
            runtime_cfg["max_alerts_per_window"]
            if runtime_cfg.get("max_alerts_per_window") is not None
            else _env_int("MAX_ALERTS_PER_RUN", 2)
        ),
    )
    max_daily = max(
        1,
        int(
            runtime_cfg["max_daily_alerts"]
            if runtime_cfg.get("max_daily_alerts") is not None
            else _env_int("MAX_DAILY_ALERTS", 10)
        ),
    )
    cooldown_h = max(
        1,
        int(
            runtime_cfg["theme_cooldown_hours"]
            if runtime_cfg.get("theme_cooldown_hours") is not None
            else _env_int("THEME_COOLDOWN_HOURS", 12)
        ),
    )
    dedup_default = "enforce" if daemon_mode else DEFAULT_DEDUP_MODE
    trigger_default = "enforce" if daemon_mode else DEFAULT_TRIGGER_MODE
    dedup_mode = str(runtime_cfg.get("dedup_mode") or _env_str("DEDUP_MODE", dedup_default)).strip().lower()
    if dedup_mode not in ("shadow", "enforce"):
        dedup_mode = dedup_default
    trigger_mode = str(runtime_cfg.get("trigger_mode") or _env_str("TRIGGER_MODE", trigger_default)).strip().lower()
    if trigger_mode not in ("shadow", "enforce"):
        trigger_mode = trigger_default
    stage1_rpd_limit = max(
        10,
        int(
            runtime_cfg["stage1_daily_rpd_limit"]
            if runtime_cfg.get("stage1_daily_rpd_limit") is not None
            else _env_int("STAGE1_DAILY_RPD_LIMIT", DEFAULT_STAGE1_DAILY_RPD_LIMIT)
        ),
    )
    if runtime_cfg.get("instant_flash_alert") is not None:
        instant_flash_alert = bool(runtime_cfg["instant_flash_alert"])
    else:
        flash_env = (os.getenv("INSTANT_FLASH_ALERT") or "").strip().lower()
        if flash_env in ("1", "true", "yes", "on"):
            instant_flash_alert = True
        elif flash_env in ("0", "false", "no", "off"):
            instant_flash_alert = False
        else:
            instant_flash_alert = bool(daemon_mode)
    active_feeds = resolve_active_feeds(runtime_cfg, daemon_mode=daemon_mode)

    screen_models = [m.strip() for m in _env_str("GEMINI_SCREEN_MODELS", DEFAULT_SCREEN_MODELS).split(",") if m.strip()]
    deep_models = [m.strip() for m in _env_str("GEMINI_DEEP_MODELS", DEFAULT_DEEP_MODELS).split(",") if m.strip()]
    screen_thinking_level = _env_str("GEMINI_SCREEN_THINKING_LEVEL", DEFAULT_SCREEN_THINKING_LEVEL)
    deep_thinking_level = _env_str("GEMINI_DEEP_THINKING_LEVEL", DEFAULT_DEEP_THINKING_LEVEL)
    deep_thinking_budget = max(0, _env_int("GEMINI_THINKING_BUDGET", DEFAULT_THINKING_BUDGET))
    screen_timeout = max(5, _env_int("GEMINI_SCREEN_TIMEOUT", DEFAULT_SCREEN_TIMEOUT))
    deep_timeout = max(5, _env_int("GEMINI_DEEP_TIMEOUT", DEFAULT_DEEP_TIMEOUT))

    started_at = _now()
    schedule_expr = (os.getenv("GITHUB_EVENT_SCHEDULE") or "").strip()
    scheduled_at, jitter_sec = compute_cron_jitter(schedule_expr, started_at)
    total_cycles, loop_interval, market_window = resolve_loop_schedule(cycles, interval_sec, now=started_at)

    if jitter_sec is not None and scheduled_at is not None:
        logger.info(
            "Cron 지연 측정: 예정 %s → 시작 %s (지연 %ds, cron=%s)",
            scheduled_at.strftime("%H:%M KST"),
            started_at.strftime("%H:%M:%S KST"),
            jitter_sec,
            schedule_expr,
        )

    if total_cycles > 1:
        logger.info(
            "장전·장중 워커 루프 시작 (총 %d회, %d초 간격, 장중구간=%s, 시작=%s)",
            total_cycles, loop_interval, market_window, started_at.strftime("%Y-%m-%d %H:%M KST"),
        )
    else:
        logger.info(
            "단일 감시 사이클 시작 (장중구간=%s, dedup=%s, trigger=%s, 시작=%s)",
            market_window, dedup_mode, trigger_mode, started_at.strftime("%Y-%m-%d %H:%M KST"),
        )

    state = StateStore(base_dir / "state.json")
    trace = TraceLogger(base_dir / "trace.jsonl")
    trace.emit({
        "ts": started_at.isoformat(),
        "type": "run_start",
        "event_schedule": schedule_expr or None,
        "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
        "jitter_sec": jitter_sec,
        "cycles": total_cycles,
        "interval_sec": loop_interval,
        "market_window": market_window,
        "dry_run": dry_run,
        "daemon_mode": daemon_mode,
        "dedup_mode": dedup_mode,
        "trigger_mode": trigger_mode,
    })

    companies: dict[str, dict[str, str]] | None = None
    batch_themes: set[str] = set()
    sent_this_run = 0
    ai_calls = 0
    skipped_cycles = 0
    successful_cycles = 0
    failed_cycles = 0
    last_cycle_err: Exception | None = None
    cycle_rows: list[dict[str, Any]] = []

    def _burst_limit_reached(now_dt: datetime) -> bool:
        if daemon_mode:
            return state.alerts_in_rolling_window(DEFAULT_DAEMON_BURST_WINDOW_MIN, now=now_dt) >= max_per_run
        return sent_this_run >= max_per_run

    def _ensure_companies(tag: str) -> dict[str, dict[str, str]]:
        nonlocal companies
        if companies is None:
            companies = load_krx_master(base_dir / "krx_master.json", allow_network=True)
            logger.info("%s KRX 상장사 DB 로드 완료: %d개 기업", tag, len(companies))
        return companies

    try:
        for cycle in range(1, total_cycles + 1):
            cycle_start = time.monotonic()
            cycle_now = _now()
            tag = f"[사이클 {cycle}/{total_cycles}]"
            row_summary: dict[str, Any] = {
                "cycle": cycle,
                "fresh_count": 0,
                "cluster_count": 0,
                "stage1_model": None,
                "stage1_sec": 0.0,
                "deferred_retried": 0,
                "alerts_sent": 0,
                "skipped_ai": False,
                "note": "",
            }

            if state.sent_today(now=cycle_now) >= max_daily:
                logger.info("%s 오늘 최대 알림(%d건) 도달 — 무료 할당량 보호를 위해 종료합니다.", tag, max_daily)
                row_summary["note"] = "일일 한도 도달"
                cycle_rows.append(row_summary)
                break
            if _burst_limit_reached(cycle_now):
                logger.info("%s 단기 알림 한도(%d건) 도달 — 이번 사이클을 종료합니다.", tag, max_per_run)
                row_summary["note"] = "실행당 한도 도달"
                cycle_rows.append(row_summary)
                break

            try:
                # 1) 이전 사이클/실행에서 지연·실패된 deferred 항목 먼저 처리
                if state.deferred:
                    snapshot = list(state.deferred)
                    remaining_deferred: list[dict[str, Any]] = []
                    for idx, entry in enumerate(snapshot):
                        now_d = _now()
                        cid = str(entry.get("cluster", {}).get("cluster_id") or "")
                        tkey = str(entry.get("theme_key") or cid)
                        tnorm = norm_key(tkey)
                        retries = int(entry.get("retries", 0))
                        expires_at = parse_iso_kst(entry.get("expires_at"))

                        if expires_at is not None and now_d > expires_at:
                            logger.info("%s Deferred TTL 만료 폐기: %s", tag, tkey)
                            trace.emit({
                                "ts": now_d.isoformat(),
                                "type": "deferred_event",
                                "cycle": cycle,
                                "action": "expired",
                                "kind": entry.get("kind"),
                                "cluster_id": cid,
                                "theme_key": tkey,
                                "catalyst_score": float(entry.get("catalyst_score", 0.0)),
                                "retries": retries,
                                "reason": "ttl_expired",
                            })
                            continue

                        if retries >= MAX_DEFERRED_RETRIES:
                            logger.info("%s Deferred 최대 재시도(%d회) 초과 폐기: %s", tag, MAX_DEFERRED_RETRIES, tkey)
                            trace.emit({
                                "ts": now_d.isoformat(),
                                "type": "deferred_event",
                                "cycle": cycle,
                                "action": "dropped_max_retries",
                                "kind": entry.get("kind"),
                                "cluster_id": cid,
                                "theme_key": tkey,
                                "catalyst_score": float(entry.get("catalyst_score", 0.0)),
                                "retries": retries,
                                "reason": "max_retries",
                            })
                            continue

                        if tnorm in batch_themes or state.in_cooldown(tkey, cooldown_h, now=now_d):
                            logger.info("%s Deferred 중복/쿨다운 스킵: %s", tag, tkey)
                            continue

                        if _burst_limit_reached(now_d) or state.sent_today(now=now_d) >= max_daily:
                            remaining_deferred.append(entry)
                            continue

                        kind = str(entry.get("kind") or "stage2_pending")
                        sent_chunks = int(entry.get("sent_chunks", 0))
                        first_seen_iso = str(entry.get("first_seen") or entry.get("created_at") or now_d.isoformat())
                        delay_note = _format_delay_note(first_seen_iso, now_d)

                        # send_pending이 청크 0 전송 전 15분 초과 시 시세 노후화로 stage2_pending 강등
                        if kind == "send_pending" and sent_chunks == 0:
                            s2_at = parse_iso_kst(entry.get("stage2_completed_at") or entry.get("created_at"))
                            if s2_at is not None and (now_d - s2_at) > timedelta(minutes=SEND_PENDING_MAX_AGE_MIN):
                                kind = "stage2_pending"
                                entry["kind"] = "stage2_pending"
                                entry.pop("message_html", None)
                                logger.info("%s Deferred send_pending 15분 초과 → stage2_pending 강등: %s", tag, tkey)
                                trace.emit({
                                    "ts": now_d.isoformat(),
                                    "type": "deferred_event",
                                    "cycle": cycle,
                                    "action": "downgraded",
                                    "kind": "stage2_pending",
                                    "cluster_id": cid,
                                    "theme_key": tkey,
                                    "catalyst_score": float(entry.get("catalyst_score", 0.0)),
                                    "retries": retries,
                                    "reason": "send_pending_stale_15m",
                                })

                        row_summary["deferred_retried"] += 1
                        cluster = EventCluster.from_dict(entry.get("cluster") or {})
                        cand = dict(entry.get("stage1") or {})

                        try:
                            if kind == "send_pending" and entry.get("message_html"):
                                msg_to_send = str(entry["message_html"])
                                if sent_chunks == 0:
                                    msg_to_send = _inject_delay_note_into_html(msg_to_send, delay_note)
                                title_str = str(entry.get("deep_title") or cluster.headline)
                                alert_meta = dict(entry.get("alert_meta") or {})
                                if dry_run:
                                    print(msg_to_send)
                                else:
                                    send_telegram(tg_token, tg_chat, msg_to_send, start_chunk=sent_chunks)
                                    state.mark_alerted(tkey, now=now_d, **alert_meta)
                                    logger.info("%s Deferred 재발송 완료(Stage 2 생략, chunk>=%d): %s", tag, sent_chunks, title_str)
                            else:
                                comps = _ensure_companies(tag)
                                msg_to_send, title_str, deep_model, alert_meta = _build_stage2_message(
                                    api_key=api_key,
                                    deep_models=deep_models,
                                    deep_thinking_level=deep_thinking_level,
                                    deep_thinking_budget=deep_thinking_budget,
                                    deep_timeout=deep_timeout,
                                    min_score=min_score,
                                    companies=comps,
                                    cluster=cluster,
                                    cand=cand,
                                    delay_note=delay_note,
                                    now=now_d,
                                    state=state,
                                )
                                ai_calls += 1
                                logger.info("%s Deferred Stage 2 재분석 완료 (%s): %s", tag, deep_model, title_str)
                                # Stage 2 성공 후 텔레그램 전송 (실패 시 send_pending 승격)
                                try:
                                    if dry_run:
                                        print(msg_to_send)
                                    else:
                                        send_telegram(tg_token, tg_chat, msg_to_send, start_chunk=0)
                                        state.mark_alerted(tkey, now=now_d, **alert_meta)
                                        logger.info("%s Deferred 텔레그램 발송 완료: %s", tag, title_str)
                                except TelegramPartialSendError as tg_exc:
                                    new_retries = retries + 1
                                    if new_retries < MAX_DEFERRED_RETRIES:
                                        entry["kind"] = "send_pending"
                                        entry["message_html"] = msg_to_send
                                        entry["deep_title"] = title_str
                                        entry["alert_meta"] = alert_meta
                                        entry["sent_chunks"] = tg_exc.sent_chunks
                                        entry["stage2_completed_at"] = now_d.isoformat()
                                        entry["retries"] = new_retries
                                        entry["reason"] = f"send_error:{str(tg_exc)[:80]}"
                                        remaining_deferred.append(entry)
                                        action = "retried_fail"
                                    else:
                                        action = "dropped_max_retries"
                                    logger.warning("%s Deferred 텔레그램 재발송 실패(chunks=%d, retries=%d): %s", tag, tg_exc.sent_chunks, new_retries, tg_exc)
                                    trace.emit({
                                        "ts": now_d.isoformat(),
                                        "type": "deferred_event",
                                        "cycle": cycle,
                                        "action": action,
                                        "kind": "send_pending",
                                        "cluster_id": cid,
                                        "theme_key": tkey,
                                        "catalyst_score": float(entry.get("catalyst_score", 0.0)),
                                        "retries": new_retries,
                                        "reason": str(tg_exc)[:120],
                                    })
                                    continue

                            # 발송 성공 후에만 batch_themes 등록
                            batch_themes.add(tnorm)
                            sent_this_run += 1
                            row_summary["alerts_sent"] += 1
                            trace.emit({
                                "ts": now_d.isoformat(),
                                "type": "deferred_event",
                                "cycle": cycle,
                                "action": "retried_ok",
                                "kind": kind,
                                "cluster_id": cid,
                                "theme_key": tkey,
                                "catalyst_score": float(entry.get("catalyst_score", 0.0)),
                                "retries": retries,
                                "reason": "ok",
                            })
                        except GeminiAuthError:
                            state.deferred = remaining_deferred + snapshot[idx:]
                            raise
                        except TelegramPartialSendError as tg_exc:
                            new_retries = retries + 1
                            if new_retries < MAX_DEFERRED_RETRIES:
                                entry["sent_chunks"] = tg_exc.sent_chunks
                                entry["retries"] = new_retries
                                entry["reason"] = f"send_error:{str(tg_exc)[:80]}"
                                remaining_deferred.append(entry)
                                action = "retried_fail"
                            else:
                                action = "dropped_max_retries"
                            logger.warning("%s Deferred send_pending 재발송 실패(chunks=%d, retries=%d): %s", tag, tg_exc.sent_chunks, new_retries, tg_exc)
                            trace.emit({
                                "ts": now_d.isoformat(),
                                "type": "deferred_event",
                                "cycle": cycle,
                                "action": action,
                                "kind": "send_pending",
                                "cluster_id": cid,
                                "theme_key": tkey,
                                "catalyst_score": float(entry.get("catalyst_score", 0.0)),
                                "retries": new_retries,
                                "reason": str(tg_exc)[:120],
                            })
                        except Exception as exc:
                            new_retries = retries + 1
                            if new_retries < MAX_DEFERRED_RETRIES:
                                entry["retries"] = new_retries
                                entry["reason"] = f"stage2_error:{str(exc)[:80]}"
                                remaining_deferred.append(entry)
                                action = "retried_fail"
                            else:
                                action = "dropped_max_retries"
                            logger.warning("%s Deferred 재처리 실패(retries=%d): %s", tag, new_retries, exc)
                            trace.emit({
                                "ts": now_d.isoformat(),
                                "type": "deferred_event",
                                "cycle": cycle,
                                "action": action,
                                "kind": kind,
                                "cluster_id": cid,
                                "theme_key": tkey,
                                "catalyst_score": float(entry.get("catalyst_score", 0.0)),
                                "retries": new_retries,
                                "reason": str(exc)[:120],
                            })

                    state.deferred = remaining_deferred
                    if not dry_run:
                        state.save(now=_now())

                # Deferred 처리만으로 한도에 도달했으면 RSS/Stage 1 스킵
                if state.sent_today(now=_now()) >= max_daily or _burst_limit_reached(_now()):
                    successful_cycles += 1
                    row_summary["note"] = "Deferred 처리로 한도 도달"
                    cycle_rows.append(row_summary)
                    logger.info("%s Deferred 발송으로 알림 한도 도달 — 이번 사이클 RSS 수집을 생략합니다.", tag)
                    break

                # 2) 실시간 콜백 인박스(data/inbox) + RSS 수집 → seen("수집함") + pending 큐 적재
                fetch_now = _now()
                inbox_raw = ingest_inbox_events(base_dir, now=fetch_now)
                fresh: list[NewsItem] = []
                batch_seen_keys: set[str] = set()
                for item in inbox_raw:
                    if any(p.search(item.title) for p in NOISE_PATTERNS):
                        continue
                    if state.is_seen(item.url, item.title) or state.is_evaluated(item.url, item.title):
                        continue
                    nk = norm_key(item.title)
                    if not nk or nk in batch_seen_keys:
                        continue
                    batch_seen_keys.add(nk)
                    fresh.append(item)

                if not inbox_only:
                    if active_feeds is not None:
                        rss_fresh = fetch_rss_news(state, now=fetch_now, feeds=active_feeds)
                    else:
                        rss_fresh = fetch_rss_news(state, now=fetch_now)
                    for item in rss_fresh:
                        nk = norm_key(item.title)
                        if nk and nk not in batch_seen_keys:
                            batch_seen_keys.add(nk)
                            fresh.append(item)

                row_summary["fresh_count"] = len(fresh)
                for item in fresh:
                    state.mark_seen(item.url, item.title, now=fetch_now)
                    state.add_pending(item, now=fetch_now)

                pending_ttl = (
                    PENDING_TTL_MARKET_MIN if is_market_watch_window(fetch_now) else PENDING_TTL_OFFHOURS_MIN
                )
                expired_pending = state.prune_pending_ttl(pending_ttl, now=fetch_now)
                for exp_item in expired_pending:
                    fs_dt = parse_iso_kst(exp_item.first_seen) or fetch_now
                    pub_dt = parse_pub_date(exp_item.published_at)
                    pub_lag = int((fs_dt - pub_dt).total_seconds()) if pub_dt is not None else None
                    trace.emit({
                        "ts": fetch_now.isoformat(),
                        "type": "article_decision",
                        "cycle": cycle,
                        "first_seen": fs_dt.isoformat(),
                        "source": exp_item.source,
                        "pub_date_raw": exp_item.published_at,
                        "pub_date": pub_dt.isoformat() if pub_dt else None,
                        "pub_lag_sec": pub_lag,
                        "url_hash": url_hash(exp_item.url),
                        "title": exp_item.title[:160],
                        "cluster_id": None,
                        "cluster_rank": None,
                        "included_in_stage1": False,
                        "stage1_score": None,
                        "stage1_selected": False,
                        "deferred_reason": "pending_ttl_expired",
                        "alert_sent": False,
                    })

                pending_items = state.get_pending_items()
                logger.info(
                    "%s 신규 수집 기사: %d건 (pending 대기 %d건, TTL 만료 %d건)",
                    tag, len(fresh), len(pending_items), len(expired_pending),
                )

                if not pending_items:
                    if not dry_run:
                        state.save(now=fetch_now)
                    skipped_cycles += 1
                    successful_cycles += 1
                    row_summary["skipped_ai"] = True
                    row_summary["note"] = "새 기사 없음"
                    logger.info("%s 대기 기사 없음 — AI 호출 스킵 (Gemini 무료 한도 보호)", tag)
                else:
                    ranked_clusters = cluster_news_ranked(pending_items, now=fetch_now)
                    clusters = ranked_clusters[:25]
                    row_summary["cluster_count"] = len(clusters)

                    primary_screen_model = screen_models[0] if screen_models else "gemini-3.5-flash-lite"
                    trig = evaluate_stage1_trigger(
                        pending_items,
                        ranked_clusters,
                        state,
                        now=fetch_now,
                        market_window=is_market_watch_window(fetch_now),
                        daily_rpd_limit=stage1_rpd_limit,
                        screen_model=primary_screen_model,
                    )
                    execute_stage1 = bool(trig["should_run"] or trigger_mode != "enforce")
                    trace.emit({
                        "ts": fetch_now.isoformat(),
                        "type": "trigger_decision",
                        "cycle": cycle,
                        "mode": trigger_mode,
                        "executed": execute_stage1,
                        **trig,
                    })

                    if not execute_stage1:
                        if not dry_run:
                            state.save(now=fetch_now)
                        skipped_cycles += 1
                        successful_cycles += 1
                        row_summary["skipped_ai"] = True
                        row_summary["note"] = f"트리거 대기({trig['reason']})"
                        logger.info(
                            "%s Stage 1 트리거 대기 (%s, tier=%s, pending=%d건, RPD=%d/%d) — AI 호출 스킵",
                            tag, trig["reason"], trig["rpd_tier"], len(pending_items), trig["rpd_used"], trig["rpd_limit"],
                        )
                    else:
                        logger.info(
                            "%s 클러스터링 완료: %d개 이벤트 (전체 %d개 중 상위 %d개 평가, 잔여 %d개 pending 유지, trigger=%s/%s)",
                            tag, len(clusters), len(ranked_clusters), len(clusters), max(0, len(ranked_clusters) - len(clusters)),
                            trigger_mode, trig["reason"],
                        )

                        s1_t0 = time.monotonic()
                        screen_raw, screen_model = call_gemini(
                            api_key,
                            screen_models,
                            SCREENING_PROMPT,
                            json.dumps({"clusters": [c.to_prompt_dict() for c in clusters], "min_score_hint": min_score}, ensure_ascii=False),
                            thinking_level=screen_thinking_level,
                            thinking_budget=0,
                            timeout=screen_timeout,
                            state=state,
                            stage="stage1",
                            now=fetch_now,
                        )
                        s1_sec = time.monotonic() - s1_t0
                        ai_calls += 1
                        row_summary["stage1_model"] = screen_model
                        row_summary["stage1_sec"] = round(s1_sec, 2)
                        logger.info("%s Stage 1 스크리닝 완료 (%s, %.1fs)", tag, screen_model, s1_sec)

                        # Stage 1에 실제 포함된 상위 25 클러스터의 기사만 evaluated 처리 (26위 이하는 pending 유지!)
                        eval_now = _now()
                        state.last_stage1_at = eval_now.isoformat()
                        for cl in clusters:
                            for item in cl.items:
                                state.mark_evaluated(item.url, item.title, now=eval_now)
                        if not dry_run:
                            state.save(now=eval_now)

                        by_id = {c.cluster_id: c for c in clusters}
                        selected_map: dict[str, dict[str, Any]] = {}
                        for ev in screen_raw.get("selected_events", []):
                            cid = ev.get("cluster_id")
                            if cid in by_id:
                                selected_map[cid] = ev

                        candidates = [
                            e for e in selected_map.values()
                            if float(e.get("catalyst_score", 0) or 0) >= min_score
                        ]
                        candidates.sort(key=lambda e: -float(e.get("catalyst_score", 0)))

                        alerted_cids: set[str] = set()
                        deferred_reasons: dict[str, str] = {}

                        if not candidates:
                            row_summary["note"] = "촉매 기준 미달"
                            logger.info("%s 촉매 점수 %.1f 이상 핵심 이슈 없음 — 다음 주기에 재탐색합니다.", tag, min_score)
                        else:
                            comps = _ensure_companies(tag)
                            ttl_min = _deferred_ttl_minutes(is_market_watch_window(_now()))

                            for cand in candidates:
                                cand_now = _now()
                                cid = str(cand["cluster_id"])
                                cluster = by_id[cid]
                                tkey = str(cand.get("theme_key") or cid)
                                tnorm = norm_key(tkey)
                                score_val = float(cand.get("catalyst_score", min_score))
                                first_seen_iso = _earliest_first_seen(cluster, cand_now)

                                # PR-3 Stage 2 이전 (T, K, C) 다중 신호 중복 판정
                                cand_tokens = top_cluster_tokens(cluster, limit=8)
                                raw_kws = list(cand.get("first_order_keywords") or []) + list(cand.get("second_order_keywords") or [])
                                cand_keywords = list(dict.fromkeys(tok for kw in raw_kws for tok in extract_normalized_tokens(str(kw))))
                                matched = match_krx_stocks(
                                    comps,
                                    cand.get("second_order_keywords") or [],
                                    cand.get("first_order_keywords") or [],
                                    cand.get("negative_keywords") or [],
                                )
                                cand_codes = [m["code"] for m in matched]

                                dedup_pre = evaluate_duplicate_candidate(
                                    theme_key=tkey,
                                    tokens=cand_tokens,
                                    keywords=cand_keywords,
                                    codes=cand_codes,
                                    catalyst_score=score_val,
                                    recent_alerts=state.recent_alerts,
                                    cooldown_hours=cooldown_h,
                                    now=cand_now,
                                    code_field="cand_codes",
                                )
                                legacy_cooldown = tnorm in batch_themes or state.in_cooldown(tkey, cooldown_h, now=cand_now)
                                if dedup_mode == "enforce":
                                    if tnorm in batch_themes:
                                        blocked_pre = True
                                        block_reason = "cooldown_skip"
                                    elif dedup_pre["is_dup"]:
                                        blocked_pre = True
                                        block_reason = f"dedup_pre_stage2:{dedup_pre['reason']}"
                                    elif dedup_pre["escalated"]:
                                        blocked_pre = False
                                        block_reason = ""
                                    elif state.in_cooldown(tkey, cooldown_h, now=cand_now):
                                        blocked_pre = True
                                        block_reason = "cooldown_skip"
                                    else:
                                        blocked_pre = False
                                        block_reason = ""
                                else:
                                    blocked_pre = legacy_cooldown
                                    block_reason = "cooldown_skip" if legacy_cooldown else ""

                                trace.emit({
                                    "ts": cand_now.isoformat(),
                                    "type": "dedup_decision",
                                    "cycle": cycle,
                                    "phase": "pre_stage2",
                                    "mode": dedup_mode,
                                    "cluster_id": cid,
                                    "theme_key": tkey,
                                    "catalyst_score": score_val,
                                    "would_block": bool(dedup_pre["is_dup"]),
                                    "blocked": blocked_pre,
                                    **dedup_pre,
                                })

                                if blocked_pre:
                                    deferred_reasons[cid] = block_reason or "cooldown_skip"
                                    logger.info("%s 중복/쿨다운 테마 스킵(%s): %s", tag, deferred_reasons[cid], tkey)
                                    continue

                                title_prefix = "[후속] " if (dedup_mode == "enforce" and dedup_pre["escalated"]) else ""

                                # 실행당/단기/일일 알림 한도 초과 시 영구 유실 대신 deferred 큐 보관
                                if _burst_limit_reached(cand_now) or state.sent_today(now=cand_now) >= max_daily:
                                    reason = "over_daily_limit" if state.sent_today(now=cand_now) >= max_daily else "over_run_limit"
                                    deferred_reasons[cid] = reason
                                    enqueue_deferred(
                                        state,
                                        {
                                            "kind": "stage2_pending",
                                            "cluster": cluster.to_dict(),
                                            "stage1": cand,
                                            "theme_key": tkey,
                                            "catalyst_score": score_val,
                                            "first_seen": first_seen_iso,
                                            "created_at": cand_now.isoformat(),
                                            "expires_at": (cand_now + timedelta(minutes=ttl_min)).isoformat(),
                                            "retries": 0,
                                            "reason": reason,
                                        },
                                        trace,
                                        cycle,
                                        cand_now,
                                    )
                                    if not dry_run:
                                        state.save(now=cand_now)
                                    logger.info("%s 알림 한도 도달 — 후순위 후보 deferred 보관: %s", tag, tkey)
                                    continue

                                # 2단 초고속 1보 푸시: Stage 2(10~15초) 대기 전에 3초 만에 1차 속보 먼저 발송
                                if instant_flash_alert:
                                    flash_msg = format_flash_alert_html(
                                        cluster,
                                        cand,
                                        escalated=bool(dedup_mode == "enforce" and dedup_pre["escalated"]),
                                        now=cand_now,
                                    )
                                    try:
                                        if dry_run:
                                            print(flash_msg)
                                        else:
                                            send_telegram(tg_token, tg_chat, flash_msg, start_chunk=0)
                                            logger.info("%s 초고속 1보 속보 푸시 발송 완료: %s", tag, cluster.headline[:60])
                                        trace.emit({
                                            "ts": cand_now.isoformat(),
                                            "type": "flash_alert_sent",
                                            "cycle": cycle,
                                            "cluster_id": cid,
                                            "theme_key": tkey,
                                            "catalyst_score": score_val,
                                            "headline": cluster.headline[:160],
                                        })
                                    except Exception as flash_exc:
                                        logger.warning("%s 1보 속보 푸시 실패 (2보 심층 리포트 계속 진행): %s", tag, flash_exc)

                                # 후보별 개별 try/except: 한 후보 실패 시 나머지 후보 중단 및 유실 방지
                                try:
                                    msg, title_str, deep_model, alert_meta = _build_stage2_message(
                                        api_key=api_key,
                                        deep_models=deep_models,
                                        deep_thinking_level=deep_thinking_level,
                                        deep_thinking_budget=deep_thinking_budget,
                                        deep_timeout=deep_timeout,
                                        min_score=min_score,
                                        companies=comps,
                                        cluster=cluster,
                                        cand=cand,
                                        delay_note=None,
                                        now=cand_now,
                                        state=state,
                                        matched_override=matched,
                                        title_prefix=title_prefix,
                                    )
                                    ai_calls += 1
                                    logger.info("%s Stage 2 심층 분석 완료 (%s): %s", tag, deep_model, title_str)
                                except GeminiAuthError:
                                    raise
                                except Exception as exc:
                                    deferred_reasons[cid] = "stage2_error"
                                    logger.warning("%s 후보 Stage 2 실패 — deferred(stage2_pending) 보관 (%s): %s", tag, tkey, exc)
                                    enqueue_deferred(
                                        state,
                                        {
                                            "kind": "stage2_pending",
                                            "cluster": cluster.to_dict(),
                                            "stage1": cand,
                                            "theme_key": tkey,
                                            "catalyst_score": score_val,
                                            "first_seen": first_seen_iso,
                                            "created_at": cand_now.isoformat(),
                                            "expires_at": (cand_now + timedelta(minutes=ttl_min)).isoformat(),
                                            "retries": 0,
                                            "reason": f"stage2_error:{str(exc)[:80]}",
                                        },
                                        trace,
                                        cycle,
                                        cand_now,
                                    )
                                    if not dry_run:
                                        state.save(now=cand_now)
                                    continue

                                # 발송 직전 2차 중복 안전망 체크 (final_codes 기준)
                                dedup_post = evaluate_duplicate_candidate(
                                    theme_key=tkey,
                                    tokens=alert_meta.get("tokens") or cand_tokens,
                                    keywords=alert_meta.get("keywords") or cand_keywords,
                                    codes=alert_meta.get("final_codes") or [],
                                    catalyst_score=score_val,
                                    recent_alerts=state.recent_alerts,
                                    cooldown_hours=cooldown_h,
                                    now=cand_now,
                                    code_field="final_codes",
                                )
                                blocked_post = bool(dedup_mode == "enforce" and dedup_post["is_dup"])
                                trace.emit({
                                    "ts": cand_now.isoformat(),
                                    "type": "dedup_decision",
                                    "cycle": cycle,
                                    "phase": "pre_send",
                                    "mode": dedup_mode,
                                    "cluster_id": cid,
                                    "theme_key": tkey,
                                    "catalyst_score": score_val,
                                    "would_block": bool(dedup_post["is_dup"]),
                                    "blocked": blocked_post,
                                    **dedup_post,
                                })
                                if blocked_post:
                                    deferred_reasons[cid] = f"dedup_pre_send:{dedup_post['reason']}"
                                    logger.info("%s 발송 직전 2차 중복 차단(%s): %s", tag, dedup_post["reason"], tkey)
                                    continue

                                try:
                                    if dry_run:
                                        print(msg)
                                    else:
                                        send_telegram(tg_token, tg_chat, msg, start_chunk=0)
                                        state.mark_alerted(tkey, now=cand_now, **alert_meta)
                                        state.save(now=cand_now)
                                        logger.info("%s 텔레그램 알림 발송 완료: %s", tag, title_str)
                                    # 발송 성공 후에만 batch_themes 등록 (실패 시 재시도 허용)
                                    batch_themes.add(tnorm)
                                    alerted_cids.add(cid)
                                    sent_this_run += 1
                                    row_summary["alerts_sent"] += 1
                                except TelegramPartialSendError as tg_exc:
                                    deferred_reasons[cid] = "send_error"
                                    logger.warning(
                                        "%s 텔레그램 발송 실패(sent_chunks=%d) — deferred(send_pending) 보관 (%s): %s",
                                        tag, tg_exc.sent_chunks, tkey, tg_exc,
                                    )
                                    enqueue_deferred(
                                        state,
                                        {
                                            "kind": "send_pending",
                                            "cluster": cluster.to_dict(),
                                            "stage1": cand,
                                            "theme_key": tkey,
                                            "catalyst_score": score_val,
                                            "first_seen": first_seen_iso,
                                            "created_at": cand_now.isoformat(),
                                            "stage2_completed_at": cand_now.isoformat(),
                                            "expires_at": (cand_now + timedelta(minutes=ttl_min)).isoformat(),
                                            "retries": 0,
                                            "reason": f"send_error:{str(tg_exc)[:80]}",
                                            "message_html": msg,
                                            "deep_title": title_str,
                                            "alert_meta": alert_meta,
                                            "sent_chunks": tg_exc.sent_chunks,
                                        },
                                        trace,
                                        cycle,
                                        cand_now,
                                    )
                                    if not dry_run:
                                        state.save(now=cand_now)
                                    continue

                        # 기사 단위 상태 전이·판정 trace 기록
                        trace_now = _now()
                        for rank, cl in enumerate(ranked_clusters, start=1):
                            cid = cl.cluster_id
                            in_s1 = rank <= 25
                            ev = selected_map.get(cid)
                            s1_score = float(ev.get("catalyst_score", 0) or 0) if ev else None
                            s1_selected = bool(ev and (s1_score or 0.0) >= min_score)
                            d_reason = deferred_reasons.get(cid) or (None if in_s1 else "cluster_rank_overflow")
                            sent_flag = cid in alerted_cids
                            for item in cl.items:
                                fs_dt = parse_iso_kst(item.first_seen) or trace_now
                                pub_dt = parse_pub_date(item.published_at)
                                pub_lag = int((fs_dt - pub_dt).total_seconds()) if pub_dt is not None else None
                                trace.emit({
                                    "ts": trace_now.isoformat(),
                                    "type": "article_decision",
                                    "cycle": cycle,
                                    "first_seen": fs_dt.isoformat(),
                                    "source": item.source,
                                    "pub_date_raw": item.published_at,
                                    "pub_date": pub_dt.isoformat() if pub_dt else None,
                                    "pub_lag_sec": pub_lag,
                                    "url_hash": url_hash(item.url),
                                    "title": item.title[:160],
                                    "cluster_id": cid,
                                    "cluster_rank": rank,
                                    "included_in_stage1": in_s1,
                                    "stage1_score": s1_score,
                                    "stage1_selected": s1_selected,
                                    "deferred_reason": d_reason,
                                    "alert_sent": sent_flag,
                                })

                        successful_cycles += 1
            except GeminiAuthError:
                raise
            except Exception as exc:
                if total_cycles == 1:
                    raise
                failed_cycles += 1
                last_cycle_err = exc
                row_summary["note"] = f"오류: {str(exc)[:50]}"
                logger.warning("%s 일시 오류 발생 — 다음 사이클에서 재시도합니다: %s", tag, exc)

            cycle_rows.append(row_summary)
            trace.emit({
                "ts": _now().isoformat(),
                "type": "cycle_summary",
                "total_cycles": total_cycles,
                **row_summary,
                "pending_remaining": len(state.pending),
                "deferred_remaining": len(state.deferred),
            })

            if cycle < total_cycles:
                check_now = _now()
                if state.sent_today(now=check_now) >= max_daily or _burst_limit_reached(check_now):
                    logger.info("%s 알림 한도 도달 — 남은 워커 사이클을 조기 종료합니다.", tag)
                    break
                elapsed = time.monotonic() - cycle_start
                wait_sec = max(0.0, loop_interval - elapsed)
                logger.info("%s 완료 (소요 %.1fs) — %.0f초 후 다음 감시 시작", tag, elapsed, wait_sec)
                if wait_sec > 0:
                    time.sleep(wait_sec)
    finally:
        end_now = _now()
        if not dry_run:
            state.save(now=end_now)
        trace.rotate(now=end_now)
        pt_today = pt_date_str(end_now)
        active_exhausted = sorted(m for m, d in state.exhausted_models.items() if d == pt_today)
        write_step_summary(
            started_at=started_at,
            schedule_expr=schedule_expr,
            scheduled_at=scheduled_at,
            jitter_sec=jitter_sec,
            total_cycles=total_cycles,
            successful_cycles=successful_cycles,
            skipped_cycles=skipped_cycles,
            ai_calls=ai_calls,
            sent_this_run=sent_this_run,
            deferred_remaining=len(state.deferred),
            cycle_rows=cycle_rows,
            pending_remaining=len(state.pending),
            ai_requests_pt_today=state.ai_requests_today(now=end_now),
            exhausted_models=active_exhausted,
        )

    logger.info(
        "감시 종료 — 성공 %d/%d 사이클 (AI 스킵 %d회, AI 호출 %d회, 알림 %d건, pending %d건, deferred %d건)",
        successful_cycles, total_cycles, skipped_cycles, ai_calls, sent_this_run, len(state.pending), len(state.deferred),
    )
    if successful_cycles == 0 and failed_cycles > 0:
        raise RuntimeError(f"모든 감시 사이클 실패: {last_cycle_err}")


def run_daemon(
    *,
    dry_run: bool = False,
    data_dir: Path | None = None,
    now_fn: Callable[[], datetime] | None = None,
    max_iterations: int | None = None,
    stop_event: threading.Event | None = None,
) -> None:
    """우분투 24/7 상시 서버 전용 무한 감시 데몬 (시간대별 적응형 주기·실시간 설정 리로드·즉시 스캔 지원)."""
    def _now() -> datetime:
        dt = now_fn() if now_fn is not None else datetime.now(KST)
        return dt.replace(tzinfo=KST) if dt.tzinfo is None else dt.astimezone(KST)

    base_dir = data_dir if data_dir is not None else Path("data")
    base_dir.mkdir(parents=True, exist_ok=True)
    inbox_dir = base_dir / "inbox"
    inbox_dir.mkdir(parents=True, exist_ok=True)
    trigger_file = base_dir / "trigger_now"

    def _has_inbox_files() -> bool:
        try:
            return any(inbox_dir.glob("*.json"))
        except OSError:
            return False

    stop_evt = stop_event if stop_event is not None else threading.Event()
    if stop_event is None and threading.current_thread() is threading.main_thread():
        def _handle_sig(signum: int, _frame: Any) -> None:
            logger.info("종료 시그널(%d) 수신 — 현재 사이클 마무리 후 안전 종료합니다.", signum)
            stop_evt.set()
        signal.signal(signal.SIGTERM, _handle_sig)
        signal.signal(signal.SIGINT, _handle_sig)

    started_at = _now()
    logger.info("News Alpha Radar 상시 데몬 시작 (PID=%d, data_dir=%s)", os.getpid(), base_dir)

    iterations = 0
    last_error: str | None = None
    next_rss_deadline = 0.0
    run_inbox_only = False

    while not stop_evt.is_set():
        iter_now = _now()
        runtime_cfg = load_runtime_config(base_dir)
        poll_sec, window_label = resolve_daemon_interval_sec(now=iter_now, runtime_cfg=runtime_cfg)
        active_feeds = resolve_active_feeds(runtime_cfg, daemon_mode=True) or RSS_FEEDS
        paused = bool(runtime_cfg.get("paused", False))

        manual_wake = False
        if trigger_file.exists():
            try:
                trig_txt = trigger_file.read_text(encoding="utf-8", errors="ignore").strip().lower()
                trigger_file.unlink(missing_ok=True)
            except OSError:
                trig_txt = ""
            if trig_txt != "inbox":
                manual_wake = True
                run_inbox_only = False
            elif time.monotonic() < next_rss_deadline:
                run_inbox_only = True

        if paused and not manual_wake:
            write_status_snapshot(
                base_dir,
                {
                    "pid": os.getpid(),
                    "mode": "daemon",
                    "state": "paused",
                    "started_at": started_at.isoformat(),
                    "heartbeat_at": iter_now.isoformat(),
                    "last_cycle_at": None,
                    "next_cycle_at": (iter_now + timedelta(seconds=10)).isoformat(),
                    "poll_interval_sec": poll_sec,
                    "window_label": window_label,
                    "active_feeds_count": len(active_feeds),
                    "last_error": last_error,
                },
            )
            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                break
            for _ in range(10):
                if stop_evt.is_set() or trigger_file.exists():
                    break
                stop_evt.wait(timeout=1.0)
            continue

        cycle_t0 = time.monotonic()
        is_fast_inbox = bool(run_inbox_only and not manual_wake and time.monotonic() < next_rss_deadline)
        try:
            run_live(
                dry_run=dry_run,
                cycles=1,
                interval_sec=0,
                data_dir=base_dir,
                now_fn=now_fn,
                daemon_mode=True,
                inbox_only=is_fast_inbox,
            )
            last_error = None
        except GeminiAuthError as auth_exc:
            last_error = str(auth_exc)
            logger.error("데몬 치명적 API 키 오류: %s — 5분 후 재확인합니다.", auth_exc)
            poll_sec = max(poll_sec, 300)
            is_fast_inbox = False
        except Exception as exc:
            last_error = str(exc)
            logger.warning("데몬 사이클 일시 오류 (다음 주기에 자동 복구): %s", exc)

        done_now = _now()
        elapsed = time.monotonic() - cycle_t0
        now_mono = time.monotonic()
        if not is_fast_inbox or next_rss_deadline <= now_mono:
            next_rss_deadline = now_mono + max(1.0, poll_sec - elapsed)
        wait_sec = max(0.5, next_rss_deadline - now_mono)
        next_at = done_now + timedelta(seconds=wait_sec)
        write_status_snapshot(
            base_dir,
            {
                "pid": os.getpid(),
                "mode": "daemon",
                "state": "error" if last_error else "running",
                "started_at": started_at.isoformat(),
                "heartbeat_at": done_now.isoformat(),
                "last_cycle_at": done_now.isoformat(),
                "last_cycle_sec": round(elapsed, 2),
                "last_cycle_kind": "inbox_fast" if is_fast_inbox else "full_rss",
                "next_cycle_at": next_at.isoformat(),
                "poll_interval_sec": poll_sec,
                "window_label": window_label,
                "active_feeds_count": len(active_feeds),
                "last_error": last_error,
            },
        )

        iterations += 1
        if max_iterations is not None and iterations >= max_iterations:
            break

        run_inbox_only = False
        while not stop_evt.is_set() and time.monotonic() < next_rss_deadline:
            if _has_inbox_files():
                logger.info("실시간 콜백 속보 감지(data/inbox) — RSS 대기 없이 즉시 분석을 시작합니다!")
                run_inbox_only = True
                break
            if trigger_file.exists():
                logger.info("즉시 스캔 신호 감지 — 대기를 중단하고 바로 감시를 시작합니다.")
                break
            rem = next_rss_deadline - time.monotonic()
            stop_evt.wait(timeout=min(0.5, max(0.05, rem)))

    write_status_snapshot(
        base_dir,
        {
            "pid": os.getpid(),
            "mode": "daemon",
            "state": "stopped",
            "started_at": started_at.isoformat(),
            "heartbeat_at": _now().isoformat(),
            "last_error": last_error,
        },
    )
    logger.info("News Alpha Radar 상시 데몬 정상 종료.")


def main() -> int:
    load_dotenv_file()
    parser = argparse.ArgumentParser(description="News Alpha Radar (24/7 Free)")
    parser.add_argument("--mode", choices=["live", "demo", "probe", "daemon"], default="live")
    parser.add_argument("--daemon", action="store_true", help="우분투 등 상시 가동 서버용 24/7 무한 루프 모드 실행")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--probe-feeds", action="store_true", help="모든 활성+후보 RSS 피드의 상태코드·항목 수·최신 pubDate 점검")
    parser.add_argument("--cycles", type=int, default=None, help="연속 감시 횟수 강제 지정 (미지정 시 장중 6회 / 그 외 1회)")
    parser.add_argument("--interval", type=int, default=None, help="연속 감시 간격(초) 강제 지정 (기본 120초)")
    parser.add_argument("--data-dir", type=str, default=None, help="상태·트레이스 저장 디렉터리 (기본 data)")
    args = parser.parse_args()
    data_dir = Path(args.data_dir) if args.data_dir else None
    try:
        if args.probe_feeds or args.mode == "probe":
            probe_feeds()
        elif args.daemon or args.mode == "daemon":
            run_daemon(dry_run=args.dry_run, data_dir=data_dir)
        elif args.mode == "demo":
            run_demo(args.dry_run, os.getenv("TELEGRAM_BOT_TOKEN", "").strip(), os.getenv("TELEGRAM_CHAT_ID", "").strip())
        else:
            run_live(args.dry_run, cycles=args.cycles, interval_sec=args.interval, data_dir=data_dir)
        return 0
    except Exception as exc:
        logger.exception("실행 오류: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
