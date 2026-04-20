"""
Binance Futures 자동 트레이딩 봇 (v3 최종)
- Binance Square 언급량 + 24hr 상승률 기반 롱 진입
- 고정 손실 ~200 USDT 이하 손절 (수수료 버퍼 5% 포함, 보수적)
- STOP_MARKET에서 timeInForce 파라미터 제거 (Binance API 스펙 준수)
- 진입 마진 파일 저장/로드 (봇 재시작 시 손절가 복원)
- 심볼 정보 캐싱 (exchange_info 중복 호출 방지)
- 손절 후 쿨다운 2루프 적용
- Square 크롤링 실패 시 명시적 경고
- 메이저/법정화폐 티커 블랙리스트 (신호 노이즈 감소)
- 5분마다 반복 실행
"""

import json
import logging
import os
import re
import time
from datetime import datetime
from decimal import ROUND_DOWN, Decimal

import requests
from binance.error import ClientError
from binance.um_futures import UMFutures
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# 환경변수 로드
# ─────────────────────────────────────────────
load_dotenv()

API_KEY        = os.getenv("BINANCE_API_KEY", "")
API_SECRET     = os.getenv("BINANCE_API_SECRET", "")
LEVERAGE       = int(os.getenv("LEVERAGE", "10"))
FIXED_USDT     = float(os.getenv("FIXED_USDT", "0"))   # 0이면 잔고 100% 사용
STOP_LOSS_USDT = float(os.getenv("STOP_LOSS_USDT", "200"))
LOOP_INTERVAL  = int(os.getenv("LOOP_INTERVAL", "300"))  # 초 단위 (기본 5분)
COOLDOWN_LOOPS = int(os.getenv("COOLDOWN_LOOPS", "2"))   # 손절 후 대기 루프 횟수

# ─────────────────────────────────────────────
# 상수
# ─────────────────────────────────────────────

MARGIN_FILE = "last_margin.json"

# Square 언급 집계 시 무시할 메이저/법정화폐 티커
# 언급량이 많아도 밈코인 신호와 무관하며 Futures 상승률 필터에서도 걸러지지만
# generate_signal 내 Top-20 슬롯을 낭비하므로 사전 제거
TICKER_BLACKLIST = {
    # ── 스테이블코인 ──────────────────────────
    "USDT",     # Tether USD          #3
    "USDC",     # USD Coin            #6
    "USDS",     # USDS                #11
    "DAI",      # Dai                 #20
    "USDE",     # Ethena USDe         #16
    "BUSD",     # Binance USD         (상장폐지, 언급 잔존)
    "TUSD",     # TrueUSD
    "FDUSD",    # First Digital USD
    "USD1",     # World Liberty USD   #22
    "PYUSD",    # PayPal USD          #24
    "USDG",     # Global Dollar       #36
    "USDF",     # Falcon USD          #40
    "XAUT",     # Tether Gold (금 연동 #32)
    "PAXG",     # PAX Gold            #35

    # ── 시총 Top 50 메이저 코인 ──────────────
    "BTC",      # Bitcoin             #1
    "ETH",      # Ethereum            #2
    "XRP",      # XRP                 #4
    "BNB",      # BNB                 #5
    "SOL",      # Solana              #7
    "TRX",      # TRON                #8
    "DOGE",     # Dogecoin            #9
    "HYPE",     # Hyperliquid         #10
    "ADA",      # Cardano             #12
    "BCH",      # Bitcoin Cash        #13
    "LINK",     # Chainlink           #14
    "XMR",      # Monero              #15
    "CC",       # Canton Network      #17
    "XLM",      # Stellar             #18
    "ZEC",      # Zcash               #19
    "M",        # MemeCore            #21  (대형 프로젝트)
    "LTC",      # Litecoin            #23
    "AVAX",     # Avalanche           #25
    "HBAR",     # Hedera              #26
    "SUI",      # Sui Network         #27
    "SHIB",     # Shiba Inu           #28 (밈이지만 시총 대형)
    "QNT",      # Quant               #29
    "TON",      # Toncoin             #30
    "CRO",      # Cronos              #31
    "TAO",      # Bittensor           #33
    "WLFI",     # World Liberty Fi    #34
    "MNT",      # Mantle              #37
    "DOT",      # Polkadot            #38
    "UNI",      # Uniswap             #39
    "MATIC",    # Polygon (구 심볼, 언급 잔존)
    "POL",      # Polygon (신 심볼)
    "APT",      # Aptos
    "OP",       # Optimism
    "ARB",      # Arbitrum
    "NEAR",     # NEAR Protocol
    "ATOM",     # Cosmos
    "FIL",      # Filecoin
    "VET",      # VeChain
    "ETC",      # Ethereum Classic
    "ALGO",     # Algorand
    "ICP",      # Internet Computer
    "AAVE",     # Aave
    "FTM",      # Fantom / Sonic
    "S",        # Sonic (FTM 리브랜딩)
    "GRT",      # The Graph

    # ── 법정화폐 / 기타 단위 ─────────────────
    "USD",
    "KRW",
    "EUR",
    "JPY",
    "GBP",
    "CNY",
    "HKD",
    "SGD",
    "AUD",
    "CAD",
    "CHF",
    "INR",
    "BRL",
    "MXN",
    "RUB",
    "TRY",
    "AED",
}
# ─────────────────────────────────────────────
# 글로벌 상태
# ─────────────────────────────────────────────

# 심볼 정보 캐시 (exchange_info 수백 KB 중복 호출 방지)
_symbol_info_cache: dict[str, dict] = {}

# 마지막 진입 시 실제 사용 마진 (파일에서 로드 또는 진입 시 갱신)
_last_entry_margin: float = 0.0


# ─────────────────────────────────────────────
# 마진 영속성 (파일 저장/로드)
# ─────────────────────────────────────────────

def save_entry_margin(usdt: float) -> None:
    """진입 마진을 파일에 저장한다. 봇 재시작 시 복원에 사용."""
    try:
        with open(MARGIN_FILE, "w") as f:
            json.dump({"margin": usdt, "saved_at": datetime.now().isoformat()}, f)
    except Exception as e:
        # 저장 실패는 치명적이지 않으나 재시작 시 폴백 발생 가능
        logging.getLogger("MemeBot").warning(f"마진 파일 저장 실패: {e}")


def load_entry_margin() -> float:
    """파일에서 진입 마진을 로드한다. 파일 없거나 실패 시 0.0 반환."""
    try:
        with open(MARGIN_FILE) as f:
            data = json.load(f)
            return float(data["margin"])
    except Exception:
        return 0.0


# ─────────────────────────────────────────────
# 로거 설정 (콘솔 + 파일)
# ─────────────────────────────────────────────
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("MemeBot")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter("[%(asctime)s] %(levelname)s │ %(message)s", "%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    fh = logging.FileHandler("trading_log.txt", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger

logger = setup_logger()

# ─────────────────────────────────────────────
# Binance UMFutures 클라이언트 초기화
# ─────────────────────────────────────────────
client = UMFutures(key=API_KEY, secret=API_SECRET)


# ═════════════════════════════════════════════
# 1. 데이터 수집
# ═════════════════════════════════════════════

def fetch_square_mentions(page_size: int = 50, max_pages: int = 3) -> dict[str, int] | None:
    """
    Binance Square 인기 포스트를 크롤링하여 $티커 언급 횟수를 집계한다.
    비공식 내부 엔드포인트 사용 — 응답 구조가 변경될 수 있음.

    TICKER_BLACKLIST에 포함된 메이저/법정화폐 티커는 집계에서 제외한다.

    Returns:
        { "PEPE": 23, "DOGE": 17, ... } 또는 크롤링 실패 시 None
    """
    url = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "lang": "en",
    }
    mention_counts: dict[str, int] = {}
    ticker_pattern = re.compile(r'\$([A-Z]{2,10})\b')
    total_articles = 0

    for page in range(1, max_pages + 1):
        params = {"type": 1, "pageNo": page, "pageSize": page_size}
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning(f"Square 크롤링 페이지 {page} 실패: {e}")
            break

        articles = (
            data.get("data", {}).get("articles")
            or data.get("data", {}).get("list")
            or []
        )
        if not articles:
            logger.debug(f"Square 페이지 {page}: 데이터 없음, 중단")
            break

        for article in articles:
            text = (article.get("title") or "") + " " + (article.get("body") or "")
            tickers = ticker_pattern.findall(text.upper())
            for ticker in tickers:
                # 블랙리스트 티커 제외 (메이저/법정화폐)
                if ticker not in TICKER_BLACKLIST:
                    mention_counts[ticker] = mention_counts.get(ticker, 0) + 1

        total_articles += len(articles)
        logger.debug(f"Square 페이지 {page} 수집 완료 ({len(articles)}건)")
        time.sleep(0.5)

    # 크롤링 실패 감지: 아무 티커도 못 찾으면 None 반환
    if not mention_counts:
        logger.error(
            "⚠️ Square 크롤링 실패 또는 응답 구조 변경 — "
            "엔드포인트를 확인하세요. 이번 루프 신호 생성 건너뜀."
        )
        return None

    logger.info(
        f"Square 언급 집계 완료: {len(mention_counts)}개 티커 "
        f"(기사 {total_articles}건 처리, 블랙리스트 제외 후)"
    )
    return mention_counts


def fetch_top_gainers(top_n: int = 30) -> dict[str, float]:
    """
    Binance Futures 24hr 티커에서 상승률 상위 N개를 반환한다.

    Returns:
        { "PEPEUSDT": 45.2, "DOGEUSDT": 31.7, ... }
    """
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        tickers = resp.json()
    except Exception as e:
        logger.error(f"선물 티커 조회 실패: {e}")
        return {}

    usdt_pairs = [
        t for t in tickers
        if t["symbol"].endswith("USDT") and float(t["priceChangePercent"]) > 0
    ]
    usdt_pairs.sort(key=lambda x: float(x["priceChangePercent"]), reverse=True)

    result = {
        t["symbol"]: float(t["priceChangePercent"])
        for t in usdt_pairs[:top_n]
    }
    logger.info(f"선물 상승률 상위 {top_n}개 조회 완료")
    return result


# ═════════════════════════════════════════════
# 2. 신호 생성
# ═════════════════════════════════════════════

def generate_signal(
    mention_counts: dict[str, int],
    top_gainers: dict[str, float],
    top_mention_n: int = 20,
) -> str | None:
    """
    Square 언급량 Top 20 × 선물 상승률 Top 30 교차 분석.
    final_score = mention_score * 0.45 + gain_score * 0.55
    가장 높은 점수의 코인 1개 반환 (없으면 None).
    """
    if not mention_counts or not top_gainers:
        logger.warning("신호 생성 불가: 데이터 부족")
        return None

    sorted_mentions = sorted(mention_counts.items(), key=lambda x: x[1], reverse=True)
    top_mentioned = {ticker: cnt for ticker, cnt in sorted_mentions[:top_mention_n]}
    max_mention = max(top_mentioned.values()) if top_mentioned else 1

    candidates = []
    for ticker, mention_cnt in top_mentioned.items():
        symbol = ticker + "USDT"
        if symbol not in top_gainers:
            continue

        mention_score = (mention_cnt / max_mention) * 100
        gain_score    = top_gainers[symbol]
        final_score   = mention_score * 0.45 + gain_score * 0.55

        candidates.append((symbol, final_score, mention_cnt, gain_score))
        logger.debug(
            f"후보: {symbol} │ 언급={mention_cnt} │ "
            f"상승률={gain_score:.1f}% │ 점수={final_score:.2f}"
        )

    if not candidates:
        logger.info("교차 분석 결과 매수 후보 없음")
        return None

    candidates.sort(key=lambda x: x[1], reverse=True)
    best = candidates[0]
    logger.info(
        f"✅ 신호 선택: {best[0]} │ 점수={best[1]:.2f} │ "
        f"언급={best[2]} │ 상승률={best[3]:.1f}%"
    )
    return best[0]


# ═════════════════════════════════════════════
# 3. 포지션 / 잔고 조회
# ═════════════════════════════════════════════

def get_open_position() -> dict | None:
    """
    현재 열려 있는 롱 선물 포지션을 반환한다.
    포지션 수량이 0보다 큰 첫 번째 항목 반환.
    """
    try:
        positions = client.get_position_risk()
        for pos in positions:
            amt = float(pos.get("positionAmt", 0))
            if amt > 0:
                logger.info(
                    f"기존 포지션 발견: {pos['symbol']} │ "
                    f"수량={amt} │ 진입가={pos['entryPrice']}"
                )
                return pos
    except Exception as e:
        logger.error(f"포지션 조회 실패: {e}")
    return None


def get_usdt_balance() -> float:
    """Futures 지갑의 사용 가능한 USDT 잔고를 반환한다."""
    try:
        balances = client.balance()
        for b in balances:
            if b["asset"] == "USDT":
                avail = float(b["availableBalance"])
                logger.info(f"USDT 잔고: {avail:.2f}")
                return avail
    except Exception as e:
        logger.error(f"잔고 조회 실패: {e}")
    return 0.0


# ═════════════════════════════════════════════
# 4. 주문 실행
# ═════════════════════════════════════════════

def set_leverage(symbol: str, leverage: int) -> bool:
    """지정 심볼에 레버리지를 설정한다."""
    try:
        client.change_leverage(symbol=symbol, leverage=leverage)
        logger.info(f"레버리지 설정: {symbol} x{leverage}")
        return True
    except ClientError as e:
        if "leverage not changed" in str(e).lower():
            return True
        logger.error(f"레버리지 설정 실패: {e}")
        return False


def get_symbol_info(symbol: str) -> dict | None:
    """
    심볼의 거래 정보 (수량 소수점, 최소 수량 등)를 조회한다.
    캐싱 적용 — exchange_info 전체(수백 KB)를 매 루프마다 호출하지 않음.
    """
    global _symbol_info_cache

    if symbol in _symbol_info_cache:
        logger.debug(f"심볼 정보 캐시 히트: {symbol}")
        return _symbol_info_cache[symbol]

    try:
        info = client.exchange_info()
        for s in info["symbols"]:
            if s["symbol"] == symbol:
                _symbol_info_cache[symbol] = s
                logger.debug(f"심볼 정보 캐시 저장: {symbol}")
                return s
    except Exception as e:
        logger.error(f"심볼 정보 조회 실패: {e}")
    return None


def round_quantity(qty: float, symbol_info: dict) -> float:
    """LOT_SIZE 필터 기준으로 수량을 올바른 소수점으로 내림 처리한다."""
    step_size = "1"
    for f in symbol_info.get("filters", []):
        if f["filterType"] == "LOT_SIZE":
            step_size = f["stepSize"]
            break

    decimal_step = Decimal(step_size)
    rounded = Decimal(str(qty)).quantize(decimal_step, rounding=ROUND_DOWN)
    return float(rounded)


def get_price_precision(symbol_info: dict) -> int:
    """PRICE_FILTER에서 tickSize 기반 소수점 자릿수를 반환한다."""
    for f in symbol_info.get("filters", []):
        if f["filterType"] == "PRICE_FILTER":
            tick = f["tickSize"].rstrip("0")
            if "." in tick:
                return len(tick.split(".")[1])
    return 4


def retry(func, retries: int = 3, delay: float = 2.0):
    """함수를 최대 retries번 재시도한다."""
    for attempt in range(1, retries + 1):
        try:
            return func()
        except Exception as e:
            logger.warning(f"재시도 {attempt}/{retries}: {e}")
            if attempt < retries:
                time.sleep(delay)
    logger.error("최대 재시도 횟수 초과")
    return None


def calc_stop_price(entry_price: float, usdt_margin: float, leverage: int,
                    stop_loss_usdt: float, price_precision: int) -> float:
    """
    레버리지를 고려한 손절가 계산.

    공식:
        notional   = usdt_margin * leverage
        loss_ratio = stop_loss_usdt / notional
        stop_price = entry_price * (1 - loss_ratio)

    호출 시 usdt_margin으로 effective_usdt (잔고 * 0.95)를 넘기므로
    실제 손절 손실은 STOP_LOSS_USDT의 약 95% (~190 USDT) 수준이다.
    이는 의도된 보수적 설정으로, 수수료/슬리피지 초과 손절을 방지한다.
    정확히 200 USDT 손절을 원한다면 usdt_margin에 원래 잔고를 넘길 것.
    """
    notional   = usdt_margin * leverage
    loss_ratio = stop_loss_usdt / notional
    stop_price = round(entry_price * (1.0 - loss_ratio), price_precision)
    return stop_price


def place_long_with_stop(symbol: str) -> bool:
    """
    시장가 롱 진입 후 즉시 고정 손실 손절 주문을 등록한다.

    Steps:
        1. 레버리지 설정
        2. USDT 잔고 확인 + effective_usdt 계산 (수수료 버퍼 5%)
        3. 현재가 + 심볼 정보 조회
        4. 수량 계산 및 소수점 정리
        5. MARKET BUY 주문
        6. 체결가 재조회
        7. 손절가 계산 (effective_usdt 기준 — 보수적)
        8. STOP_MARKET 손절 주문 (timeInForce 없음 — Binance 스펙)
        9. 진입 마진 파일 저장
    """
    global _last_entry_margin

    # 1. 레버리지 설정
    if not set_leverage(symbol, LEVERAGE):
        return False

    # 2. 잔고 확인
    usdt = FIXED_USDT if FIXED_USDT > 0 else get_usdt_balance()
    if usdt < 5:
        logger.warning(f"잔고 부족: {usdt:.2f} USDT")
        return False

    # 수수료 및 슬리피지 버퍼 5% 적용
    # - 주문 수량은 원래 usdt 기준 (실제 명목가 최대화)
    # - 손절가 계산은 effective_usdt 기준 (손절 손실 보수적 설정 ~190 USDT)
    effective_usdt = usdt * 0.95
    logger.info(f"마진 (수수료 버퍼 적용): {usdt:.2f} → {effective_usdt:.2f} USDT (손절가 기준)")

    # 3. 현재가 조회
    try:
        price_data = client.ticker_price(symbol=symbol)
        current_price = float(price_data["price"])
    except Exception as e:
        logger.error(f"현재가 조회 실패: {e}")
        return False

    # 4. 심볼 정보 (캐시 활용)
    sym_info = get_symbol_info(symbol)
    if not sym_info:
        return False

    # 5. 수량 계산 (원래 usdt 기준으로 명목가 최대화)
    raw_qty = (usdt * LEVERAGE) / current_price
    qty = round_quantity(raw_qty, sym_info)
    if qty <= 0:
        logger.error(f"수량 계산 오류: raw={raw_qty}, rounded={qty}")
        return False

    logger.info(
        f"진입 준비: {symbol} │ 현재가={current_price} │ "
        f"사용USDT={usdt:.2f} │ 레버리지=x{LEVERAGE} │ 수량={qty}"
    )

    # 6. MARKET BUY 주문
    def do_buy():
        return client.new_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            quantity=qty,
        )

    entry_order = retry(do_buy)
    if not entry_order:
        logger.error("진입 주문 실패")
        return False

    logger.info(f"✅ 진입 완료: orderId={entry_order.get('orderId')}")

    # 7. 체결가 재조회 (슬리피지 반영)
    time.sleep(1)
    try:
        pos = get_open_position()
        entry_price = float(pos["entryPrice"]) if pos else current_price
    except Exception:
        entry_price = current_price

    # 8. 손절가 계산 (effective_usdt 기준)
    price_precision = get_price_precision(sym_info)
    stop_price = calc_stop_price(
        entry_price=entry_price,
        usdt_margin=effective_usdt,
        leverage=LEVERAGE,
        stop_loss_usdt=STOP_LOSS_USDT,
        price_precision=price_precision,
    )

    logger.info(
        f"손절 주문 등록: 진입가={entry_price} │ "
        f"손절가={stop_price} │ 예상 손실 한도≤{STOP_LOSS_USDT} USDT "
        f"(유효 마진={effective_usdt:.2f} USDT)"
    )

    # 9. STOP_MARKET 손절 주문
    # Binance Futures 스펙: STOP_MARKET + closePosition=true 조합에서
    # timeInForce 파라미터를 보내면 API 에러 발생 가능 → 파라미터 제거
    def do_stop():
        return client.new_order(
            symbol=symbol,
            side="SELL",
            type="STOP_MARKET",
            stopPrice=stop_price,
            closePosition="true",
        )

    stop_order = retry(do_stop)
    if not stop_order:
        logger.error("⚠️ 손절 주문 등록 실패! 수동 확인 필요")
        return False

    logger.info(f"✅ 손절 주문 완료: orderId={stop_order.get('orderId')} │ 손절가={stop_price}")

    # 10. 진입 마진 파일 저장 (재시작 시 복원용)
    _last_entry_margin = effective_usdt
    save_entry_margin(effective_usdt)

    return True


# ═════════════════════════════════════════════
# 5. 포지션 모니터링
# ═════════════════════════════════════════════

def monitor_position(pos: dict) -> None:
    """
    열린 포지션의 현재 손익을 로그로 출력한다.
    손절 주문이 사라진 경우 재등록을 시도한다.
    """
    symbol     = pos["symbol"]
    entry_px   = float(pos["entryPrice"])
    amt        = float(pos["positionAmt"])
    unrealized = float(pos.get("unRealizedProfit", 0))

    logger.info(
        f"📊 포지션 모니터링 │ {symbol} │ "
        f"진입가={entry_px} │ 수량={amt} │ 미실현손익={unrealized:+.2f} USDT"
    )

    try:
        open_orders = client.get_orders(symbol=symbol)
        stop_orders = [o for o in open_orders if o["type"] == "STOP_MARKET"]
        if not stop_orders:
            logger.warning(f"⚠️ {symbol} 손절 주문 없음! 재등록 시도...")
            _reregister_stop(symbol, entry_px)
        else:
            logger.info(f"손절 주문 활성: {len(stop_orders)}개")
    except Exception as e:
        logger.error(f"주문 조회 실패: {e}")


def _reregister_stop(symbol: str, entry_price: float) -> None:
    """
    손절 주문이 사라진 경우 재등록한다. (안전장치)

    우선순위:
        1. _last_entry_margin (메모리) — 정상 운영 중 재등록 시
        2. last_margin.json (파일) — 봇 재시작 후 재등록 시
        3. 현재 잔고 * 0.95 (폴백) — 파일도 없는 경우, 경고 출력
    """
    global _last_entry_margin

    sym_info = get_symbol_info(symbol)
    if not sym_info:
        return

    if _last_entry_margin > 0:
        usdt_margin = _last_entry_margin
        logger.debug(f"재등록 마진: 메모리 저장값 ({usdt_margin:.2f} USDT)")
    else:
        usdt_margin = load_entry_margin()
        if usdt_margin > 0:
            logger.debug(f"재등록 마진: 파일 저장값 ({usdt_margin:.2f} USDT)")
        else:
            usdt_margin = get_usdt_balance() * 0.95
            logger.warning(
                f"⚠️ 재등록 마진: 저장값 없음, 현재 잔고 폴백 ({usdt_margin:.2f} USDT). "
                "진입 당시 마진과 다를 수 있으므로 손절가를 수동 확인 권장."
            )

    price_precision = get_price_precision(sym_info)
    stop_price = calc_stop_price(
        entry_price=entry_price,
        usdt_margin=usdt_margin,
        leverage=LEVERAGE,
        stop_loss_usdt=STOP_LOSS_USDT,
        price_precision=price_precision,
    )

    # timeInForce 제거 (STOP_MARKET + closePosition=true 스펙 준수)
    def do_stop():
        return client.new_order(
            symbol=symbol,
            side="SELL",
            type="STOP_MARKET",
            stopPrice=stop_price,
            closePosition="true",
        )

    result = retry(do_stop)
    if result:
        logger.info(f"✅ 손절 주문 재등록 완료: {stop_price}")
    else:
        logger.error("손절 주문 재등록 실패! 수동 개입 필요")


# ═════════════════════════════════════════════
# 메인 루프
# ═════════════════════════════════════════════

def print_startup_status() -> None:
    """시작 시 잔고 및 포지션 상태를 출력한다."""
    global _last_entry_margin

    logger.info("=" * 55)
    logger.info("  Binance Meme Futures Bot (v3)")
    logger.info("=" * 55)
    logger.info(
        f"레버리지: x{LEVERAGE} │ 손절 한도: ≤{STOP_LOSS_USDT} USDT │ "
        f"주기: {LOOP_INTERVAL}초 │ 쿨다운: {COOLDOWN_LOOPS}루프"
    )

    # 파일에서 마진 복원 시도
    _last_entry_margin = load_entry_margin()
    if _last_entry_margin > 0:
        logger.info(f"저장된 진입 마진 복원: {_last_entry_margin:.2f} USDT")
    else:
        logger.info("저장된 진입 마진 없음 (신규 시작)")

    balance = get_usdt_balance()
    logger.info(f"현재 USDT 잔고: {balance:.2f}")

    pos = get_open_position()
    if pos:
        if _last_entry_margin <= 0:
            logger.warning(
                "⚠️ 재시작 감지: 기존 포지션 있으나 진입 마진 복원 실패. "
                "손절 주문 수동 확인 권장."
            )
        else:
            logger.info(
                f"재시작 감지: 기존 포지션 있음, 마진 복원 완료 ({_last_entry_margin:.2f} USDT)"
            )
        logger.info(
            f"기존 포지션: {pos['symbol']} │ "
            f"수량={pos['positionAmt']} │ 진입가={pos['entryPrice']}"
        )
    else:
        logger.info("기존 포지션: 없음")

    logger.info("=" * 55)


def main() -> None:
    """메인 루프: 5분마다 신호 생성 및 주문 실행."""
    print_startup_status()

    cooldown_remaining = 0      # 남은 쿨다운 루프 수
    prev_had_position  = False  # 직전 루프에 포지션이 있었는지

    while True:
        loop_start = datetime.now().strftime("%H:%M:%S")
        logger.info(f"\n{'─'*45}")
        logger.info(f"루프 시작: {loop_start}")

        try:
            existing_pos = get_open_position()
            currently_has_position = existing_pos is not None

            # 손절 감지: 직전엔 포지션 있었는데 지금 없으면 청산된 것
            if prev_had_position and not currently_has_position:
                logger.warning(
                    f"🔴 포지션 청산 감지 (손절 또는 TP). "
                    f"쿨다운 {COOLDOWN_LOOPS}루프 시작."
                )
                cooldown_remaining = COOLDOWN_LOOPS

            prev_had_position = currently_has_position

            if currently_has_position:
                # 포지션 있으면 모니터링만
                monitor_position(existing_pos)

            elif cooldown_remaining > 0:
                # 쿨다운 중 — 데이터 수집 포함 모든 진입 로직 건너뜀
                logger.info(
                    f"⏳ 쿨다운 중 — 남은 루프: {cooldown_remaining}. 신호 생성 건너뜀."
                )
                cooldown_remaining -= 1

            else:
                # 포지션 없고 쿨다운도 끝났으면 신호 생성
                logger.info("포지션 없음 → 신호 생성 시작")

                mentions = fetch_square_mentions()
                if mentions is None:
                    logger.warning("Square 데이터 없음. 이번 루프 건너뜀.")
                else:
                    gainers = fetch_top_gainers()
                    signal  = generate_signal(mentions, gainers)

                    if signal:
                        logger.info(f"🚀 진입 신호: {signal}")
                        success = place_long_with_stop(signal)
                        if not success:
                            logger.warning("진입 실패, 다음 루프에서 재시도")
                    else:
                        logger.info("유효한 신호 없음, 대기")

        except Exception as e:
            logger.error(f"루프 오류: {e}", exc_info=True)

        logger.info(f"다음 실행까지 {LOOP_INTERVAL}초 대기...")
        time.sleep(LOOP_INTERVAL)


if __name__ == "__main__":
    main()
