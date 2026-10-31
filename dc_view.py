#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
디시인사이드 여러 갤러리의 인기글(개념글)을 파싱해서 텔레그램으로 보내는 스크립트.

- 한 번 순회(수집 → 전송)가 끝나면 정상 종료합니다. (반복은 GitHub Actions schedule 이 담당)
- 여러 갤러리 지원 (config.json 의 galleries 에 URL 을 그대로 붙여넣기)
- 각 갤러리 N페이지(기본 3) 파싱, 중복 글은 seen.json 으로 영구 필터링
- 글 구조: 제목 + 내용  ->  사진/영상(앨범 묶음)  ->  원문 링크
- 이미지는 핫링크 차단 때문에 서버에서 내려받아 '파일로 직접 업로드' (링크 미리보기 X)
- GIF(움직이는 것)는 animation, mp4 는 video 로 전송, 깨질 만한 이미지는 자동 변환/문서 전송
- 환경변수 MAX_RUN_MINUTES 를 주면 그 시간이 지난 뒤에는 새 글을 시작하지 않고 종료합니다.
"""
import argparse
import html
import io
import json
import os
import random
import shutil
import subprocess
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
import re
import sys
import time
from urllib.parse import urlparse, parse_qs, urlencode, urljoin

import requests
from bs4 import BeautifulSoup
from PIL import Image, ImageFile

ImageFile.LOAD_TRUNCATED_IMAGES = True
Image.MAX_IMAGE_PIXELS = 200_000_000

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "new_config.json")
STATE_PATH = os.path.join(BASE_DIR, "new_seen.json")

DEFAULT_CONFIG = {
    "token": "",
    "chat_id": "",  # 비워두면 봇에게 아무 메시지나 보낸 뒤 실행 시 자동 감지
    "galleries": [
        "https://gall.dcinside.com/mgallery/board/lists/?id=stockus&exception_mode=recommend",
        "https://gall.dcinside.com/mgallery/board/lists/?id=krstock&exception_mode=recommend",
        "https://gall.dcinside.com/mgallery/board/lists/?id=tenbagger&exception_mode=recommend",
        "https://gall.dcinside.com/mgallery/board/lists/?id=nikkei&exception_mode=recommend",
    ],
    "pages": 3,
    "post_delay_min": 2,  # 글 하나 전송 완료 후 최소 대기(초)
    "post_delay_max": 5,  # 글 하나 전송 완료 후 최대 대기(초)
    "max_media_per_post": 30,
    "parallel_posts": 2,  # 전송 중에 미리 다운로드해 둘 글 개수
    "parallel_media": 4,  # 글 하나 안에서 동시에 받을 미디어 개수
    "gif_to_mp4": True,  # ffmpeg 가 있으면 GIF 를 mp4 로 바꿔 앨범(한 메시지)에 포함
    "max_text_chars": 3500,  # 본문 최대 길이(넘으면 잘라서 '…' 표시)
    "disable_notification": False,
}

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

TG_PHOTO_MAX = 10 * 1024 * 1024  # sendPhoto 10MB
TG_FILE_MAX = 50 * 1024 * 1024  # 봇 API 업로드 50MB
MEDIA_FETCH_MAX = 120 * 1024 * 1024
POST_BUILD_TIMEOUT = 300  # 글 하나 다운로드/변환 최대 대기(초)


def log(*a):
    print(time.strftime("[%H:%M:%S]"), *a, flush=True)


# ──────────────────────────── 설정 / 상태 ────────────────────────────
def load_config():
    if not os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        print(f"config.json 을 만들었습니다. token 을 입력하고 다시 실행하세요: {CONFIG_PATH}")
        sys.exit(0)
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = json.load(f)
    for k, v in DEFAULT_CONFIG.items():
        cfg.setdefault(k, v)
    if os.environ.get("TELEGRAM_TOKEN"):
        cfg["token"] = os.environ["TELEGRAM_TOKEN"]
    if os.environ.get("TELEGRAM_CHAT_ID"):
        cfg["chat_id"] = os.environ["TELEGRAM_CHAT_ID"]
    return cfg


def save_config(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def load_state():
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, encoding="utf-8") as f:
                st = json.load(f)
            st.setdefault("seen", {})
            st.setdefault("fail", {})
            return st
        except Exception:
            pass
    return {"seen": {}, "fail": {}}


def save_state(st):
    # 너무 커지지 않게 최근 20000개만 유지
    if len(st["seen"]) > 20000:
        items = sorted(st["seen"].items(), key=lambda kv: kv[1])[-20000:]
        st["seen"] = dict(items)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False)
    os.replace(tmp, STATE_PATH)


# ──────────────────────────── 텔레그램 ────────────────────────────
class TGError(Exception):
    def __init__(self, code, desc):
        super().__init__(f"{code}: {desc}")
        self.code, self.desc = code, desc


class Telegram:
    def __init__(self, token, chat_id, silent=False):
        self.api = f"https://api.telegram.org/bot{token}"
        self.chat_id = chat_id
        self.silent = silent
        self.s = requests.Session()

    def call(self, method, data=None, files=None, retries=6):
        data = dict(data or {})
        if method.startswith("send"):
            data.setdefault("chat_id", self.chat_id)
            if self.silent:
                data.setdefault("disable_notification", "true")
        last = None
        for i in range(retries):
            try:
                if files:
                    size = sum(len(v[1]) for v in files.values()) / 1024
                    log(f"  텔레그램 업로드: {method} ({size:.0f}KB)")
                r = self.s.post(f"{self.api}/{method}", data=data, files=files, timeout=180)
            except requests.RequestException as e:
                log(f"  텔레그램 통신 오류({e}) 재시도 {i + 1}/{retries}")
                last = TGError(0, str(e))
                time.sleep(2 + i * 2)
                continue
            try:
                j = r.json()
            except Exception:
                j = {}
            if j.get("ok"):
                time.sleep(1.2)  # 채팅당 전송 속도 제한 완화
                return j["result"]
            desc = j.get("description", r.text[:200])
            if r.status_code == 429:
                wait = j.get("parameters", {}).get("retry_after", 5)
                log(f"텔레그램 속도제한, {wait + 1}초 대기")
                time.sleep(wait + 1)
                last = TGError(429, desc)
                continue
            if r.status_code >= 500:
                time.sleep(3 + i * 3)
                last = TGError(r.status_code, desc)
                continue
            raise TGError(r.status_code, desc)
        raise last or TGError(0, "unknown")

    # --- 텍스트 ---
    def send_text(self, text, preview=False):
        for chunk in split_text(text, 4000):
            self.call(
                "sendMessage",
                {
                    "text": chunk,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": "false" if preview else "true",
                },
            )

    # --- 개별 미디어 ---
    def send_one(self, m, caption=None):
        kind = m["kind"]
        data = {}
        if kind == "video":
            data["supports_streaming"] = "true"
            if m.get("w") and m.get("h"):
                data.update(width=str(m["w"]), height=str(m["h"]), duration=str(m.get("dur") or 0))
        if kind == "document":
            data["disable_content_type_detection"] = "true"
        if caption:
            data["caption"] = caption
            data["parse_mode"] = "HTML"
        files = {kind: (m["name"], m["bytes"], m["mime"])}
        self.call("send" + kind.capitalize(), data, files)

    # --- 앨범 (캡션은 첫 장에 붙음 → 한 덩어리 메시지로 보임) ---
    def send_group(self, items, caption=None):
        if len(items) == 1:
            return self.send_one_safe(items[0], caption)
        media, files = [], {}
        for i, m in enumerate(items):
            key = f"f{i}"
            entry = {"type": m["kind"], "media": f"attach://{key}"}
            if m["kind"] == "video":
                entry["supports_streaming"] = True
                if m.get("w") and m.get("h"):
                    entry.update(width=m["w"], height=m["h"], duration=m.get("dur") or 0)
            if i == 0 and caption:
                entry["caption"] = caption
                entry["parse_mode"] = "HTML"
            media.append(entry)
            files[key] = (m["name"], m["bytes"], m["mime"])
        try:
            self.call("sendMediaGroup", {"media": json.dumps(media)}, files)
        except TGError as e:
            log(f"앨범 전송 실패({e}) → 개별 전송으로 전환")
            for i, m in enumerate(items):
                self.send_one_safe(m, caption if i == 0 else None)

    # 개별 전송: 실패 시 문서로 → 그래도 실패면 원본 URL 텍스트
    def send_one_safe(self, m, caption=None):
        try:
            self.send_one(m, caption)
            return
        except TGError as e:
            log(f"{m['kind']} 전송 실패({e})")
        if m["kind"] != "document":
            try:
                self.send_one(dict(m, kind="document"), caption)
                return
            except TGError as e:
                log(f"문서 전송도 실패({e})")
        if caption:
            self.send_text(caption)
        self.send_text(f"⚠️ 미디어 전송 실패: {html.escape(m['url'])}")


def split_text(text, limit):
    if len(text) <= limit:
        return [text]
    out, cur = [], ""
    for line in text.split("\n"):
        while len(line) > limit:
            if cur:
                out.append(cur)
                cur = ""
            out.append(line[:limit])
            line = line[limit:]
        if len(cur) + len(line) + 1 > limit:
            out.append(cur)
            cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur:
        out.append(cur)
    return out


def detect_chat_id(cfg):
    r = requests.get(f"https://api.telegram.org/bot{cfg['token']}/getUpdates", timeout=30)
    j = r.json()
    if not j.get("ok"):
        raise SystemExit(f"토큰 오류: {j.get('description')}")
    chats = []
    for u in j["result"]:
        for k in ("message", "channel_post", "my_chat_member", "edited_message"):
            if k in u and "chat" in u[k]:
                chats.append(u[k]["chat"])
    if not chats:
        raise SystemExit(
            "chat_id 를 찾지 못했습니다. 텔레그램에서 봇에게 아무 메시지(/start)를 보내거나 "
            "그룹에 봇을 초대하고 메시지를 보낸 뒤 다시 실행하세요."
        )
    chat = chats[-1]
    cfg["chat_id"] = str(chat["id"])
    save_config(cfg)
    log(f"chat_id 자동 감지 완료: {cfg['chat_id']} ({chat.get('title') or chat.get('first_name')})")


# ──────────────────────────── 디시 파싱 ────────────────────────────
class DC:
    """스레드마다 별도 세션을 써서 병렬 다운로드에서도 안전."""

    def __init__(self):
        self.local = threading.local()

    @property
    def s(self):
        if not hasattr(self.local, "s"):
            sess = requests.Session()
            sess.headers.update(
                {
                    "User-Agent": UA,
                    "Accept-Language": "ko-KR,ko;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                }
            )
            self.local.s = sess
        return self.local.s

    def get(self, url, referer=None, stream=False, retries=3):
        h = {"Referer": referer or "https://gall.dcinside.com/"}
        last = None
        for i in range(retries):
            try:
                # (연결 timeout, 읽기 timeout): 응답이 멈춰도 무한 대기하지 않게
                r = self.s.get(url, headers=h, timeout=(10, 30), stream=stream)
                if r.status_code == 200:
                    return r
                last = Exception(f"HTTP {r.status_code}")
            except requests.RequestException as e:
                last = e
            time.sleep(1.5 * (i + 1))
        raise last

    def html(self, url, referer=None):
        r = self.get(url, referer)
        r.encoding = r.encoding or "utf-8"
        return BeautifulSoup(r.text, "html.parser")


def parse_gallery_url(url):
    """사용자가 붙여넣은 목록 URL -> (base, gall_id, 유지할 쿼리)"""
    u = urlparse(url.strip())
    q = parse_qs(u.query)
    gid = (q.get("id") or [""])[0]
    if not gid:
        raise ValueError(f"URL 에 id= 가 없습니다: {url}")
    path = u.path
    if "/mgallery/" in path:
        base = "mgallery/board"
    elif "/mini/" in path:
        base = "mini/board"
    elif "/person/" in path:
        base = "person/board"
    else:
        base = "board"
    keep = {k: v[0] for k, v in q.items() if k not in ("id", "page", "no")}
    keep.setdefault("exception_mode", "recommend")
    return base, gid, keep


def list_url(base, gid, keep, page):
    params = {"id": gid, **keep, "page": page}
    return f"https://gall.dcinside.com/{base}/lists/?{urlencode(params)}"


def view_url(base, gid, no):
    return f"https://gall.dcinside.com/{base}/view/?id={gid}&no={no}"


def parse_list(soup):
    name = ""
    t = soup.find("title")
    if t:
        name = re.sub(r"\s*-\s*커뮤니티 포털.*$", "", t.get_text(strip=True))
    posts = []
    for tr in soup.select("tr.ub-content"):
        no = tr.get("data-no")
        if not no or not no.isdigit():
            continue  # 설문 등
        if tr.get("data-type") == "icon_notice":
            continue  # 공지
        a = tr.select_one("td.gall_tit a[href*='no=']")
        title = a.get_text(strip=True) if a else ""
        posts.append({"no": no, "title": title})
    return name, posts


def abs_url(u, base="https://gall.dcinside.com/"):
    u = (u or "").strip()
    if not u or u.startswith(("data:", "javascript:", "#")):
        return ""
    if u.startswith("//"):
        return "https:" + u
    return urljoin(base, u)


def parse_view(soup):
    title_el = soup.select_one("span.title_subject") or soup.select_one(".gallview_head .title")
    title = title_el.get_text(strip=True) if title_el else ""
    body = soup.select_one("div.write_div") or soup.select_one(".gallview_contents")
    if not body:
        return title, "", [], []

    # 텍스트 (미디어/스크립트 제거 후)
    work = BeautifulSoup(str(body), "html.parser")
    for t in work.find_all(["script", "style", "img", "video", "iframe", "noscript"]):
        t.decompose()
    for br in work.find_all("br"):
        br.replace_with("\n")
    for blk in work.find_all(["p", "div", "li", "tr"]):
        blk.append("\n")
    text = work.get_text()
    text = text.replace("\xa0", " ")
    text = re.sub(r"-\s*dc official App", "", text, flags=re.I)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()

    # 미디어 (문서 순서대로)
    media, ext_links, seen = [], [], set()
    for el in body.find_all(["img", "video", "iframe"]):
        if el.name == "img":
            src = ""
            for attr in ("data-original", "data-src", "src"):
                src = abs_url(el.get(attr))
                if src:
                    break
            if not src or "dccon.php" in src or "/dccon/" in src:  # 디시콘 제외
                continue
            if src not in seen:
                seen.add(src)
                media.append({"type": "img", "url": src})
        elif el.name == "video":
            src = ""
            for cand in (el.get("data-src"), el.get("src")):
                src = abs_url(cand)
                if src:
                    break
            if not src:
                s = el.find("source")
                src = abs_url(s.get("src")) if s else ""
            if src and src not in seen:
                seen.add(src)
                media.append({"type": "video", "url": src})
        else:  # iframe: 유튜브 등 외부 영상은 업로드 불가 → 링크로 안내
            src = abs_url(el.get("src") or el.get("data-src"))
            if src and src not in seen:
                seen.add(src)
                ext_links.append(src)
    return title, text, media, ext_links


# ──────────────────────────── 미디어 준비 ────────────────────────────
def fetch_bytes(dc, url, referer, attempts=3):
    """다운로드 후 Content-Length 와 실제 크기를 비교. 잘렸으면 재시도(잘린 GIF/영상이 '깨짐'의 주원인)."""
    last = None
    for n in range(1, attempts + 1):
        t0 = time.time()
        try:
            r = dc.get(url, referer=referer, stream=True)
            clen = r.headers.get("Content-Length")
            enc = r.headers.get("Content-Encoding")
            log(f"  다운로드 시작: {url[:90]} ({r.headers.get('Content-Type', '?')}, {clen or '?'} bytes)")
            buf, total = io.BytesIO(), 0
            for chunk in r.iter_content(256 * 1024):
                total += len(chunk)
                if total > MEDIA_FETCH_MAX:
                    raise ValueError("파일이 너무 큼")
                if time.time() - t0 > 120:
                    raise TimeoutError("다운로드 120초 초과")
                buf.write(chunk)
            if clen and clen.isdigit() and not enc and total != int(clen):
                raise IOError(f"불완전 다운로드({total}/{clen} bytes)")
            log(f"  다운로드 완료: {total / 1024:.0f}KB, {time.time() - t0:.1f}초")
            return buf.getvalue(), r.headers.get("Content-Type", "")
        except Exception as e:
            last = e
            log(f"  다운로드 재시도 {n}/{attempts}: {e}")
            time.sleep(1.5 * n)
    raise last


def sniff(data):
    if data[:3] == b"\xff\xd8\xff":
        return "jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp"
    if data[4:8] == b"ftyp":
        return "mp4"
    if data[:4] == b"\x1a\x45\xdf\xa3":
        return "webm"
    return ""


def to_jpeg(im, max_side=None, quality=90):
    if max_side and max(im.size) > max_side:
        im = im.copy()
        im.thumbnail((max_side, max_side), Image.LANCZOS)
    if im.mode in ("RGBA", "LA", "P"):
        im = im.convert("RGBA")
        bg = Image.new("RGB", im.size, (255, 255, 255))
        bg.paste(im, mask=im.split()[-1])
        im = bg
    elif im.mode != "RGB":
        im = im.convert("RGB")
    out = io.BytesIO()
    im.save(out, "JPEG", quality=quality, optimize=True)
    return out.getvalue()


def _find_ffmpeg():
    exe = shutil.which("ffmpeg")
    if exe:
        return exe
    try:  # pip install imageio-ffmpeg 하면 ffmpeg 바이너리가 같이 설치됨(윈도우 추천)
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return None


FFMPEG = _find_ffmpeg()
GIF_TO_MP4 = True


def gif_fully_decodes(data):
    """모든 프레임이 끝까지 디코딩되는지 확인(잘린 GIF 감지). Pillow 는 기본이 '잘려도 조용히 통과'라 따로 검사."""
    old = ImageFile.LOAD_TRUNCATED_IMAGES
    ImageFile.LOAD_TRUNCATED_IMAGES = False
    try:
        im = Image.open(io.BytesIO(data))
        for i in range(getattr(im, "n_frames", 1)):
            im.seek(i)
            im.load()
        return True
    except Exception:
        return False
    finally:
        ImageFile.LOAD_TRUNCATED_IMAGES = old


def probe_media(data):
    """ffmpeg -i 출력 파싱: 코덱/픽셀포맷/해상도/길이/오디오 코덱/faststart 여부."""
    info = {"vcodec": None, "pix": None, "w": 0, "h": 0, "dur": 0, "acodec": None, "faststart": True}
    # faststart: 최상위 박스 순서에서 moov 가 mdat 보다 앞이어야 스트리밍/미리보기 정상
    try:
        pos, seen_mdat = 0, False
        while pos + 8 <= len(data):
            size = int.from_bytes(data[pos:pos + 4], "big")
            typ = data[pos + 4:pos + 8]
            if typ == b"mdat":
                seen_mdat = True
            if typ == b"moov":
                info["faststart"] = not seen_mdat
                break
            if size == 1:
                size = int.from_bytes(data[pos + 8:pos + 16], "big")
            if size < 8:
                break
            pos += size
    except Exception:
        pass
    if not FFMPEG:
        return info
    try:
        with tempfile.TemporaryDirectory() as td:
            src = os.path.join(td, "in.bin")
            with open(src, "wb") as f:
                f.write(data)
            r = subprocess.run([FFMPEG, "-hide_banner", "-i", src], capture_output=True, timeout=30)
        err = r.stderr.decode("utf-8", "ignore")
        m = re.search(r"Video: (\w+).*?, (\w+)(?:\([^)]*\))?[,\s].*?(\d{1,5})x(\d{1,5})", err)
        if m:
            info.update(vcodec=m.group(1), pix=m.group(2), w=int(m.group(3)), h=int(m.group(4)))
        m = re.search(r"Audio: (\w+)", err)
        if m:
            info["acodec"] = m.group(1)
        m = re.search(r"Duration: (\d+):(\d+):(\d+(?:\.\d+)?)", err)
        if m:
            secs = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
            info["dur"] = max(1, round(secs)) if secs > 0 else 0
    except Exception as e:
        log(f"  영상 정보 조회 실패({e})")
    return info


def ffmpeg_run(data, args, in_ext="bin", timeout=180):
    with tempfile.TemporaryDirectory() as td:
        src, dst = os.path.join(td, "in." + in_ext), os.path.join(td, "out.mp4")
        with open(src, "wb") as f:
            f.write(data)
        subprocess.run([FFMPEG, "-y", "-loglevel", "error", "-i", src, *args, dst], check=True, timeout=timeout, capture_output=True)
        with open(dst, "rb") as f:
            return f.read()


# 짝수 해상도(최소 2) 보장 + yuv420p: 홀수 해상도/특수 픽셀포맷이 텔레그램에서 재생 안 되는 흔한 원인
_VF = "scale=max(2\\,trunc(iw/2)*2):max(2\\,trunc(ih/2)*2),format=yuv420p"


def gif_to_mp4(data):
    """움직이는 GIF → 텔레그램 호환 mp4(h264). 실패하면 None."""
    try:
        return ffmpeg_run(
            data,
            ["-vf", _VF, "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
             "-movflags", "+faststart", "-an", "-fps_mode", "vfr"],
            in_ext="gif", timeout=120,
        )
    except Exception as e:
        err = getattr(e, "stderr", b"")
        log(f"  GIF→mp4 변환 실패({e}) {err[-200:].decode('utf-8', 'ignore') if err else ''}")
        return None


def normalize_mp4(data):
    """이미 mp4 인 영상도 텔레그램 호환으로 보정(h264/yuv420p/aac/faststart/짝수 해상도). (bytes, info) 반환."""
    info = probe_media(data)
    if not FFMPEG:
        return data, info
    need_v = info["vcodec"] not in (None, "h264") or info["pix"] not in (None, "yuv420p") or info["w"] % 2 or info["h"] % 2
    need_a = info["acodec"] not in (None, "aac")
    if not (need_v or need_a or not info["faststart"]):
        return data, info
    try:
        log(f"  영상 보정: vcodec={info['vcodec']} pix={info['pix']} {info['w']}x{info['h']} audio={info['acodec']} faststart={info['faststart']}")
        args = []
        args += ["-vf", _VF, "-c:v", "libx264", "-preset", "veryfast", "-crf", "23"] if need_v else ["-c:v", "copy"]
        args += ["-c:a", "aac", "-b:a", "128k"] if need_a else ["-c:a", "copy"]
        args += ["-movflags", "+faststart"]
        out = ffmpeg_run(data, args, in_ext="mp4")
        return out, probe_media(out)
    except Exception as e:
        log(f"  영상 보정 실패({e}) → 원본 그대로 사용")
        return data, info


def prepare(dc, item, idx, referer):
    """다운로드 후 텔레그램이 확실히 받는 형태로 정규화. 실패하면 None."""
    url = item["url"]
    try:
        data, ctype = fetch_bytes(dc, url, referer)
    except Exception as e:
        log(f"  다운로드 실패 {url[:80]} ({e})")
        return None
    if not data or data.lstrip()[:1] == b"<":  # HTML 에러 페이지
        log(f"  이미지가 아닌 응답 {url[:80]}")
        return None
    fmt = sniff(data)
    log(f"  파일 형식 판별: {fmt or '알 수 없음'}")
    base = {"url": url}

    # 영상
    if fmt in ("mp4", "webm") or item["type"] == "video":
        if len(data) > TG_FILE_MAX:
            log("  영상 50MB 초과 → 링크로 대체")
            return {**base, "kind": "link"}
        if fmt == "mp4":
            data, info = normalize_mp4(data)
            if len(data) > TG_FILE_MAX:
                return {**base, "kind": "link"}
            return {**base, "kind": "video", "bytes": data, "name": f"video{idx}.mp4", "mime": "video/mp4",
                    "w": info["w"], "h": info["h"], "dur": info["dur"]}
        ext = fmt or "bin"
        return {**base, "kind": "document", "bytes": data, "name": f"video{idx}.{ext}", "mime": "application/octet-stream"}

    # 이미지
    try:
        im = Image.open(io.BytesIO(data))
        w, h = im.size
        animated = getattr(im, "is_animated", False) and getattr(im, "n_frames", 1) > 1
    except Exception as e:
        log(f"  이미지 해석 실패 {url[:80]} ({e}) → 문서로 전송")
        if len(data) <= TG_FILE_MAX:
            return {**base, "kind": "document", "bytes": data, "name": f"file{idx}", "mime": "application/octet-stream"}
        return None

    if animated and fmt in ("gif", "webp"):
        if len(data) > TG_FILE_MAX:
            return {**base, "kind": "link"}
        if fmt == "gif" and not gif_fully_decodes(data):
            log(f"  ⚠ GIF 가 끝까지 디코딩되지 않음(손상/잘림): {url[:80]}")
            try:  # 한 번 더 받아서 확인
                data2, _ = fetch_bytes(dc, url, referer, attempts=1)
                if gif_fully_decodes(data2):
                    data = data2
                    log("  재다운로드로 정상 GIF 확보")
            except Exception:
                pass
        if GIF_TO_MP4 and FFMPEG:
            mp4 = gif_to_mp4(data)
            if mp4 and len(mp4) <= TG_FILE_MAX:
                info = probe_media(mp4)
                return {**base, "kind": "video", "bytes": mp4, "name": f"gif{idx}.mp4", "mime": "video/mp4",
                        "w": info["w"] or w, "h": info["h"] or h, "dur": info["dur"]}
        ext = "gif" if fmt == "gif" else "webp"
        return {**base, "kind": "animation", "bytes": data, "name": f"anim{idx}.{ext}", "mime": f"image/{ext}"}

    ratio_bad = max(w, h) / max(1, min(w, h)) > 20
    if ratio_bad:  # 극단적 비율(긴 세로짤)은 사진 불가 → 원본 그대로 문서
        if len(data) <= TG_FILE_MAX:
            return {**base, "kind": "document", "bytes": data, "name": f"long{idx}.{fmt or 'jpg'}", "mime": "application/octet-stream"}
        return {**base, "kind": "link"}

    ok_native = fmt in ("jpeg", "png") and len(data) <= TG_PHOTO_MAX and (w + h) <= 10000
    if ok_native:
        ext = "jpg" if fmt == "jpeg" else "png"
        return {**base, "kind": "photo", "bytes": data, "name": f"img{idx}.{ext}", "mime": f"image/{'jpeg' if ext == 'jpg' else 'png'}"}

    # 변환 필요: webp/bmp/정지 gif/용량·해상도 초과 -> JPEG 로 재인코딩
    try:
        side = 4096 if (w + h) > 10000 else None
        out = to_jpeg(im, max_side=side)
        q = 85
        while len(out) > TG_PHOTO_MAX and q >= 50:
            out = to_jpeg(im, max_side=min(max(w, h), 3000), quality=q)
            q -= 10
        if len(out) <= TG_PHOTO_MAX:
            return {**base, "kind": "photo", "bytes": out, "name": f"img{idx}.jpg", "mime": "image/jpeg"}
    except Exception as e:
        log(f"  이미지 변환 실패 ({e})")
    if len(data) <= TG_FILE_MAX:
        return {**base, "kind": "document", "bytes": data, "name": f"img{idx}.{fmt or 'bin'}", "mime": "application/octet-stream"}
    return None


# ──────────────────────────── 전송 로직 ────────────────────────────
def plain_len(*parts):
    return sum(len(x) for x in parts)


def build_post(dc, cfg, base, gid, gname, p):
    """[워커 스레드] 글 페이지 파싱 + 미디어 전부 다운로드/변환. 전송은 하지 않음."""
    purl = view_url(base, gid, p["no"])
    soup = dc.html(purl, referer=list_url(base, gid, {}, 1))
    title, text, media, ext = parse_view(soup)
    title = title or p["title"] or f"글 {p['no']}"
    max_media = int(cfg["max_media_per_post"])
    skipped = max(0, len(media) - max_media)
    media = media[:max_media]
    log(f"  ⬇ {p['no']} {title[:30]} (미디어 {len(media)}) 다운로드 시작")
    with ThreadPoolExecutor(max_workers=max(1, int(cfg["parallel_media"]))) as ex:
        prepared = list(ex.map(lambda a: prepare(dc, a[1], a[0], purl), list(enumerate(media, 1))))
    items, failed = [], []
    for it, m in zip(media, prepared):
        if m is None or m["kind"] == "link":
            failed.append(it["url"])
        else:
            items.append(m)
    log(f"  ✔ {p['no']} 다운로드 완료 (성공 {len(items)} / 실패 {len(failed)})")
    return {
        "no": p["no"], "url": purl, "gname": gname, "title": title, "text": text,
        "items": items, "failed": failed, "ext": ext, "skipped": skipped,
    }


def send_post(tg, cfg, post):
    """[메인 스레드] 한 글을 최대한 한 덩어리로 전송: 앨범 첫 장 캡션 = 제목+내용+링크."""
    limit = int(cfg["max_text_chars"])
    text = post["text"]
    if len(text) > limit:
        text = text[:limit].rstrip() + "…"
    head_plain = f"[{post['gname']}] {post['title']}"
    head = f"<b>{html.escape(head_plain)}</b>"

    tail_plain, tail = [], []  # 링크 구역
    for l in post["ext"]:
        tail_plain.append(f"▶️ 외부영상: {l}")
        tail.append(f"▶️ 외부영상: {html.escape(l)}")
    if post["failed"]:
        tail_plain.append("⚠️ 업로드 못한 미디어:")
        tail.append("⚠️ 업로드 못한 미디어:")
        for u in post["failed"]:
            tail_plain.append(u)
            tail.append(html.escape(u))
    if post["skipped"]:
        tail_plain.append(f"(미디어 {post['skipped']}개 생략)")
        tail.append(f"(미디어 {post['skipped']}개 생략)")
    tail_plain.append(f"🔗 {post['url']}")
    tail.append(f"🔗 {html.escape(post['url'])}")
    tail_html, tail_pl = "\n".join(tail), "\n".join(tail_plain)
    body_html = f"\n\n{html.escape(text)}" if text else ""

    items = post["items"]
    if not items:  # 미디어 없음 → 텍스트 한 덩어리
        full = head + body_html + "\n\n" + tail_html
        tg.send_text(full if len(head_plain) + len(text) + len(tail_pl) < 3900 else head + body_html)
        if len(head_plain) + len(text) + len(tail_pl) >= 3900:
            tg.send_text(tail_html)
        return

    # 캡션 한도 1024자: 다 들어가면 앨범 하나로 끝, 아니면 글 본문은 텍스트로 먼저
    if plain_len(head_plain, text, tail_pl) + 4 <= 1024:
        caption = head + body_html + "\n\n" + tail_html
    else:
        tg.send_text(head + body_html)
        caption = tail_html if len(tail_pl) <= 1024 else None
        if caption is None:
            tg.send_text(tail_html)

    # 앨범(사진/영상, 10개씩) → 그 뒤에 GIF(animation)/문서
    album = [m for m in items if m["kind"] in ("photo", "video")]
    singles = [m for m in items if m["kind"] not in ("photo", "video")]
    for i in range(0, len(album), 10):
        tg.send_group(album[i : i + 10], caption)
        caption = None
    for m in singles:
        tg.send_one_safe(m, caption)
        caption = None
    if caption:  # 미디어가 모두 실패해서 캡션이 남은 경우
        tg.send_text(caption)


def collect_gallery(dc, url, pages):
    base, gid, keep = parse_gallery_url(url)
    gname, found, seen_no = gid, [], set()
    for p in range(1, pages + 1):
        try:
            soup = dc.html(list_url(base, gid, keep, p))
        except Exception as e:
            log(f"  {gid} {p}페이지 로드 실패: {e}")
            continue
        n, posts = parse_list(soup)
        if n:
            gname = n
        for po in posts:
            if po["no"] in seen_no:
                continue  # 페이지 간 중복 제거
            seen_no.add(po["no"])
            found.append(po)
        time.sleep(1.0)
    return base, gid, gname, found


def run_once(cfg, tg, dc, state, mark_only=False, deadline=None):
    """모든 갤러리를 한 번 순회하고 끝낸다. deadline(시각)이 지나면 남은 글은 다음 실행으로 넘긴다."""

    def time_up():
        return deadline is not None and time.time() > deadline

    seen_urls = set()
    for url in cfg["galleries"]:
        if url in seen_urls:  # config 에 같은 URL 이 중복돼 있어도 한 번만 처리
            continue
        seen_urls.add(url)
        if time_up():
            log("시간 상한 도달 → 남은 갤러리는 다음 실행에서 처리")
            break
        try:
            base, gid, gname, posts = collect_gallery(dc, url, int(cfg["pages"]))
        except Exception as e:
            log(f"갤러리 처리 실패 {url}: {e}")
            continue
        new = [p for p in posts if f"{gid}:{p['no']}" not in state["seen"]]
        new.sort(key=lambda p: int(p["no"]))  # 오래된 글부터
        log(f"[{gname}] 전체 {len(posts)}개 중 신규 {len(new)}개")
        if mark_only:
            for p in new:
                state["seen"][f"{gid}:{p['no']}"] = int(time.time())
            save_state(state)
            continue

        window = max(1, int(cfg["parallel_posts"]))
        with ThreadPoolExecutor(max_workers=window) as ex:
            futs = {}

            def submit(i):
                if i < len(new) and i not in futs:
                    futs[i] = ex.submit(build_post, dc, cfg, base, gid, gname, new[i])

            for i in range(window):  # 앞의 몇 개를 미리 다운로드 시작
                submit(i)
            for i, p in enumerate(new):
                if time_up():
                    log("시간 상한 도달 → 남은 글은 다음 실행에서 처리")
                    break
                key = f"{gid}:{p['no']}"
                try:
                    post = futs[i].result(timeout=POST_BUILD_TIMEOUT)  # 이 글 다운로드 끝날 때까지만 대기
                    submit(i + window)  # 다음 글은 이미 병렬로 받는 중
                    send_post(tg, cfg, post)
                    state["seen"][key] = int(time.time())
                    state["fail"].pop(key, None)
                    log(f"  ✉ {p['no']} 전송 완료")
                except Exception as e:
                    submit(i + window)
                    n = state["fail"].get(key, 0) + 1
                    state["fail"][key] = n
                    log(f"  글 {p['no']} 실패({n}/3): {e!r}")
                    if n >= 3:
                        state["seen"][key] = int(time.time())
                finally:
                    futs.pop(i, None)
                save_state(state)
                wait = random.uniform(float(cfg["post_delay_min"]), float(cfg["post_delay_max"]))
                log(f"  {wait:.0f}초 대기 (이 동안 다음 글 다운로드 진행)")
                time.sleep(wait)
            # 남은 미리받기 작업은 취소 (이미 도는 것만 끝나길 기다림)
            for f in futs.values():
                f.cancel()
        save_state(state)


def main():
    ap = argparse.ArgumentParser(description="DCInside → Telegram (1회 실행 후 종료)")
    ap.add_argument("--mark-seen", action="store_true", help="현재 보이는 글을 전송 없이 '본 글'로만 표시(첫 실행 폭주 방지)")
    args = ap.parse_args()
    ap.add_argument("--once", action="store_true", help="호환용(무시됨, 항상 1회 실행)")
    ap.add_argument("--max-minutes", type=float, default=0, help="이 시간(분)이 지나면 새 글을 시작하지 않고 종료(0=제한 없음)")

    cfg = load_config()
    if not cfg["token"] or "여기에" in cfg["token"]:
        raise SystemExit(f"config.json 의 token 을 입력하세요: {CONFIG_PATH}")
    if not cfg["chat_id"] and not args.mark_seen:
        detect_chat_id(cfg)

    global GIF_TO_MP4
    GIF_TO_MP4 = bool(cfg.get("gif_to_mp4", True))
    if GIF_TO_MP4 and not FFMPEG:
        log("⚠ ffmpeg 없음 → GIF/영상 보정을 못 합니다. 'pip install imageio-ffmpeg' 로 설치하면 GIF 깨짐이 크게 줄어듭니다")
    tg = Telegram(cfg["token"], cfg["chat_id"], cfg["disable_notification"])
    dc, state = DC(), load_state()

    # MAX_RUN_MINUTES 환경변수가 있으면 그 시간 안에서만 새 글을 처리 (없으면 제한 없음)
    max_min = float(os.environ.get("MAX_RUN_MINUTES") or 0)
    deadline = time.time() + max_min * 60 if max_min > 0 else None

    try:
        run_once(cfg, tg, dc, state, mark_only=args.mark_seen, deadline=deadline)
    except Exception as e:
        save_state(state)
        log(f"순회 중 오류: {e!r}")
        sys.exit(1)

    save_state(state)
    log("순회 완료. 종료")
    sys.exit(0)


if __name__ == "__main__":
    main()