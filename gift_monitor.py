import json
import os
import sys
import time
from datetime import datetime
from urllib.parse import parse_qs, unquote, urlparse

import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ================== 설정 ==================
KEYWORDS = [
    "선착순 기프티콘",
]

RESULT_FILE = "giftcon_results.json"

def load_existing_links():
    if not os.path.exists(RESULT_FILE):
        return set()
    try:
        with open(RESULT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return {item.get('링크') for item in data if item.get('링크')}
    except:
        return set()

def save_results(new_results):
    if not new_results:
        print(json.dumps({
            "status": "success",
            "new_count": 0,
            "message": "새로운 결과 없음"
        }, ensure_ascii=False, indent=2))
        return

    existing_data = []
    if os.path.exists(RESULT_FILE):
        try:
            with open(RESULT_FILE, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except:
            existing_data = []

    existing_data.extend(new_results)

    with open(RESULT_FILE, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=2)

    print(json.dumps({
        "status": "success",
        "new_count": len(new_results),
        "total_count": len(existing_data),
        "new_items": new_results
    }, ensure_ascii=False, indent=2))

def scroll_to_end(driver):
    SCROLL_PAUSE_TIME = 3
    last_height = driver.execute_script("return document.body.scrollHeight")
    attempts = 0
    max_attempts = 30
    
    print("   📜 이미지 무한 스크롤 시작...")
    while attempts < max_attempts:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
        attempts += 1
    print(f"   📜 스크롤 완료 ({attempts}회)")

def get_google_images_url(keyword):
    # tbs=qdr:w → 지난 일주일
    return f"https://www.google.com/search?q={keyword.replace(' ', '+')}&tbm=isch&tbs=qdr:w&hl=ko&gl=kr"

def scrape_google(keyword, existing_links):
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--lang=ko-KR")
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = None
    results = []
    try:
        driver = uc.Chrome(options=options, use_subprocess=True)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        url = get_google_images_url(keyword)
        print(f"   → 검색 중: {keyword}")
        driver.get(url)
        
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "img"))
        )
        time.sleep(4)
        
        scroll_to_end(driver)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        containers = soup.select('div.isv-r, div.H8Rx8c, div[data-ri]')
        
        print(f"   발견된 이미지 컨테이너: {len(containers)}개")
        
        for container in containers:
            try:
                # 이미지 URL (직접 이미지 링크)
                img = container.select_one('img')
                if not img:
                    continue
                title = img.get('alt', '').strip() or "제목 없음"
                image_url = img.get('src') or img.get('data-src', '')
                
                if not image_url or image_url.startswith('data:'):
                    continue
                
                # 원본 페이지 링크 (해당 이벤트/게시글 페이지)
                page_link = ""
                # 가장 정확한 링크 선택: VFACy 클래스나 a 태그 중 href가 긴 것 우선
                link_tags = container.select('a.VFACy, a[href^="https://"]')
                for a in link_tags:
                    href = a.get('href', '')
                    if href and not href.startswith('/search') and 'google.com' not in href:
                        if '/url?q=' in href:
                            try:
                                parsed = urlparse(href)
                                qs = parse_qs(parsed.query)
                                page_link = unquote(qs.get('q', [''])[0])
                            except:
                                page_link = href
                        else:
                            page_link = href
                        if page_link:
                            break
                
                # page_link가 없으면 fallback
                if not page_link:
                    a_tag = container.select_one('a')
                    if a_tag and a_tag.has_attr('href'):
                        href = a_tag['href']
                        if '/url?q=' in href:
                            try:
                                parsed = urlparse(href)
                                qs = parse_qs(parsed.query)
                                page_link = unquote(qs.get('q', [''])[0])
                            except:
                                page_link = href
                
                final_page_link = page_link if page_link else image_url  # fallback
                
                if final_page_link in existing_links:
                    continue
                
                text_lower = (title + " " + final_page_link).lower()
                
                if any(kw in text_lower for kw in ["기프티콘", "선착순", "추첨", "증정", "나눔", "뿌리기"]):
                    results.append({
                        "검색키워드": keyword,
                        "제목": title[:250],
                        "링크": final_page_link,      # ← 원본 페이지 링크 (이벤트 페이지)
                        "이미지URL": image_url,       # ← 진짜 이미지 직접 링크
                        "수집시간": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
            except:
                continue
                
    except Exception as e:
        print(f"[{keyword}] 에러: {e}", file=sys.stderr)
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
    
    return results

# ================== 메인 ==================
if __name__ == "__main__":
    print(f"🚀 선착순 기프티콘 이미지 탭 모니터링 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print("   지난 일주일 기준 | JSON 출력 | 링크 분리 완료")
    
    existing_links = load_existing_links()
    print(f"   기존 등록된 링크 수: {len(existing_links)}개")
    
    all_new_results = []
    
    for kw in KEYWORDS:
        new_data = scrape_google(kw, existing_links)
        if new_data:
            all_new_results.extend(new_data)
            for item in new_data:
                existing_links.add(item.get('링크'))
        time.sleep(6)
    
    save_results(all_new_results)
    
    print(f"\n🎯 실행 완료 (신규 발견: {len(all_new_results)}건)")