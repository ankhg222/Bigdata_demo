import csv
import json
import os
import re
import shutil
import subprocess
import time
import random
import threading
from datetime import datetime
from html import unescape as html_unescape
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

# =====================================
# Configuration
# =====================================
BASE = "https://itviec.com"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}
LISTING_PAGES = 20  # crawl more listing pages per query to build a larger job pool
TARGET_JOB_URLS = 2000
MAX_WORKERS = 5
AUTOSAVE_PATH = os.path.join(SCRIPT_DIR, "autosave.csv")
FINAL_CSV = os.path.join(SCRIPT_DIR, "hybrid_jobs_fixed.csv")
AUTOSAVE_INTERVAL = 5  # save after every 5 successful jobs
DRIVER_LOCK = threading.Lock()
MAX_LISTING_PAGES_LIMIT = 500  # absolute safety cap when crawling all pages
ALL_MAX_LISTING_PAGES = 45  # observed number of pages for 'All' on site; cap to avoid extra requests

# thread-local storage for per-worker driver
_thread_local = threading.local()
_all_drivers: List[webdriver.Chrome] = []

# known skills to extract from text when JobPosting.skills absent
KNOWN_SKILLS = [
    "python","java","c#","c++",
    "javascript","typescript",
    "react","angular","vue",
    "nodejs","nestjs",
    "docker","kubernetes",
    "aws","azure","gcp",
    "sql","mysql","postgresql","mongodb",
    "redis","kafka","spark","airflow",
    "tensorflow","pytorch",
    "fastapi","flask","django",
    "git","linux",
    "ai","machine learning",
    "deep learning","nlp","llm",
    "langchain","llamaindex",
    "computer vision"
]

# logging counters
_stats = {
    "total_urls": 0,
    "success": 0,
    "errors": 0,
    "start_time": datetime.now()
}

# storage
_all_jobs: List[Dict] = []
_seen_urls: set = set()

AUTOSAVE_COLUMNS = [
    "title",
    "company",
    "salary",
    "location",
    "description",
    "skills",
    "url",
    "crawl_time",
]

LISTING_URL_MIN_CANDIDATES = 10

# Per-query page caps when we know a query has a finite result set.
QUERY_PAGE_CAPS = {
    "developer": 11,
}

LISTING_QUERIES = [
    "",
    "developer",
    "engineer",
    "data",
    "ai",
    "devops",
    "tester",
    "frontend",
    "backend",
    "fullstack",
    "mobile",
    "product",
    "manager",
    "architect",
    "python",
    "java",
    "sql",
    "security",
]

# =====================================
# Utilities
# =====================================

def detect_chrome_binary_and_version() -> tuple[Optional[str], Optional[int]]:
    """Try to locate Chrome binary and return (path, major_version)."""
    candidates = [
        shutil.which("chrome"),
        shutil.which("chrome.exe"),
        shutil.which("google-chrome"),
        os.path.join(os.environ.get("ProgramFiles", ""), "Google", "Chrome", "Application", "chrome.exe"),
        os.path.join(os.environ.get("ProgramFiles(x86)", ""), "Google", "Chrome", "Application", "chrome.exe"),
        os.path.join(os.environ.get("LOCALAPPDATA", ""), "Google", "Chrome", "Application", "chrome.exe"),
    ]
    for candidate in candidates:
        if not candidate or not os.path.exists(candidate):
            continue
        try:
            output = subprocess.check_output([candidate, "--version"], text=True, stderr=subprocess.STDOUT).strip()
            match = re.search(r"(\d+)\.", output)
            if match:
                return candidate, int(match.group(1))
        except Exception:
            continue
    return None, None


def safe_quit_driver(driver: Optional[webdriver.Chrome]) -> None:
    """Quit selenium driver ignoring exceptions."""
    if not driver:
        return
    try:
        driver.quit()
    except Exception:
        pass


def create_driver() -> webdriver.Chrome:
    """Create a Chrome webdriver instance with configured options."""
    with DRIVER_LOCK:
        chrome_binary, chrome_major = detect_chrome_binary_and_version()
        chrome_major = chrome_major or 148
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument(f"--user-agent={USER_AGENT}")
        if chrome_binary:
            options.binary_location = chrome_binary
        service = Service()
        driver = webdriver.Chrome(service=service, options=options)
        _all_drivers.append(driver)
        return driver


def get_thread_driver() -> webdriver.Chrome:
    """Get or create a selenium driver tied to the current thread."""
    driver = getattr(_thread_local, "driver", None)
    if driver and getattr(driver, "session_id", None):
        return driver
    driver = create_driver()
    _thread_local.driver = driver
    return driver


# Requests session that may be populated after Selenium login
REQUESTS_SESSION: Optional[requests.Session] = None
COOKIE_FILE = os.path.join(SCRIPT_DIR, "itviec_cookies.json")


def save_cookies_to_file(cookies: List[dict], path: str = COOKIE_FILE) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cookies, f)
    except Exception:
        pass


def load_cookies_from_file(path: str = COOKIE_FILE) -> Optional[List[dict]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def login_with_selenium_export_session(username: str, password: str, save_cookies: bool = True) -> Optional[requests.Session]:
    """Use Selenium to log in, then export cookies into a requests.Session()."""
    global REQUESTS_SESSION
    try:
        driver = get_thread_driver()
        login_url = f"{BASE}/login"
        driver.get(login_url)
        time.sleep(1)
        # Try common selectors for email/username and password fields
        input_candidates = [
            ("input[name=email]", username),
            ("input[name=username]", username),
            ("input[type=email]", username),
            ("#email", username),
            ("input[name=password]", password),
            ("input[type=password]", password),
            ("#password", password),
        ]
        # fill username/email
        for sel, val in input_candidates:
            if "email" in sel or "username" in sel or "type=email" in sel or sel.startswith("#email"):
                try:
                    el = driver.find_element("css selector", sel)
                    el.clear()
                    el.send_keys(val)
                    break
                except Exception:
                    continue
        # fill password
        for sel, val in input_candidates:
            if "password" in sel or "type=password" in sel or sel.startswith("#password"):
                try:
                    el = driver.find_element("css selector", sel)
                    el.clear()
                    el.send_keys(val)
                    break
                except Exception:
                    continue

        # submit the form: try common buttons
        try:
            btn = driver.find_element("css selector", "form button[type=submit]")
            btn.click()
        except Exception:
            try:
                btn = driver.find_element("css selector", "button.login-button")
                btn.click()
            except Exception:
                try:
                    driver.execute_script("document.querySelector('form').submit()")
                except Exception:
                    pass

        # wait for navigation / login processing
        time.sleep(3)

        # export cookies to requests.Session
        s = requests.Session()
        s.headers.update(HEADERS)
        for c in driver.get_cookies():
            try:
                s.cookies.set(c.get("name"), c.get("value"), domain=c.get("domain"))
            except Exception:
                try:
                    s.cookies.set(c.get("name"), c.get("value"))
                except Exception:
                    pass

        if save_cookies:
            try:
                save_cookies_to_file(driver.get_cookies(), COOKIE_FILE)
            except Exception:
                pass

        REQUESTS_SESSION = s
        return s
    except Exception as e:
        print("Selenium login failed:", e)
        return None


def try_restore_session_from_cookies() -> Optional[requests.Session]:
    global REQUESTS_SESSION
    cookies = load_cookies_from_file(COOKIE_FILE)
    if not cookies:
        return None
    s = requests.Session()
    s.headers.update(HEADERS)
    for c in cookies:
        try:
            s.cookies.set(c.get("name"), c.get("value"), domain=c.get("domain"))
        except Exception:
            try:
                s.cookies.set(c.get("name"), c.get("value"))
            except Exception:
                pass
    try:
        r = s.get(BASE, headers=HEADERS, timeout=10)
        txt = r.text.lower()
        if "sign in" in txt or "đăng nhập" in txt:
            return None
        REQUESTS_SESSION = s
        return s
    except Exception:
        return None


def export_session_from_driver(driver: webdriver.Chrome, save_cookies: bool = True) -> Optional[requests.Session]:
    """Create a requests.Session from an active Selenium driver by exporting cookies."""
    global REQUESTS_SESSION
    try:
        s = requests.Session()
        s.headers.update(HEADERS)
        cookies = driver.get_cookies()
        for c in cookies:
            try:
                s.cookies.set(c.get("name"), c.get("value"), domain=c.get("domain"))
            except Exception:
                try:
                    s.cookies.set(c.get("name"), c.get("value"))
                except Exception:
                    pass
        if save_cookies:
            try:
                save_cookies_to_file(cookies, COOKIE_FILE)
            except Exception:
                pass
        REQUESTS_SESSION = s
        return s
    except Exception:
        return None


def manual_login_and_export(timeout: int = 300) -> Optional[requests.Session]:
    """Open a browser for manual login, wait for user or auto-detect, then export cookies.

    - Opens the site in Selenium and lets the user log in interactively.
    - You can press Enter in the console after login to proceed, or wait until auto-detection succeeds.
    """
    try:
        driver = get_thread_driver()
        login_url = f"{BASE}/login"
        print("Opening browser for manual login. Please log in to itviec in the opened window.")
        print("After logging in, press Enter here to continue, or wait for automatic detection.")
        driver.get(login_url)

        # auto-detect login for up to `timeout` seconds
        start = time.time()
        while True:
            # check page content for login indicators
            try:
                html = driver.page_source.lower()
                if "đăng xuất" in html or "logout" in html or "profile" in html:
                    # likely logged in
                    print("Detected login via page content.")
                    break
                if "sign in" not in html and "đăng nhập" not in html:
                    # no explicit signin text, assume logged in
                    print("No signin text found; assuming logged in.")
                    break
            except Exception:
                pass
            # allow user to press Enter to continue immediately
            if os.name == "nt":
                # on Windows, input() is fine
                pass
            # non-blocking check for user input isn't straightforward; instead ask them to press Enter
            # We poll with a small sleep and check elapsed time
            if time.time() - start > timeout:
                print("Manual login timeout reached.")
                break
            # short sleep
            time.sleep(1)

        # Also allow explicit confirmation
        try:
            input("If you have completed login in the browser, press Enter to export cookies (or CTRL+C to abort): ")
        except Exception:
            pass

        s = export_session_from_driver(driver, save_cookies=True)
        if s:
            print("Exported cookies to requests.Session. Subsequent requests will use logged-in session.")
        else:
            print("Failed to export cookies from Selenium driver.")
        return s
    except Exception as e:
        print("manual_login_and_export failed:", e)
        return None


def parse_jobposting_jsonld(soup: BeautifulSoup) -> Optional[Dict]:
    """Parse JSON-LD and return the first JobPosting dict if present."""
    def is_jobposting(item: Dict) -> bool:
        item_type = item.get("@type")
        if isinstance(item_type, list):
            return "JobPosting" in item_type
        return item_type == "JobPosting"
    for script in soup.find_all("script", type="application/ld+json"):
        text = (script.string or script.get_text() or "").strip()
        if not text:
            continue
        try:
            data = json.loads(text)
        except Exception:
            continue
        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            if isinstance(data.get("@graph"), list):
                items = data["@graph"]
            else:
                items = [data]
        for item in items:
            if isinstance(item, dict) and is_jobposting(item):
                return item
    return None


def clean_text(value: Optional[str]) -> str:
    """Render HTML/text to a cleaned single-line string."""
    if value is None:
        return ""
    text = str(value)
    text = BeautifulSoup(html_unescape(text), "html.parser").get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def is_dead_job_page(soup: Optional[BeautifulSoup]) -> bool:
    """Detect ITviec missing-job pages."""
    if soup is None:
        return False
    text = clean_text(soup.get_text(" ", strip=True)).lower()
    # quick negative: if the page doesn't contain the Oops message, it's not a dead page
    if "oops! the job you're looking for doesn't exist" not in text:
        return False

    # If JSON-LD JobPosting is present, treat as live job page despite the Oops block
    try:
        jp = parse_jobposting_jsonld(soup)
        if jp:
            return False
    except Exception:
        pass

    # If there is a clear job-title on the page, consider it live
    try:
        h1 = soup.find("h1")
        if h1 and clean_text(h1.get_text()).strip():
            return False
    except Exception:
        pass

    # Otherwise consider it a dead/missing job page
    return True


def extract_listing_job_urls(soup: BeautifulSoup) -> List[str]:
    """Extract candidate job detail URLs from a listing page soup."""
    urls: List[str] = []
    seen: set = set()

    def add_href(href: str) -> None:
        try:
            # resolve relative/absolute hrefs to absolute URLs and preserve query
            full = requests.compat.urljoin(BASE, href)
            parsed = urlparse(full)
            path = (parsed.path or "").rstrip("/")
            # only job detail paths (case-insensitive)
            if "/it-jobs/" not in path.lower():
                return
            slug = path.rsplit("/", 1)[-1] if "/" in path else ""
            # ignore tag/skill listing slugs
            if slug.startswith("tag-") or slug.startswith("skill-"):
                return
            # normalize full keeping query params but strip fragment
            full_norm = parsed._replace(fragment="").geturl()
            if not full_norm.startswith(BASE):
                return
            if full_norm in seen:
                return
            seen.add(full_norm)
            urls.append(full_norm)
        except Exception:
            return

    # First pass: collect anchors (preserve order of appearance)
    for a in soup.find_all("a"):
        href = a.get("href") or a.get("data-href") or a.get("data-url")
        if href:
            try:
                add_href(href)
            except Exception:
                continue

    # Also consider other elements that may carry clickable URLs (buttons, divs with data attributes, onclick handlers)
    # This handles sites that attach URLs to data-href/data-url or use onclick="location.href='...'
    for tag in soup.find_all(True):
        # skip tags already handled
        if tag.name == "a":
            continue
        # data-href / data-url
        for attr in ("data-job-url", "data-href", "data-url", "data-link", "data-target"):
            val = tag.get(attr)
            if val:
                try:
                    add_href(val)
                except Exception:
                    pass
        # onclick handlers containing a URL
        onclick = tag.get("onclick")
        if onclick and "/it-jobs/" in onclick:
            # try to extract the URL between quotes, allow query string and varied chars
            m = re.search(r'(https?://[^\'"<>\s]+|/it-jobs/[^\'"<>\s]+)', onclick)
            if m:
                try:
                    add_href(m.group(0))
                except Exception:
                    pass

    # Also scan raw text and attributes for any /it-jobs/ references in case of unusual markup
    raw = str(soup)
    for m in re.finditer(r'(?:https?://[^\'"<>\s]+|/it-jobs/[^\'"<>\s]+)', raw):
        try:
            add_href(m.group(0))
        except Exception:
            pass

    return urls


def fetch_listing_page_selenium(url: str) -> Optional[BeautifulSoup]:
    """Open a listing page in Selenium and return rendered soup."""
    try:
        driver = get_thread_driver()
        driver.get(url)
        # initial short wait for first paint
        time.sleep(1)

        # Repeatedly scroll to bottom and attempt to trigger lazy-loading
        # and "load more" buttons. Stop when the page source size becomes
        # stable across a couple iterations or when timeout is reached.
        scroll_pause = 0.7
        max_loops = 12
        last_len = 0
        stable_count = 0
        for _ in range(max_loops):
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except Exception:
                pass
            time.sleep(scroll_pause + random.uniform(0, 0.4))

            # Try clicking common "load more"/"show more" buttons if present
            for sel in ("button.load-more", "a.load-more", ".btn-load-more", ".load-more", "button.show-more", "a.show-more"):
                try:
                    els = driver.find_elements("css selector", sel)
                    for el in els:
                        try:
                            if el.is_displayed():
                                try:
                                    el.click()
                                except Exception:
                                    driver.execute_script("arguments[0].click();", el)
                                time.sleep(0.3)
                        except Exception:
                            continue
                except Exception:
                    continue

            # allow small additional wait for lazy items to render
            time.sleep(0.3)
            html = driver.page_source
            cur_len = len(html)
            if cur_len == last_len:
                stable_count += 1
            else:
                stable_count = 0
            last_len = cur_len
            if stable_count >= 2:
                break

        # final small scroll to ensure elements loaded
        try:
            driver.execute_script("window.scrollTo(0, 0); window.scrollTo(0, document.body.scrollHeight);")
        except Exception:
            pass
        time.sleep(0.8 + random.random() * 0.6)
        return BeautifulSoup(driver.page_source, "html.parser")
    except Exception:
        return None


# =====================================
# Salary parsing & normalization
# =====================================

SALARY_VISIBLE_UNIT_RE = re.compile(r"(?:\b(?:usd|vnd|vnđ|triệu|million)\b|\$)", flags=re.IGNORECASE)


def _normalize_salary_phrase(text: str) -> str:
    if not text:
        return "Unknown"
    out = clean_text(text)
    if not out:
        return "Unknown"

    low = out.lower()
    if any(phrase in low for phrase in ("sign in to view salary", "login to view salary", "salary hidden")):
        return "Hidden"
    if any(phrase in low for phrase in ("negotiable", "thương lượng", "thoả thuận", "thỏa thuận")):
        return "Negotiable"
    if any(phrase in low for phrase in ("competitive",)):
        return "Competitive"
    if any(phrase in low for phrase in ("attractive",)):
        return "Attractive"

    leading_unit = re.match(r"(?i)^\s*(usd|vnd|vnđ|triệu|million)\s+(.+)$", out)
    if leading_unit:
        token = leading_unit.group(1)
        if token.lower() == "usd":
            token = "USD"
        out = f"{leading_unit.group(2).strip()} {token}".strip()

    has_dollar = "$" in out
    out = out.replace("$", "")
    out = out.replace("\u2013", "-").replace("\u2014", "-")
    out = re.sub(r"(?i)^\s*(usd|vnd|vnđ|triệu|million)\s+", "", out)
    out = re.sub(r"(?i)\s+(usd|vnd|vnđ|triệu|million)\s*$", r" \1", out)
    out = re.sub(r"(?<=\d)[,\s](?=\d{3}\b)", "", out)
    out = re.sub(r"(?<=\d)\.(?=\d{3}\b)", "", out)
    out = re.sub(r"\s*[-–to]+\s*", "-", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+", " ", out).strip()

    if has_dollar and not re.search(r"\busd\b", out, flags=re.IGNORECASE):
        out = f"{out} USD".strip()

    out = re.sub(r"\s+-\s+", "-", out)
    out = re.sub(r"\s+", " ", out).strip()
    out = re.sub(r"\busd\b", "USD", out, flags=re.IGNORECASE)
    return out or "Unknown"


def normalize_salary_text(s: str, full_text: Optional[str] = None) -> str:
    """Normalize a raw salary fragment without guessing from unrelated text."""
    return _normalize_salary_phrase(s)


def _extract_salary_metadata_from_dom(soup: Optional[BeautifulSoup]) -> Optional[str]:
    if not soup:
        return None
    node = soup.find(id="jd-main")
    if not node:
        return None
    raw = node.get("data-jobs-save-data-layer-value")
    if not raw:
        return None
    try:
        data = json.loads(html_unescape(raw))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    salary_range = data.get("salary_range")
    if not salary_range:
        return None
    normalized = _normalize_salary_phrase(str(salary_range))
    return normalized if normalized != "Unknown" else None


def _clean_salary_number(value: Any) -> str:
    if value is None:
        return ""
    text = clean_text(str(value))
    text = re.sub(r"(?<=\d)[,\s](?=\d{3}\b)", "", text)
    text = re.sub(r"(?<=\d)\.(?=\d{3}\b)", "", text)
    text = re.sub(r"(?<=\d)\s+(?=\d)", "", text)
    return text.strip()


def _format_base_salary(base_salary: Any) -> Optional[str]:
    if not base_salary:
        return None

    def build_amount(min_value: Any, max_value: Any, currency: Optional[str]) -> Optional[str]:
        parts = []
        min_text = _clean_salary_number(min_value)
        max_text = _clean_salary_number(max_value)
        if min_text and max_text and min_text != max_text:
            parts.append(f"{min_text}-{max_text}")
        elif min_text or max_text:
            parts.append(min_text or max_text)
        else:
            return None
        if currency:
            parts.append(currency)
        return _normalize_salary_phrase(" ".join(parts))

    if isinstance(base_salary, dict):
        currency = (
            base_salary.get("currency")
            or base_salary.get("currencyCode")
            or base_salary.get("unitText")
            or base_salary.get("unitCode")
        )

        value = base_salary.get("value")
        if isinstance(value, dict):
            currency = currency or value.get("currency") or value.get("currencyCode") or value.get("unitText") or value.get("unitCode")
            built = build_amount(value.get("minValue"), value.get("maxValue"), currency)
            if built:
                return built
            scalar_value = value.get("value") or value.get("amount") or value.get("price")
            if scalar_value is not None:
                amount = _clean_salary_number(scalar_value)
                if amount:
                    return _normalize_salary_phrase(f"{amount} {currency}".strip()) if currency else _normalize_salary_phrase(amount)

        built = build_amount(base_salary.get("minValue"), base_salary.get("maxValue"), currency)
        if built:
            return built

        scalar_value = base_salary.get("value")
        if scalar_value is not None and not isinstance(scalar_value, dict):
            amount = _clean_salary_number(scalar_value)
            if amount:
                return _normalize_salary_phrase(f"{amount} {currency}".strip()) if currency else _normalize_salary_phrase(amount)

        for key in ("currency", "currencyCode", "unitText", "unitCode"):
            if key in base_salary and base_salary.get(key):
                amount = _clean_salary_number(base_salary.get("value") or base_salary.get("minValue") or base_salary.get("maxValue"))
                if amount:
                    return _normalize_salary_phrase(f"{amount} {base_salary.get(key)}".strip())

    if isinstance(base_salary, str):
        normalized = _normalize_salary_phrase(base_salary)
        return normalized if normalized != "Unknown" else None

    if isinstance(base_salary, (int, float)):
        return _normalize_salary_phrase(str(int(base_salary) if float(base_salary).is_integer() else base_salary))

    return None


def _strict_salary_regex_fallback(soup: Optional[BeautifulSoup]) -> str:
    if not soup:
        return "Unknown"

    try:
        raw_text = soup.get_text(separator="\n")
    except Exception:
        return "Unknown"

    for raw_line in raw_text.splitlines():
        line = clean_text(raw_line)
        if not line:
            continue
        normalized = _normalize_salary_phrase(line)
        if normalized in ("Hidden", "Negotiable", "Competitive", "Attractive"):
            return normalized

        if not SALARY_VISIBLE_UNIT_RE.search(line):
            continue

        range_match = re.search(
            r"(?<!\d)(\$?\d[\d,\.\s]*\d|\d+)\s*(?:-|to|–)\s*(\$?\d[\d,\.\s]*\d|\d+)\s*(usd|vnd|vnđ|triệu|million)\b",
            line,
            flags=re.IGNORECASE,
        )
        if range_match:
            left = range_match.group(1).replace("$", "")
            right = range_match.group(2).replace("$", "")
            unit = range_match.group(3)
            return _normalize_salary_phrase(f"{left}-{right} {unit}")

        dollar_range = re.search(r"\$(\d[\d,\.\s]*\d|\d+)\s*(?:-|to|–)\s*\$(\d[\d,\.\s]*\d|\d+)", line, flags=re.IGNORECASE)
        if dollar_range:
            left = _clean_salary_number(dollar_range.group(1))
            right = _clean_salary_number(dollar_range.group(2))
            return _normalize_salary_phrase(f"{left}-{right} USD")

        dollar_single = re.search(r"\$(\d[\d,\.\s]*\d|\d+)", line)
        if dollar_single:
            amount = _clean_salary_number(dollar_single.group(1))
            if amount:
                return _normalize_salary_phrase(f"{amount} USD")

        single_match = re.search(
            r"(?<!\d)(\d[\d,\.\s]*\d|\d+)\s*(usd|vnd|vnđ|triệu|million)\b",
            line,
            flags=re.IGNORECASE,
        )
        if single_match:
            amount = _clean_salary_number(single_match.group(1))
            unit = single_match.group(2)
            return _normalize_salary_phrase(f"{amount} {unit}")

        if re.search(r"(?<!\w)\d{8,}(?!\w)", line):
            continue

    return "Unknown"


def normalize_salary(jobposting: Dict, soup: Optional[BeautifulSoup] = None) -> str:
    """Extract salary using ITviec's own data sources in priority order."""
    if soup is not None:
        salary_range = _extract_salary_metadata_from_dom(soup)
        if salary_range:
            return salary_range

        salary_box = soup.select_one(".salary.text-success-color")
        if salary_box:
            salary_text = _normalize_salary_phrase(salary_box.get_text(" ", strip=True))
            if salary_text != "Unknown":
                return salary_text

    base_salary = jobposting.get("baseSalary") if isinstance(jobposting, dict) else None
    base_salary_text = _format_base_salary(base_salary)
    if base_salary_text:
        return base_salary_text

    return _strict_salary_regex_fallback(soup)


def extract_salary_from_dom(soup: Optional[BeautifulSoup]) -> str:
    """Extract salary from rendered ITviec DOM without scanning unrelated content first."""
    if not soup:
        return "Unknown"

    salary_range = _extract_salary_metadata_from_dom(soup)
    if salary_range:
        return salary_range

    salary_box = soup.select_one(".salary.text-success-color")
    if salary_box:
        salary_text = _normalize_salary_phrase(salary_box.get_text(" ", strip=True))
        if salary_text != "Unknown":
            return salary_text

    jobposting = parse_jobposting_jsonld(soup)
    if jobposting:
        base_salary_text = _format_base_salary(jobposting.get("baseSalary"))
        if base_salary_text:
            return base_salary_text

    return _strict_salary_regex_fallback(soup)


# =====================================
# Location parsing
# =====================================

CITY_MAP = {
    "hà nội": "HN",
    "ha noi": "HN",
    "hn": "HN",
    "hồ chí minh": "HCM",
    "ho chi minh": "HCM",
    "hcm": "HCM",
    "đà nẵng": "DN",
    "da nang": "DN",
    "dn": "DN",
}


def normalize_location(jobposting: Dict, soup: Optional[BeautifulSoup] = None) -> str:
    """Return comma-separated normalized locations (e.g., 'HN,HCM,Remote')."""
    locs: List[str] = []
    job_location = jobposting.get("jobLocation") or []
    if isinstance(job_location, dict):
        job_location = [job_location]

    def is_detailed_address(txt: str) -> bool:
        low = txt.lower()
        if "not available" in low:
            return True
        if re.search(r"\b(số|tòa|phường|ward|street|đường|floor|khu|building)\b", low):
            return True
        if re.search(r"\d+\s*(?:/|-|,|\.)?\d*", low) and any(c.isdigit() for c in low):
            return True
        return False

    for place in job_location:
        if not isinstance(place, dict):
            continue
        address = place.get("address") or {}
        for field in ("addressRegion", "addressLocality", "streetAddress"):
            value = address.get(field)
            if not value:
                continue
            text = clean_text(value)
            low = text.lower()
            if is_detailed_address(text):
                continue
            # map known city names
            for city_key, code in CITY_MAP.items():
                if city_key in low and code not in locs:
                    locs.append(code)
                    break
            else:
                if len(text) <= 30 and text not in locs:
                    locs.append(text)

    # fallbacks: scan page text for city tokens
    if not locs and soup is not None:
        page = clean_text(soup.get_text(separator=" \n ")).lower()
        if "remote" in page and "Remote" not in locs:
            locs.append("Remote")
        if "hybrid" in page and "Hybrid" not in locs:
            locs.append("Hybrid")
        for city_key, code in CITY_MAP.items():
            if city_key in page and code not in locs:
                locs.append(code)

    # dedupe preserving order and remove empty
    final = []
    for l in locs:
        if l and l not in final:
            final.append(l)
    return ",".join(final)


# =====================================
# Skills extraction
# =====================================


def normalize_skills(jobposting: Dict, title: str, description: str) -> List[str]:
    """Return deduped list of skills from JobPosting or by scanning text for KNOWN_SKILLS."""
    skills_value = jobposting.get("skills") or jobposting.get("skillsRequired")
    skills: List[str] = []
    if isinstance(skills_value, str):
        skills = [clean_text(s) for s in re.split(r"[,;/\n]", skills_value) if s.strip()]
    elif isinstance(skills_value, list):
        for item in skills_value:
            if isinstance(item, str):
                skills.append(clean_text(item))
            elif isinstance(item, dict):
                skill_text = item.get("name") or item.get("@id") or item.get("value")
                if skill_text:
                    skills.append(clean_text(skill_text))
    # fallback: scan title + description for known skills
    if not skills:
        txt = f"{title} {description}".lower()
        found: List[str] = []
        for sk in KNOWN_SKILLS:
            pattern = r"\b" + sk.replace("+", "\\+").replace("#", "\\#") + r"\b"
            if re.search(pattern, txt, flags=re.IGNORECASE):
                found.append(sk)
        skills = [s.capitalize() for s in dict.fromkeys(found)]  # dedupe preserving order
    # final cleanup: remove short navigation noise
    cleaned = []
    for s in skills:
        s_clean = re.sub(r"[^\w\+\#\s]", "", s).strip()
        if len(s_clean) > 1 and s_clean.lower() not in ["apply", "login", "jobs"]:
            cleaned.append(s_clean)
    return cleaned


# =====================================
# Description cleaning
# =====================================

START_SECTIONS = [
    r"Job Description", r"Job Responsibilities", r"Responsibilities", r"Requirements",
    r"Mô tả công việc", r"Yêu cầu công việc", r"Quyền lợi", r"The Job"
]
END_PATTERNS = [
    r"Best IT Companies", r"Reviews include", r"FanPage", r"Ai yêu Miền", r"This website uses a security service",
    r"MB Bank yêu cầu ứng viên"  # known marketing block
]


def extract_description(jobposting: Dict) -> str:
    """Return cleaned description keeping only job-relevant sections."""
    raw = jobposting.get("description") or ""
    text = BeautifulSoup(html_unescape(raw), "html.parser").get_text("\n", strip=True)
    text = re.sub(r"\r\n|\r", "\n", text)
    # If any stop marker occurs, cut everything after the earliest marker
    stop_markers = [
        "MB Bank yêu cầu ứng viên", "Thông tin cá nhân", "Nguồn Tuyển dụng",
        "Vì sao Bạn nên đảm bảo đầy đủ thông tin", "Hồ sơ của Bạn sẽ được đánh giá",
        "Chủ động liên hệ phỏng vấn", "Ứng viên vui lòng kiểm tra email",
        "Cập nhật ngay thông tin mới nhất", "FanPage", "Ai yêu Miền Bắc",
        "Ai yêu Miền Trung", "Ai yêu Miền Nam"
    ]

    lower_text = text.lower()
    earliest_cut = None
    for marker in stop_markers:
        idx = lower_text.find(marker.lower())
        if idx != -1 and (earliest_cut is None or idx < earliest_cut):
            earliest_cut = idx

    if earliest_cut is not None:
        text = text[:earliest_cut]

    # Prefer starting at a clear job section header; otherwise keep whole text but filter
    start_idx = None
    for pat in START_SECTIONS:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            start_idx = m.start()
            break
    clipped = text if start_idx is None else text[start_idx:]

    # strip known marketing/end patterns
    additional_end_patterns = [
        r"Ai yêu Miền Bắc hơn MBers", r"Ai yêu Miền Trung hơn MBers", r"Ai yêu Miền Nam hơn MBers",
        r"Cập nhật ngay thông tin", r"Cập nhật ngay", r"FanPage"
    ]
    for pat in END_PATTERNS + additional_end_patterns:
        m = re.search(pat, clipped, flags=re.IGNORECASE)
        if m:
            clipped = clipped[: m.start()]
            break

    # keep only allowed sections or lines that are likely part of job description
    lines = [l.strip() for l in clipped.split("\n") if l.strip()]
    filtered = []
    for line in lines:
        low = line.lower()
        # drop marketing / update / fanpage lines
        if any(token in low for token in ["fanpage", "best it companies", "reviews include", "sign in to view salary", "cập nhật ngay", "ai yêu miền"]):
            continue
        # drop contact/application procedure noise
        if any(token in low for token in ["ứng viên", "hồ sơ", "thông tin cá nhân", "số điện thoại", "email", "nguồn tuyển dụng", "ứng tuyển", "liên hệ", "kiểm tra email", "phỏng vấn"]):
            continue
        # drop address / building / ward details
        if re.search(r"\b(số|tòa|phường|ward|đường|street|building|floor|khu)\b", low):
            continue
        # skip very short noise lines
        if len(line) < 10:
            continue
        filtered.append(line)

    out = "\n".join(filtered).strip()
    # ensure we keep only meaningful job sections; cap length
    return out[:16000]


# =====================================
# URL collection
# =====================================


def get_job_urls_itviec(query: str = "", pages: Optional[int] = LISTING_PAGES) -> List[str]:
    """Scrape itviec search pages and return candidate job detail URLs.

    The function opens listing pages in Selenium first, then falls back to
    requests if the rendered page does not yield enough job URLs.
    """
    urls: List[str] = []
    # If pages is None, attempt to crawl all pages until the last page is detected
    crawl_all = pages is None
    pages_param = pages if pages is not None else MAX_LISTING_PAGES_LIMIT
    if crawl_all:
        pages_param = min(pages_param, ALL_MAX_LISTING_PAGES)
    consecutive_empty = 0
    detected_last_page: Optional[int] = None

    for page in range(1, pages_param + 1):
        print(f"[itviec] GET PAGE {page}")
        try:
            if query:
                listing_url = f"{BASE}/it-jobs?page={page}&query={requests.utils.quote(query)}&source=search_job"
            else:
                listing_url = f"{BASE}/it-jobs?page={page}&source=search_job"

            rendered_soup = fetch_listing_page_selenium(listing_url)
            page_urls: List[str] = []

            if rendered_soup is not None:
                page_urls = extract_listing_job_urls(rendered_soup)

            # If we found very few URLs, try fallback to raw requests HTML
            if len(page_urls) < LISTING_URL_MIN_CANDIDATES:
                resp = requests.get(listing_url, headers=HEADERS, timeout=15)
                soup = BeautifulSoup(resp.text, "html.parser")
                fallback_urls = extract_listing_job_urls(soup)
                if len(fallback_urls) > len(page_urls):
                    page_urls = fallback_urls

            # If crawling all pages, attempt to detect the last page number from pagination links
            if crawl_all and rendered_soup is not None:
                try:
                    page_nums: List[int] = []
                    for a in rendered_soup.find_all("a", href=True):
                        m = re.search(r"[?&]page=(\d+)", a["href"])
                        if m:
                            page_nums.append(int(m.group(1)))
                    if page_nums:
                        max_page = max(page_nums)
                        # if we detect a last page smaller than our current cap, set it
                        if detected_last_page is None or max_page > detected_last_page:
                            detected_last_page = max_page
                            # tighten the crawling upper bound but respect global safety cap
                            pages_param = min(max_page, MAX_LISTING_PAGES_LIMIT)
                except Exception:
                    pass

            # If this page appears to be empty (no URLs) or a dead page, track consecutive empties
            if not page_urls:
                # check for explicit dead page marker
                dead = False
                try:
                    if rendered_soup is not None and is_dead_job_page(rendered_soup):
                        dead = True
                except Exception:
                    dead = False
                consecutive_empty += 1
            else:
                consecutive_empty = 0

            # Stop if we've seen several consecutive empty/dead pages (likely beyond last page)
            if consecutive_empty >= 3:
                print(f"[itviec] stopping: {consecutive_empty} consecutive empty pages at page={page}")
                break

            if not page_urls:
                print("[itviec] NO JOB URLS ON PAGE:", page)
                continue

            for full in page_urls:
                if full not in urls:
                    urls.append(full)
            # Lightweight quick-crawl for discovered URLs on this page using requests
            try:
                for u in list(page_urls):
                    if u in _seen_urls:
                        continue
                    # attempt a fast requests-only crawl to populate autosave quickly
                    try:
                        quick = quick_crawl(u)
                        if quick:
                            _all_jobs.append(quick)
                            _seen_urls.add(u)
                    except Exception:
                        pass
                # write full autosave after processing this page so you can observe job rows
                autosave_now(AUTOSAVE_PATH)
            except Exception:
                pass
            # (autosave_urls/progress removed) -- no per-page URL files
        except Exception as e:
            print("[itviec] ERROR:", e)
        time.sleep(random.uniform(0.2, 0.6))
    return urls


def collect_job_urls_itviec(target_urls: int = TARGET_JOB_URLS, skip_all: bool = False) -> List[str]:
    """Collect unique Itviec job URLs across several search queries until the target is reached."""
    collected: List[str] = []
    seen: set = set()

    # Crawl ALL first, then the remaining queries. When resuming from an autosave
    # that already contains rows, skip ALL and continue with the next query.
    ordered_queries = [query for query in LISTING_QUERIES if query]
    if not skip_all:
        ordered_queries = [""] + ordered_queries
    else:
        print("[itviec] RESUME MODE: skipping ALL and continuing with the next queries")

    for index, query in enumerate(ordered_queries, start=1):
        if len(collected) >= target_urls:
            break

        label = query or "ALL"
        print(f"[itviec] QUERY {index}/{len(ordered_queries)}: {label}")
        if not query:
            pages_arg = ALL_MAX_LISTING_PAGES
        else:
            pages_arg = QUERY_PAGE_CAPS.get(query, LISTING_PAGES)
        before_count = len(collected)
        for url in get_job_urls_itviec(query=query, pages=pages_arg):
            if url in seen:
                continue
            seen.add(url)
            collected.append(url)
            if len(collected) >= target_urls:
                break
        print(f"[itviec] QUERY DONE: {label} (+{len(collected) - before_count} new URLs, total={len(collected)})")

    return collected


# =====================================
# Resume / Autosave
# =====================================


def load_autosave(path: str = AUTOSAVE_PATH) -> None:
    """Load autosave CSV to resume previous run if present."""
    if not os.path.exists(path):
        return
    if os.path.getsize(path) == 0:
        print(f"Autosave file is empty, starting fresh: {path}")
        return
    try:
        df = pd.read_csv(path)
        if df.empty and len(df.columns) == 0:
            print(f"Autosave file has no columns, starting fresh: {path}")
            return
        if "url" not in df.columns:
            print(f"Autosave file missing url column, starting fresh: {path}")
            return
        for _, row in df.iterrows():
            _all_jobs.append(row.to_dict())
            _seen_urls.add(row.get("url"))
        print(f"Resumed from {path}: {_all_jobs and len(_all_jobs) or 0} jobs")
    except pd.errors.EmptyDataError:
        print(f"Autosave file is empty, starting fresh: {path}")
    except Exception as e:
        print("Failed to load autosave:", e)


def autosave_now(path: str = AUTOSAVE_PATH) -> None:
    """Write current jobs to autosave CSV."""
    try:
        # sanitize text fields so each job occupies exactly one CSV row
        df = pd.DataFrame(_all_jobs, columns=AUTOSAVE_COLUMNS)
        for col in ["title", "company", "salary", "location", "description", "skills"]:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).apply(lambda s: re.sub(r"\s+", " ", s.replace('\n', ' | ')).strip())

        tmp = path + ".tmp"
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        try:
            # atomic replace to avoid partial writes
            os.replace(tmp, path)
        except Exception:
            # fallback: try remove and rename
            try:
                if os.path.exists(path):
                    os.remove(path)
                os.replace(tmp, path)
            except Exception:
                pass
        print(f"AUTOSAVE WRITTEN: {path} ({len(df)} rows)")
    except Exception as e:
        print("Autosave failed:", e)


def ensure_autosave_file(path: str = AUTOSAVE_PATH) -> None:
    """Create an empty autosave file with the correct header if it does not exist."""
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return
    try:
        pd.DataFrame(columns=AUTOSAVE_COLUMNS).to_csv(path, index=False, encoding="utf-8-sig")
        print(f"AUTOSAVE INITIALIZED: {path}")
    except Exception as e:
        print("Failed to initialize autosave:", e)





# =====================================
# Crawl single job
# =====================================


def fetch_page_requests(url: str, timeout: int = 15) -> Optional[BeautifulSoup]:
    """Fetch page using requests and return BeautifulSoup if successful."""
    try:
        sess = REQUESTS_SESSION or requests
        # when using requests module directly, call requests.get; when using a Session, use its get
        if isinstance(sess, requests.Session):
            resp = sess.get(url, headers=HEADERS, timeout=timeout)
        else:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return None
        return BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return None


def quick_crawl(url: str) -> Optional[Dict]:
    """Lightweight crawl using requests only to extract JobPosting JSON-LD and return a job dict.

    This is used during listing collection to populate `autosave.csv` per listing page.
    It avoids creating Selenium drivers and only returns results when JSON-LD is present.
    """
    try:
        soup = fetch_page_requests(url)
        if not soup:
            return None
        if is_dead_job_page(soup):
            return None
        jobposting = parse_jobposting_jsonld(soup)
        if not jobposting:
            return None
        title = clean_text(jobposting.get("title") or "")
        if not title:
            h1 = soup.find("h1")
            if h1:
                title = clean_text(h1.get_text())
        company = clean_text((jobposting.get("hiringOrganization") or {}).get("name"))
        salary = normalize_salary(jobposting, soup)
        # If salary is hidden in JSON-LD, try a lightweight Selenium fetch to see rendered DOM
        if salary == "Hidden":
            try:
                rendered = fetch_page_selenium(url)
                if rendered:
                    # try to parse JSON-LD from rendered DOM
                    jp2 = parse_jobposting_jsonld(rendered)
                    if jp2:
                        salary = normalize_salary(jp2, rendered)
                    if salary == "Hidden":
                        # fallback heuristics from DOM
                        dom_salary = extract_salary_from_dom(rendered)
                        if dom_salary and dom_salary not in ("Unknown", "Hidden"):
                            salary = dom_salary
            except Exception:
                pass
        location = normalize_location(jobposting, soup)
        description = extract_description(jobposting)
        skills_list = normalize_skills(jobposting, title, description)
        return {
            "title": title,
            "company": company,
            "salary": salary,
            "location": location,
            "description": description,
            "skills": ", ".join(skills_list[:10]),
            "url": url,
            "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception:
        return None


def fetch_page_selenium(url: str) -> Optional[BeautifulSoup]:
    """Fetch page using a per-thread selenium driver and return BeautifulSoup."""
    try:
        driver = get_thread_driver()
        driver.get(url)
        # minimal sleep to allow dynamic JSON-LD to load
        time.sleep(random.uniform(0.8, 1.5))
        html = driver.page_source
        if "403 Forbidden" in html or "This website uses a security service" in html:
            return None
        return BeautifulSoup(html, "html.parser")
    except Exception:
        return None


def valid_job_url(url: str) -> bool:
    """Return True if URL looks like a job detail page on itviec."""
    if not url.startswith(BASE):
        return False
    if "/it-jobs/" not in url:
        return False
    if re.search(r"-\d+(?:\?|$)", url):
        return True
    return False


def crawl_job(url: str) -> Optional[Dict]:
    """Crawl a single job URL.

    Strategy: prefer requests; parse JSON-LD; if missing then fallback to selenium.
    """
    if not valid_job_url(url):
        return None
    try:
        # try requests first
        soup = fetch_page_requests(url)
        if is_dead_job_page(soup):
            print("SKIP DEAD JOB:", url)
            _stats["errors"] += 1
            return None
        jobposting = parse_jobposting_jsonld(soup) if soup else None
        used_selenium = False
        if not jobposting:
            # fallback to selenium
            soup = fetch_page_selenium(url)
            if is_dead_job_page(soup):
                print("SKIP DEAD JOB:", url)
                _stats["errors"] += 1
                return None
            used_selenium = True
            if soup:
                jobposting = parse_jobposting_jsonld(soup)
        if not jobposting:
            # skip pages without JobPosting
            _stats["errors"] += 1
            return None
        # title
        title = clean_text(jobposting.get("title") or "")
        if not title and soup:
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(strip=True)
        company = clean_text((jobposting.get("hiringOrganization") or {}).get("name"))
        # salary
        salary = normalize_salary(jobposting, soup)
        # If salary is hidden in JSON-LD, retry with Selenium-rendered DOM to capture dynamic salary
        if salary == "Hidden":
            try:
                rendered = fetch_page_selenium(url)
                if rendered:
                    # prefer JSON-LD from rendered DOM
                    jp2 = parse_jobposting_jsonld(rendered)
                    if jp2:
                        salary = normalize_salary(jp2, rendered)
                    if salary == "Hidden":
                        # fallback heuristics
                        dom_salary = extract_salary_from_dom(rendered)
                        if dom_salary and dom_salary not in ("Unknown", "Hidden"):
                            salary = dom_salary
            except Exception:
                pass
        # location
        location = normalize_location(jobposting, soup)
        # description
        description = extract_description(jobposting)
        # skills
        skills_list = normalize_skills(jobposting, title, description)
        result = {
            "title": title,
            "company": company,
            "salary": salary,
            "location": location,
            "description": description,
            "skills": ", ".join(skills_list[:10]),
            "url": url,
            "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        _stats["success"] += 1
        return result
    except Exception as e:
        _stats["errors"] += 1
        print("CRAWL ERROR:", url, e)
        return None


# =====================================
# Main runner
# =====================================


def main() -> None:
    """Main entry: collect URLs, crawl with thread pool, autosave and final save."""
    ensure_autosave_file(AUTOSAVE_PATH)
    load_autosave(AUTOSAVE_PATH)
    skip_all = len(_all_jobs) > 0

    # Try to restore a saved requests session from cookies; if none, open browser for manual login
    restored = try_restore_session_from_cookies()
    if restored:
        print("Restored requests session from saved cookies.")
    else:
        print("No valid saved cookies found. Opening browser to allow manual login...")
        # This will open a Selenium browser and wait for you to log in, then export cookies
        try:
            manual_login_and_export()
        except Exception as e:
            print("Manual login step failed:", e)
    urls = collect_job_urls_itviec(target_urls=TARGET_JOB_URLS, skip_all=skip_all)
    # filter and dedupe
    unique_urls: List[str] = []
    for u in urls:
        if u not in _seen_urls and valid_job_url(u):
            unique_urls.append(u)
            _seen_urls.add(u)
    _stats["total_urls"] = len(unique_urls)
    print("\nTOTAL URLS (itviec):", _stats["total_urls"])
    if _stats["total_urls"] < TARGET_JOB_URLS:
        print(
            f"[WARN] Source only provided {_stats['total_urls']} unique job URLs, "
            f"below target {TARGET_JOB_URLS}."
        )
    start = datetime.now()
    # autosave immediately so the file is visible even before the first successful crawl
    autosave_now(AUTOSAVE_PATH)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(crawl_job, u): u for u in unique_urls}
        completed = 0
        for fut in as_completed(futures):
            url = futures[fut]
            try:
                r = fut.result()
            except Exception as e:
                r = None
                print("Worker exception for:", url, e)
            if r:
                _all_jobs.append(r)
                completed += 1
                if len(_all_jobs) % AUTOSAVE_INTERVAL == 0:
                    autosave_now(AUTOSAVE_PATH)
                    print("AUTOSAVED", len(_all_jobs))
            # small backoff between jobs
            time.sleep(random.uniform(0.1, 0.4))
    # write whatever has been collected, even if fewer than AUTOSAVE_INTERVAL jobs succeeded
    if _all_jobs:
        autosave_now(AUTOSAVE_PATH)
    # teardown drivers
    for d in _all_drivers:
        safe_quit_driver(d)
    # final save: dedupe by url
    df = pd.DataFrame(_all_jobs)
    if not df.empty:
        df.drop_duplicates(subset=["url"], inplace=True)
    # sanitize text fields before final save to ensure one job per CSV row
    if not df.empty:
        for col in ["title", "company", "salary", "location", "description", "skills"]:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).apply(lambda s: re.sub(r"\s+", " ", s.replace('\n', ' | ')).strip())
    tmp_final = FINAL_CSV + ".tmp"
    df.to_csv(tmp_final, index=False, encoding="utf-8-sig")
    try:
        os.replace(tmp_final, FINAL_CSV)
    except Exception:
        try:
            if os.path.exists(FINAL_CSV):
                os.remove(FINAL_CSV)
            os.replace(tmp_final, FINAL_CSV)
        except Exception:
            # last resort: write directly
            df.to_csv(FINAL_CSV, index=False, encoding="utf-8-sig")
    elapsed = (datetime.now() - start).total_seconds()
    total_done = len(df) if not df.empty else 0
    minutes = elapsed / 60 if elapsed > 0 else 1
    rate = total_done / minutes
    print("\nDONE")
    print("TOTAL JOBS CRAWLED:", total_done)
    print("SUCCESS:", _stats["success"], "ERRORS:", _stats["errors"])
    print(f"RATE: {rate:.2f} job/min")
    print("Elapsed:", f"{elapsed:.1f}s")


if __name__ == "__main__":
    main()
