#!/usr/bin/env python3
"""
behance_users_scraper.py

Usage:
  python scrapers/behance_users_scraper.py --query "web design" --country IN --max-users 200 --output out/behance_users.csv

What it does:
- Searches Behance Users using the users search URL with field + country filters.
- Scrolls/paginates collecting user profile URLs up to max users.
- Visits each user profile and extracts:
    profile_name, username, profile_url, is_team, appreciations, followers,
    website, email (if visible), services, tools, last_active, score, source, raw_html(optional)
- Scores each profile on 3 signals (studio/team OR individual >=100 appreciations OR active within 6 months).
- Writes CSV and JSON output.

Notes:
- Run small batches first (--max-users 10) to confirm selectors.
- Must have Playwright installed and browsers downloaded:
    pip install -r requirements.txt
    playwright install
"""

import argparse
import asyncio
import csv
import json
import random
import re
import time
import os
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

from playwright.async_api import async_playwright, Page, Browser, TimeoutError as PWTimeoutError

# -------------------------
# Configuration
# -------------------------
SCROLL_PAUSE = 1.0
REQUEST_DELAY_MIN = 0.8
REQUEST_DELAY_MAX = 2.2
NAV_TIMEOUT = 30_000  # ms
DEFAULT_MAX_USERS = 100
SIX_MONTHS_AGO = datetime.utcnow() - timedelta(days=180)


# -------------------------
# Utility helpers
# -------------------------
def safe_text(s: Optional[str]) -> str:
    return s.strip() if s else ""


def parse_int(value: Optional[str]) -> int:
    if not value:
        return 0
    try:
        # remove commas and non-digits
        v = re.sub(r"[^\d]", "", value)
        return int(v) if v else 0
    except Exception:
        return 0


async def safe_eval(page: Page, js: str, retries: int = 6, sleep: float = 0.3):
    for _ in range(retries):
        try:
            return await page.evaluate(js)
        except Exception:
            await asyncio.sleep(sleep)
    return None


async def safe_query_text(page: Page, selector: str) -> Optional[str]:
    try:
        el = await page.query_selector(selector)
        if not el:
            return None
        txt = await el.inner_text()
        return safe_text(txt)
    except Exception:
        return None


async def element_attr(page: Page, selector: str, attr: str) -> Optional[str]:
    try:
        el = await page.query_selector(selector)
        if not el:
            return None
        return await el.get_attribute(attr)
    except Exception:
        return None


def iso_or_none(dtstr: Optional[str]) -> Optional[str]:
    if not dtstr:
        return None
    # try parse some common formats, otherwise return raw
    try:
        # handle YYYY-MM-DD or ISO
        parsed = datetime.fromisoformat(dtstr)
        return parsed.isoformat()
    except Exception:
        try:
            parsed = datetime.strptime(dtstr.strip(), "%b %d, %Y")
            return parsed.isoformat()
        except Exception:
            return dtstr


# -------------------------
# Scoring
# -------------------------
def compute_score(is_team: bool, appreciations: int, last_active_iso: Optional[str]) -> int:
    """
    Score rules (as agreed):
     - Studio/team profile => +1 (is_team True)
     - Individual with strong portfolio (>=100 appreciations) => +1
     - Actively posting within last 6 months => +1
    Returns score 0..3
    """
    score = 0
    if is_team:
        score += 1
    if appreciations >= 100:
        score += 1
    try:
        if last_active_iso:
            try:
                last_dt = datetime.fromisoformat(last_active_iso)
            except Exception:
                last_dt = datetime.strptime(last_active_iso[:10], "%Y-%m-%d")
            if last_dt >= SIX_MONTHS_AGO:
                score += 1
    except Exception:
        # ignore parse failures
        pass
    return score


# -------------------------
# Search page helpers (collect user links)
# -------------------------
async def ensure_scroll_load_users(page: Page, timeout: int = 20):
    """Scroll users listing page until it stops growing or timeout."""
    start = time.time()
    last_height = await safe_eval(page, "() => document.body.scrollHeight") or 0
    while True:
        try:
            await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            pass
        await asyncio.sleep(SCROLL_PAUSE)
        new_height = await safe_eval(page, "() => document.body.scrollHeight") or 0
        if new_height == last_height:
            # try one more time after short wait
            await asyncio.sleep(0.6)
            new_height = await safe_eval(page, "() => document.body.scrollHeight") or 0
            if new_height == last_height:
                break
        last_height = new_height
        if time.time() - start > timeout:
            break


async def extract_user_links_from_search(page: Page) -> List[str]:
    """
    Extract ONLY valid user profile URLs.
    Filters out:
        - /about
        - /membership
        - /careers
        - marketing/footer links
        - project/gallery URLs
    Ensures username format.
    """
    links = set()

    try:
        anchors = await page.query_selector_all("a[href]")
        for a in anchors:
            try:
                href = await a.get_attribute("href")
                if not href:
                    continue

                # Normalize to absolute
                if href.startswith("/"):
                    href = "https://www.behance.net" + href

                # Must be Behance domain
                if "behance.net" not in href:
                    continue

                # Block junk URLs
                blocked_paths = [
                    "/about", "/membership", "/careers", "/jobs",
                    "/company", "/pro", "/search", "/onboarding",
                    "/onboard", "/gallery/", "/projects/"
                ]
                if any(bp in href for bp in blocked_paths):
                    continue

                # Must match the format: behance.net/<username> only
                # Username cannot contain slashes or query params
                m = re.match(r"^https?://(www\.)?behance\.net/([A-Za-z0-9._-]{2,60})$", href)
                if m:
                    links.add(href)

            except Exception:
                continue

    except Exception:
        pass

    return list(links)


# -------------------------
# Extract user profile data
# -------------------------
async def extract_user_data(page: Page, profile_url: str, visited) -> Dict:
    out = {
        "profile_name": "",
        "username": "",
        "profile_url": profile_url,
        "is_team": False,
        "appreciations": 0,
        "followers": 0,
        "website": "",
        "email": "",
        "services": "",
        "tools": "",
        "last_active": "",
        "score": 0,
        "source": "behance_users",
        "raw_html": ""
    }

    try:
        await page.goto(profile_url, timeout=NAV_TIMEOUT)
    except PWTimeoutError:
        try:
            await page.goto(profile_url, timeout=NAV_TIMEOUT * 2)
        except Exception:
            return out

    # allow rendering
    await asyncio.sleep(0.8)

    # profile name and username
    name = await safe_query_text(page, "h1, .Profile-userName, .Profile-header h1") or ""
    if not name:
        # fallback: meta og:title or title tag
        try:
            meta_title = await page.get_attribute('meta[property="og:title"]', 'content')
            name = safe_text(meta_title) if meta_title else ""
        except Exception:
            name = ""
    out["profile_name"] = name

    # username extraction from URL
    try:
        parts = profile_url.rstrip("/").split("/")
        out["username"] = parts[-1] if parts else ""
    except Exception:
        print(f"Ignoring this profile - unable to parse username {profile_url}")
        return
    
    if out["username"] in visited:
        print(f"[skip] Already scraped {out["username"]}")
        return

    print(f"[+] Scraping {out["username"]} → {profile_url}")

    # detect team/org: Behance shows team badges or multiple owners; try several heuristics
    is_team = False
    try:
        # 1) Explicit team label (common)
        possible_team_text = await safe_query_text(page, ".Profile-header .profile-type, .profile-type, .Profile--type, .Profile-subtitle")
        if possible_team_text and "team" in possible_team_text.lower():
            is_team = True

        # 2) Look for "Members" or multiple avatars in header
        # count avatar images in header region
        try:
            header_avatars = await page.query_selector_all(".Profile-header img, .Profile-profileImg img, .Profile-avatars img, .Profile-header .avatars img")
            if header_avatars and len(header_avatars) >= 2:
                is_team = True
        except Exception:
            pass

        # 3) Fallback: scan raw HTML for keywords that indicate organization/team
        if not is_team:
            page_html = await page.content()
            if re.search(r"\b(team|members|agency|studios|collective|studio)\b", page_html, flags=re.I):
                # ensure this isn't just the word "studio" inside a project title; narrow by checking profile header area
                # quick heuristic: presence of "Members" or "Team" near "profile" meta
                if re.search(r"(Members|Team|Members:|Team members|Our team)", page_html, flags=re.I):
                    is_team = True
    except Exception:
        is_team = is_team
    out["is_team"] = bool(is_team)

    # appreciations & followers (common counters)
    appreciations = 0
    followers = 0
    try:
        # Try several DOM selectors first (fast)
        cand_selectors = [
            ".Profile-statistics [data-stat='appreciations']",
            ".Profile-stats .appreciations",
            ".Appreciations-count",
            "a[href$='/appreciations']",
            ".Profile-statistics .Appreciations",
            ".stats .appreciations",
            ".Profile-stats .Profile-stat--appreciations"
        ]
        for sel in cand_selectors:
            txt = await safe_query_text(page, sel)
            if txt:
                appreciations = parse_int(txt)
                if appreciations > 0:
                    break

        # followers selectors
        follower_selectors = [
            ".Profile-statistics [data-stat='followers']",
            ".Followers-count",
            ".followers",
            ".Profile-stats .followers",
            ".stats .followers",
            "a[href$='/followers']"
        ]
        for sel in follower_selectors:
            txt = await safe_query_text(page, sel)
            if txt:
                followers = parse_int(txt)
                if followers > 0:
                    break

        # If DOM selectors failed (most likely), fallback to regex on page HTML
        if appreciations == 0 or followers == 0:
            html = await page.content()
            # regex patterns: look for number + label near it
            # examples: "1,234 appreciations", "Appreciations 1,234", "234 followers"
            app_patterns = [
                r"([\d,\.]{2,})\s+appreciations",
                r"appreciations\s*[:\-]?\s*([\d,\.]{1,})",
                r"Appreciations[:\s]*([\d,\.]+)"
            ]
            foll_patterns = [
                r"([\d,\.]{2,})\s+followers",
                r"followers\s*[:\-]?\s*([\d,\.]{1,})",
                r"Followers[:\s]*([\d,\.]+)"
            ]
            for pat in app_patterns:
                m = re.search(pat, html, flags=re.I)
                if m:
                    appreciations = parse_int(m.group(1))
                    break
            for pat in foll_patterns:
                m = re.search(pat, html, flags=re.I)
                if m:
                    followers = parse_int(m.group(1))
                    break

        # Final safety: if still zero, maybe the profile shows counts differently.
        # Try to locate any numeric counters in header and infer which is which by context.
        if appreciations == 0 or followers == 0:
            try:
                # collect candidate numeric spans inside header area
                header_html = await page.evaluate("() => document.querySelector('.Profile-header')?.innerText || ''")
                nums = re.findall(r"([\d,\.]+)", header_html)
                # heuristics: if there are two large numbers, first is appreciations, second followers
                if len(nums) >= 2:
                    if appreciations == 0:
                        appreciations = parse_int(nums[0])
                    if followers == 0:
                        followers = parse_int(nums[1])
            except Exception:
                pass

    except Exception:
        appreciations = appreciations
        followers = followers

    out["appreciations"] = appreciations
    out["followers"] = followers


    try:
        maybe_follow = await safe_query_text(page, ".Profile-statistics [data-stat='followers'], .Followers-count, .followers")
        if maybe_follow:
            followers = parse_int(maybe_follow)
    except Exception:
        followers = followers
    out["appreciations"] = appreciations
    out["followers"] = followers

    # --- SOCIAL & WEBSITE EXTRACTION ---
    website = ""
    instagram_url = ""
    linkedin_url = ""
    dribbble_url = ""
    twitter_url = ""
    x_url = ""
    behance_message_url = ""

    try:
        candidate_links = []

        # 1. New Behance verified social links (most reliable)
        social_links = await page.query_selector_all("a.VerifiedSocial-accountContent-g6W, a[class*='VerifiedSocial']")
        for el in social_links:
            try:
                href = await el.get_attribute("href")
                if href:
                    candidate_links.append(href)
            except:
                continue

        # 2. Legacy Behance containers (fallback for older profiles)
        legacy_selectors = [
            ".Profile-onTheWeb a[href]",
            ".Profile-links a[href]",
            ".Profile-info a[href]",
            ".Profile-content a[href]",
            ".UserInfo-onTheWeb a[href]",
            ".user-info a[href]",
        ]

        for sel in legacy_selectors:
            els = await page.query_selector_all(sel)
            for el in els:
                try:
                    href = await el.get_attribute("href")
                    if href:
                        candidate_links.append(href)
                except:
                    continue

        # remove adobe / behance / junk
        cleaned_links = []
        blocked_domains = [
            "adobe.com",
            "behance.net",
            "itunes.apple.com",
            "apps.apple.com",
            "play.google.com",
        ]

        for href in candidate_links:
            if any(block in href for block in blocked_domains):
                continue
            cleaned_links.append(href)

        # classify
        for href in cleaned_links:
            lower = href.lower()

            if "instagram.com" in lower and not instagram_url:
                instagram_url = href

            elif "linkedin.com" in lower and not linkedin_url:
                linkedin_url = href

            elif "dribbble.com" in lower and not dribbble_url:
                dribbble_url = href

            elif "twitter.com" in lower or "x.com" in lower:
                if "x.com" in lower:
                    x_url = href
                if not twitter_url:
                    twitter_url = href

            # first external non-social = website
            elif website == "" and lower.startswith("http"):
                website = href

        # Behance DM
        try:
            msg_btn = await page.query_selector("a[href*='/message']")
            if msg_btn:
                dm = await msg_btn.get_attribute("href")
                if dm.startswith("/"):
                    dm = "https://www.behance.net" + dm
                behance_message_url = dm
        except:
            pass

    except Exception as e:
        print("SOCIAL EXTRACTION ERROR:", e)

    out["website"] = website
    out["instagram_url"] = instagram_url
    out["linkedin_url"] = linkedin_url
    out["dribbble_url"] = dribbble_url
    out["twitter_url"] = twitter_url
    out["x_url"] = x_url
    out["behance_message_url"] = behance_message_url


    # --- DESCRIPTION & EMAIL EXTRACTION ---
        # --- DESCRIPTION + EMAIL EXTRACTION (v8 fully stable) ---

    description = ""
    email = ""

    try:
        # ALL possible description/bio selectors
        desc_selectors = [
            ".Profile-description",
            ".Profile-about",
            ".Profile-section--about",
            ".ProfileContent-description",
            ".Profile-infoContent",
            ".Profile-infoContainer",
            "[data-testid='profile-bio']",
            ".UserInfo-bio",
            ".UserInfo-text",
            ".UserInfo-description",
            ".UserInfo-readMoreOrLessContent-Ywr",
            ".ReadMore-content-F2D",
            "[class*='ReadMore']",
            "[class*='UserInfo']",
            "[class*='ProfileInfo']",
            ".ProfileInfo-description",
            ".ProfileInfo-panel",
            ".ProfileInfoPanel-content",
            ".ownersDetails-contents",
            ".ownersDetails",
            ".profileOwnerInfo",
        ]

        text_chunks = []

        for sel in desc_selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    txt = await el.inner_text()
                    if not txt:
                        continue
                    cleaned = txt.strip()

                    # Ignore tiny junk
                    if len(cleaned) < 10:
                        continue

                    # Ignore UI noise
                    blacklist = [
                        "Message", "Follow", "Tools", "On The Web",
                        "Stats", "Project Views", "Appreciations",
                        "Adobe", "Behance Pro", "Hire Me"
                    ]
                    if any(b.lower() in cleaned.lower() for b in blacklist):
                        continue

                    # Ignore image-noise
                    if "adobe-pro-bg" in cleaned.lower():
                        continue

                    text_chunks.append(cleaned)
            except:
                pass

        # combine unique chunks
        if text_chunks:
            uniq = []
            for t in text_chunks:
                if t not in uniq:
                    uniq.append(t)
            description = "\n\n".join(uniq)

        # ---------- EMAIL EXTRACTION ----------
        # strict email regex (only real TLDs)
        email_pattern = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.(com|co|in|io|ai|org|net|dev|studio|design|co\.in)"

        # Clean description from image noise
        if description:
            desc_clean = re.sub(r"[A-Za-z0-9._-]+\.(png|jpg|jpeg|svg|gif)", "", description, flags=re.I)
            desc_clean = re.sub(r"url\([^)]+\)", "", desc_clean, flags=re.I)

            m = re.search(email_pattern, desc_clean)
            if m:
                email = m.group(0)

        # Fallback → search HTML (only if email not found)
        if not email:
            html = await page.content()

            # Remove image assets
            html = re.sub(r"[A-Za-z0-9._-]+\.(png|jpg|jpeg|svg|gif)", "", html, flags=re.I)
            html = re.sub(r'src="[^"]+"', "", html, flags=re.I)
            html = re.sub(r"url\([^)]+\)", "", html, flags=re.I)

            m = re.search(email_pattern, html)
            if m:
                email = m.group(0)

    except Exception as e:
        print("DESCRIPTION/EMAIL EXTRACTION ERROR:", e)

    out["description"] = description
    out["email"] = email


    # services & tools (profile sections / text)
    services = ""
    tools = ""
    try:
        # many Behance profiles list "Services" or "Tools" in profile metadata
        # try to read metadata blocks
        meta_text = []
        blocks = await page.query_selector_all(".Profile-info, .Profile-details, .Profile-metadata, .Profile-about li, .Profile-about")
        for b in blocks[:12]:
            try:
                t = await b.inner_text()
                if t:
                    meta_text.append(safe_text(t))
            except Exception:
                continue
        combined = " | ".join(meta_text)
        # heuristics: split into services and tools by keywords
        if combined:
            # attempt to capture services lines (common words)
            # we'll fallback to storing combined in services
            services = combined
            # try to extract tools (Figma / Illustrator / Photoshop / Sketch / Webflow)
            tools_found = []
            for tool in ["Figma", "Illustrator", "Photoshop", "Sketch", "Webflow", "Shopify", "XD", "Adobe"]:
                if tool.lower() in combined.lower():
                    tools_found.append(tool)
            if tools_found:
                tools = ", ".join(tools_found)
    except Exception:
        services = services
        tools = tools
    out["services"] = safe_text(services)[:1000]
    out["tools"] = safe_text(tools)

    # last_active: find most recent project date on profile (best-effort)
    last_active_iso = ""
    try:
        # attempt to find time element in project list
        time_el = await page.query_selector("time, .Project-date, .project-date")
        if time_el:
            dt = await time_el.get_attribute("datetime") or await time_el.inner_text()
            last_active_iso = iso_or_none(safe_text(dt))
        else:
            # fallback: look for project card date text
            pd = await safe_query_text(page, ".ProjectCard-date, .project-date, .Project-meta time")
            last_active_iso = iso_or_none(pd)
    except Exception:
        last_active_iso = ""
    out["last_active"] = last_active_iso or ""

    # compute score
    try:
        out["score"] = compute_score(out["is_team"], out["appreciations"], out["last_active"])
    except Exception:
        out["score"] = 0

    return out


# -------------------------
# Main flow
# -------------------------
async def run_user_scrape(field: str, country: str, max_users: int, jsonl_file, visited, VISITED_PATH, headless: bool = True):
    print(f"[+] Starting Behance USERS scraper (field={field}, country={country}, max={max_users})")
    users_collected: List[str] = []
    users_data: List[Dict] = []

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/115.0 Safari/537.36"
            ),
            storage_state="../behance_state.json"  # use saved logged-in state
        )

        page = await context.new_page()

        # Build users search URL
        # Example: https://www.behance.net/search/users?field=web+design&country=IN&userAvailability=isAvailableFullTime
        q = field.replace(" ", "+")
        search_url = f"https://www.behance.net/search/users?field={q}&country={country}&userAvailability=isAvailableFullTime&sort=appreciations&time=all"
        print("[+] Loading search URL:", search_url)
        await page.goto(search_url, timeout=NAV_TIMEOUT)
        await asyncio.sleep(1.0)

        # Scroll & collect
        last_count = 0
        attempts = 0
        while len(users_collected) < max_users and attempts < 5:
            await ensure_scroll_load_users(page, timeout=7)
            links = await extract_user_links_from_search(page)
            for link in links:
                if link not in users_collected:
                    users_collected.append(link)
                    if len(users_collected) >= max_users:
                        break
            if len(users_collected) == last_count:
                attempts += 1
            else:
                last_count = len(users_collected)
                attempts = 0
            print(f"[+] Collected user profiles: {len(users_collected)} / {max_users}")
            await asyncio.sleep(0.6 + random.random() * 0.8)

        users_collected = users_collected[:max_users]
        print(f"[+] Final user profiles to visit: {len(users_collected)}")

        # Visit each user
        for idx, profile in enumerate(users_collected, start=1):
            try:
                print(f"[{idx}/{len(users_collected)}] Visiting: {profile}")
                data = await extract_user_data(page, profile, visited)
                if data:
                    jsonl_file.write(json.dumps(data, ensure_ascii=False) + "\n")
                    users_data.append(data)
                    visited.add(data["username"])
            except Exception as e:
                print("  (!) Error extracting profile:", e)
                traceback.print_exc()
            await asyncio.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))


        await context.close()
        await browser.close()

    json.dump(list(visited), open(VISITED_PATH, "w"))
    
    # # Save output
    # output.parent.mkdir(parents=True, exist_ok=True)
    # csv_path = output.with_suffix(".csv")
    # json_path = output.with_suffix(".json")

    # headers = [
    #     "profile_name", "username", "profile_url", "is_team",
    #     "appreciations", "followers",
    #     "website", "instagram_url", "linkedin_url", "dribbble_url",
    #     "twitter_url", "x_url", "behance_message_url",
    #     "services", "tools", "last_active", "score", "source",
    #     "description", "email"
    # ]


    # with open(csv_path, "w", newline="", encoding="utf-8") as f:
    #     writer = csv.DictWriter(f, fieldnames=headers)
    #     writer.writeheader()
    #     for r in users_data:
    #         # ensure keys exist
    #         row = {k: r.get(k, "") for k in headers}
    #         writer.writerow(row)

    # with open(json_path, "w", encoding="utf-8") as f:
    #     json.dump(users_data, f, ensure_ascii=False, indent=2)

    # print(f"[+] Saved CSV -> {csv_path}")
    # print(f"[+] Saved JSON -> {json_path}")
    print("[+] Done.")


# -------------------------
# CLI
# -------------------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--field", type=str, default="web design", help="Behance user field (e.g., 'web design')")
    ap.add_argument("--country", type=str, default="IN", help="Country code (e.g., IN)")
    ap.add_argument("--max-users", type=int, default=DEFAULT_MAX_USERS, help="Max user profiles to collect")
    ap.add_argument("--headless", action="store_true", help="Run browser headless (recommended)")
    return ap.parse_args()


def main():
    args = parse_args()

    # RAW OUTPUT DIRECTORY
    RAW_DIR = "data/raw"
    os.makedirs(RAW_DIR, exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    jsonl_path = os.path.join(RAW_DIR, f"behance_{today}.jsonl")

    # open file in append mode
    jsonl_file = open(jsonl_path, "a", encoding="utf-8")

    VISITED_PATH = "data/visited_usernames.json"

    # load visited usernames
    if os.path.isfile(VISITED_PATH):
        visited = set(json.load(open(VISITED_PATH)))
    else:
        visited = set()

    headless_flag = True if args.headless else True
    asyncio.run(run_user_scrape(args.field, args.country, args.max_users, jsonl_file, visited, VISITED_PATH, headless=headless_flag))

    jsonl_file.close()


if __name__ == "__main__":
    main()