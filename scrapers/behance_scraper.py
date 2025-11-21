#!/usr/bin/env python3
"""
Behance Scraper V2
- Correct project scraping
- Accurate studio/owner extraction
- Accurate website extraction via owner profile
- Studio-only filtering
- Stable infinite scroll
- Cleans tracking params
- Outputs CSV + JSON
"""

import argparse
import asyncio
import csv
import json
import random
import time
from pathlib import Path
from typing import List, Dict, Optional

from playwright.async_api import async_playwright, Page, Browser, TimeoutError as PWTimeoutError


# ============================================
# Utility
# ============================================

def safe_text(s: Optional[str]) -> str:
    return s.strip() if s else ""


async def safe_eval(page: Page, js_code: str, retries: int = 5):
    """Safely evaluate JS, retrying if navigation/context reset happens."""
    for _ in range(retries):
        try:
            return await page.evaluate(js_code)
        except Exception:
            await asyncio.sleep(0.3)
    return 0


async def ensure_scroll_load(page: Page, timeout: int = 30):
    """Stable infinite scroll that avoids 'execution context destroyed'."""
    start = time.time()
    last_height = await safe_eval(page, "() => document.body.scrollHeight")

    while True:
        try:
            await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            await asyncio.sleep(0.4)
            continue

        await asyncio.sleep(1.2)
        new_height = await safe_eval(page, "() => document.body.scrollHeight")

        if new_height == last_height:
            await asyncio.sleep(0.7)
            new_height = await safe_eval(page, "() => document.body.scrollHeight")
            if new_height == last_height:
                break

        last_height = new_height

        if time.time() - start > timeout:
            break


# ============================================
# Studio Detection Heuristics
# ============================================

STUDIO_KEYWORDS = [
    "studio", "agency", "collective", "team", "design studio", "creative studio",
    "branding studio", "creative agency", "design agency", "partners", "studios",
    "llp", "pvt", "co."
]


async def _get_text_snippet(page: Page, selector: str) -> str:
    try:
        el = await page.query_selector(selector)
        if el:
            return safe_text(await el.inner_text()).lower()
    except:
        pass
    return ""


async def is_likely_studio(page: Page, profile_url: str) -> bool:
    """Returns True if Behance profile looks like a studio."""
    if not profile_url:
        return False

    try:
        await page.goto(profile_url, timeout=30000)
        await asyncio.sleep(0.8)

        # Rule 1: keywords in full HTML
        html = (await page.content()).lower()
        if any(kw in html for kw in STUDIO_KEYWORDS):
            return True

        # Rule 2: keywords in bio sections
        bio_selectors = [
            'div.Profile-about', '.Profile-header', '.Profile-bio',
            '.ProfileDetails', '.Profile-info', 'section.Profile-bio'
        ]
        for sel in bio_selectors:
            text = await _get_text_snippet(page, sel)
            if any(kw in text for kw in STUDIO_KEYWORDS):
                return True

        # Rule 3: number of gallery projects
        projects = await page.query_selector_all('a[href*="/gallery/"]')
        if len(projects) >= 3:
            return True

    except:
        return False

    return False


# ============================================
# Extract project links
# ============================================

async def extract_project_links_from_search(page: Page) -> List[str]:
    """Extract real Behance project URLs (gallery pages only)."""
    anchors = await page.query_selector_all('a[href*="/gallery/"]')
    links = []

    for a in anchors:
        try:
            href = await a.get_attribute("href")
            if not href:
                continue

            if href.startswith("/"):
                href = "https://www.behance.net" + href

            if "/gallery/" in href and href not in links:
                href = href.split("?")[0]  # remove tracking params
                links.append(href)

        except:
            continue

    return links


# ============================================
# Extract project data
# ============================================

async def extract_project_data(page: Page, url: str) -> Optional[Dict]:
    out = {
        "studio_name": "",
        "project_title": "",
        "project_url": url,
        "owner_profile_url": "",
        "website": "",
        "tags": "",
        "publish_date": "",
        "source": "behance",
    }

    try:
        await page.goto(url, timeout=30000)
    except:
        return None

    await asyncio.sleep(1)

    # -------- PROJECT TITLE --------
    try:
        el = await page.query_selector("h1")
        if el:
            out["project_title"] = safe_text(await el.inner_text())
    except:
        pass

    # -------- OWNER / PROFILE --------
    owner_name = ""
    owner_profile = ""

    owner_selectors = [
        'a[data-testid="project-owner"]',
        'a[data-testid="owner-link"]',
        'a.UserInfo-link',
        'a[href*="/u/"]',
        'div.ProjectOwner a',
    ]

    for sel in owner_selectors:
        el = await page.query_selector(sel)
        if el:
            owner_name = safe_text(await el.inner_text())
            href = await el.get_attribute("href")

            if href:
                if href.startswith("/"):
                    owner_profile = "https://www.behance.net" + href
                else:
                    owner_profile = href
            break

    out["studio_name"] = owner_name
    out["owner_profile_url"] = owner_profile

    # -------- STUDIO FILTER --------
    is_studio = False
    try:
        if owner_profile:
            is_studio = await is_likely_studio(page, owner_profile)
        else:
            # fallback: title contains "studio"
            if "studio" in out["project_title"].lower():
                is_studio = True
    except:
        is_studio = False

    if not is_studio:
        print(f"  [-] Skipping individual: {url}")
        return None

    # -------- WEBSITE (from profile) --------
    website = ""
    try:
        if owner_profile:
            await page.goto(owner_profile, timeout=30000)
            await asyncio.sleep(0.8)

            links = await page.query_selector_all('a[href^="http"]:not([href*="behance.net"])')

            for a in links:
                href = await a.get_attribute("href")
                if href and href.startswith("http"):
                    website = href
                    break

            await page.goto(url, timeout=30000)
    except:
        pass

    out["website"] = website

    # -------- TAGS --------
    try:
        tag_els = await page.query_selector_all('a[href*="/search?"]')
        tags = []
        for t in tag_els:
            txt = safe_text(await t.inner_text())
            if txt and len(txt) <= 30:
                tags.append(txt)
        out["tags"] = ", ".join(tags[:6])
    except:
        pass

    # -------- DATE --------
    try:
        t = await page.query_selector("time")
        if t:
            date_val = await t.get_attribute("datetime") or await t.inner_text()
            out["publish_date"] = safe_text(date_val)
    except:
        pass

    return out


# ============================================
# Run Scraper
# ============================================

async def run_scrape(query: str, max_results: int, output: Path):
    print(f"[+] Starting Behance scraper for query: {query}")
    print(f"[+] Target studio results: {max_results}")

    results = []
    visited = set()
    all_links = []

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Load Behance search page
        search_url = f"https://www.behance.net/search?content=projects&search={query.replace(' ', '%20')}"
        print("[+] Loading search page:", search_url)

        await page.goto(search_url, timeout=30000)
        await asyncio.sleep(1.0)

        # INITIAL BATCH OF PROJECT LINKS
        print("[+] Collecting initial project links...")
        await ensure_scroll_load(page, timeout=20)
        initial_links = await extract_project_links_from_search(page)
        all_links.extend(initial_links)

        print(f"[+] Initial collected links: {len(all_links)}")

        saved = 0
        pointer = 0

        # MAIN LOOP → continue until we have EXACTLY `max_results` studios
        while saved < max_results:

            # Need more links?
            if pointer >= len(all_links):
                print("[+] Scrolling for more results...")
                await ensure_scroll_load(page, timeout=20)

                new_links = await extract_project_links_from_search(page)
                new_count = 0

                # Add new unique links
                for ln in new_links:
                    if ln not in visited and ln not in all_links:
                        all_links.append(ln)
                        new_count += 1

                print(f"[+] Found {new_count} new unique links (total: {len(all_links)})")

                # If NO new links found → Behance loaded everything
                if new_count == 0:
                    print("[!] No more new links available. Stopping early.")
                    break

            # Select next link to visit
            link = all_links[pointer]
            pointer += 1

            if link in visited:
                continue
            visited.add(link)

            print(f"[{saved}/{max_results}] Visiting:", link)

            try:
                data = await extract_project_data(page, link)

                if data:
                    results.append(data)
                    saved += 1
                    print(f"  [+] Saved studio {saved}/{max_results}")
                else:
                    print("  [-] Skipped (not a studio)")

            except Exception as e:
                print("   (!) Extraction error:", e)

            await asyncio.sleep(random.uniform(1.0, 2.0))

        # CLOSE BROWSER
        await browser.close()

    # SAVE OUTPUT FILES
    output.parent.mkdir(parents=True, exist_ok=True)

    csv_path = output.with_suffix(".csv")
    json_path = output.with_suffix(".json")

    headers = [
        "studio_name", "project_title", "project_url", "owner_profile_url",
        "website", "tags", "publish_date", "source"
    ]

    # CSV
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in results:
            writer.writerow({k: row.get(k, "") for k in headers})

    # JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"[+] Saved CSV -> {csv_path}")
    print(f"[+] Saved JSON -> {json_path}")
    print("[+] Finished.")


# ============================================
# CLI
# ============================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", required=True)
    ap.add_argument("--max-results", type=int, default=10)
    ap.add_argument("--output", default="out/behance_v2.csv")
    args = ap.parse_args()

    out_path = Path(args.output)
    asyncio.run(run_scrape(args.query, args.max_results, out_path))


if __name__ == "__main__":
    main()
