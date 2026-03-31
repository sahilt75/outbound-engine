from playwright.sync_api import sync_playwright

STATE_PATH = "behance_state.json"

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        page = context.new_page()
        page.goto("https://www.behance.net/")

        print("[*] Please log in manually in the opened browser window.")
        input("Press Enter after logging in...")

        # Save storage state for future sessions
        context.storage_state(path=STATE_PATH)
        print(f"[+] Saved storage state to {STATE_PATH}")

        browser.close()

if __name__ == "__main__":
    main()