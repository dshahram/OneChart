# OneChart — Setup Guide

This puts **OneChart** on the internet with fully automatic weekday data updates. After setup, you never touch it again — fresh market data for ~11,000 tickers appears every weekday on its own.

**What you're building:** your chart website (via GitHub Pages). A robot (GitHub Action) builds the market dataset on first run, then refreshes it every weekday after the US market closes.

**Time needed:** about 15 minutes of clicking, then ~45 minutes while the robot builds the dataset (you just wait).

---

## Step 1: Create a GitHub account (skip if you have one)

1. Go to [github.com](https://github.com) and click **Sign up**.
2. Follow the prompts. The free plan is all you need.
3. Verify your email.

## Step 2: Create the repository

1. Click the **+** icon (top-right) → **New repository**.
2. **Repository name:** `OneChart` (must match — the site URL is `https://YOUR-USERNAME.github.io/OneChart/`).
3. Choose **Public** (required — free GitHub Pages only works on public repos).
4. Do **NOT** check "Add a README file".
5. Click **Create repository**.

## Step 3: Upload the files

1. On your new repo page, click **"uploading an existing file"**.
2. Unzip the package on your computer. You'll see:
   - `index.html`
   - `universe.json`
   - `preview.png`
   - `SETUP.md` (this guide)
   - a `scripts` folder (containing `refresh.py`)
   - a `.github` folder (containing `workflows/refresh-data.yml`)
3. **Drag ALL of these** (files AND folders) into the GitHub upload area.
   - ⚠️ If the `.github` folder doesn't upload (some browsers hide dot-folders), use the fallback in Troubleshooting below.
4. Click **Commit changes**.

## Step 4: Turn on GitHub Pages

1. In your repo, click **Settings** → **Pages** (left sidebar).
2. Under "Build and deployment": **Source** → **Deploy from a branch**; **Branch** → **main**, folder **/ (root)**.
3. Click **Save**. Wait 1–2 minutes for your URL: `https://YOUR-USERNAME.github.io/OneChart/`

## Step 5: Build the dataset (REQUIRED — one click)

The site needs its market data, which the robot builds for you:

1. In your repo, click the **Actions** tab.
2. Click **Refresh market data** (left sidebar).
3. Click **Run workflow** → **Run workflow**.
4. Wait ~45 minutes. The Actions page shows a yellow dot while it works, green check ✅ when done.
5. Visit your site — the chart now loads with ~11,000 tickers and "Data as of" today's date.

> The first run is slow because it fetches 10 years of history for every US stock and ETF. Daily refreshes after that take ~20 minutes.

## Step 6: Allow the robot to save (one setting)

1. In your repo: **Settings** → **Actions** → **General**.
2. Under "Workflow permissions", select **Read and write permissions**.
3. Click **Save**.

Without this, the robot can't save the refreshed data.

## Step 7 (optional): Usage stats

The site includes privacy-friendly analytics (GoatCounter — no cookies, no personal data):

1. Go to [goatcounter.com](https://www.goatcounter.com), sign up free.
2. When it asks for a site code, enter `onechart`.
3. Done — visits start appearing in your GoatCounter dashboard. (If you pick a different code, tell Muse so the site can be updated.)

---

## How it works going forward

- **Every weekday ~2pm PT**, the robot fetches new prices, headlines, fundamentals, and inflation data, then updates the site. Weekends/holidays: it checks and does nothing.
- **The repo history is reset daily** (a "force push") to keep it from growing forever. This is normal — you'll see "onechart-data-bot force-pushed" in the commit list. Your `index.html` is never touched by this.
- **"Data as of"** on the page always shows freshness.
- Your watchlists and settings are saved **in your browser only** — visitors to your link get fresh defaults.

## Troubleshooting

**The site shows "Building your market dataset":**
- You haven't run the workflow yet (Step 5). Go to Actions → Refresh market data → Run workflow.

**The `.github` folder didn't upload:**
1. In your repo: **Add file** → **Create new file**.
2. Filename: `.github/workflows/refresh-data.yml`
3. Paste the contents of the `refresh-data.yml` file from the package.
4. **Commit changes**.

**The Action shows a red X:**
- Click into the run to see the error. Common cause: Yahoo Finance rate-limiting — it usually succeeds on the next run. Re-run manually via Actions → Run workflow.
- If it fails on the "push" step with a permissions error, do Step 6 above.

**A ticker has no data:**
- Very new IPOs or delisted tickers may be missing. The robot picks up new listings automatically each day.

---

## What's in the package

| File | What it is |
|------|-----------|
| `index.html` | Your OneChart app |
| `universe.json` | Ticker list (~11,000 symbols) — data files are built by the robot |
| `preview.png` | Social preview card (shows when you share the link) |
| `scripts/refresh.py` | The robot: backfills once, then refreshes daily |
| `.github/workflows/refresh-data.yml` | Schedule + safe push logic for the robot |
| `SETUP.md` | This guide |
