# Stock Chart — Self-Hosted Setup Guide

This puts your stock chart on the internet with **fully automatic daily data updates**. After the one-time setup below, you never touch it again — fresh market data appears every weekday evening on its own.

**What you're building:** a personal website (via GitHub Pages) that runs your chart. A robot (GitHub Action) refreshes the data file every weekday after the US market closes.

**Time needed:** about 10 minutes, most of it waiting for uploads.

---

## Step 1: Create a GitHub account (skip if you have one)

1. Go to [github.com](https://github.com) and click **Sign up**.
2. Follow the prompts (email, password, username). The free plan is all you need.
3. Verify your email when they send the confirmation.

## Step 2: Create a repository (this is just a folder for your website's files)

1. Once logged in, click the **+** icon in the top-right corner → **New repository**.
2. **Repository name:** `stock-chart` (or anything you like).
3. Choose **Public** (required — free GitHub Pages only works on public repos).
4. Do **NOT** check "Add a README file". Leave everything else as-is.
5. Click **Create repository**.

You'll land on an empty repo page. That's expected.

## Step 3: Upload the files

1. On your new repo page, click the **"uploading an existing file"** link (it's in the middle of the page).
2. Open the ZIP package on your computer and unzip it. You'll see:
   - `index.html`
   - `data.json`
   - a `scripts` folder (containing `refresh.py`)
   - a `.github` folder (containing `workflows/refresh-data.yml`)
3. **Drag ALL of these** (both files AND both folders) into the GitHub upload area. The folder structure matters — make sure `scripts` and `.github` upload as folders, not flattened.
   - ⚠️ If the `.github` folder doesn't upload (some browsers hide folders starting with a dot), don't worry — there's a fallback in the Troubleshooting section below.
4. Scroll down, then click the green **Commit changes** button.

## Step 4: Turn on GitHub Pages (this publishes your site)

1. In your repo, click **Settings** (top menu, near the right).
2. In the left sidebar, click **Pages**.
3. Under "Build and deployment":
   - **Source:** select **Deploy from a branch**.
   - **Branch:** select **main**, and keep the folder as **/ (root)**.
4. Click **Save**.
5. Wait 1–2 minutes. Refresh the page — GitHub will show your site URL at the top, looking like:
   `https://YOUR-USERNAME.github.io/stock-chart/`

## Step 5: Open your site and check it works

1. Visit your site URL from Step 4.
2. The chart should load and show **"Data as of Oct 2, 2026"** near the top.
3. Try the command bar (press `/`) and type: `compare starbucks to nike over the past 5.5 years`
4. If the chart loads and responds — you're done with setup! 🎉

## Step 6: Test the auto-update (optional but recommended)

You don't have to wait for the schedule — you can trigger a refresh manually:

1. In your repo, click the **Actions** tab (top menu).
2. Click **Refresh market data** in the left sidebar.
3. Click **Run workflow** → **Run workflow** (green button).
4. Wait ~10 minutes, then check the Actions page — it should show a green checkmark ✅.
5. Your site's data is now refreshed. (It'll only change visibly on trading days when there's new data.)

---

## How it works going forward

- **Every weekday at ~2pm PT** (after US market closes), the robot fetches the latest prices for all 662 tickers, refreshes news headlines, and updates your site automatically.
- **Weekends/holidays:** the robot checks, sees no new data, and does nothing.
- **You do nothing.** No uploads, no buttons, no approvals. Just open your link anytime.
- The **"Data as of"** label on the chart always tells you how fresh the data is.

## Troubleshooting

**The site shows 404 or "There isn't a GitHub Pages site here":**
- Wait 2–3 minutes after enabling Pages, then hard-refresh (Cmd+Shift+R on Mac, Ctrl+Shift+R on Windows).
- Double-check Settings → Pages shows your site URL.

**The chart loads but says "Could not load data.json":**
- Make sure `data.json` is in the **root** of your repo (not inside a folder), next to `index.html`.
- Check the file actually uploaded (it should show as ~11 MB in the repo file list).

**The `.github` folder didn't upload:**
1. In your repo, click **Add file** → **Create new file**.
2. In the filename box, type exactly: `.github/workflows/refresh-data.yml`
3. Copy-paste the entire contents of the `refresh-data.yml` file from the package into the editor.
4. Click **Commit changes**. The auto-update will now work.

**The Action shows a red X (failed):**
- Click into the failed run to see the error. Common cause: Yahoo Finance temporarily blocking requests — it usually works on the next scheduled run.
- You can re-run it manually via Actions → Refresh market data → Run workflow.

**I want to change the chart itself (not the data):**
- The chart code lives in `index.html`. Data updates never touch it — only `data.json` changes. If you want chart changes, ask Muse to rebuild the page.

---

## What's in the package

| File | What it is |
|------|-----------|
| `index.html` | Your chart app (loads data from `data.json` on open) |
| `data.json` | All market data — 662 tickers, 10 years, news headlines (~11 MB) |
| `scripts/refresh.py` | The robot's script: fetches fresh prices from Yahoo Finance |
| `.github/workflows/refresh-data.yml` | Tells GitHub when to run the robot (weekdays after close) |
| `SETUP.md` | This guide |
