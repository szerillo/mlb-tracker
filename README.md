# MLB HP Umpire Tracker

Auto-refreshing website that shows today's MLB HP umpires with offense-adjustment percentages, plus projected HPs for the rest of each series.

**Data refresh:** daily at 10am ET via GitHub Actions, plus live schedule/officials pulls on every page load.

---

## Files in this repo

```
mlb-tracker/
├── index.html                     # The website (main file)
├── data/
│   ├── umps.json                  # Auto-refreshed daily
│   ├── fatigue.json               # Auto-refreshed daily (bullpen fatigue)
│   ├── hitters.json               # Static (update when you send new projections)
│   └── pitchers.json              # Static (update when you send new projections)
├── scripts/
│   ├── refresh_umps.py            # Called by the daily workflow
│   └── compute_fatigue.py         # Called by the daily workflow
└── .github/workflows/
    └── daily.yml                  # GitHub Actions scheduled job
```

---

## One-time setup (about 10 minutes)

You do this once. After it's done, the site updates itself every morning and you never touch anything again.

### Step 1 — Create a free GitHub account

1. Go to **https://github.com/join**
2. Enter email, pick a username, password.
3. Verify the email. That's it.

### Step 2 — Create an empty repository

1. While logged in, click the **+** icon top-right → **New repository**.
2. Name it anything (e.g. `mlb-ump-tracker`). Leave everything else default (public is fine).
3. Click **Create repository**.

### Step 3 — Upload these files

1. On the new repo page, click **uploading an existing file** (the link in the middle of the page).
2. Drag the entire contents of this `mlb-tracker` folder onto the upload area (keep folder structure).
   - Tip: On Mac/Windows, open this folder, select all files, drag them in.
   - Or, upload as a zip then extract, but most browsers handle drag-drop of many files fine.
3. Scroll down, click **Commit changes**.

You should now see `index.html`, `data/`, `scripts/`, `.github/`, and `README.md` in the repo.

### Step 4 — Hook up Cloudflare Pages (deploys your site)

1. Go to **https://dash.cloudflare.com/sign-up** and make a free account (email + password).
2. Once logged in, sidebar → **Workers & Pages** → click **Create** → **Pages** → **Connect to Git**.
3. Click **Connect GitHub** and authorize it to see your repos.
4. Pick the repo you just made.
5. Leave all build settings empty (this is a plain-HTML site — no build step needed). Click **Save and Deploy**.
6. Wait ~30 seconds. Cloudflare will give you a URL like `mlb-ump-tracker.pages.dev`.

That URL is your live site. Bookmark it.

### Step 5 — Enable the GitHub Action

1. On the GitHub repo page, click the **Actions** tab.
2. If you see a yellow banner saying workflows are disabled, click **I understand my workflows, go ahead and enable them**.
3. The "Daily data refresh" workflow is now armed.

**To test it immediately:** click the workflow name → **Run workflow** button (top-right) → **Run workflow**. It'll take about 60 seconds. When it finishes, Cloudflare will redeploy within another 60 seconds.

---

## How the site stays fresh

| Data | How often | What happens |
|---|---|---|
| Today's games, umpires posted by MLB, first pitch times | Every page load + every 15 min when games are close | Browser fetches directly from MLB Stats API (no action needed) |
| Umpire adjustment stats | Daily at 10am ET | GitHub Action re-pulls UmpScorecards, commits new `umps.json`, Cloudflare redeploys |
| Bullpen fatigue | Daily at 10am ET | GitHub Action recomputes from last 5 days of MLB box scores |
| Hitter projections (wOBA, FLD, BSR, splits) | When you send me a new Excel | I regenerate `hitters.json` and commit it |
| Pitcher projections (FIP) | When you send me a new Excel | I regenerate `pitchers.json` and commit it |

---

## Forcing a refresh manually

If you want fresh data *right now* (not waiting for tomorrow morning):

1. Go to GitHub repo → **Actions** tab
2. Click **Daily data refresh**
3. Click **Run workflow** → **Run workflow**
4. Wait ~60 seconds. Your site will redeploy automatically.

---

## Updating projections (splits, wOBA, FIP, FLD, BSR)

You just send me a new Excel in the same format as before. I convert it to JSON and commit the updated files. Your site refreshes automatically.

---

## Troubleshooting

**Site shows old umpire data:**
- Check GitHub Actions tab — did today's workflow run? If it failed, click into the failure to see why.
- The free GitHub plan auto-disables workflows in repos with no activity for 60 days. Just visit the repo any time to reset the clock.

**Site won't load:**
- Visit Cloudflare Pages dashboard → your site → Deployments. Check if the latest deploy is "Success."
- If deployments stopped, re-connect the GitHub integration.

**"Workflow not running":**
- Actions tab should show scheduled runs. Cron jobs in GitHub Actions have some jitter (up to ~1 hour off). If you don't see a run within a few hours of 10am ET, trigger manually.

---

## Credits

- Umpire metric source: [UmpScorecards](https://umpscorecards.com) (pitch-by-pitch run impact of missed ball/strike calls)
- Schedule + officials: [MLB Stats API](https://statsapi.mlb.com)
- Projections: ATC (wOBA, FIP), ZIPS (FLD, BSR), Steamer (splits)
