# apps

Scripts and tools that process the project's financial data.

All scripts are run from the **project root** (`C:\dev\workspace\personal\`).  
All scripts read from `data/` and write results to `output/` and/or `apps/dashboard/finance.db`.

---

## SnapshotSituation.py

Generates monthly and annual financial situation snapshots from contracts, periodic expenses and income data.

**Reads:**
- `data/contracts.json` — fixed monthly recurring bills (private leases, mortgages, utilities…)
- `data/periodic_expenses.json` — irregular yearly payments (heating oil, water, property tax…)
- `data/incomes.json` — salary and benefit records per person
- `data/snapshot_months.json` — list of months to generate in batch mode

**Writes:**
- `output/snapshots/YYYY-MM.json` — monthly situation snapshots
- `output/snapshots/annual/YYYY.json` — annual package snapshots (current year + 5 ahead)
- `apps/dashboard/finance.db` — upserts every generated snapshot into the database

**Usage:**

```bash
# Regenerate all months from data/snapshot_months.json + annual packages for current year+5
python apps/SnapshotSituation.py

# Generate a single specific month
python apps/SnapshotSituation.py 2026 5
```

**Monthly snapshot** — shows the average monthly financial picture for a given month:
- Per-person active contracts at their plan cost for that month
- Per-person periodic expenses averaged over 12 months (e.g. 650 € property tax in October → ~54 €/month)
- Monthly net income (salary + meal vouchers + child allowance, no bonuses)
- Global totals: income, expenses, net balance

**Annual snapshot** — shows the real annual picture including bonuses:
- Per-person contract costs for the actual months they are active in that year (prorated, not × 12)
- Per-person periodic expenses as their real annual total
- Full annual income including performance bonus and end-of-year bonus
- Global totals: annual income, annual expenses, net annual balance

> To add a new month to the batch, edit `data/snapshot_months.json` and re-run.

---

## DataManager.py

SQLite migration tool. Keeps `apps/dashboard/finance.db` in sync with the JSON source files in `data/`.

**Reads:**
- All JSON files in `data/` (addresses, people, real estate, bank accounts, contracts, periodic expenses, mortgages, incomes, permissions, timeline)
- All snapshot files in `output/snapshots/`

**Writes / modifies:**
- `apps/dashboard/finance.db` (default)

**Commands:**

```bash
# Create (or reset) the database from JSON source files — this is the normal dev workflow
python apps/DataManager.py create

# Export the database back to JSON files (useful for inspecting or recovering data)
python apps/DataManager.py export

# Validate integrity: compare record counts between DB and JSON, run foreign key checks
python apps/DataManager.py validate
```

**Optional arguments** (all have defaults):

| Argument | Default | Description |
|---|---|---|
| `--db PATH` | `apps/dashboard/finance.db` | Path to the SQLite database |
| `--data PATH` | `data/` | Path to the JSON data directory |
| `--snapshots PATH` | `output/snapshots/` | Path to the snapshots directory |

> `create` drops and recreates all tables — use `export` first if you have unsaved changes in the DB.

---

## dashboard

Node.js/Express web application. Serves the personal finance dashboard on `http://localhost:3100`.

**Reads:**
- `apps/dashboard/finance.db` — single source of truth at runtime (loaded via sql.js WASM)

**On startup:**
- **Development** (default, `NODE_ENV` unset): automatically reimports `finance.db` from JSON files by running `DataManager.py create`. Any changes to JSON data files are picked up on restart.
- **Production** (`NODE_ENV=production`): creates an empty schema if the database does not exist; leaves an existing database untouched.

**Start (Windows):**

```bat
apps\dashboard\start.bat
```

This sets `PORT=3100` and runs `node server.js`.

**Start (Linux/macOS):**

```bash
bash apps/dashboard/start.sh
```

**Manual start:**

```bash
cd apps/dashboard
node server.js            # development (default)
NODE_ENV=production node server.js   # production
```

**Features:**
- Role-based view switching (public / family / consultant) via cookie
- Snapshot carousel: monthly and annual financial snapshots with income/expense breakdown per person
- Real estate section: property details, active mortgage plans per lender
- Contracts section: active recurring bills per person, colour-coded by category
- Periodic expenses section: irregular payments with monthly average
- Economic indicators bar (fixed bottom): live ECB mortgage rate for Luxembourg + commodity price charts (electricity Belgium, SP95 Luxembourg, heating oil Belgium, inflation Belgium) combining live Yahoo Finance market data with official ECB/HICP statistics

**Development workflow:**

1. Edit data in `data/*.json`
2. Run `python apps/SnapshotSituation.py` to regenerate snapshots
3. Restart the dashboard — `DataManager.py create` runs automatically and reloads the database

> The database file `apps/dashboard/finance.db` is auto-generated and should not be committed to source control. JSON files in `data/` are the authoritative source.
