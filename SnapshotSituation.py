#!/usr/bin/env python3
"""SnapshotSituation.py — generate monthly financial situation snapshots.

Usage:
    python apps/SnapshotSituation.py                      # regenerate all months (auto-detects first plan)
    python apps/SnapshotSituation.py YYYY MM              # generate a single specific month
    python apps/SnapshotSituation.py --plan <slug>        # specify plan slug
    python apps/SnapshotSituation.py --plan <slug> YYYY MM

Reads:
    data/budget-plans/<plan>/contracts.json
    data/budget-plans/<plan>/periodic_expenses.json
    data/budget-plans/<plan>/incomes.json
    data/budget-plans/<plan>/snapshot_months.json

Writes:
    output/snapshots/<plan>/YYYY-MM.json
    output/snapshots/<plan>/annual/YYYY.json
"""

import sys
import json
import sqlite3
from datetime import date, datetime
from pathlib import Path

SCRIPT_DIR   = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DB_PATH      = PROJECT_ROOT / "apps" / "dashboard" / "finance.db"

MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

BUDGET_PLANS_DIR = PROJECT_ROOT / "data" / "budget-plans"
PEOPLE_DIR       = PROJECT_ROOT / "data" / "people"
SHARED_DIR       = PROJECT_ROOT / "data" / "shared"


def resolve_data_dir(plan_slug: str = None) -> Path:
    """Resolve the budget-plan directory for a given slug.
    Falls back to the first plan folder found if no slug is given."""
    if plan_slug:
        p = BUDGET_PLANS_DIR / plan_slug
        if not p.exists():
            raise FileNotFoundError(f"Budget-plan not found: {p}")
        return p
    plans = [d for d in BUDGET_PLANS_DIR.iterdir() if d.is_dir()] if BUDGET_PLANS_DIR.exists() else []
    if plans:
        return plans[0]
    raise FileNotFoundError(f"No budget-plans found in {BUDGET_PLANS_DIR}")


# These are set at startup in __main__ — see bottom of file
PLAN_DIR   = None
OUTPUT_DIR = None


def read_json_file(path: Path) -> dict:
    """Read a JSON file from an absolute path. Returns empty dict if file missing."""
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_plan_members_data(plan_dir: Path) -> tuple:
    """Load merged datasets for all members of a plan plus virtual data.

    Returns (contracts, incomes, periodic_expenses) with virtual items appended.
    All items retain their original numeric IDs and owner_id/person_id fields.
    """
    plan = read_json_file(plan_dir / "plan.json")
    member_slugs = plan.get("members", [])

    all_contracts = []
    all_incomes   = []
    all_periodic  = []

    for slug in member_slugs:
        person_dir = PEOPLE_DIR / slug
        all_contracts += read_json_file(person_dir / "contracts.json").get("contracts", [])
        all_incomes   += read_json_file(person_dir / "incomes.json").get("incomes", [])
        all_periodic  += read_json_file(person_dir / "periodic_expenses.json").get("periodic_expenses", [])

    # Virtual data — additive only, scoped to this plan
    all_contracts += read_json_file(plan_dir / "virtual_contracts.json").get("virtual_contracts", [])
    all_incomes   += read_json_file(plan_dir / "virtual_incomes.json").get("virtual_incomes", [])
    all_periodic  += read_json_file(plan_dir / "virtual_periodic_expenses.json").get("virtual_periodic_expenses", [])

    return all_contracts, all_incomes, all_periodic


def upsert_snapshot_to_db(snapshot: dict, snap_type: str):
    """Upsert a snapshot into finance.db. Silently skips if DB not found."""
    if not DB_PATH.exists():
        return
    if PLAN_DIR is None:
        return
    plan_id = PLAN_DIR.name
    try:
        conn = sqlite3.connect(str(DB_PATH))
        year  = snapshot["year"]
        month = snapshot.get("month")  # None for annual
        label = snapshot["label"]
        generated_at = snapshot.get("generated_at", datetime.utcnow().isoformat() + "Z")
        content = json.dumps(snapshot, ensure_ascii=False)
        if snap_type == "monthly":
            conn.execute(
                """INSERT INTO snapshots (plan_id, type, year, month, label, generated_at, content)
                   VALUES (?, 'monthly', ?, ?, ?, ?, ?)
                   ON CONFLICT(plan_id, year, month) WHERE type='monthly'
                   DO UPDATE SET label=excluded.label, generated_at=excluded.generated_at, content=excluded.content""",
                (plan_id, year, month, label, generated_at, content)
            )
        else:
            conn.execute(
                """INSERT INTO snapshots (plan_id, type, year, month, label, generated_at, content)
                   VALUES (?, 'annual', ?, NULL, ?, ?, ?)
                   ON CONFLICT(plan_id, year) WHERE type='annual'
                   DO UPDATE SET label=excluded.label, generated_at=excluded.generated_at, content=excluded.content""",
                (plan_id, year, label, generated_at, content)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  [WARN] Could not upsert snapshot to DB: {e}")


def resolve_active_plan(plans: list, target: date) -> dict | None:
    """Return the first plan whose date range covers *target* (first day of month).
    Returns None if all plans have expired — the contract no longer applies.
    """
    for plan in plans:
        start = date.fromisoformat(plan["start_date"]) if plan.get("start_date") else None
        end   = date.fromisoformat(plan["end_date"])   if plan.get("end_date")   else None
        if (start is None or start <= target) and (end is None or end >= target):
            return plan
    # If the last plan has an explicit end_date that has passed, the contract is over.
    last = plans[-1] if plans else None
    if last and last.get("end_date") and date.fromisoformat(last["end_date"]) < target:
        return None
    # If the last plan hasn't started yet, the contract is not yet active.
    if last and last.get("start_date") and date.fromisoformat(last["start_date"]) > target:
        return None
    return last  # open-ended fallback (e.g. plan with no end_date)


def compute_income(record: dict) -> dict:
    """Compute MONTHLY income — only what is effectively received each month.

    Bonuses are excluded: they are annual/irregular and belong in annual snapshots.
    Transportation allowance is also excluded — already integrated in net salary.
    """
    b = record.get("other_benefits", {})

    net_monthly = record.get("avg_net_monthly_salary", 0) or 0
    meal_v      = b.get("meal_vouchers", 0) or 0
    child_allow = b.get("child_allowance", 0) or 0

    total = round(net_monthly + meal_v + child_allow, 2)

    result = {
        "net_monthly_salary": net_monthly,
        "meal_vouchers":      meal_v,
        "child_allowance":    child_allow,
        "total_monthly_net":  total,
    }

    if "consultant_relevant_benefits" in record:
        result["consultant_relevant_benefits"] = record["consultant_relevant_benefits"]

    return result


def compute_annual_income(record: dict) -> dict:
    """Compute full annual income including bonuses — for annual package snapshots."""
    b = record.get("other_benefits", {})

    net_monthly = record.get("avg_net_monthly_salary", 0) or 0
    meal_v      = b.get("meal_vouchers", 0) or 0
    child_allow = b.get("child_allowance", 0) or 0
    perf_bonus  = ((b.get("performance_bonus", {}) or {}).get("net_amount", 0)) or 0
    eoy_bonus   = ((b.get("end_of_year_bonus",  {}) or {}).get("net_amount", 0)) or 0

    net_salary_annual  = round(net_monthly  * 12, 2)
    meal_v_annual      = round(meal_v       * 12, 2)
    child_allow_annual = round(child_allow  * 12, 2)
    total = round(net_salary_annual + meal_v_annual + child_allow_annual + perf_bonus + eoy_bonus, 2)

    result = {
        "net_salary_annual":      net_salary_annual,
        "meal_vouchers_annual":   meal_v_annual,
        "child_allowance_annual": child_allow_annual,
        "performance_bonus":      perf_bonus,
        "end_of_year_bonus":      eoy_bonus,
        "total_annual_net":       total,
    }

    if "consultant_relevant_benefits" in record:
        result["consultant_relevant_benefits"] = record["consultant_relevant_benefits"]

    return result


def compute_contract_annual_cost(contract: dict, year: int) -> tuple[int, int]:
    """Return (annual_cost, months_active) for a contract in the given year."""
    total = 0
    months_active = 0
    for month in range(1, 13):
        plan = resolve_active_plan(contract.get("periods", []), date(year, month, 1))
        if plan:
            months_active += 1
            total += plan["monthly_cost"]
    return total, months_active


_EMPTY_INCOME = {
    "net_monthly_salary": 0,
    "meal_vouchers":      0,
    "child_allowance":    0,
    "total_monthly_net":  0,
}


def generate_snapshot(year: int, month: int) -> dict:
    target = date(year, month, 1)

    contracts, incomes, periodic_expenses = load_plan_members_data(PLAN_DIR)

    income_contracts  = [c for c in contracts if c.get("direction") == "income"]
    expense_contracts = [c for c in contracts if c.get("direction", "expense") == "expense"]

    # Collect all person IDs referenced across the data
    person_ids = sorted(set(
        [c["owner_id"] for c in contracts]
        + [e["owner_id"] for e in periodic_expenses]
        + [i["person_id"] for i in incomes]
    ))

    people_entries   = []
    total_net_income = 0.0
    total_contracts  = 0.0
    total_periodic   = 0.0

    for pid in person_ids:
        # Best income record: prefer exact year, else the most recent available
        person_incomes = [i for i in incomes if i["person_id"] == pid]
        income_record = next(
            (i for i in person_incomes if int(i["year"]) == year), None
        )
        if income_record is None and person_incomes:
            income_record = max(person_incomes, key=lambda i: int(i["year"]))

        income_breakdown = compute_income(income_record) if income_record else dict(_EMPTY_INCOME)

        # Override salary from employment contract if available
        person_income_cs = [c for c in income_contracts if c["owner_id"] == pid]
        for ic in person_income_cs:
            period = resolve_active_plan(ic.get("periods", []), target)
            if period:
                income_breakdown["net_monthly_salary"] = period["monthly_cost"]
                income_breakdown["gross_monthly_salary"] = period.get("gross_monthly")
                income_breakdown["total_monthly_net"] = round(
                    period["monthly_cost"]
                    + income_breakdown.get("meal_vouchers", 0)
                    + income_breakdown.get("child_allowance", 0), 2)
                break

        # Active contracts for this person on target month
        person_contracts = []
        for c in expense_contracts:
            if c["owner_id"] != pid:
                continue
            plan = resolve_active_plan(c.get("periods", []), target)
            if plan is None:
                continue
            entry = {
                "id":           c["id"],
                "title":        c["title"],
                "category":     c["category"],
                "monthly_cost": plan["monthly_cost"],
            }
            for opt in ("property_id", "title_i18n", "category_i18n", "consultant_relevant"):
                if opt in c:
                    entry[opt] = c[opt]
            person_contracts.append(entry)

        # Periodic expenses for this person (annual total / 12)
        person_periodic = []
        for e in periodic_expenses:
            if e["owner_id"] != pid:
                continue
            annual_total = sum(p["amount"] for p in e.get("payments", []))
            monthly_avg  = round(annual_total / 12, 2)
            entry = {
                "id":           e["id"],
                "title":        e["title"],
                "category":     e["category"],
                "annual_total": annual_total,
                "monthly_avg":  monthly_avg,
            }
            for opt in ("property_id", "title_i18n", "category_i18n", "consultant_relevant"):
                if opt in e:
                    entry[opt] = e[opt]
            person_periodic.append(entry)

        total_exp = round(
            sum(c["monthly_cost"] for c in person_contracts)
            + sum(e["monthly_avg"] for e in person_periodic),
            2,
        )

        total_net_income += income_breakdown["total_monthly_net"]
        total_contracts  += sum(c["monthly_cost"] for c in person_contracts)
        total_periodic   += sum(e["monthly_avg"]  for e in person_periodic)

        people_entries.append({
            "person_id":              pid,
            "income":                 income_breakdown,
            "contracts":              person_contracts,
            "periodic_expenses":      person_periodic,
            "total_monthly_expenses": total_exp,
        })

    total_expenses = round(total_contracts + total_periodic, 2)

    snapshot = {
        "year":         year,
        "month":        month,
        "label":        f"{MONTH_NAMES[month - 1]} {year}",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "people":       people_entries,
        "summary": {
            "total_net_income":   round(total_net_income, 2),
            "total_contracts":    round(total_contracts,  2),
            "total_periodic_avg": round(total_periodic,   2),
            "total_expenses":     total_expenses,
            "net_balance":        round(total_net_income - total_expenses, 2),
        },
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{year:04d}-{month:02d}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    upsert_snapshot_to_db(snapshot, "monthly")
    print(f"✓ Snapshot written: {out_path}")
    return snapshot


def generate_annual_snapshot(year: int) -> dict:
    """Generate a full annual package snapshot — includes bonuses and actual payment totals."""
    contracts, incomes, periodic_expenses = load_plan_members_data(PLAN_DIR)

    income_contracts  = [c for c in contracts if c.get("direction") == "income"]
    expense_contracts = [c for c in contracts if c.get("direction", "expense") == "expense"]

    person_ids = sorted(set(
        [c["owner_id"] for c in contracts]
        + [e["owner_id"] for e in periodic_expenses]
        + [i["person_id"] for i in incomes]
    ))

    people_entries        = []
    total_annual_income   = 0.0
    total_annual_expenses = 0.0

    for pid in person_ids:
        person_incomes = [i for i in incomes if i["person_id"] == pid]
        income_record  = next((i for i in person_incomes if int(i["year"]) == year), None)
        if income_record is None and person_incomes:
            income_record = max(person_incomes, key=lambda i: int(i["year"]))

        income_breakdown = compute_annual_income(income_record) if income_record else {
            "net_salary_annual": 0, "meal_vouchers_annual": 0, "child_allowance_annual": 0,
            "performance_bonus": 0, "end_of_year_bonus": 0, "total_annual_net": 0,
        }

        # Override annual salary from employment contracts if available
        person_income_cs = [c for c in income_contracts if c["owner_id"] == pid]
        if person_income_cs:
            annual_salary = 0.0
            for m in range(1, 13):
                t = date(year, m, 1)
                for ic in person_income_cs:
                    period = resolve_active_plan(ic.get("periods", []), t)
                    if period:
                        annual_salary += period["monthly_cost"]
                        break
            meal_v      = income_breakdown.get("meal_vouchers_annual", 0)
            child_allow = income_breakdown.get("child_allowance_annual", 0)
            perf        = income_breakdown.get("performance_bonus", 0)
            eoy         = income_breakdown.get("end_of_year_bonus", 0)
            income_breakdown["net_salary_annual"] = round(annual_salary, 2)
            income_breakdown["total_annual_net"]  = round(annual_salary + meal_v + child_allow + perf + eoy, 2)

        person_contracts = []
        for c in expense_contracts:
            if c["owner_id"] != pid:
                continue
            annual_cost, months_active = compute_contract_annual_cost(c, year)
            if months_active == 0:
                continue
            entry = {
                "id":            c["id"],
                "title":         c["title"],
                "category":      c["category"],
                "annual_cost":   annual_cost,
                "months_active": months_active,
            }
            for opt in ("property_id", "title_i18n", "category_i18n", "consultant_relevant"):
                if opt in c:
                    entry[opt] = c[opt]
            person_contracts.append(entry)

        person_periodic = []
        for e in periodic_expenses:
            if e["owner_id"] != pid:
                continue
            annual_total = sum(p["amount"] for p in e.get("payments", []))
            entry = {
                "id":           e["id"],
                "title":        e["title"],
                "category":     e["category"],
                "annual_total": annual_total,
            }
            for opt in ("property_id", "title_i18n", "category_i18n", "consultant_relevant"):
                if opt in e:
                    entry[opt] = e[opt]
            person_periodic.append(entry)

        person_annual_exp = round(
            sum(c["annual_cost"]  for c in person_contracts)
            + sum(e["annual_total"] for e in person_periodic),
            2,
        )

        total_annual_income   += income_breakdown["total_annual_net"]
        total_annual_expenses += person_annual_exp

        people_entries.append({
            "person_id":             pid,
            "income":                income_breakdown,
            "contracts":             person_contracts,
            "periodic_expenses":     person_periodic,
            "total_annual_expenses": person_annual_exp,
        })

    annual_dir = OUTPUT_DIR / "annual"
    annual_dir.mkdir(parents=True, exist_ok=True)
    out_path = annual_dir / f"{year:04d}.json"

    snapshot = {
        "year":         year,
        "type":         "annual",
        "label":        str(year),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "people":       people_entries,
        "summary": {
            "total_annual_net_income": round(total_annual_income,   2),
            "total_annual_expenses":   round(total_annual_expenses,  2),
            "net_annual_balance":      round(total_annual_income - total_annual_expenses, 2),
        },
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    upsert_snapshot_to_db(snapshot, "annual")
    print(f"✓ Annual snapshot written: {out_path}")
    return snapshot


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Generate monthly/annual financial snapshots')
    parser.add_argument('--plan', default=None, help='Budget-plan slug (auto-detects if omitted)')
    parser.add_argument('year',  nargs='?', type=int, help='Year (YYYY)')
    parser.add_argument('month', nargs='?', type=int, help='Month (MM)')
    parsed = parser.parse_args()

    # Initialise global PLAN_DIR and OUTPUT_DIR based on resolved plan
    PLAN_DIR   = resolve_data_dir(parsed.plan)
    plan_slug  = PLAN_DIR.name
    OUTPUT_DIR = PROJECT_ROOT / "output" / "snapshots" / plan_slug
    print(f"[snapshot] Plan: {plan_slug}  plan_dir: {PLAN_DIR}  output: {OUTPUT_DIR}")

    if parsed.year and parsed.month:
        y, m = parsed.year, parsed.month
        if not (2000 <= y <= 2100):
            print("Error: Year must be between 2000 and 2100"); sys.exit(1)
        if not (1 <= m <= 12):
            print("Error: Month must be between 1 and 12"); sys.exit(1)
        generate_snapshot(y, m)
    elif parsed.year or parsed.month:
        print("Error: provide both YEAR and MONTH, or neither"); sys.exit(1)
    else:
        # Batch mode
        config = read_json_file(PLAN_DIR / "snapshot_months.json")
        months = config.get("months", [])
        if not months:
            print(f"No months configured in {PLAN_DIR / 'snapshot_months.json'}")
            sys.exit(1)
        print(f"Regenerating {len(months)} monthly snapshot(s)…")
        for entry in months:
            try:
                y2, m2 = int(entry[:4]), int(entry[5:7])
                generate_snapshot(y2, m2)
            except Exception as exc:
                print(f"  ✗ {entry}: {exc}")

        current_year = datetime.utcnow().year
        annual_years = list(range(current_year, current_year + 6))
        print(f"\nRegenerating annual snapshots for {annual_years[0]}–{annual_years[-1]}…")
        for y2 in annual_years:
            try:
                generate_annual_snapshot(y2)
            except Exception as exc:
                print(f"  ✗ {y2}: {exc}")
