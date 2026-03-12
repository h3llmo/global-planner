#!/usr/bin/env python3
"""
DataManager.py — SQLite migration tool for personal finance data.

Commands:
  create   — Create/reset SQLite DB from JSON source files
  export   — Export SQLite DB back to JSON files
  validate — Compare DB content against JSON files for integrity

Default DB path: apps/dashboard/finance.db
Default data dir: data/
"""

import argparse
import json
import sqlite3
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT  = Path(__file__).resolve().parent.parent
DEFAULT_DB    = PROJECT_ROOT / 'apps' / 'dashboard' / 'finance.db'
DEFAULT_DATA  = PROJECT_ROOT / 'data'
SNAPSHOTS_DIR = PROJECT_ROOT / 'output' / 'snapshots'


def read_json(data_dir: Path, filename: str):
    p = data_dir / filename
    if not p.exists():
        print(f"  [WARN] {filename} not found, skipping.")
        return None
    with open(p, 'r', encoding='utf-8') as f:
        return json.load(f)


def write_json(path: Path, data, indent=2):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)


# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS addresses (
    id             INTEGER PRIMARY KEY,
    street_number  TEXT,
    street_name    TEXT NOT NULL,
    postal_code    TEXT NOT NULL,
    city           TEXT NOT NULL,
    country_code   TEXT NOT NULL,
    country        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS people (
    id                   INTEGER PRIMARY KEY,
    name                 TEXT NOT NULL,
    birth_date           TEXT,
    gender               TEXT,
    address_id           INTEGER REFERENCES addresses(id),
    national_number_be   TEXT,
    cns_lu               TEXT,
    id_card_number       TEXT,
    id_card_expiry       TEXT,
    phone                TEXT,
    email                TEXT,
    role                 TEXT,
    occupation_location  TEXT,
    occupation_company   TEXT,
    occupation_position  TEXT
);

CREATE TABLE IF NOT EXISTS properties (
    id          INTEGER PRIMARY KEY,
    address_id  INTEGER NOT NULL REFERENCES addresses(id),
    type        TEXT NOT NULL,
    status      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS property_owners (
    property_id  INTEGER NOT NULL REFERENCES properties(id),
    person_id    INTEGER NOT NULL REFERENCES people(id),
    PRIMARY KEY (property_id, person_id)
);

CREATE TABLE IF NOT EXISTS bank_accounts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id  INTEGER NOT NULL REFERENCES people(id),
    bank       TEXT NOT NULL,
    country    TEXT NOT NULL,
    iban       TEXT NOT NULL UNIQUE,
    bic        TEXT,
    type       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bank_cards (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id  INTEGER NOT NULL REFERENCES bank_accounts(id),
    card_number TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS contracts (
    id                   INTEGER PRIMARY KEY,
    title                TEXT NOT NULL,
    category             TEXT NOT NULL,
    category_i18n        TEXT,
    direction            TEXT NOT NULL DEFAULT 'expense',
    owner_id             INTEGER NOT NULL REFERENCES people(id),
    property_id          INTEGER REFERENCES properties(id),
    nominal              REAL,
    taeg                 REAL,
    consultant_relevant  INTEGER NOT NULL DEFAULT 1,
    notes                TEXT,
    employment_start     TEXT,
    contract_type        TEXT,
    employer_address_id  INTEGER REFERENCES addresses(id)
);

CREATE TABLE IF NOT EXISTS contract_periods (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_id   INTEGER NOT NULL REFERENCES contracts(id),
    monthly_cost  REAL NOT NULL,
    gross_monthly REAL,
    start_date    TEXT,
    end_date      TEXT,
    rate          REAL,
    rate_type     TEXT
);

CREATE TABLE IF NOT EXISTS periodic_expenses (
    id                   INTEGER PRIMARY KEY,
    title                TEXT NOT NULL,
    title_i18n           TEXT,
    category             TEXT NOT NULL,
    category_i18n        TEXT,
    owner_id             INTEGER NOT NULL REFERENCES people(id),
    property_id          INTEGER REFERENCES properties(id),
    consultant_relevant  INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS periodic_expense_payments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    expense_id  INTEGER NOT NULL REFERENCES periodic_expenses(id),
    label       TEXT NOT NULL,
    amount      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS mortgages (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_ref      TEXT NOT NULL UNIQUE,
    contract_id       INTEGER REFERENCES contracts(id),
    owner_id          INTEGER NOT NULL REFERENCES people(id),
    property_id       INTEGER NOT NULL REFERENCES properties(id),
    nominal           REAL,
    rate              REAL,
    taeg              REAL,
    months            INTEGER,
    monthly_payment   REAL,
    first_payment     TEXT,
    last_payment      TEXT,
    effective_date    TEXT,
    offer_date        TEXT,
    total_amount      REAL,
    total_interest    REAL,
    total_accessory   REAL,
    total_insurance   REAL,
    capital_amortized REAL
);

CREATE TABLE IF NOT EXISTS incomes (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id                   INTEGER NOT NULL REFERENCES people(id),
    year                        INTEGER NOT NULL,
    avg_gross_monthly_salary    REAL,
    avg_net_monthly_salary      REAL,
    health_insurance            INTEGER,
    transportation_allowance    REAL,
    meal_vouchers               REAL,
    performance_bonus_gross     REAL,
    performance_bonus_net       REAL,
    end_of_year_bonus_gross     REAL,
    end_of_year_bonus_net       REAL,
    child_allowance             REAL,
    UNIQUE (person_id, year)
);

CREATE TABLE IF NOT EXISTS income_consultant_benefits (
    income_id    INTEGER NOT NULL REFERENCES incomes(id),
    benefit_key  TEXT NOT NULL,
    PRIMARY KEY (income_id, benefit_key)
);

CREATE TABLE IF NOT EXISTS permissions (
    id           TEXT PRIMARY KEY,
    description  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS roles (
    id          TEXT PRIMARY KEY,
    label       TEXT NOT NULL,
    description TEXT,
    is_default  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS role_permissions (
    role_id        TEXT NOT NULL REFERENCES roles(id),
    permission_id  TEXT NOT NULL REFERENCES permissions(id),
    PRIMARY KEY (role_id, permission_id)
);

CREATE TABLE IF NOT EXISTS timeline_projects (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT NOT NULL,
    description         TEXT,
    start_date          TEXT,
    estimated_end_date  TEXT,
    currency            TEXT DEFAULT 'EUR'
);

CREATE TABLE IF NOT EXISTS timeline_milestones (
    id          INTEGER PRIMARY KEY,
    project_id  INTEGER NOT NULL REFERENCES timeline_projects(id),
    title       TEXT NOT NULL,
    description TEXT,
    start_date  TEXT,
    end_date    TEXT,
    status      TEXT,
    priority    TEXT,
    budget      REAL,
    notes       TEXT
);

-- Snapshots: content stored as JSON text (always read whole, no deep querying needed)
CREATE TABLE IF NOT EXISTS snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    type         TEXT NOT NULL CHECK(type IN ('monthly', 'annual')),
    year         INTEGER NOT NULL,
    month        INTEGER,
    label        TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    content      TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_monthly ON snapshots(year, month) WHERE type = 'monthly';
CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_annual  ON snapshots(year)        WHERE type = 'annual';
"""

# ── Drop all tables (for clean recreate) ─────────────────────────────────────
DROP_SQL = """
PRAGMA foreign_keys = OFF;
DROP TABLE IF EXISTS income_consultant_benefits;
DROP TABLE IF EXISTS incomes;
DROP TABLE IF EXISTS mortgage_quarterly_costs;
DROP TABLE IF EXISTS mortgages;
DROP TABLE IF EXISTS contract_periods;
DROP TABLE IF EXISTS contracts;
DROP TABLE IF EXISTS periodic_expense_payments;
DROP TABLE IF EXISTS periodic_expenses;
DROP TABLE IF EXISTS bank_cards;
DROP TABLE IF EXISTS bank_accounts;
DROP TABLE IF EXISTS property_owners;
DROP TABLE IF EXISTS properties;
DROP TABLE IF EXISTS people;
DROP TABLE IF EXISTS addresses;
DROP TABLE IF EXISTS role_permissions;
DROP TABLE IF EXISTS roles;
DROP TABLE IF EXISTS permissions;
DROP TABLE IF EXISTS timeline_milestones;
DROP TABLE IF EXISTS timeline_projects;
DROP TABLE IF EXISTS snapshots;
PRAGMA foreign_keys = ON;
"""


# ── Import helpers ────────────────────────────────────────────────────────────

def import_addresses(conn, data):
    if not data: return 0
    rows = data.get('addresses', [])
    conn.executemany(
        "INSERT INTO addresses (id, street_number, street_name, postal_code, city, country_code, country) VALUES (?,?,?,?,?,?,?)",
        [(r['id'], r.get('street_number'), r['street_name'], r['postal_code'], r['city'], r['country_code'], r['country'])
         for r in rows]
    )
    return len(rows)


def import_people(conn, data):
    if not data: return 0
    rows = data.get('people', [])
    for r in rows:
        card = r.get('id_card') or {}
        contact = r.get('contact') or {}
        occ = r.get('occupation') or {}
        conn.execute(
            """INSERT INTO people (id, name, birth_date, gender, address_id, national_number_be, cns_lu,
               id_card_number, id_card_expiry, phone, email, role,
               occupation_location, occupation_company, occupation_position)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (r['id'], r['name'], r.get('birth_date'), r.get('gender'), r.get('address_id'),
             r.get('national_number_be'), r.get('cns_lu'),
             card.get('number'), card.get('expiry'),
             contact.get('phone'), contact.get('email'), r.get('role'),
             occ.get('location'), occ.get('company'), occ.get('position'))
        )
    return len(rows)


def import_real_estate(conn, data):
    if not data: return 0
    rows = data.get('properties', [])
    for r in rows:
        conn.execute(
            "INSERT INTO properties (id, address_id, type, status) VALUES (?,?,?,?)",
            (r['id'], r['address_id'], r['type'], r['status'])
        )
        for pid in r.get('owner_ids', []):
            conn.execute("INSERT INTO property_owners (property_id, person_id) VALUES (?,?)", (r['id'], pid))
    return len(rows)


def import_bank_accounts(conn, data):
    if not data: return 0
    rows = data.get('bank_accounts', [])
    for r in rows:
        cur = conn.execute(
            "INSERT INTO bank_accounts (person_id, bank, country, iban, bic, type) VALUES (?,?,?,?,?,?)",
            (r['person_id'], r['bank'], r['country'], r['iban'], r.get('bic'), r['type'])
        )
        account_id = cur.lastrowid
        for card in (r.get('cards') or []):
            conn.execute("INSERT INTO bank_cards (account_id, card_number) VALUES (?,?)", (account_id, card))
    return len(rows)


def import_contracts(conn, data):
    if not data: return 0
    rows = data.get('contracts', [])
    for r in rows:
        consultant = r.get('consultant_relevant')
        consultant_int = 1 if consultant is None or consultant is True else 0
        direction = r.get('direction', 'expense')
        conn.execute(
            """INSERT INTO contracts (id, title, category, category_i18n, direction, owner_id, property_id, nominal, taeg,
               consultant_relevant, notes, employment_start, contract_type, employer_address_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (r['id'], r['title'], r['category'], r.get('category_i18n'), direction, r['owner_id'],
             r.get('property_id'), r.get('nominal'), r.get('taeg'), consultant_int, r.get('notes'),
             r.get('employment_start'), r.get('contract_type'), r.get('employer_address_id'))
        )
        for plan in r.get('periods', r.get('plans', [])):
            conn.execute(
                """INSERT INTO contract_periods (contract_id, monthly_cost, gross_monthly, start_date, end_date, rate, rate_type)
                   VALUES (?,?,?,?,?,?,?)""",
                (r['id'], plan['monthly_cost'], plan.get('gross_monthly'), plan.get('start_date'), plan.get('end_date'),
                 plan.get('rate'), plan.get('rate_type'))
            )
    return len(rows)


def import_periodic_expenses(conn, data):
    if not data: return 0
    rows = data.get('periodic_expenses', [])
    for r in rows:
        consultant = r.get('consultant_relevant')
        consultant_int = 1 if consultant is None or consultant is True else 0
        conn.execute(
            """INSERT INTO periodic_expenses (id, title, title_i18n, category, category_i18n,
               owner_id, property_id, consultant_relevant) VALUES (?,?,?,?,?,?,?,?)""",
            (r['id'], r['title'], r.get('title_i18n'), r['category'], r.get('category_i18n'),
             r['owner_id'], r.get('property_id'), consultant_int)
        )
        for pmt in r.get('payments', []):
            conn.execute(
                "INSERT INTO periodic_expense_payments (expense_id, label, amount) VALUES (?,?,?)",
                (r['id'], pmt['label'], pmt['amount'])
            )
    return len(rows)


def import_mortgages(conn, data):
    if not data: return 0
    rows = data if isinstance(data, list) else []
    # Resolve contract_id from contracts table by matching title pattern or ref
    for r in rows:
        # Try to match by contract reference in contracts notes or by known mapping
        contract_id = None
        cur = conn.execute("SELECT id FROM contracts WHERE notes LIKE ?", (f"%{r['contract']}%",))
        row = cur.fetchone()
        if row:
            contract_id = row[0]
        conn.execute(
            """INSERT INTO mortgages (contract_ref, contract_id, owner_id, property_id, nominal, rate, taeg,
               months, monthly_payment, first_payment, last_payment, effective_date, offer_date,
               total_amount, total_interest, total_accessory, total_insurance, capital_amortized)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (r['contract'], contract_id, r['owner_id'], r['property_id'],
             r.get('nominal'), r.get('rate'), r.get('taeg'), r.get('months'),
             r.get('monthly_payment'), r.get('first_payment'), r.get('last_payment'),
             r.get('effective_date'), r.get('offer_date'),
             r.get('total_amount'), r.get('total_interest'), r.get('total_accessory'),
             r.get('total_insurance'), r.get('capital_amortized'))
        )
    return len(rows)


def import_incomes(conn, data):
    if not data: return 0
    rows = data.get('incomes', [])
    for r in rows:
        b = r.get('other_benefits') or {}
        pb = b.get('performance_bonus') or {}
        eyb = b.get('end_of_year_bonus') or {}
        cur = conn.execute(
            """INSERT INTO incomes (person_id, year, avg_gross_monthly_salary, avg_net_monthly_salary,
               health_insurance, transportation_allowance, meal_vouchers,
               performance_bonus_gross, performance_bonus_net,
               end_of_year_bonus_gross, end_of_year_bonus_net, child_allowance)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (r['person_id'], int(r['year']), r.get('avg_gross_monthly_salary'),
             r.get('avg_net_monthly_salary'),
             1 if b.get('health_insurance') else 0,
             b.get('transportation_allowance'), b.get('meal_vouchers'),
             pb.get('amount'), pb.get('net_amount'),
             eyb.get('amount'), eyb.get('net_amount'),
             b.get('child_allowance'))
        )
        income_id = cur.lastrowid
        for key in (r.get('consultant_relevant_benefits') or []):
            conn.execute(
                "INSERT INTO income_consultant_benefits (income_id, benefit_key) VALUES (?,?)",
                (income_id, key)
            )
    return len(rows)


def import_permissions(conn, data):
    if not data: return 0
    default_role = data.get('default_role', 'public')
    for p in data.get('permissions', []):
        conn.execute("INSERT INTO permissions (id, description) VALUES (?,?)", (p['id'], p['description']))
    for r in data.get('roles', []):
        conn.execute(
            "INSERT INTO roles (id, label, description, is_default) VALUES (?,?,?,?)",
            (r['id'], r['label'], r.get('description'), 1 if r['id'] == default_role else 0)
        )
        for perm_id in r.get('permissions', []):
            conn.execute("INSERT INTO role_permissions (role_id, permission_id) VALUES (?,?)", (r['id'], perm_id))
    return len(data.get('roles', []))


def import_timeline(conn, data):
    if not data: return 0
    proj = data.get('project', {})
    cur = conn.execute(
        "INSERT INTO timeline_projects (name, description, start_date, estimated_end_date, currency) VALUES (?,?,?,?,?)",
        (proj['name'], proj.get('description'), proj.get('startDate'), proj.get('estimatedEndDate'), proj.get('currency', 'EUR'))
    )
    project_id = cur.lastrowid
    milestones = proj.get('milestones', [])
    for m in milestones:
        conn.execute(
            """INSERT INTO timeline_milestones (id, project_id, title, description, start_date, end_date,
               status, priority, budget, notes) VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (m['id'], project_id, m['title'], m.get('description'), m.get('startDate'), m.get('endDate'),
             m.get('status'), m.get('priority'), m.get('budget'), m.get('notes'))
        )
    return len(milestones)


def import_snapshots(conn, snapshots_dir: Path):
    count = 0
    if not snapshots_dir.exists():
        return 0
    # Monthly snapshots
    for f in snapshots_dir.glob('????-??.json'):
        with open(f, 'r', encoding='utf-8') as fh:
            snap = json.load(fh)
        conn.execute(
            """INSERT OR REPLACE INTO snapshots (type, year, month, label, generated_at, content)
               VALUES ('monthly', ?, ?, ?, ?, ?)""",
            (snap['year'], snap['month'], snap['label'],
             snap.get('generated_at', datetime.now(timezone.utc).isoformat()),
             json.dumps(snap))
        )
        count += 1
    # Annual snapshots
    annual_dir = snapshots_dir / 'annual'
    if annual_dir.exists():
        for f in annual_dir.glob('????.json'):
            with open(f, 'r', encoding='utf-8') as fh:
                snap = json.load(fh)
            conn.execute(
                """INSERT OR REPLACE INTO snapshots (type, year, month, label, generated_at, content)
                   VALUES ('annual', ?, NULL, ?, ?, ?)""",
                (snap['year'], snap['label'],
                 snap.get('generated_at', datetime.now(timezone.utc).isoformat()),
                 json.dumps(snap))
            )
            count += 1
    return count


# ── Export helpers ────────────────────────────────────────────────────────────

def export_addresses(conn):
    rows = conn.execute("SELECT id, street_number, street_name, postal_code, city, country_code, country FROM addresses ORDER BY id").fetchall()
    return {'addresses': [
        {'id': r[0], 'street_number': r[1], 'street_name': r[2], 'postal_code': r[3],
         'city': r[4], 'country_code': r[5], 'country': r[6]}
        for r in rows
    ]}


def export_people(conn):
    rows = conn.execute("SELECT * FROM people ORDER BY id").fetchall()
    cols = [d[0] for d in conn.execute("SELECT * FROM people LIMIT 0").description]
    result = []
    for r in rows:
        d = dict(zip(cols, r))
        out = {'id': d['id'], 'name': d['name']}
        if d.get('birth_date'):        out['birth_date'] = d['birth_date']
        if d.get('gender'):            out['gender'] = d['gender']
        if d.get('address_id'):        out['address_id'] = d['address_id']
        if d.get('national_number_be'): out['national_number_be'] = d['national_number_be']
        if d.get('cns_lu'):            out['cns_lu'] = d['cns_lu']
        if d.get('role'):              out['role'] = d['role']
        if d.get('id_card_number'):
            out['id_card'] = {'number': d['id_card_number'], 'expiry': d['id_card_expiry']}
        if d.get('phone') or d.get('email'):
            out['contact'] = {k: v for k, v in {'phone': d.get('phone'), 'email': d.get('email')}.items() if v}
        if d.get('occupation_company'):
            out['occupation'] = {k: v for k, v in {
                'location': d.get('occupation_location'),
                'company':  d.get('occupation_company'),
                'position': d.get('occupation_position')
            }.items() if v}
        result.append(out)
    return {'people': result}


def export_real_estate(conn):
    props = conn.execute("SELECT id, address_id, type, status FROM properties ORDER BY id").fetchall()
    result = []
    for p in props:
        owners = [r[0] for r in conn.execute(
            "SELECT person_id FROM property_owners WHERE property_id=? ORDER BY person_id", (p[0],)).fetchall()]
        result.append({'id': p[0], 'address_id': p[1], 'owner_ids': owners, 'type': p[2], 'status': p[3]})
    return {'properties': result}


def export_bank_accounts(conn):
    accounts = conn.execute("SELECT id, person_id, bank, country, iban, bic, type FROM bank_accounts ORDER BY id").fetchall()
    result = []
    for a in accounts:
        cards = [r[0] for r in conn.execute("SELECT card_number FROM bank_cards WHERE account_id=?", (a[0],)).fetchall()]
        entry = {'person_id': a[1], 'bank': a[2], 'country': a[3], 'iban': a[4], 'type': a[6]}
        if a[5]: entry['bic'] = a[5]
        if cards: entry['cards'] = cards
        result.append(entry)
    return {'bank_accounts': result}


def export_contracts(conn):
    contracts = conn.execute("SELECT id, title, category, category_i18n, direction, owner_id, property_id, nominal, taeg, consultant_relevant, notes, employment_start, contract_type, employer_address_id FROM contracts ORDER BY id").fetchall()
    result = []
    for c in contracts:
        periods_rows = conn.execute(
            "SELECT monthly_cost, gross_monthly, start_date, end_date, rate, rate_type FROM contract_periods WHERE contract_id=? ORDER BY id", (c[0],)).fetchall()
        periods = []
        for p in periods_rows:
            period = {'monthly_cost': p[0]}
            if p[1] is not None: period['gross_monthly'] = p[1]
            period['start_date'] = p[2]
            period['end_date']   = p[3]
            if p[4] is not None: period['rate'] = p[4]
            if p[5] is not None: period['rate_type'] = p[5]
            periods.append(period)
        entry = {'id': c[0], 'title': c[1], 'category': c[2]}
        if c[3]: entry['category_i18n'] = c[3]
        if c[4] and c[4] != 'expense': entry['direction'] = c[4]
        entry['owner_id'] = c[5]
        if c[6] is not None: entry['property_id'] = c[6]
        if c[7] is not None: entry['nominal'] = c[7]
        if c[8] is not None: entry['taeg'] = c[8]
        entry['consultant_relevant'] = bool(c[9])
        if c[10]: entry['notes'] = c[10]
        if c[11] is not None: entry['employment_start'] = c[11]
        if c[12] is not None: entry['contract_type'] = c[12]
        if c[13] is not None: entry['employer_address_id'] = c[13]
        entry['periods'] = periods
        result.append(entry)
    return {'contracts': result}


def export_periodic_expenses(conn):
    expenses = conn.execute("SELECT id, title, title_i18n, category, category_i18n, owner_id, property_id, consultant_relevant FROM periodic_expenses ORDER BY id").fetchall()
    result = []
    for e in expenses:
        pmts = conn.execute("SELECT label, amount FROM periodic_expense_payments WHERE expense_id=? ORDER BY id", (e[0],)).fetchall()
        entry = {'id': e[0], 'title': e[1]}
        if e[2]: entry['title_i18n'] = e[2]
        entry['category'] = e[3]
        if e[4]: entry['category_i18n'] = e[4]
        entry['consultant_relevant'] = bool(e[7])
        entry['owner_id'] = e[5]
        if e[6] is not None: entry['property_id'] = e[6]
        entry['payments'] = [{'label': p[0], 'amount': p[1]} for p in pmts]
        result.append(entry)
    return {'periodic_expenses': result}


def export_mortgages(conn):
    rows = conn.execute("""SELECT contract_ref, owner_id, property_id, nominal, rate, taeg, months,
        monthly_payment, first_payment, last_payment, effective_date, offer_date,
        total_amount, total_interest, total_accessory, total_insurance, capital_amortized
        FROM mortgages ORDER BY id""").fetchall()
    result = []
    for r in rows:
        entry = {'contract': r[0], 'owner_id': r[1], 'property_id': r[2]}
        for key, val in zip(['nominal','rate','taeg','months','monthly_payment','first_payment',
                             'last_payment','effective_date','offer_date','total_amount',
                             'total_interest','total_accessory','total_insurance','capital_amortized'], r[3:]):
            if val is not None:
                entry[key] = val
        entry['quarterly_costs'] = []  # stripped intentionally
        result.append(entry)
    return result  # top-level array (matches original mortgages.json format)


def export_incomes(conn):
    rows = conn.execute("SELECT id, person_id, year, avg_gross_monthly_salary, avg_net_monthly_salary, health_insurance, transportation_allowance, meal_vouchers, performance_bonus_gross, performance_bonus_net, end_of_year_bonus_gross, end_of_year_bonus_net, child_allowance FROM incomes ORDER BY person_id, year").fetchall()
    result = []
    for r in rows:
        income_id = r[0]
        benefits = [b[0] for b in conn.execute("SELECT benefit_key FROM income_consultant_benefits WHERE income_id=?", (income_id,)).fetchall()]
        entry = {'person_id': r[1], 'year': str(r[2]),
                 'avg_gross_monthly_salary': r[3], 'avg_net_monthly_salary': r[4]}
        if benefits:
            entry['consultant_relevant_benefits'] = benefits
        other = {}
        if r[5]: other['health_insurance'] = bool(r[5])
        if r[6] is not None: other['transportation_allowance'] = r[6]
        if r[7] is not None: other['meal_vouchers'] = r[7]
        if r[8] is not None: other['performance_bonus'] = {'amount': r[8], 'net_amount': r[9]}
        if r[10] is not None: other['end_of_year_bonus'] = {'amount': r[10], 'net_amount': r[11]}
        if r[12] is not None: other['child_allowance'] = r[12]
        if other:
            entry['other_benefits'] = other
        result.append(entry)
    return {'incomes': result}


def export_permissions(conn):
    perms = conn.execute("SELECT id, description FROM permissions ORDER BY id").fetchall()
    roles = conn.execute("SELECT id, label, description, is_default FROM roles ORDER BY id").fetchall()
    default_role = next((r[0] for r in roles if r[3]), 'public')
    roles_out = []
    for r in roles:
        role_perms = [rp[0] for rp in conn.execute("SELECT permission_id FROM role_permissions WHERE role_id=? ORDER BY permission_id", (r[0],)).fetchall()]
        entry = {'id': r[0], 'label': r[1]}
        if r[2]: entry['description'] = r[2]
        entry['permissions'] = role_perms
        roles_out.append(entry)
    return {
        'permissions': [{'id': p[0], 'description': p[1]} for p in perms],
        'roles': roles_out,
        'default_role': default_role
    }


def export_timeline(conn):
    proj = conn.execute("SELECT id, name, description, start_date, estimated_end_date, currency FROM timeline_projects ORDER BY id LIMIT 1").fetchone()
    if not proj: return {'project': {'name': '', 'milestones': []}}
    milestones = conn.execute("SELECT id, title, description, start_date, end_date, status, priority, budget, notes FROM timeline_milestones WHERE project_id=? ORDER BY id", (proj[0],)).fetchall()
    ms_out = []
    for m in milestones:
        entry = {'id': m[0], 'title': m[1]}
        if m[2]: entry['description'] = m[2]
        if m[3]: entry['startDate'] = m[3]
        if m[4]: entry['endDate'] = m[4]
        entry['status']   = m[5]
        entry['priority'] = m[6]
        entry['budget']   = m[7]
        if m[8]: entry['notes'] = m[8]
        ms_out.append(entry)
    out = {'name': proj[1]}
    if proj[2]: out['description'] = proj[2]
    if proj[3]: out['startDate'] = proj[3]
    if proj[4]: out['estimatedEndDate'] = proj[4]
    if proj[5]: out['currency'] = proj[5]
    out['milestones'] = ms_out
    return {'project': out}


def export_snapshots(conn, snapshots_dir: Path):
    count = 0
    monthly = conn.execute("SELECT year, month, content FROM snapshots WHERE type='monthly' ORDER BY year, month").fetchall()
    for year, month, content in monthly:
        path = snapshots_dir / f"{year}-{month:02d}.json"
        write_json(path, json.loads(content))
        count += 1
    annual = conn.execute("SELECT year, content FROM snapshots WHERE type='annual' ORDER BY year").fetchall()
    for year, content in annual:
        path = snapshots_dir / 'annual' / f"{year}.json"
        write_json(path, json.loads(content))
        count += 1
    return count


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_create(args):
    data_dir     = Path(args.data)
    db_path      = Path(args.db)
    snapshots_dir = Path(args.snapshots) if args.snapshots else SNAPSHOTS_DIR

    print(f"Creating database: {db_path}")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.executescript(DROP_SQL)
    conn.executescript(SCHEMA_SQL)

    steps = [
        ("addresses",          lambda: import_addresses(conn, read_json(data_dir, 'addresses.json'))),
        ("people",             lambda: import_people(conn, read_json(data_dir, 'people.json'))),
        ("real_estate",        lambda: import_real_estate(conn, read_json(data_dir, 'real_estate.json'))),
        ("bank_accounts",      lambda: import_bank_accounts(conn, read_json(data_dir, 'bank_accounts.json'))),
        ("contracts",          lambda: import_contracts(conn, read_json(data_dir, 'contracts.json'))),
        ("periodic_expenses",  lambda: import_periodic_expenses(conn, read_json(data_dir, 'periodic_expenses.json'))),
        ("mortgages",          lambda: import_mortgages(conn, read_json(data_dir, 'mortgages.json'))),
        ("incomes",            lambda: import_incomes(conn, read_json(data_dir, 'incomes.json'))),
        ("permissions",        lambda: import_permissions(conn, read_json(data_dir, 'permissions.json'))),
        ("timeline",           lambda: import_timeline(conn, read_json(data_dir, 'time_line.json'))),
        ("snapshots",          lambda: import_snapshots(conn, snapshots_dir)),
    ]

    for name, fn in steps:
        n = fn()
        print(f"  ✓ {name}: {n} records")

    conn.commit()
    conn.close()
    print(f"\nDatabase created at: {db_path}")


def cmd_export(args):
    db_path       = Path(args.db)
    data_dir      = Path(args.data)
    snapshots_dir = Path(args.snapshots) if args.snapshots else SNAPSHOTS_DIR

    if not db_path.exists():
        print(f"Error: database not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Exporting from: {db_path}")
    conn = sqlite3.connect(str(db_path))

    exports = [
        ('addresses.json',        lambda: export_addresses(conn)),
        ('people.json',           lambda: export_people(conn)),
        ('real_estate.json',      lambda: export_real_estate(conn)),
        ('bank_accounts.json',    lambda: export_bank_accounts(conn)),
        ('contracts.json',        lambda: export_contracts(conn)),
        ('periodic_expenses.json',lambda: export_periodic_expenses(conn)),
        ('mortgages.json',        lambda: export_mortgages(conn)),
        ('incomes.json',          lambda: export_incomes(conn)),
        ('permissions.json',      lambda: export_permissions(conn)),
        ('time_line.json',        lambda: export_timeline(conn)),
    ]

    for filename, fn in exports:
        data = fn()
        write_json(data_dir / filename, data)
        count = len(data) if isinstance(data, list) else len(next(iter(data.values()), []))
        print(f"  ✓ {filename}: {count} records")

    n = export_snapshots(conn, snapshots_dir)
    print(f"  ✓ snapshots: {n} files")

    conn.close()
    print(f"\nExport complete → {data_dir}")


def cmd_validate(args):
    db_path  = Path(args.db)
    data_dir = Path(args.data)
    snapshots_dir = Path(args.snapshots) if args.snapshots else SNAPSHOTS_DIR

    if not db_path.exists():
        print(f"Error: database not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    errors = []

    def check(label, json_count, db_count):
        status = "✓" if json_count == db_count else "✗"
        msg = f"  {status} {label}: JSON={json_count}  DB={db_count}"
        print(msg)
        if json_count != db_count:
            errors.append(f"{label}: JSON={json_count} vs DB={db_count}")

    # Count comparisons
    def count_db(table): return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    def count_json(filename, key):
        d = read_json(data_dir, filename)
        if d is None: return 0
        return len(d.get(key, d) if isinstance(d, dict) else d)

    print("\n── Record count comparisons ─────────────────────")
    check("addresses",                count_json('addresses.json', 'addresses'),               count_db('addresses'))
    check("people",                   count_json('people.json', 'people'),                     count_db('people'))
    check("properties",               count_json('real_estate.json', 'properties'),            count_db('properties'))
    check("bank_accounts",            count_json('bank_accounts.json', 'bank_accounts'),       count_db('bank_accounts'))
    check("contracts",                count_json('contracts.json', 'contracts'),               count_db('contracts'))
    check("periodic_expenses",        count_json('periodic_expenses.json', 'periodic_expenses'), count_db('periodic_expenses'))
    check("mortgages",                count_json('mortgages.json', None),                      count_db('mortgages'))
    check("incomes",                  count_json('incomes.json', 'incomes'),                   count_db('incomes'))

    # Snapshot count
    monthly_json  = len(list(snapshots_dir.glob('????-??.json'))) if snapshots_dir.exists() else 0
    annual_json   = len(list((snapshots_dir/'annual').glob('????.json'))) if (snapshots_dir/'annual').exists() else 0
    monthly_db    = conn.execute("SELECT COUNT(*) FROM snapshots WHERE type='monthly'").fetchone()[0]
    annual_db     = conn.execute("SELECT COUNT(*) FROM snapshots WHERE type='annual'").fetchone()[0]
    check("monthly snapshots", monthly_json, monthly_db)
    check("annual snapshots",  annual_json,  annual_db)

    # FK integrity
    print("\n── Foreign key integrity ────────────────────────")
    conn.execute("PRAGMA foreign_keys = ON")
    fk_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_errors:
        for e in fk_errors:
            print(f"  ✗ FK violation: {e}")
            errors.append(str(e))
    else:
        print("  ✓ All foreign key constraints satisfied")

    conn.close()

    if errors:
        print(f"\n⚠  {len(errors)} issue(s) found.")
        sys.exit(1)
    else:
        print(f"\n✓  All checks passed.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Personal Finance — SQLite Data Manager')
    parser.add_argument('--db',        default=str(DEFAULT_DB),   help='Path to SQLite database file')
    parser.add_argument('--data',      default=str(DEFAULT_DATA), help='Path to JSON data directory')
    parser.add_argument('--snapshots', default=None,              help='Path to snapshots directory (default: output/snapshots)')

    subparsers = parser.add_subparsers(dest='command', required=True)
    subparsers.add_parser('create',   help='Create/reset SQLite DB from JSON files')
    subparsers.add_parser('export',   help='Export SQLite DB back to JSON files')
    subparsers.add_parser('validate', help='Validate DB integrity against JSON files')

    args = parser.parse_args()

    if   args.command == 'create':   cmd_create(args)
    elif args.command == 'export':   cmd_export(args)
    elif args.command == 'validate': cmd_validate(args)


if __name__ == '__main__':
    main()
