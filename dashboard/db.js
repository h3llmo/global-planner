/**
 * db.js — SQLite data access module for the finance dashboard.
 *
 * Single shared database (finance.db) for all budget plans.
 * Call initAllPlans() at startup, then setActivePlan(slug) per request.
 * All per-plan queries filter by plan_id = _activePlanSlug.
 */

'use strict';

const fs            = require('fs');
const path          = require('path');
const { execSync }  = require('child_process');
const initSqlJs     = require('sql.js');

const PROJECT_ROOT    = path.resolve(__dirname, '..', '..');
const DATA_MANAGER    = path.join(PROJECT_ROOT, 'apps', 'DataManager.py');
const SNAPSHOT_SCRIPT = path.join(PROJECT_ROOT, 'apps', 'SnapshotSituation.py');
const BUDGET_PLANS_DIR = path.join(PROJECT_ROOT, 'data', 'budget-plans');
const PEOPLE_DIR       = path.join(PROJECT_ROOT, 'data', 'people');

// Single DB paths
const DB_COMMITTED_PATH = path.join(__dirname, 'finance.db');   // committed to git, read-only on Vercel
const DB_PROD_PATH      = '/tmp/finance.db';                    // writable on Vercel
const DB_DEV_PATH       = path.join(__dirname, 'finance.db');

function getDbPath() {
  return process.env.NODE_ENV === 'production' ? DB_PROD_PATH : DB_DEV_PATH;
}

// SQLite schema — mirrors DataManager.py SCHEMA_SQL (kept in sync manually)
// All per-plan tables include plan_id TEXT NOT NULL for multi-plan separation.
const SCHEMA_SQL = `
PRAGMA foreign_keys = ON;

-- Global tables (no plan_id)
CREATE TABLE IF NOT EXISTS budget_plans (
  id             TEXT PRIMARY KEY,
  name           TEXT NOT NULL,
  description    TEXT,
  currency       TEXT DEFAULT 'EUR',
  owner_auth0_id TEXT,
  created_at     TEXT
);
CREATE TABLE IF NOT EXISTS plan_members (
  plan_id     TEXT NOT NULL REFERENCES budget_plans(id),
  person_slug TEXT NOT NULL,
  PRIMARY KEY (plan_id, person_slug)
);
CREATE TABLE IF NOT EXISTS budget_plan_access (
  plan_id     TEXT NOT NULL REFERENCES budget_plans(id),
  auth0_email TEXT NOT NULL,
  role        TEXT NOT NULL DEFAULT 'consultant',
  PRIMARY KEY (plan_id, auth0_email)
);
CREATE TABLE IF NOT EXISTS permissions (
  id          TEXT PRIMARY KEY,
  description TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS roles (
  id          TEXT PRIMARY KEY,
  label       TEXT NOT NULL,
  description TEXT,
  is_default  INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS role_permissions (
  role_id       TEXT NOT NULL REFERENCES roles(id),
  permission_id TEXT NOT NULL REFERENCES permissions(id),
  PRIMARY KEY (role_id, permission_id)
);

-- Per-plan tables (all have plan_id)
CREATE TABLE IF NOT EXISTS addresses (
  plan_id      TEXT NOT NULL REFERENCES budget_plans(id),
  id           INTEGER NOT NULL,
  street_number TEXT,
  street_name  TEXT NOT NULL,
  postal_code  TEXT NOT NULL,
  city         TEXT NOT NULL,
  country_code TEXT NOT NULL,
  country      TEXT NOT NULL,
  PRIMARY KEY (plan_id, id)
);
CREATE TABLE IF NOT EXISTS people (
  plan_id              TEXT NOT NULL REFERENCES budget_plans(id),
  id                   INTEGER NOT NULL,
  name                 TEXT NOT NULL,
  birth_date           TEXT,
  gender               TEXT,
  address_id           INTEGER,
  national_number_be   TEXT,
  cns_lu               TEXT,
  id_card_number       TEXT,
  id_card_expiry       TEXT,
  phone                TEXT,
  email                TEXT,
  auth0_email          TEXT,
  role                 TEXT,
  occupation_location  TEXT,
  occupation_company   TEXT,
  occupation_position  TEXT,
  PRIMARY KEY (plan_id, id),
  FOREIGN KEY (plan_id, address_id) REFERENCES addresses(plan_id, id)
);
CREATE TABLE IF NOT EXISTS properties (
  plan_id    TEXT NOT NULL REFERENCES budget_plans(id),
  id         INTEGER NOT NULL,
  address_id INTEGER NOT NULL,
  type       TEXT NOT NULL,
  status     TEXT NOT NULL,
  PRIMARY KEY (plan_id, id),
  FOREIGN KEY (plan_id, address_id) REFERENCES addresses(plan_id, id)
);
CREATE TABLE IF NOT EXISTS property_owners (
  plan_id     TEXT NOT NULL REFERENCES budget_plans(id),
  property_id INTEGER NOT NULL,
  person_id   INTEGER NOT NULL,
  PRIMARY KEY (plan_id, property_id, person_id),
  FOREIGN KEY (plan_id, property_id) REFERENCES properties(plan_id, id),
  FOREIGN KEY (plan_id, person_id)   REFERENCES people(plan_id, id)
);
CREATE TABLE IF NOT EXISTS bank_accounts (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  plan_id   TEXT NOT NULL REFERENCES budget_plans(id),
  person_id INTEGER NOT NULL,
  bank      TEXT NOT NULL,
  country   TEXT NOT NULL,
  iban      TEXT NOT NULL,
  bic       TEXT,
  type      TEXT NOT NULL,
  UNIQUE (plan_id, iban),
  FOREIGN KEY (plan_id, person_id) REFERENCES people(plan_id, id)
);
CREATE TABLE IF NOT EXISTS bank_cards (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  account_id  INTEGER NOT NULL REFERENCES bank_accounts(id),
  card_number TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS contracts (
  plan_id             TEXT NOT NULL REFERENCES budget_plans(id),
  id                  INTEGER NOT NULL,
  title               TEXT NOT NULL,
  category            TEXT NOT NULL,
  category_i18n       TEXT,
  direction           TEXT NOT NULL DEFAULT 'expense',
  owner_id            INTEGER NOT NULL,
  property_id         INTEGER,
  nominal             REAL,
  taeg                REAL,
  consultant_relevant INTEGER NOT NULL DEFAULT 1,
  notes               TEXT,
  employment_start    TEXT,
  contract_type       TEXT,
  employer_address_id INTEGER,
  PRIMARY KEY (plan_id, id),
  FOREIGN KEY (plan_id, owner_id) REFERENCES people(plan_id, id)
);
CREATE TABLE IF NOT EXISTS contract_periods (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  plan_id       TEXT NOT NULL REFERENCES budget_plans(id),
  contract_id   INTEGER NOT NULL,
  monthly_cost  REAL NOT NULL,
  gross_monthly REAL,
  start_date    TEXT,
  end_date      TEXT,
  rate          REAL,
  rate_type     TEXT,
  FOREIGN KEY (plan_id, contract_id) REFERENCES contracts(plan_id, id)
);
CREATE TABLE IF NOT EXISTS periodic_expenses (
  plan_id             TEXT NOT NULL REFERENCES budget_plans(id),
  id                  INTEGER NOT NULL,
  title               TEXT NOT NULL,
  title_i18n          TEXT,
  category            TEXT NOT NULL,
  category_i18n       TEXT,
  owner_id            INTEGER NOT NULL,
  property_id         INTEGER,
  consultant_relevant INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (plan_id, id),
  FOREIGN KEY (plan_id, owner_id) REFERENCES people(plan_id, id)
);
CREATE TABLE IF NOT EXISTS periodic_expense_payments (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  plan_id    TEXT NOT NULL REFERENCES budget_plans(id),
  expense_id INTEGER NOT NULL,
  label      TEXT NOT NULL,
  amount     REAL NOT NULL,
  FOREIGN KEY (plan_id, expense_id) REFERENCES periodic_expenses(plan_id, id)
);
CREATE TABLE IF NOT EXISTS mortgages (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  plan_id           TEXT NOT NULL REFERENCES budget_plans(id),
  contract_ref      TEXT NOT NULL,
  contract_id       INTEGER,
  owner_id          INTEGER NOT NULL,
  property_id       INTEGER NOT NULL,
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
  capital_amortized REAL,
  UNIQUE (plan_id, contract_ref)
);
CREATE TABLE IF NOT EXISTS incomes (
  id                       INTEGER PRIMARY KEY AUTOINCREMENT,
  plan_id                  TEXT NOT NULL REFERENCES budget_plans(id),
  person_id                INTEGER NOT NULL,
  year                     INTEGER NOT NULL,
  avg_gross_monthly_salary REAL,
  avg_net_monthly_salary   REAL,
  health_insurance         INTEGER,
  transportation_allowance REAL,
  meal_vouchers            REAL,
  performance_bonus_gross  REAL,
  performance_bonus_net    REAL,
  end_of_year_bonus_gross  REAL,
  end_of_year_bonus_net    REAL,
  child_allowance          REAL,
  UNIQUE (plan_id, person_id, year),
  FOREIGN KEY (plan_id, person_id) REFERENCES people(plan_id, id)
);
CREATE TABLE IF NOT EXISTS income_consultant_benefits (
  income_id   INTEGER NOT NULL REFERENCES incomes(id),
  benefit_key TEXT NOT NULL,
  PRIMARY KEY (income_id, benefit_key)
);
CREATE TABLE IF NOT EXISTS timeline_projects (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  plan_id            TEXT NOT NULL REFERENCES budget_plans(id),
  name               TEXT NOT NULL,
  description        TEXT,
  start_date         TEXT,
  estimated_end_date TEXT,
  currency           TEXT DEFAULT 'EUR'
);
CREATE TABLE IF NOT EXISTS timeline_milestones (
  plan_id     TEXT NOT NULL REFERENCES budget_plans(id),
  id          INTEGER NOT NULL,
  project_id  INTEGER NOT NULL REFERENCES timeline_projects(id),
  title       TEXT NOT NULL,
  description TEXT,
  start_date  TEXT,
  end_date    TEXT,
  status      TEXT,
  priority    TEXT,
  budget      REAL,
  notes       TEXT,
  PRIMARY KEY (plan_id, id)
);
CREATE TABLE IF NOT EXISTS snapshots (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  plan_id      TEXT NOT NULL REFERENCES budget_plans(id),
  type         TEXT NOT NULL CHECK(type IN ('monthly','annual')),
  year         INTEGER NOT NULL,
  month        INTEGER,
  label        TEXT NOT NULL,
  generated_at TEXT NOT NULL,
  content      TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_monthly ON snapshots(plan_id, year, month) WHERE type='monthly';
CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_annual  ON snapshots(plan_id, year)        WHERE type='annual';
CREATE TABLE IF NOT EXISTS virtual_contracts (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  plan_id      TEXT NOT NULL REFERENCES budget_plans(id),
  title        TEXT NOT NULL,
  category     TEXT NOT NULL,
  direction    TEXT NOT NULL DEFAULT 'expense',
  owner_id     INTEGER,
  monthly_cost REAL,
  start_date   TEXT,
  end_date     TEXT,
  notes        TEXT
);
CREATE TABLE IF NOT EXISTS virtual_incomes (
  id                     INTEGER PRIMARY KEY AUTOINCREMENT,
  plan_id                TEXT NOT NULL REFERENCES budget_plans(id),
  person_id              INTEGER,
  year                   INTEGER NOT NULL,
  avg_net_monthly_salary REAL,
  notes                  TEXT
);
CREATE TABLE IF NOT EXISTS virtual_periodic_expenses (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  plan_id  TEXT NOT NULL REFERENCES budget_plans(id),
  title    TEXT NOT NULL,
  category TEXT NOT NULL,
  owner_id INTEGER,
  payments TEXT,
  notes    TEXT
);
`;

/**
 * Initialize (or reimport) a single plan into the shared database.
 * In development: runs DataManager.py --db <path> --plan <slug> create
 * In production: no-op (DB is pre-built and copied from /var/task)
 */
async function initDb(slug) {
  const isProd = process.env.NODE_ENV === 'production';
  if (isProd) return; // DB shipped pre-built; runtime creates plans via createPlan()

  const dbPath = getDbPath();
  console.log(`[db] Importing plan "${slug}" into ${dbPath}...`);
  try {
    execSync(`python "${SNAPSHOT_SCRIPT}" --plan "${slug}"`, {
      stdio: 'inherit', cwd: PROJECT_ROOT,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' },
    });
    execSync(`python "${DATA_MANAGER}" --db "${dbPath}" --plan "${slug}" create`, {
      stdio: 'inherit', cwd: PROJECT_ROOT,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' },
    });
    console.log(`[db] Import complete for "${slug}".`);
    // Invalidate cached DB so it reloads from the freshly written file
    _db = null;
  } catch (e) {
    console.error(`[db] Import failed for "${slug}":`, e.message);
    if (!_db) await getDb(); // ensure DB is at least loaded
  }
}

/** Return slugs from local data/budget-plans/ (used at startup to trigger JSON->DB import) */
function _getLocalPlanSlugs() {
  if (!fs.existsSync(BUDGET_PLANS_DIR)) return [];
  return fs.readdirSync(BUDGET_PLANS_DIR)
    .filter(d => { try { return fs.statSync(path.join(BUDGET_PLANS_DIR, d)).isDirectory(); } catch { return false; } });
}

// In-memory cache — populated after initAllPlans(), updated by createPlan()
let _plansMeta = [];

/** (Re)load _plansMeta from the shared database */
async function _refreshPlansMeta() {
  try {
    const db    = await getDb();
    const plans = query(db, 'SELECT id, name, description, currency FROM budget_plans ORDER BY created_at, id');
    _plansMeta  = plans.map(p => {
      const members = query(db, 'SELECT person_slug FROM plan_members WHERE plan_id=? ORDER BY person_slug', [p.id])
        .map(r => r.person_slug);
      const access = query(db, 'SELECT auth0_email, role FROM budget_plan_access WHERE plan_id=?', [p.id])
        .map(r => ({ auth0_email: r.auth0_email, role: r.role }));
      return { ...p, members, access };
    });
  } catch (e) {
    console.warn('[db] _refreshPlansMeta failed:', e.message);
    _plansMeta = [];
  }
}

/** Return the cached plan list (populated by initAllPlans / createPlan) */
function listPlans() {
  return _plansMeta;
}

/** Initialize DBs for all plans found in data/budget-plans/, then populate cache */
async function initAllPlans() {
  const slugs = _getLocalPlanSlugs();
  if (slugs.length === 0) {
    console.warn('[db] No budget-plans found — starting with empty schema.');
    await getDb(); // ensure schema exists
  } else {
    for (const slug of slugs) {
      await initDb(slug);
    }
  }
  await _refreshPlansMeta();
}

/**
 * Create a new plan in the shared DB — no filesystem writes to data/budget-plans/.
 */
async function createPlan({ id, name, description, currency, members, access }) {
  const db = await getDb();
  const existing = queryOne(db, 'SELECT id FROM budget_plans WHERE id=?', [id]);
  if (existing) throw new Error(`A plan with id "${id}" already exists.`);

  const today = new Date().toISOString().slice(0, 10);
  db.run('INSERT INTO budget_plans (id, name, description, currency, created_at) VALUES (?,?,?,?,?)',
    [id, name, description || '', currency || 'EUR', today]);

  // Seed permissions & roles from shared config
  const permFile = path.join(PROJECT_ROOT, 'data', 'shared', 'permissions.json');
  if (fs.existsSync(permFile)) {
    try {
      const cfg = JSON.parse(fs.readFileSync(permFile, 'utf8'));
      for (const p of (cfg.permissions || []))
        db.run('INSERT OR IGNORE INTO permissions (id, description) VALUES (?,?)', [p.id, p.description]);
      for (const r of (cfg.roles || [])) {
        db.run('INSERT OR IGNORE INTO roles (id, label, description, is_default) VALUES (?,?,?,?)',
          [r.id, r.label, r.description || '', r.id === cfg.default_role ? 1 : 0]);
        for (const pid of (r.permissions || []))
          db.run('INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?,?)', [r.id, pid]);
      }
    } catch (e) { console.warn('[db] createPlan: could not seed permissions:', e.message); }
  }

  for (const slug of (members || []))
    db.run('INSERT OR IGNORE INTO plan_members (plan_id, person_slug) VALUES (?,?)', [id, slug]);

  for (const a of (access || []))
    if (a.email?.trim())
      db.run('INSERT OR IGNORE INTO budget_plan_access (plan_id, auth0_email, role) VALUES (?,?,?)',
        [id, a.email.trim(), a.role || 'consultant']);

  _persistDb();
  await _refreshPlansMeta();
  return { id, name, url: `/plan/${id}` };
}

// -- Single DB state -----------------------------------------------------------
let _db          = null;
let _activePlanSlug = null;

/** Set the active plan slug for subsequent DB queries in this request cycle */
function setActivePlan(slug) {
  _activePlanSlug = slug;
}

/** @type {Promise<import('sql.js').SqlJsStatic>|null} */
let _sqlPromise = null;
async function _getSql() {
  if (!_sqlPromise) _sqlPromise = initSqlJs({
    locateFile: file => path.join(__dirname, 'node_modules/sql.js/dist', file),
  });
  return _sqlPromise;
}

/** Load the single shared DB from disk (or create empty schema) */
async function _loadDb() {
  const dbPath = getDbPath();
  const isProd = process.env.NODE_ENV === 'production';

  // Production: copy committed DB to /tmp if not already there
  if (isProd && !fs.existsSync(dbPath) && fs.existsSync(DB_COMMITTED_PATH)) {
    try {
      fs.copyFileSync(DB_COMMITTED_PATH, dbPath);
      console.log(`[db] Copied committed DB to ${dbPath}`);
    } catch (e) {
      console.warn('[db] Could not copy DB to /tmp:', e.message);
    }
  }

  const SQL = await _getSql();
  if (fs.existsSync(dbPath)) {
    const buffer = fs.readFileSync(dbPath);
    _db = new SQL.Database(buffer);
    console.log(`[db] Loaded DB from ${dbPath}`);
  } else {
    _db = new SQL.Database();
    _db.run(SCHEMA_SQL);
    _persistDb();
    console.log(`[db] Created new DB at ${dbPath}`);
  }
  return _db;
}

/** Persist in-memory DB to disk */
function _persistDb() {
  if (!_db) return;
  try {
    fs.writeFileSync(getDbPath(), Buffer.from(_db.export()));
  } catch (e) {
    console.warn('[db] Could not persist DB:', e.message);
  }
}

/** Return the shared DB (loads once and caches) */
let _dbLoadPromise = null;
async function getDb() {
  if (_db) return _db;
  if (_dbLoadPromise) return _dbLoadPromise;
  _dbLoadPromise = _loadDb().then(db => { _dbLoadPromise = null; return db; });
  return _dbLoadPromise;
}

/** Run a SELECT and return rows as array of plain objects */
function query(db, sql, params = []) {
  const stmt    = db.prepare(sql);
  const results = [];
  stmt.bind(params);
  while (stmt.step()) results.push(stmt.getAsObject());
  stmt.free();
  return results;
}

/** Run a SELECT and return first row or null */
function queryOne(db, sql, params = []) {
  const rows = query(db, sql, params);
  return rows.length > 0 ? rows[0] : null;
}

// -- Public API ----------------------------------------------------------------

async function getAddresses() {
  const db = await getDb();
  const rows = query(db,
    'SELECT id, street_number, street_name, postal_code, city, country_code, country FROM addresses WHERE plan_id=? ORDER BY id',
    [_activePlanSlug]);
  return { addresses: rows };
}

async function getPeople() {
  const db   = await getDb();
  const rows = query(db, 'SELECT * FROM people WHERE plan_id=? ORDER BY id', [_activePlanSlug]);
  const people = rows.map(r => {
    const out = { id: r.id, name: r.name };
    if (r.birth_date)         out.birth_date         = r.birth_date;
    if (r.gender)             out.gender             = r.gender;
    if (r.address_id != null) out.address_id         = r.address_id;
    if (r.national_number_be) out.national_number_be = r.national_number_be;
    if (r.cns_lu)             out.cns_lu             = r.cns_lu;
    if (r.role)               out.role               = r.role;
    if (r.id_card_number)     out.id_card = { number: r.id_card_number, expiry: r.id_card_expiry };
    if (r.phone || r.email) {
      out.contact = {};
      if (r.phone) out.contact.phone = r.phone;
      if (r.email) out.contact.email = r.email;
    }
    if (r.occupation_company)
      out.occupation = { location: r.occupation_location, company: r.occupation_company, position: r.occupation_position };
    return out;
  });
  return { people };
}

async function getRealEstate() {
  const db    = await getDb();
  const props = query(db,
    'SELECT id, address_id, type, status FROM properties WHERE plan_id=? ORDER BY id',
    [_activePlanSlug]);
  const properties = props.map(p => {
    const owners = query(db,
      'SELECT person_id FROM property_owners WHERE plan_id=? AND property_id=? ORDER BY person_id',
      [_activePlanSlug, p.id]).map(r => r.person_id);
    return { id: p.id, address_id: p.address_id, owner_ids: owners, type: p.type, status: p.status };
  });
  return { properties };
}

async function getBankAccounts() {
  const db       = await getDb();
  const accounts = query(db,
    'SELECT id, person_id, bank, country, iban, bic, type FROM bank_accounts WHERE plan_id=? ORDER BY id',
    [_activePlanSlug]);
  const bank_accounts = accounts.map(a => {
    const cards = query(db, 'SELECT card_number FROM bank_cards WHERE account_id=?', [a.id])
      .map(r => r.card_number);
    const entry = { id: a.id, person_id: a.person_id, bank: a.bank, country: a.country, iban: a.iban, type: a.type };
    if (a.bic)        entry.bic   = a.bic;
    if (cards.length) entry.cards = cards;
    return entry;
  });
  return { bank_accounts };
}

async function getContracts() {
  const db        = await getDb();
  const contracts = query(db,
    'SELECT id, title, category, category_i18n, direction, owner_id, property_id, nominal, taeg, consultant_relevant, notes, employment_start, contract_type, employer_address_id FROM contracts WHERE plan_id=? ORDER BY id',
    [_activePlanSlug]);
  const result = contracts.map(c => {
    const periods = query(db,
      'SELECT monthly_cost, gross_monthly, start_date, end_date, rate, rate_type FROM contract_periods WHERE plan_id=? AND contract_id=? ORDER BY id',
      [_activePlanSlug, c.id]
    ).map(p => {
      const period = { monthly_cost: p.monthly_cost, start_date: p.start_date, end_date: p.end_date };
      if (p.gross_monthly != null) period.gross_monthly = p.gross_monthly;
      if (p.rate      != null) period.rate      = p.rate;
      if (p.rate_type)         period.rate_type = p.rate_type;
      return period;
    });
    const entry = { id: c.id, title: c.title, category: c.category };
    if (c.category_i18n)          entry.category_i18n    = c.category_i18n;
    if (c.direction && c.direction !== 'expense') entry.direction = c.direction;
    entry.owner_id = c.owner_id;
    if (c.property_id     != null) entry.property_id        = c.property_id;
    if (c.nominal         != null) entry.nominal            = c.nominal;
    if (c.taeg            != null) entry.taeg               = c.taeg;
    entry.consultant_relevant = !!c.consultant_relevant;
    if (c.notes)                   entry.notes              = c.notes;
    if (c.employment_start != null) entry.employment_start  = c.employment_start;
    if (c.contract_type    != null) entry.contract_type     = c.contract_type;
    if (c.employer_address_id != null) entry.employer_address_id = c.employer_address_id;
    entry.periods = periods;
    return entry;
  });
  return { contracts: result };
}

async function getPeriodicExpenses() {
  const db       = await getDb();
  const expenses = query(db,
    'SELECT id, title, title_i18n, category, category_i18n, owner_id, property_id, consultant_relevant FROM periodic_expenses WHERE plan_id=? ORDER BY id',
    [_activePlanSlug]);
  const periodic_expenses = expenses.map(e => {
    const payments = query(db,
      'SELECT label, amount FROM periodic_expense_payments WHERE plan_id=? AND expense_id=? ORDER BY id',
      [_activePlanSlug, e.id]).map(p => ({ label: p.label, amount: p.amount }));
    const entry = { id: e.id, title: e.title };
    if (e.title_i18n)    entry.title_i18n    = e.title_i18n;
    entry.category = e.category;
    if (e.category_i18n) entry.category_i18n = e.category_i18n;
    entry.consultant_relevant = !!e.consultant_relevant;
    entry.owner_id = e.owner_id;
    if (e.property_id != null) entry.property_id = e.property_id;
    entry.payments = payments;
    return entry;
  });
  return { periodic_expenses };
}

async function getMortgages() {
  const db   = await getDb();
  const rows = query(db,
    `SELECT id, contract_ref, owner_id, property_id, nominal, rate, taeg, months,
      monthly_payment, first_payment, last_payment, effective_date, offer_date,
      total_amount, total_interest, total_accessory, total_insurance, capital_amortized
     FROM mortgages WHERE plan_id=? ORDER BY id`,
    [_activePlanSlug]);
  return rows.map(r => {
    const entry = { id: r.id, contract: r.contract_ref, owner_id: r.owner_id, property_id: r.property_id };
    const fields = ['nominal','rate','taeg','months','monthly_payment','first_payment','last_payment',
                    'effective_date','offer_date','total_amount','total_interest','total_accessory',
                    'total_insurance','capital_amortized'];
    fields.forEach(k => { if (r[k] != null) entry[k] = r[k]; });
    entry.quarterly_costs = [];
    return entry;
  });
}

async function getIncomes() {
  const db   = await getDb();
  const rows = query(db,
    `SELECT id, person_id, year, avg_gross_monthly_salary, avg_net_monthly_salary,
      health_insurance, transportation_allowance, meal_vouchers,
      performance_bonus_gross, performance_bonus_net,
      end_of_year_bonus_gross, end_of_year_bonus_net, child_allowance
     FROM incomes WHERE plan_id=? ORDER BY person_id, year`,
    [_activePlanSlug]);
  const incomes = rows.map(r => {
    const benefits = query(db,
      'SELECT benefit_key FROM income_consultant_benefits WHERE income_id=?', [r.id])
      .map(b => b.benefit_key);
    const entry = { id: r.id, person_id: r.person_id, year: String(r.year),
                    avg_gross_monthly_salary: r.avg_gross_monthly_salary,
                    avg_net_monthly_salary:   r.avg_net_monthly_salary };
    if (benefits.length) entry.consultant_relevant_benefits = benefits;
    const other = {};
    if (r.health_insurance)                other.health_insurance         = !!r.health_insurance;
    if (r.transportation_allowance != null) other.transportation_allowance = r.transportation_allowance;
    if (r.meal_vouchers            != null) other.meal_vouchers            = r.meal_vouchers;
    if (r.performance_bonus_gross  != null) other.performance_bonus        = { amount: r.performance_bonus_gross, net_amount: r.performance_bonus_net };
    if (r.end_of_year_bonus_gross  != null) other.end_of_year_bonus        = { amount: r.end_of_year_bonus_gross, net_amount: r.end_of_year_bonus_net };
    if (r.child_allowance          != null) other.child_allowance          = r.child_allowance;
    if (Object.keys(other).length) entry.other_benefits = other;
    return entry;
  });
  return { incomes };
}

async function getPermissionsConfig() {
  const db          = await getDb();
  const permissions = query(db, 'SELECT id, description FROM permissions ORDER BY id')
    .map(r => ({ id: r.id, description: r.description }));
  const rolesRaw    = query(db, 'SELECT id, label, description, is_default FROM roles ORDER BY id');
  const default_role = (rolesRaw.find(r => r.is_default) || {}).id || 'public';
  const roles = rolesRaw.map(r => {
    const perms = query(db,
      'SELECT permission_id FROM role_permissions WHERE role_id=? ORDER BY permission_id', [r.id])
      .map(p => p.permission_id);
    const entry = { id: r.id, label: r.label };
    if (r.description) entry.description = r.description;
    entry.permissions = perms;
    return entry;
  });
  return { permissions, roles, default_role };
}

async function getTimeline() {
  const db   = await getDb();
  const proj = queryOne(db,
    'SELECT id, name, description, start_date, estimated_end_date, currency FROM timeline_projects WHERE plan_id=? LIMIT 1',
    [_activePlanSlug]);
  if (!proj) return { project: { name: '', milestones: [] } };
  const milestones = query(db,
    'SELECT id, title, description, start_date, end_date, status, priority, budget, notes FROM timeline_milestones WHERE plan_id=? AND project_id=? ORDER BY id',
    [_activePlanSlug, proj.id]
  ).map(m => {
    const entry = { id: m.id, title: m.title };
    if (m.description) entry.description = m.description;
    if (m.start_date)  entry.startDate   = m.start_date;
    if (m.end_date)    entry.endDate     = m.end_date;
    entry.status   = m.status;
    entry.priority = m.priority;
    entry.budget   = m.budget;
    if (m.notes)       entry.notes = m.notes;
    return entry;
  });
  const project = { name: proj.name };
  if (proj.description)        project.description      = proj.description;
  if (proj.start_date)         project.startDate        = proj.start_date;
  if (proj.estimated_end_date) project.estimatedEndDate = proj.estimated_end_date;
  if (proj.currency)           project.currency         = proj.currency;
  project.milestones = milestones;
  return { project };
}

async function listSnapshotMonths() {
  const db   = await getDb();
  const rows = query(db,
    "SELECT year, month FROM snapshots WHERE plan_id=? AND type='monthly' ORDER BY year, month",
    [_activePlanSlug]);
  return rows.map(r => `${r.year}-${String(r.month).padStart(2, '0')}`);
}

async function listSnapshotYears() {
  const db   = await getDb();
  const rows = query(db,
    "SELECT year FROM snapshots WHERE plan_id=? AND type='annual' ORDER BY year",
    [_activePlanSlug]);
  return rows.map(r => r.year);
}

async function getSnapshot(year, month) {
  const db  = await getDb();
  const row = queryOne(db,
    "SELECT content FROM snapshots WHERE plan_id=? AND type='monthly' AND year=? AND month=?",
    [_activePlanSlug, parseInt(year), parseInt(month)]);
  return row ? JSON.parse(row.content) : null;
}

async function getAnnualSnapshot(year) {
  const db  = await getDb();
  const row = queryOne(db,
    "SELECT content FROM snapshots WHERE plan_id=? AND type='annual' AND year=?",
    [_activePlanSlug, parseInt(year)]);
  return row ? JSON.parse(row.content) : null;
}

/** Find a person in the active plan whose auth0_email matches the given email */
async function getPersonByAuth0Email(email) {
  if (!email) return null;
  const db = await getDb();
  return queryOne(db,
    'SELECT id, name FROM people WHERE plan_id=? AND auth0_email=?',
    [_activePlanSlug, email]);
}

/** List all available people from the data/people directory (not plan-specific) */
function listPeople() {
  if (!fs.existsSync(PEOPLE_DIR)) return [];
  return fs.readdirSync(PEOPLE_DIR)
    .filter(d => {
      try {
        return fs.statSync(path.join(PEOPLE_DIR, d)).isDirectory()
          && fs.existsSync(path.join(PEOPLE_DIR, d, 'profile.json'));
      } catch { return false; }
    })
    .map(slug => {
      try {
        const p = JSON.parse(fs.readFileSync(path.join(PEOPLE_DIR, slug, 'profile.json'), 'utf8'));
        return { id: slug, name: p.name, occupation: p.occupation || null };
      } catch { return { id: slug, name: slug }; }
    });
}

// ── Write helpers ──────────────────────────────────────────────────────────────

function _nextId(db, table) {
  const row = queryOne(db, `SELECT COALESCE(MAX(id), 0) + 1 AS next FROM ${table} WHERE plan_id=?`, [_activePlanSlug]);
  return row ? row.next : 1;
}

async function createAddress(data) {
  const db = await getDb();
  const id = _nextId(db, 'addresses');
  db.run(
    'INSERT INTO addresses (plan_id,id,street_number,street_name,postal_code,city,country_code,country) VALUES (?,?,?,?,?,?,?,?)',
    [_activePlanSlug, id, data.street_number||null, data.street_name, data.postal_code, data.city, data.country_code, data.country]
  );
  _persistDb(); return { id };
}

async function updateAddress(id, data) {
  const db = await getDb();
  db.run(
    'UPDATE addresses SET street_number=?,street_name=?,postal_code=?,city=?,country_code=?,country=? WHERE plan_id=? AND id=?',
    [data.street_number||null, data.street_name, data.postal_code, data.city, data.country_code, data.country, _activePlanSlug, Number(id)]
  );
  _persistDb();
}

async function deleteAddress(id) {
  const db = await getDb();
  db.run('DELETE FROM addresses WHERE plan_id=? AND id=?', [_activePlanSlug, Number(id)]);
  _persistDb();
}

async function createPerson(data) {
  const db = await getDb();
  const id = _nextId(db, 'people');
  db.run(
    `INSERT INTO people (plan_id,id,name,birth_date,gender,address_id,national_number_be,cns_lu,
      id_card_number,id_card_expiry,phone,email,auth0_email,role,
      occupation_location,occupation_company,occupation_position)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [_activePlanSlug, id, data.name, data.birth_date||null, data.gender||null, data.address_id||null,
     data.national_number_be||null, data.cns_lu||null, data.id_card_number||null, data.id_card_expiry||null,
     data.phone||null, data.email||null, data.auth0_email||null, data.role||null,
     data.occupation_location||null, data.occupation_company||null, data.occupation_position||null]
  );
  _persistDb(); return { id };
}

async function updatePerson(id, data) {
  const db = await getDb();
  db.run(
    `UPDATE people SET name=?,birth_date=?,gender=?,address_id=?,national_number_be=?,cns_lu=?,
      id_card_number=?,id_card_expiry=?,phone=?,email=?,auth0_email=?,role=?,
      occupation_location=?,occupation_company=?,occupation_position=?
     WHERE plan_id=? AND id=?`,
    [data.name, data.birth_date||null, data.gender||null, data.address_id||null,
     data.national_number_be||null, data.cns_lu||null, data.id_card_number||null, data.id_card_expiry||null,
     data.phone||null, data.email||null, data.auth0_email||null, data.role||null,
     data.occupation_location||null, data.occupation_company||null, data.occupation_position||null,
     _activePlanSlug, Number(id)]
  );
  _persistDb();
}

async function deletePerson(id) {
  const db = await getDb();
  db.run('DELETE FROM people WHERE plan_id=? AND id=?', [_activePlanSlug, Number(id)]);
  _persistDb();
}

async function createProperty(data) {
  const db = await getDb();
  const id = _nextId(db, 'properties');
  db.run('INSERT INTO properties (plan_id,id,address_id,type,status) VALUES (?,?,?,?,?)',
    [_activePlanSlug, id, data.address_id, data.type, data.status]);
  for (const oid of (data.owner_ids || []))
    db.run('INSERT OR IGNORE INTO property_owners (plan_id,property_id,person_id) VALUES (?,?,?)', [_activePlanSlug, id, oid]);
  _persistDb(); return { id };
}

async function updateProperty(id, data) {
  const db = await getDb();
  db.run('UPDATE properties SET address_id=?,type=?,status=? WHERE plan_id=? AND id=?',
    [data.address_id, data.type, data.status, _activePlanSlug, Number(id)]);
  db.run('DELETE FROM property_owners WHERE plan_id=? AND property_id=?', [_activePlanSlug, Number(id)]);
  for (const oid of (data.owner_ids || []))
    db.run('INSERT OR IGNORE INTO property_owners (plan_id,property_id,person_id) VALUES (?,?,?)', [_activePlanSlug, Number(id), oid]);
  _persistDb();
}

async function deleteProperty(id) {
  const db = await getDb();
  db.run('DELETE FROM property_owners WHERE plan_id=? AND property_id=?', [_activePlanSlug, Number(id)]);
  db.run('DELETE FROM properties WHERE plan_id=? AND id=?', [_activePlanSlug, Number(id)]);
  _persistDb();
}

async function createBankAccount(data) {
  const db = await getDb();
  db.run('INSERT INTO bank_accounts (plan_id,person_id,bank,country,iban,bic,type) VALUES (?,?,?,?,?,?,?)',
    [_activePlanSlug, data.person_id, data.bank, data.country, data.iban, data.bic||null, data.type]);
  _persistDb();
}

async function updateBankAccount(id, data) {
  const db = await getDb();
  db.run('UPDATE bank_accounts SET person_id=?,bank=?,country=?,iban=?,bic=?,type=? WHERE id=? AND plan_id=?',
    [data.person_id, data.bank, data.country, data.iban, data.bic||null, data.type, Number(id), _activePlanSlug]);
  _persistDb();
}

async function deleteBankAccount(id) {
  const db = await getDb();
  db.run('DELETE FROM bank_cards WHERE account_id=?', [Number(id)]);
  db.run('DELETE FROM bank_accounts WHERE id=? AND plan_id=?', [Number(id), _activePlanSlug]);
  _persistDb();
}

async function createContract(data) {
  const db = await getDb();
  const id = _nextId(db, 'contracts');
  db.run(
    `INSERT INTO contracts (plan_id,id,title,category,category_i18n,direction,owner_id,property_id,
      nominal,taeg,consultant_relevant,notes,employment_start,contract_type,employer_address_id)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [_activePlanSlug, id, data.title, data.category, data.category_i18n||null,
     data.direction||'expense', data.owner_id, data.property_id||null,
     data.nominal||null, data.taeg||null, data.consultant_relevant!==false?1:0,
     data.notes||null, data.employment_start||null, data.contract_type||null, data.employer_address_id||null]
  );
  for (const p of (data.periods || [])) {
    db.run('INSERT INTO contract_periods (plan_id,contract_id,monthly_cost,gross_monthly,start_date,end_date,rate,rate_type) VALUES (?,?,?,?,?,?,?,?)',
      [_activePlanSlug, id, p.monthly_cost, p.gross_monthly||null, p.start_date||null, p.end_date||null, p.rate||null, p.rate_type||null]);
  }
  _persistDb(); return { id };
}

async function updateContract(id, data) {
  const db = await getDb();
  db.run(
    `UPDATE contracts SET title=?,category=?,category_i18n=?,direction=?,owner_id=?,property_id=?,
      nominal=?,taeg=?,consultant_relevant=?,notes=?,employment_start=?,contract_type=?,employer_address_id=?
     WHERE plan_id=? AND id=?`,
    [data.title, data.category, data.category_i18n||null, data.direction||'expense', data.owner_id,
     data.property_id||null, data.nominal||null, data.taeg||null, data.consultant_relevant!==false?1:0,
     data.notes||null, data.employment_start||null, data.contract_type||null, data.employer_address_id||null,
     _activePlanSlug, Number(id)]
  );
  if (data.periods !== undefined) {
    db.run('DELETE FROM contract_periods WHERE plan_id=? AND contract_id=?', [_activePlanSlug, Number(id)]);
    for (const p of (data.periods || [])) {
      db.run('INSERT INTO contract_periods (plan_id,contract_id,monthly_cost,gross_monthly,start_date,end_date,rate,rate_type) VALUES (?,?,?,?,?,?,?,?)',
        [_activePlanSlug, Number(id), p.monthly_cost, p.gross_monthly||null, p.start_date||null, p.end_date||null, p.rate||null, p.rate_type||null]);
    }
  }
  _persistDb();
}

async function deleteContract(id) {
  const db = await getDb();
  db.run('DELETE FROM contract_periods WHERE plan_id=? AND contract_id=?', [_activePlanSlug, Number(id)]);
  db.run('DELETE FROM contracts WHERE plan_id=? AND id=?', [_activePlanSlug, Number(id)]);
  _persistDb();
}

async function createPeriodicExpense(data) {
  const db = await getDb();
  const id = _nextId(db, 'periodic_expenses');
  db.run(
    `INSERT INTO periodic_expenses (plan_id,id,title,title_i18n,category,category_i18n,owner_id,property_id,consultant_relevant)
     VALUES (?,?,?,?,?,?,?,?,?)`,
    [_activePlanSlug, id, data.title, data.title_i18n||null, data.category, data.category_i18n||null,
     data.owner_id, data.property_id||null, data.consultant_relevant!==false?1:0]
  );
  for (const p of (data.payments || [])) {
    db.run('INSERT INTO periodic_expense_payments (plan_id,expense_id,label,amount) VALUES (?,?,?,?)',
      [_activePlanSlug, id, p.label, p.amount]);
  }
  _persistDb(); return { id };
}

async function updatePeriodicExpense(id, data) {
  const db = await getDb();
  db.run(
    `UPDATE periodic_expenses SET title=?,title_i18n=?,category=?,category_i18n=?,owner_id=?,property_id=?,consultant_relevant=?
     WHERE plan_id=? AND id=?`,
    [data.title, data.title_i18n||null, data.category, data.category_i18n||null, data.owner_id,
     data.property_id||null, data.consultant_relevant!==false?1:0, _activePlanSlug, Number(id)]
  );
  if (data.payments !== undefined) {
    db.run('DELETE FROM periodic_expense_payments WHERE plan_id=? AND expense_id=?', [_activePlanSlug, Number(id)]);
    for (const p of (data.payments || [])) {
      db.run('INSERT INTO periodic_expense_payments (plan_id,expense_id,label,amount) VALUES (?,?,?,?)',
        [_activePlanSlug, Number(id), p.label, p.amount]);
    }
  }
  _persistDb();
}

async function deletePeriodicExpense(id) {
  const db = await getDb();
  db.run('DELETE FROM periodic_expense_payments WHERE plan_id=? AND expense_id=?', [_activePlanSlug, Number(id)]);
  db.run('DELETE FROM periodic_expenses WHERE plan_id=? AND id=?', [_activePlanSlug, Number(id)]);
  _persistDb();
}

async function createMortgage(data) {
  const db = await getDb();
  db.run(
    `INSERT INTO mortgages (plan_id,contract_ref,contract_id,owner_id,property_id,nominal,rate,taeg,months,
      monthly_payment,first_payment,last_payment,effective_date,offer_date,
      total_amount,total_interest,total_accessory,total_insurance,capital_amortized)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [_activePlanSlug, data.contract_ref, data.contract_id||null, data.owner_id, data.property_id,
     data.nominal||null, data.rate||null, data.taeg||null, data.months||null, data.monthly_payment||null,
     data.first_payment||null, data.last_payment||null, data.effective_date||null, data.offer_date||null,
     data.total_amount||null, data.total_interest||null, data.total_accessory||null,
     data.total_insurance||null, data.capital_amortized||null]
  );
  _persistDb();
}

async function updateMortgage(id, data) {
  const db = await getDb();
  db.run(
    `UPDATE mortgages SET contract_ref=?,owner_id=?,property_id=?,nominal=?,rate=?,taeg=?,months=?,
      monthly_payment=?,first_payment=?,last_payment=?,effective_date=?,offer_date=?,
      total_amount=?,total_interest=?,total_accessory=?,total_insurance=?,capital_amortized=?
     WHERE id=? AND plan_id=?`,
    [data.contract_ref, data.owner_id, data.property_id, data.nominal||null, data.rate||null,
     data.taeg||null, data.months||null, data.monthly_payment||null, data.first_payment||null,
     data.last_payment||null, data.effective_date||null, data.offer_date||null, data.total_amount||null,
     data.total_interest||null, data.total_accessory||null, data.total_insurance||null,
     data.capital_amortized||null, Number(id), _activePlanSlug]
  );
  _persistDb();
}

async function deleteMortgage(id) {
  const db = await getDb();
  db.run('DELETE FROM mortgages WHERE id=? AND plan_id=?', [Number(id), _activePlanSlug]);
  _persistDb();
}

async function createIncome(data) {
  const db = await getDb();
  db.run(
    `INSERT INTO incomes (plan_id,person_id,year,avg_gross_monthly_salary,avg_net_monthly_salary,
      health_insurance,transportation_allowance,meal_vouchers,performance_bonus_gross,performance_bonus_net,
      end_of_year_bonus_gross,end_of_year_bonus_net,child_allowance)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [_activePlanSlug, data.person_id, data.year, data.avg_gross_monthly_salary||null, data.avg_net_monthly_salary||null,
     data.health_insurance?1:0, data.transportation_allowance||null, data.meal_vouchers||null,
     data.performance_bonus_gross||null, data.performance_bonus_net||null,
     data.end_of_year_bonus_gross||null, data.end_of_year_bonus_net||null, data.child_allowance||null]
  );
  const row = queryOne(await getDb(), 'SELECT id FROM incomes WHERE plan_id=? AND person_id=? AND year=?', [_activePlanSlug, data.person_id, data.year]);
  if (row) {
    for (const k of (data.consultant_relevant_benefits || []))
      db.run('INSERT OR IGNORE INTO income_consultant_benefits (income_id,benefit_key) VALUES (?,?)', [row.id, k]);
  }
  _persistDb();
}

async function updateIncome(id, data) {
  const db = await getDb();
  db.run(
    `UPDATE incomes SET person_id=?,year=?,avg_gross_monthly_salary=?,avg_net_monthly_salary=?,
      health_insurance=?,transportation_allowance=?,meal_vouchers=?,performance_bonus_gross=?,performance_bonus_net=?,
      end_of_year_bonus_gross=?,end_of_year_bonus_net=?,child_allowance=?
     WHERE id=? AND plan_id=?`,
    [data.person_id, data.year, data.avg_gross_monthly_salary||null, data.avg_net_monthly_salary||null,
     data.health_insurance?1:0, data.transportation_allowance||null, data.meal_vouchers||null,
     data.performance_bonus_gross||null, data.performance_bonus_net||null,
     data.end_of_year_bonus_gross||null, data.end_of_year_bonus_net||null, data.child_allowance||null,
     Number(id), _activePlanSlug]
  );
  db.run('DELETE FROM income_consultant_benefits WHERE income_id=?', [Number(id)]);
  for (const k of (data.consultant_relevant_benefits || []))
    db.run('INSERT OR IGNORE INTO income_consultant_benefits (income_id,benefit_key) VALUES (?,?)', [Number(id), k]);
  _persistDb();
}

async function deleteIncome(id) {
  const db = await getDb();
  db.run('DELETE FROM income_consultant_benefits WHERE income_id=?', [Number(id)]);
  db.run('DELETE FROM incomes WHERE id=? AND plan_id=?', [Number(id), _activePlanSlug]);
  _persistDb();
}

async function createMilestone(data) {
  const db = await getDb();
  const id = _nextId(db, 'timeline_milestones');
  const proj = queryOne(db, 'SELECT id FROM timeline_projects WHERE plan_id=? LIMIT 1', [_activePlanSlug]);
  if (!proj) {
    db.run('INSERT INTO timeline_projects (plan_id,name,currency) VALUES (?,?,?)', [_activePlanSlug, 'Timeline', 'EUR']);
  }
  const project = queryOne(db, 'SELECT id FROM timeline_projects WHERE plan_id=? LIMIT 1', [_activePlanSlug]);
  db.run(
    `INSERT INTO timeline_milestones (plan_id,id,project_id,title,description,start_date,end_date,status,priority,budget,notes)
     VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
    [_activePlanSlug, id, project.id, data.title, data.description||null,
     data.start_date||null, data.end_date||null, data.status||'planned', data.priority||'medium',
     data.budget||null, data.notes||null]
  );
  _persistDb(); return { id };
}

async function updateMilestone(id, data) {
  const db = await getDb();
  db.run(
    `UPDATE timeline_milestones SET title=?,description=?,start_date=?,end_date=?,status=?,priority=?,budget=?,notes=?
     WHERE plan_id=? AND id=?`,
    [data.title, data.description||null, data.start_date||null, data.end_date||null,
     data.status||'planned', data.priority||'medium', data.budget||null, data.notes||null,
     _activePlanSlug, Number(id)]
  );
  _persistDb();
}

async function deleteMilestone(id) {
  const db = await getDb();
  db.run('DELETE FROM timeline_milestones WHERE plan_id=? AND id=?', [_activePlanSlug, Number(id)]);
  _persistDb();
}

module.exports = {
  initDb,
  initAllPlans,
  setActivePlan,
  listPlans,
  getAddresses,
  getPeople,
  getRealEstate,
  getBankAccounts,
  getContracts,
  getPeriodicExpenses,
  getMortgages,
  getIncomes,
  getPermissionsConfig,
  getTimeline,
  listSnapshotMonths,
  listSnapshotYears,
  getSnapshot,
  getAnnualSnapshot,
  getPersonByAuth0Email,
  listPeople,
  createPlan,
  getDb,
  query,
  queryOne,
  persistDb: _persistDb,
  createAddress, updateAddress, deleteAddress,
  createPerson, updatePerson, deletePerson,
  createProperty, updateProperty, deleteProperty,
  createBankAccount, updateBankAccount, deleteBankAccount,
  createContract, updateContract, deleteContract,
  createPeriodicExpense, updatePeriodicExpense, deletePeriodicExpense,
  createMortgage, updateMortgage, deleteMortgage,
  createIncome, updateIncome, deleteIncome,
  createMilestone, updateMilestone, deleteMilestone,
};
