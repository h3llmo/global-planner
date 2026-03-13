/**
 * db.js — SQLite data access module for the finance dashboard.
 *
 * Supports multiple budget-plans: each plan gets its own DB file (finance-<slug>.db).
 * Call initAllPlans() at startup, then setActivePlan(slug) per request.
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

function getDbPath(slug) {
  return path.join(__dirname, `finance-${slug}.db`);
}

// SQLite schema — mirrors DataManager.py SCHEMA_SQL (kept in sync manually)
const SCHEMA_SQL = `
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS addresses (id INTEGER PRIMARY KEY, street_number TEXT, street_name TEXT NOT NULL, postal_code TEXT NOT NULL, city TEXT NOT NULL, country_code TEXT NOT NULL, country TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS people (id INTEGER PRIMARY KEY, name TEXT NOT NULL, birth_date TEXT, gender TEXT, address_id INTEGER REFERENCES addresses(id), national_number_be TEXT, cns_lu TEXT, id_card_number TEXT, id_card_expiry TEXT, phone TEXT, email TEXT, auth0_email TEXT, role TEXT, occupation_location TEXT, occupation_company TEXT, occupation_position TEXT);
CREATE TABLE IF NOT EXISTS properties (id INTEGER PRIMARY KEY, address_id INTEGER NOT NULL REFERENCES addresses(id), type TEXT NOT NULL, status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS property_owners (property_id INTEGER NOT NULL REFERENCES properties(id), person_id INTEGER NOT NULL REFERENCES people(id), PRIMARY KEY (property_id, person_id));
CREATE TABLE IF NOT EXISTS bank_accounts (id INTEGER PRIMARY KEY AUTOINCREMENT, person_id INTEGER NOT NULL REFERENCES people(id), bank TEXT NOT NULL, country TEXT NOT NULL, iban TEXT NOT NULL UNIQUE, bic TEXT, type TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS bank_cards (id INTEGER PRIMARY KEY AUTOINCREMENT, account_id INTEGER NOT NULL REFERENCES bank_accounts(id), card_number TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS contracts (id INTEGER PRIMARY KEY, title TEXT NOT NULL, category TEXT NOT NULL, category_i18n TEXT, direction TEXT NOT NULL DEFAULT 'expense', owner_id INTEGER NOT NULL REFERENCES people(id), property_id INTEGER REFERENCES properties(id), nominal REAL, taeg REAL, consultant_relevant INTEGER NOT NULL DEFAULT 1, notes TEXT, employment_start TEXT, contract_type TEXT, employer_address_id INTEGER REFERENCES addresses(id));
CREATE TABLE IF NOT EXISTS contract_periods (id INTEGER PRIMARY KEY AUTOINCREMENT, contract_id INTEGER NOT NULL REFERENCES contracts(id), monthly_cost REAL NOT NULL, gross_monthly REAL, start_date TEXT, end_date TEXT, rate REAL, rate_type TEXT);
CREATE TABLE IF NOT EXISTS periodic_expenses (id INTEGER PRIMARY KEY, title TEXT NOT NULL, title_i18n TEXT, category TEXT NOT NULL, category_i18n TEXT, owner_id INTEGER NOT NULL REFERENCES people(id), property_id INTEGER REFERENCES properties(id), consultant_relevant INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS periodic_expense_payments (id INTEGER PRIMARY KEY AUTOINCREMENT, expense_id INTEGER NOT NULL REFERENCES periodic_expenses(id), label TEXT NOT NULL, amount REAL NOT NULL);
CREATE TABLE IF NOT EXISTS mortgages (id INTEGER PRIMARY KEY AUTOINCREMENT, contract_ref TEXT NOT NULL UNIQUE, contract_id INTEGER REFERENCES contracts(id), owner_id INTEGER NOT NULL REFERENCES people(id), property_id INTEGER NOT NULL REFERENCES properties(id), nominal REAL, rate REAL, taeg REAL, months INTEGER, monthly_payment REAL, first_payment TEXT, last_payment TEXT, effective_date TEXT, offer_date TEXT, total_amount REAL, total_interest REAL, total_accessory REAL, total_insurance REAL, capital_amortized REAL);
CREATE TABLE IF NOT EXISTS incomes (id INTEGER PRIMARY KEY AUTOINCREMENT, person_id INTEGER NOT NULL REFERENCES people(id), year INTEGER NOT NULL, avg_gross_monthly_salary REAL, avg_net_monthly_salary REAL, health_insurance INTEGER, transportation_allowance REAL, meal_vouchers REAL, performance_bonus_gross REAL, performance_bonus_net REAL, end_of_year_bonus_gross REAL, end_of_year_bonus_net REAL, child_allowance REAL, UNIQUE (person_id, year));
CREATE TABLE IF NOT EXISTS income_consultant_benefits (income_id INTEGER NOT NULL REFERENCES incomes(id), benefit_key TEXT NOT NULL, PRIMARY KEY (income_id, benefit_key));
CREATE TABLE IF NOT EXISTS permissions (id TEXT PRIMARY KEY, description TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS roles (id TEXT PRIMARY KEY, label TEXT NOT NULL, description TEXT, is_default INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS role_permissions (role_id TEXT NOT NULL REFERENCES roles(id), permission_id TEXT NOT NULL REFERENCES permissions(id), PRIMARY KEY (role_id, permission_id));
CREATE TABLE IF NOT EXISTS timeline_projects (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, description TEXT, start_date TEXT, estimated_end_date TEXT, currency TEXT DEFAULT 'EUR');
CREATE TABLE IF NOT EXISTS timeline_milestones (id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL REFERENCES timeline_projects(id), title TEXT NOT NULL, description TEXT, start_date TEXT, end_date TEXT, status TEXT, priority TEXT, budget REAL, notes TEXT);
CREATE TABLE IF NOT EXISTS snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL CHECK(type IN ('monthly','annual')), year INTEGER NOT NULL, month INTEGER, label TEXT NOT NULL, generated_at TEXT NOT NULL, content TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_monthly ON snapshots(year, month) WHERE type='monthly';
CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_annual  ON snapshots(year)        WHERE type='annual';
CREATE TABLE IF NOT EXISTS budget_plans (id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, currency TEXT DEFAULT 'EUR', owner_auth0_id TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS budget_plan_access (plan_id TEXT NOT NULL REFERENCES budget_plans(id), auth0_user_id TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'viewer', PRIMARY KEY (plan_id, auth0_user_id));
CREATE TABLE IF NOT EXISTS virtual_contracts (id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id TEXT NOT NULL, title TEXT NOT NULL, category TEXT NOT NULL, direction TEXT NOT NULL DEFAULT 'expense', owner_id INTEGER REFERENCES people(id), monthly_cost REAL, start_date TEXT, end_date TEXT, notes TEXT);
CREATE TABLE IF NOT EXISTS virtual_incomes (id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id TEXT NOT NULL, person_id INTEGER REFERENCES people(id), year INTEGER NOT NULL, avg_net_monthly_salary REAL, notes TEXT);
CREATE TABLE IF NOT EXISTS virtual_periodic_expenses (id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id TEXT NOT NULL, title TEXT NOT NULL, category TEXT NOT NULL, owner_id INTEGER REFERENCES people(id), payments TEXT, notes TEXT);
`;

/**
 * Initialize the database based on environment:
 *   development — reimport from JSON files via DataManager.py
 *   production  — create empty schema if DB doesn't exist
 *
 * @param {string} slug  Budget-plan slug (e.g. 'foyer-pascual-anaelle')
 */
async function initDb(slug) {
  const isProd  = process.env.NODE_ENV === 'production';
  const dbPath  = getDbPath(slug);

  if (isProd) {
    if (!fs.existsSync(dbPath)) {
      console.log(`[db] Production mode — creating empty schema at ${dbPath}`);
      const SQL = await _getSql();
      const emptyDb = new SQL.Database();
      emptyDb.run(SCHEMA_SQL);
      const data = emptyDb.export();
      try {
        fs.writeFileSync(dbPath, Buffer.from(data));
        emptyDb.close();
        console.log('[db] Empty database created.');
      } catch (writeErr) {
        console.warn('[db] Filesystem is read-only (%s) — DB will be memory-only.', writeErr.code);
        _planState[slug] = { memoryOnlyDb: emptyDb, db: null, loadedMtime: 0 };
      }
    } else {
      console.log(`[db] Production mode — using existing database for ${slug}.`);
    }
    return;
  }

  // Development: reimport from JSON
  console.log(`[db] Importing data for plan: ${slug}…`);
  try {
    execSync(`python "${SNAPSHOT_SCRIPT}" --plan "${slug}"`, {
      stdio: 'inherit', cwd: PROJECT_ROOT,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' },
    });
    execSync(`python "${DATA_MANAGER}" --db "${dbPath}" --plan "${slug}" create`, {
      stdio: 'inherit', cwd: PROJECT_ROOT,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' },
    });
    console.log(`[db] Import complete for ${slug}.`);
  } catch (e) {
    console.error(`[db] Import failed for ${slug}:`, e.message);
    if (!fs.existsSync(dbPath)) {
      const SQL = await _getSql();
      const emptyDb = new SQL.Database();
      emptyDb.run(SCHEMA_SQL);
      const data = emptyDb.export();
      fs.writeFileSync(dbPath, Buffer.from(data));
      emptyDb.close();
    }
  }
  // Force reload
  if (_planState[slug]) {
    _planState[slug].db = null;
    _planState[slug].loadedMtime = 0;
  }
}

/** Initialize DBs for all plans found in data/budget-plans/ */
async function initAllPlans() {
  const slugs = _getLocalPlanSlugs();
  if (slugs.length === 0) {
    console.warn('[db] No budget-plans found — starting with empty schema.');
    return;
  }
  for (const slug of slugs) {
    await initDb(slug);
  }
}

/** List plan slugs from the filesystem (local budget-plans directory) */
function _getLocalPlanSlugs() {
  if (!fs.existsSync(BUDGET_PLANS_DIR)) return [];
  return fs.readdirSync(BUDGET_PLANS_DIR)
    .filter(d => fs.statSync(path.join(BUDGET_PLANS_DIR, d)).isDirectory());
}

/** Read plan.json for all local plans and return metadata array */
function listPlans() {
  return _getLocalPlanSlugs().map(slug => {
    const planFile = path.join(BUDGET_PLANS_DIR, slug, 'plan.json');
    if (!fs.existsSync(planFile)) return { id: slug, name: slug, members: [] };
    try {
      return JSON.parse(fs.readFileSync(planFile, 'utf8'));
    } catch {
      return { id: slug, name: slug, members: [] };
    }
  });
}

// ── Per-plan DB state ──────────────────────────────────────────────────────────
// Each slug maps to { db, loadedMtime, loadPromise, memoryOnlyDb }
const _planState = {};
let _activePlanSlug = null;

/** Set the active plan slug for subsequent DB queries in this request cycle */
function setActivePlan(slug) {
  _activePlanSlug = slug;
}

/** @type {import('sql.js').SqlJsStatic|null} */
let _sqlPromise = null;
async function _getSql() {
  if (!_sqlPromise) _sqlPromise = initSqlJs({
    locateFile: file => require('path').join(__dirname, 'node_modules/sql.js/dist', file),
  });
  return _sqlPromise;
}

/** Load (or return cached) the DB for the given slug */
async function _getDbForSlug(slug) {
  if (!_planState[slug]) _planState[slug] = { db: null, loadedMtime: 0, loadPromise: null, memoryOnlyDb: null };
  const state = _planState[slug];

  if (state.memoryOnlyDb) return state.memoryOnlyDb;

  const dbPath = getDbPath(slug);
  const mtime  = fs.existsSync(dbPath) ? fs.statSync(dbPath).mtimeMs : 0;
  if (state.db && mtime <= state.loadedMtime) return state.db;
  if (state.loadPromise) return state.loadPromise;

  state.loadPromise = (async () => {
    const SQL    = await _getSql();
    const buffer = fs.readFileSync(dbPath);
    if (state.db) { try { state.db.close(); } catch (_) {} }
    state.db          = new SQL.Database(buffer);
    state.loadedMtime = mtime;
    state.loadPromise = null;
    return state.db;
  })();
  return state.loadPromise;
}

/** Return the DB for the currently active plan */
async function getDb() {
  const slug = _activePlanSlug;
  if (!slug) throw new Error('No active plan set — call setActivePlan(slug) first.');
  return _getDbForSlug(slug);
}

/** Run a SELECT and return rows as array of plain objects */
function query(db, sql, params = []) {
  const stmt    = db.prepare(sql);
  const results = [];
  stmt.bind(params);
  while (stmt.step()) {
    results.push(stmt.getAsObject());
  }
  stmt.free();
  return results;
}

/** Run a SELECT and return first row or null */
function queryOne(db, sql, params = []) {
  const rows = query(db, sql, params);
  return rows.length > 0 ? rows[0] : null;
}

// ── Public API ────────────────────────────────────────────────────────────────

async function getAddresses() {
  const db = await getDb();
  const rows = query(db, 'SELECT id, street_number, street_name, postal_code, city, country_code, country FROM addresses ORDER BY id');
  return { addresses: rows };
}

async function getPeople() {
  const db   = await getDb();
  const rows = query(db, 'SELECT * FROM people ORDER BY id');
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
    if (r.occupation_company) {
      out.occupation = { location: r.occupation_location, company: r.occupation_company, position: r.occupation_position };
    }
    return out;
  });
  return { people };
}

async function getRealEstate() {
  const db    = await getDb();
  const props = query(db, 'SELECT id, address_id, type, status FROM properties ORDER BY id');
  const properties = props.map(p => {
    const owners = query(db, 'SELECT person_id FROM property_owners WHERE property_id=? ORDER BY person_id', [p.id])
      .map(r => r.person_id);
    return { id: p.id, address_id: p.address_id, owner_ids: owners, type: p.type, status: p.status };
  });
  return { properties };
}

async function getBankAccounts() {
  const db       = await getDb();
  const accounts = query(db, 'SELECT id, person_id, bank, country, iban, bic, type FROM bank_accounts ORDER BY id');
  const bank_accounts = accounts.map(a => {
    const cards = query(db, 'SELECT card_number FROM bank_cards WHERE account_id=?', [a.id]).map(r => r.card_number);
    const entry = { person_id: a.person_id, bank: a.bank, country: a.country, iban: a.iban, type: a.type };
    if (a.bic)          entry.bic   = a.bic;
    if (cards.length)   entry.cards = cards;
    return entry;
  });
  return { bank_accounts };
}

async function getContracts() {
  const db        = await getDb();
  const contracts = query(db, 'SELECT id, title, category, category_i18n, direction, owner_id, property_id, nominal, taeg, consultant_relevant, notes, employment_start, contract_type, employer_address_id FROM contracts ORDER BY id');
  const result = contracts.map(c => {
    const periods = query(db,
      'SELECT monthly_cost, gross_monthly, start_date, end_date, rate, rate_type FROM contract_periods WHERE contract_id=? ORDER BY id',
      [c.id]
    ).map(p => {
      const period = { monthly_cost: p.monthly_cost, start_date: p.start_date, end_date: p.end_date };
      if (p.gross_monthly != null) period.gross_monthly = p.gross_monthly;
      if (p.rate     != null) period.rate      = p.rate;
      if (p.rate_type)        period.rate_type = p.rate_type;
      return period;
    });
    const entry = { id: c.id, title: c.title, category: c.category };
    if (c.category_i18n)    entry.category_i18n    = c.category_i18n;
    if (c.direction && c.direction !== 'expense') entry.direction = c.direction;
    entry.owner_id = c.owner_id;
    if (c.property_id != null) entry.property_id  = c.property_id;
    if (c.nominal     != null) entry.nominal       = c.nominal;
    if (c.taeg        != null) entry.taeg          = c.taeg;
    entry.consultant_relevant = !!c.consultant_relevant;
    if (c.notes)               entry.notes         = c.notes;
    if (c.employment_start != null) entry.employment_start    = c.employment_start;
    if (c.contract_type    != null) entry.contract_type       = c.contract_type;
    if (c.employer_address_id != null) entry.employer_address_id = c.employer_address_id;
    entry.periods = periods;
    return entry;
  });
  return { contracts: result };
}

async function getPeriodicExpenses() {
  const db       = await getDb();
  const expenses = query(db, 'SELECT id, title, title_i18n, category, category_i18n, owner_id, property_id, consultant_relevant FROM periodic_expenses ORDER BY id');
  const periodic_expenses = expenses.map(e => {
    const payments = query(db, 'SELECT label, amount FROM periodic_expense_payments WHERE expense_id=? ORDER BY id', [e.id])
      .map(p => ({ label: p.label, amount: p.amount }));
    const entry = { id: e.id, title: e.title };
    if (e.title_i18n)       entry.title_i18n    = e.title_i18n;
    entry.category = e.category;
    if (e.category_i18n)    entry.category_i18n = e.category_i18n;
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
  const rows = query(db, `SELECT contract_ref, owner_id, property_id, nominal, rate, taeg, months,
    monthly_payment, first_payment, last_payment, effective_date, offer_date,
    total_amount, total_interest, total_accessory, total_insurance, capital_amortized
    FROM mortgages ORDER BY id`);
  return rows.map(r => {
    const entry = { contract: r.contract_ref, owner_id: r.owner_id, property_id: r.property_id };
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
  const rows = query(db, `SELECT id, person_id, year, avg_gross_monthly_salary, avg_net_monthly_salary,
    health_insurance, transportation_allowance, meal_vouchers,
    performance_bonus_gross, performance_bonus_net, end_of_year_bonus_gross, end_of_year_bonus_net, child_allowance
    FROM incomes ORDER BY person_id, year`);
  const incomes = rows.map(r => {
    const benefits = query(db, 'SELECT benefit_key FROM income_consultant_benefits WHERE income_id=?', [r.id])
      .map(b => b.benefit_key);
    const entry = { person_id: r.person_id, year: String(r.year),
                    avg_gross_monthly_salary: r.avg_gross_monthly_salary,
                    avg_net_monthly_salary:   r.avg_net_monthly_salary };
    if (benefits.length) entry.consultant_relevant_benefits = benefits;
    const other = {};
    if (r.health_insurance)           other.health_insurance         = !!r.health_insurance;
    if (r.transportation_allowance != null) other.transportation_allowance = r.transportation_allowance;
    if (r.meal_vouchers        != null) other.meal_vouchers          = r.meal_vouchers;
    if (r.performance_bonus_gross != null) other.performance_bonus   = { amount: r.performance_bonus_gross, net_amount: r.performance_bonus_net };
    if (r.end_of_year_bonus_gross != null) other.end_of_year_bonus   = { amount: r.end_of_year_bonus_gross, net_amount: r.end_of_year_bonus_net };
    if (r.child_allowance      != null) other.child_allowance        = r.child_allowance;
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
    const perms = query(db, 'SELECT permission_id FROM role_permissions WHERE role_id=? ORDER BY permission_id', [r.id])
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
  const proj = queryOne(db, 'SELECT id, name, description, start_date, estimated_end_date, currency FROM timeline_projects LIMIT 1');
  if (!proj) return { project: { name: '', milestones: [] } };
  const milestones = query(db,
    'SELECT id, title, description, start_date, end_date, status, priority, budget, notes FROM timeline_milestones WHERE project_id=? ORDER BY id',
    [proj.id]
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
  const rows = query(db, "SELECT year, month FROM snapshots WHERE type='monthly' ORDER BY year, month");
  return rows.map(r => `${r.year}-${String(r.month).padStart(2, '0')}`);
}

async function listSnapshotYears() {
  const db   = await getDb();
  const rows = query(db, "SELECT year FROM snapshots WHERE type='annual' ORDER BY year");
  return rows.map(r => r.year);
}

async function getSnapshot(year, month) {
  const db  = await getDb();
  const row = queryOne(db,
    "SELECT content FROM snapshots WHERE type='monthly' AND year=? AND month=?",
    [parseInt(year), parseInt(month)]
  );
  return row ? JSON.parse(row.content) : null;
}

async function getAnnualSnapshot(year) {
  const db  = await getDb();
  const row = queryOne(db,
    "SELECT content FROM snapshots WHERE type='annual' AND year=?",
    [parseInt(year)]
  );
  return row ? JSON.parse(row.content) : null;
}

/** Find a person in the active plan's DB whose auth0_email matches the given email */
async function getPersonByAuth0Email(email) {
  if (!email) return null;
  const db = await getDb();
  return queryOne(db, 'SELECT id, name FROM people WHERE auth0_email = ?', [email]);
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
};
