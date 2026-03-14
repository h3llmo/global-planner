// Load .env (no-op if vars already set — Vercel injects them at runtime)
require('dotenv').config();

const express      = require('express');
const path         = require('path');
const fs           = require('fs');
const cookieParser = require('cookie-parser');
const { auth, requiresAuth } = require('express-openid-connect');
const db           = require('./db');
const snapshot     = require('./snapshot');

const app  = express();
const PORT = process.env.PORT || 3000;

// Auth0 OpenID Connect — active in all environments.
const secret        = process.env.AUTH0_SESSION_SECRET;
const clientID      = process.env.AUTH0_CLIENT_ID;
const clientSecret  = process.env.AUTH0_CLIENT_SECRET;
const issuerBaseURL = process.env.AUTH0_ISSUER_BASE_URL;

if (!secret || !clientID || !clientSecret || !issuerBaseURL) {
  console.error('[auth] Missing Auth0 env vars — check your .env file:', {
    AUTH0_SESSION_SECRET:  !!secret,
    AUTH0_CLIENT_ID:       !!clientID,
    AUTH0_CLIENT_SECRET:   !!clientSecret,
    AUTH0_ISSUER_BASE_URL: !!issuerBaseURL,
  });
  process.exit(1);
}

const BASE_URL = process.env.BASE_URL ||
  (process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : `http://localhost:${PORT}`);

app.use(auth({
  authRequired:  false,
  auth0Logout:   true,
  secret, clientID, clientSecret, issuerBaseURL,
  baseURL:       BASE_URL,
  authorizationParams: {
    response_type: 'code',
    scope: 'openid profile email',
  },
}));
console.log('[auth] Auth0 enabled, baseURL:', BASE_URL);

app.use(express.static(path.join(__dirname, 'public')));
app.use(cookieParser());
app.use(express.json());

// Start DB initialization for all plans at startup.
const _dbReady = db.initAllPlans();

// Middleware: wait for DB to be ready before any request
app.use((req, res, next) => {
  _dbReady.then(() => next()).catch(err => {
    console.error('[server] DB not ready:', err.message);
    res.status(503).send('Service starting — please retry in a moment.');
  });
});

function getActiveRole(req, permissionsConfig) {
  if (!req.oidc || !req.oidc.isAuthenticated()) return 'public';

  // Manual role override via cookie (allows testing as other roles)
  const cookie = req.cookies && req.cookies.role;
  const validIds = permissionsConfig.roles.map(r => r.id);
  if (cookie && validIds.includes(cookie)) return cookie;

  // Authenticated user linked to a person in this plan → owner
  if (req.linkedPerson) return 'owner';

  // Trusted partner with explicit access grant
  if (req.linkedAccess) return req.linkedAccess.role;

  return permissionsConfig.default_role;
}

function getRolePermissions(roleId, permissionsConfig) {
  const role = permissionsConfig.roles.find(r => r.id === roleId);
  return new Set(role ? role.permissions : []);
}

function filterPeople(people, perms) {
  return people.map(person => {
    const isChild = person.role === 'child';
    const filtered = { ...person };

    if (isChild && !perms.has('display_children_personal_info')) {
      delete filtered.birth_date;
      delete filtered.gender;
      delete filtered.id_card;
    }

    if (!isChild && !perms.has('display_personal_info')) {
      delete filtered.national_number_be;
      delete filtered.cns_lu;
      delete filtered.id_card;
      delete filtered.contact;
    }

    return filtered;
  });
}

function filterIncomes(incomes, perms) {
  if (perms.has('display_amounts')) return incomes;
  return incomes.map(r => {
    const b = r.other_benefits || {};
    const filteredBenefits = {};
    if ('health_insurance' in b) filteredBenefits.health_insurance = b.health_insurance;
    return { person_id: r.person_id, year: r.year, other_benefits: filteredBenefits };
  });
}

function filterContracts(contracts, perms) {
  if (perms.has('display_amounts')) return contracts;
  return contracts.map(c => ({
    ...c,
    periods: (c.periods || []).map(p => ({ start_date: p.start_date, end_date: p.end_date })),
  }));
}

function filterPeriodicExpenses(periodicExpenses, perms) {
  if (perms.has('display_amounts')) return periodicExpenses;
  return periodicExpenses.map(({ payments, ...rest }) => rest);
}

function filterSnapshotForConsultant(snapshot) {
  // Monthly snapshots no longer contain bonuses, so income is already correct.
  // We only need to strip non-relevant periodic expenses and recompute totals.
  const newPeople = snapshot.people.map(p => {
    const filteredPeriodic  = (p.periodic_expenses || []).filter(e => e.consultant_relevant !== false);
    const filteredContracts = (p.contracts || []).filter(c => c.consultant_relevant !== false);

    const totalExp = Math.round((
      filteredContracts.reduce((s, c) => s + (c.monthly_cost || 0), 0) +
      filteredPeriodic.reduce((s, e)  => s + (e.monthly_avg  || 0), 0)
    ) * 100) / 100;

    return { ...p, contracts: filteredContracts, periodic_expenses: filteredPeriodic, total_monthly_expenses: totalExp };
  });

  const totalNetIncome = Math.round(newPeople.reduce((s, p) => s + (p.income.total_monthly_net || 0), 0) * 100) / 100;
  const totalContracts = Math.round(newPeople.reduce((s, p) => s + p.contracts.reduce((t, c) => t + (c.monthly_cost || 0), 0), 0) * 100) / 100;
  const totalPeriodic  = Math.round(newPeople.reduce((s, p) => s + p.periodic_expenses.reduce((t, e) => t + (e.monthly_avg || 0), 0), 0) * 100) / 100;
  const totalExpenses  = Math.round((totalContracts + totalPeriodic) * 100) / 100;

  return {
    ...snapshot,
    people: newPeople,
    summary: snapshot.summary ? {
      ...snapshot.summary,
      total_net_income:   totalNetIncome,
      total_contracts:    totalContracts,
      total_periodic_avg: totalPeriodic,
      total_expenses:     totalExpenses,
      net_balance:        Math.round((totalNetIncome - totalExpenses) * 100) / 100,
    } : null,
  };
}

function filterAnnualSnapshotForConsultant(snapshot) {
  const newPeople = snapshot.people.map(p => {
    const inc      = { ...p.income };
    const relevant = (inc.consultant_relevant_benefits || []).map(k => k + '_annual');

    // Strip bonuses — not stable recurring income for loan purposes
    delete inc.performance_bonus;
    delete inc.end_of_year_bonus;
    inc.total_annual_net = Math.round(
      ((inc.net_salary_annual || 0) + relevant.reduce((s, k) => s + (inc[k] || 0), 0)) * 100
    ) / 100;

    const filteredContracts = (p.contracts || []).filter(c => c.consultant_relevant !== false);
    const filteredPeriodic  = (p.periodic_expenses || []).filter(e => e.consultant_relevant !== false);

    const totalExp = Math.round((
      filteredContracts.reduce((s, c) => s + (c.annual_cost  || 0), 0) +
      filteredPeriodic.reduce((s, e)  => s + (e.annual_total || 0), 0)
    ) * 100) / 100;

    return { ...p, income: inc, contracts: filteredContracts, periodic_expenses: filteredPeriodic, total_annual_expenses: totalExp };
  });

  const totalIncome   = Math.round(newPeople.reduce((s, p) => s + (p.income.total_annual_net || 0), 0) * 100) / 100;
  const totalExpenses = Math.round(newPeople.reduce((s, p) => s + (p.total_annual_expenses  || 0), 0) * 100) / 100;

  return {
    ...snapshot,
    people: newPeople,
    summary: snapshot.summary ? {
      total_annual_net_income: totalIncome,
      total_annual_expenses:   totalExpenses,
      net_annual_balance:      Math.round((totalIncome - totalExpenses) * 100) / 100,
    } : null,
  };
}

function filterAnnualSnapshot(snapshot, perms, role) {
  if (!perms.has('display_amounts')) {
    return {
      ...snapshot,
      people: snapshot.people.map(p => ({
        person_id: p.person_id,
        income: { total_annual_net: null },
        contracts: (p.contracts || []).map(({ id, title, title_i18n, category, category_i18n, property_id }) => {
          const e = { id, title, category };
          if (title_i18n)          e.title_i18n    = title_i18n;
          if (category_i18n)       e.category_i18n = category_i18n;
          if (property_id != null) e.property_id   = property_id;
          return e;
        }),
        periodic_expenses: (p.periodic_expenses || []).map(({ id, title, title_i18n, category, category_i18n, property_id }) => {
          const e = { id, title, category };
          if (title_i18n)          e.title_i18n    = title_i18n;
          if (category_i18n)       e.category_i18n = category_i18n;
          if (property_id != null) e.property_id   = property_id;
          return e;
        }),
        total_annual_expenses: null,
      })),
      summary: null,
    };
  }
  if (role === 'consultant') return filterAnnualSnapshotForConsultant(snapshot);
  return snapshot;
}

function filterSnapshot(snapshot, perms, role) {
  if (!perms.has('display_amounts')) {
    return {
      ...snapshot,
      people: snapshot.people.map(p => ({
        person_id: p.person_id,
        income: { total_monthly_net: null },
        contracts: p.contracts.map(({ id, title, title_i18n, category, category_i18n, property_id }) => {
          const e = { id, title, category };
          if (title_i18n)      e.title_i18n    = title_i18n;
          if (category_i18n)   e.category_i18n = category_i18n;
          if (property_id != null) e.property_id = property_id;
          return e;
        }),
        periodic_expenses: p.periodic_expenses.map(({ id, title, title_i18n, category, category_i18n, property_id }) => {
          const e = { id, title, category };
          if (title_i18n)      e.title_i18n    = title_i18n;
          if (category_i18n)   e.category_i18n = category_i18n;
          if (property_id != null) e.property_id = property_id;
          return e;
        }),
        total_monthly_expenses: null,
      })),
      summary: null,
    };
  }
  if (role === 'consultant') return filterSnapshotForConsultant(snapshot);
  return snapshot; // owner / admin: full detail
}

function filterMortgages(mortgages, perms) {
  if (perms.has('display_amounts')) return mortgages;
  return mortgages.map(m => ({
    contract: m.contract,
    property_id: m.property_id,
    months: m.months,
    first_payment: m.first_payment,
    last_payment: m.last_payment,
  }));
}

function filterBankAccounts(bankAccounts, perms) {
  if (perms.has('display_bank_accounts')) return bankAccounts;
  return [];
}

// ── Mortgage rate proxy (ECB SDW) ────────────────────────────────────────────
// Series: LU house purchase loans, households, fixed rate > 5 years, new business
const ECB_URL = 'https://data-api.ecb.europa.eu/service/data/MIR/M.LU.B.A2B.K.R.A.2250.EUR.N' +
                '?lastNObservations=24';
let _rateCache = null;
let _rateCacheAt = 0;
const RATE_CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours

/** Parse ECB CSV response → { dates, rates, latest }
 *  Detects TIME_PERIOD and OBS_VALUE column positions from the header row.
 */
function parseEcbCsv(text) {
  const allLines = text.trim().split('\n');
  const header   = allLines[0].split(',').map(h => h.trim());
  const tIdx     = header.indexOf('TIME_PERIOD');
  const vIdx     = header.indexOf('OBS_VALUE');
  if (tIdx === -1 || vIdx === -1) throw new Error('Unexpected CSV format');

  const dates = [], rates = [];
  for (const line of allLines.slice(1)) {
    const parts  = line.split(',');
    const period = parts[tIdx]?.trim();
    const value  = parseFloat(parts[vIdx]);
    if (period && !isNaN(value)) {
      dates.push(period);
      rates.push(Math.round(value * 100) / 100);
    }
  }
  return { dates, rates, latest: rates[rates.length - 1] ?? null };
}

app.get('/api/mortgage-rates', async (req, res) => {
  if (_rateCache && Date.now() - _rateCacheAt < RATE_CACHE_TTL) {
    return res.json(_rateCache);
  }
  try {
    const resp = await fetch(ECB_URL, { headers: { Accept: 'text/csv' } });
    if (!resp.ok) throw new Error(`ECB API ${resp.status}`);
    const { dates, rates, latest } = parseEcbCsv(await resp.text());
    _rateCache = { dates, rates, latest, source: 'ECB MIR' };
    _rateCacheAt = Date.now();
    res.json(_rateCache);
  } catch (err) {
    console.error('ECB rate fetch failed:', err.message);
    res.status(503).json({ error: err.message });
  }
});

// ── Economic indicators proxy (ECB HICP sub-indices) ─────────────────────────
const INDICATORS = [
  { key: 'electricity', series: 'M.BE.N.045100.4.ANR' },
  { key: 'heatingOil',  series: 'M.BE.N.045300.4.ANR' },
  { key: 'sp95',        series: 'M.LU.N.045220.4.ANR'  },
  { key: 'inflation',   series: 'M.BE.N.000000.4.ANR'  },
];

// Yahoo Finance tickers for live commodity market prices (no API key required)
const COMMODITY_TICKERS = {
  heatingOil: 'BZ=F',   // Brent crude futures — best proxy for heating oil
  sp95:       'RB=F',   // RBOB Gasoline futures — proxy for SP95 pump price
};

let _indicatorsCache = null;
let _indicatorsCacheAt = 0;
const COMMODITY_CACHE_TTL = 60 * 60 * 1000; // 1 hour for market data
let _hicpCache = null;
let _hicpCacheAt = 0;
let _commodityCache = {};
let _commodityCacheAt = {};

/** Fetch monthly OHLC from Yahoo Finance for a ticker, last N months */
async function fetchYahooMonthly(ticker, months = 24) {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?interval=1mo&range=${Math.ceil(months/12) + 1}y`;
  const resp = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
  if (!resp.ok) throw new Error(`Yahoo ${ticker}: ${resp.status}`);
  const j = await resp.json();
  const result = j?.chart?.result?.[0];
  if (!result) throw new Error(`Yahoo ${ticker}: no result`);

  const timestamps = result.timestamp || [];
  const closes     = result.indicators?.quote?.[0]?.close || [];
  const currency   = result.meta?.currency || 'USD';

  const dates = [], rates = [];
  timestamps.forEach((ts, i) => {
    const v = closes[i];
    if (v != null) {
      dates.push(new Date(ts * 1000).toISOString().slice(0, 7));
      rates.push(Math.round(v * 100) / 100);
    }
  });
  // Keep last `months` entries
  return { dates: dates.slice(-months), rates: rates.slice(-months), latest: rates[rates.length - 1] ?? null, currency };
}

/** Get EUR/USD rate from ECB (daily, very recent) */
async function fetchEurUsd() {
  const url = 'https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?lastNObservations=1';
  const r = await fetch(url, { headers: { Accept: 'text/csv' } });
  if (!r.ok) return null;
  const parsed = parseEcbCsv(await r.text());
  return parsed.latest; // USD per 1 EUR (e.g. 1.18)
}

app.get('/api/indicators', async (req, res) => {
  if (_indicatorsCache && Date.now() - _indicatorsCacheAt < COMMODITY_CACHE_TTL) {
    return res.json(_indicatorsCache);
  }
  try {
    // Fetch HICP trends — cached 24h (monthly data doesn't change more often)
    if (!_hicpCache || Date.now() - _hicpCacheAt > RATE_CACHE_TTL) {
      const hicpResults = await Promise.all(
        INDICATORS.map(async ({ key, series }) => {
          const url = `https://data-api.ecb.europa.eu/service/data/ICP/${series}?lastNObservations=24`;
          const r = await fetch(url, { headers: { Accept: 'text/csv' } });
          if (!r.ok) throw new Error(`ECB ICP ${series} → ${r.status}`);
          const parsed = parseEcbCsv(await r.text());
          return [key, { ...parsed, unit: '%yoy', source: 'ECB HICP', dataType: 'hicp' }];
        })
      );
      _hicpCache   = Object.fromEntries(hicpResults);
      _hicpCacheAt = Date.now();
    }
    const result = JSON.parse(JSON.stringify(_hicpCache)); // deep clone

    // Attempt to enrich volatile commodities with live Yahoo Finance market data
    try {
      const eurUsd = await fetchEurUsd(); // for USD→EUR conversion
      for (const [key, ticker] of Object.entries(COMMODITY_TICKERS)) {
        if (_commodityCache[key] && Date.now() - (_commodityCacheAt[key] || 0) < COMMODITY_CACHE_TTL) {
          result[key].market = _commodityCache[key];
          continue;
        }
        try {
          const mkt = await fetchYahooMonthly(ticker, 24);
          // Convert to EUR if needed
          if (mkt.currency === 'USD' && eurUsd) {
            mkt.rates  = mkt.rates.map(v => Math.round(v / eurUsd * 100) / 100);
            mkt.latest = mkt.rates[mkt.rates.length - 1] ?? null;
            mkt.currency = 'EUR';
          }
          // Add unit label
          mkt.unit   = key === 'heatingOil' ? 'EUR/barrel' : 'EUR/gallon';
          mkt.source = 'Yahoo Finance';
          _commodityCache[key]   = mkt;
          _commodityCacheAt[key] = Date.now();
          result[key].market = mkt;
        } catch (e) {
          console.warn(`Market data unavailable for ${key}:`, e.message);
        }
      }
    } catch (e) {
      console.warn('Commodity market enrichment failed:', e.message);
    }

    _indicatorsCache    = result;
    _indicatorsCacheAt  = Date.now();
    res.json(result);
  } catch (err) {
    console.error('ECB indicators fetch failed:', err.message);
    res.status(503).json({ error: err.message });
  }
});


// ── Plan selector API ─────────────────────────────────────────────────────────

app.get('/api/plans', (req, res) => {
  res.json(db.listPlans());
});

app.get('/api/people', (req, res) => {
  res.json(db.listPeople());
});

app.post('/api/plans', async (req, res) => {
  try {
    const { id, name, description, currency, members, access } = req.body;
    if (!id || !name) return res.status(400).json({ error: 'id and name are required.' });
    if (!/^[a-z0-9][a-z0-9-]*[a-z0-9]?$/.test(id))
      return res.status(400).json({ error: 'id must use lowercase letters, numbers, and hyphens.' });

    const result = await db.createPlan({ id, name, description, currency, members, access });
    res.status(201).json(result);
  } catch (err) {
    console.error('[api/plans POST]', err);
    const status = err.message.includes('already exists') ? 409 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ── Plan selector page (home) ─────────────────────────────────────────────────

app.get('/', (req, res) => {
  const plans = db.listPlans();
  if (plans.length === 1) return res.redirect(`/plan/${plans[0].id}`);
  const oidcUser = (req.oidc && req.oidc.isAuthenticated())
    ? { name: req.oidc.user.name, picture: req.oidc.user.picture }
    : null;
  const template = fs.readFileSync(path.join(__dirname, 'views', 'plans.html'), 'utf8');
  const html = template
    .replace('__PLANS_JSON__',         JSON.stringify(plans))
    .replace('__PEOPLE_JSON__',        JSON.stringify(db.listPeople()))
    .replace('__USER_JSON__',          JSON.stringify(oidcUser))
    .replace('__IS_PRODUCTION_JSON__', JSON.stringify(true));
  res.send(html);
});

// ── Plan-scoped router ────────────────────────────────────────────────────────

const planRouter = express.Router({ mergeParams: true });

// Middleware: validate plan slug, set active DB, and link auth0 user → person or partner
planRouter.use(async (req, res, next) => {
  try {
    const { slug } = req.params;
    const plans = db.listPlans();
    const plan  = plans.find(p => p.id === slug);
    if (!plan) return res.status(404).send(`Plan "${slug}" not found.`);
    db.setActivePlan(slug);
    req.activePlan = plan;

    if (req.oidc && req.oidc.isAuthenticated() && req.oidc.user.email) {
      const email    = req.oidc.user.email;
      const authUser = req.oidc.user;

      // Check if this user is a plan member (person profile with matching auth0_email)
      req.linkedPerson = await db.getPersonByAuth0Email(email) || null;

      // First-time login: auto-create a person entry from Auth0 profile
      if (!req.linkedPerson) {
        try {
          const displayName = authUser.name || authUser.email;
          const result = await db.createPerson({
            name:        displayName,
            email:       authUser.email,
            auth0_email: authUser.email,
          });
          console.log(`[auth] Auto-created person "${displayName}" (id=${result.id}) for ${email} in plan "${slug}"`);
          req.linkedPerson = { id: result.id, name: displayName };
        } catch (e) {
          // May fail if person already exists (race) or plan has no people table yet — not fatal
          console.warn(`[auth] Could not auto-create person for ${email}:`, e.message);
          const entry = (plan.access || []).find(a => a.auth0_email === email);
          if (entry) req.linkedAccess = entry;
        }
      }
    }

    next();
  } catch (err) {
    next(err);
  }
});

app.use('/plan/:slug', planRouter);

planRouter.post('/role', requiresAuth(), async (req, res) => {
  const { role } = req.body;
  try {
    const permissionsConfig = await db.getPermissionsConfig();
    const validIds = permissionsConfig.roles.map(r => r.id);
    if (!validIds.includes(role)) return res.status(400).json({ error: 'Invalid role' });
    res.cookie('role', role, { httpOnly: true, sameSite: 'lax' });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

planRouter.get('/snapshots', async (req, res) => {
  try {
    const months = await db.listSnapshotMonths();
    res.json(months);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

planRouter.get('/snapshots/annual', async (req, res) => {
  try {
    const years = await db.listSnapshotYears();
    res.json(years);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

planRouter.get('/snapshots/annual/:year', async (req, res) => {
  const { year } = req.params;
  if (!/^\d{4}$/.test(year)) return res.status(400).json({ error: 'Invalid year format. Expected YYYY.' });
  try {
    const permissionsConfig = await db.getPermissionsConfig();
    const activeRole = getActiveRole(req, permissionsConfig);
    const perms      = getRolePermissions(activeRole, permissionsConfig);
    const snapshot   = await db.getAnnualSnapshot(year);
    if (!snapshot) return res.status(404).json({ error: 'Annual snapshot not found.' });
    res.json(filterAnnualSnapshot(snapshot, perms, activeRole));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

planRouter.get('/snapshots/:month', async (req, res) => {
  const { month } = req.params;
  if (!/^\d{4}-\d{2}$/.test(month)) {
    return res.status(400).json({ error: 'Invalid month format. Expected YYYY-MM.' });
  }
  try {
    const [year, mon] = month.split('-');
    const permissionsConfig = await db.getPermissionsConfig();
    const activeRole = getActiveRole(req, permissionsConfig);
    const perms      = getRolePermissions(activeRole, permissionsConfig);
    const snapshot   = await db.getSnapshot(year, mon);
    if (!snapshot) return res.status(404).json({ error: 'Snapshot not found.' });
    res.json(filterSnapshot(snapshot, perms, activeRole));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /plan/:slug/api/snapshots/preview-auto
planRouter.get('/api/snapshots/preview-auto', async (req, res) => {
  try {
    const months = await snapshot.generateAutoMonths(req.params.slug);
    res.json({ months });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /plan/:slug/api/snapshots/generate
// Body: { mode: 'auto'|'manual', months?: ['2026-04', ...] }
planRouter.post('/api/snapshots/generate', async (req, res) => {
  try {
    const { mode = 'auto', months } = req.body;
    const plan = db.listPlans().find(p => p.id === req.params.slug);
    if (!plan) return res.status(404).json({ error: 'Plan not found' });
    const result = await snapshot.generateSnapshots(plan.id, { mode, months });
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── CRUD API Routes ────────────────────────────────────────────────────────────

// Addresses
planRouter.post('/api/addresses', async (req, res) => {
  try { res.status(201).json(await db.createAddress(req.body)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.put('/api/addresses/:id', async (req, res) => {
  try { await db.updateAddress(req.params.id, req.body); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.delete('/api/addresses/:id', async (req, res) => {
  try { await db.deleteAddress(req.params.id); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// People
planRouter.post('/api/people', async (req, res) => {
  try { res.status(201).json(await db.createPerson(req.body)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.put('/api/people/:id', async (req, res) => {
  try { await db.updatePerson(req.params.id, req.body); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.delete('/api/people/:id', async (req, res) => {
  try { await db.deletePerson(req.params.id); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// Properties
planRouter.post('/api/properties', async (req, res) => {
  try { res.status(201).json(await db.createProperty(req.body)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.put('/api/properties/:id', async (req, res) => {
  try { await db.updateProperty(req.params.id, req.body); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.delete('/api/properties/:id', async (req, res) => {
  try { await db.deleteProperty(req.params.id); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// Bank Accounts
planRouter.post('/api/bank-accounts', async (req, res) => {
  try { await db.createBankAccount(req.body); res.status(201).json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.put('/api/bank-accounts/:id', async (req, res) => {
  try { await db.updateBankAccount(req.params.id, req.body); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.delete('/api/bank-accounts/:id', async (req, res) => {
  try { await db.deleteBankAccount(req.params.id); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// Contracts
planRouter.post('/api/contracts', async (req, res) => {
  try { res.status(201).json(await db.createContract(req.body)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.put('/api/contracts/:id', async (req, res) => {
  try { await db.updateContract(req.params.id, req.body); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.delete('/api/contracts/:id', async (req, res) => {
  try { await db.deleteContract(req.params.id); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// Periodic Expenses
planRouter.post('/api/periodic-expenses', async (req, res) => {
  try { res.status(201).json(await db.createPeriodicExpense(req.body)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.put('/api/periodic-expenses/:id', async (req, res) => {
  try { await db.updatePeriodicExpense(req.params.id, req.body); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.delete('/api/periodic-expenses/:id', async (req, res) => {
  try { await db.deletePeriodicExpense(req.params.id); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// Mortgages
planRouter.post('/api/mortgages', async (req, res) => {
  try { await db.createMortgage(req.body); res.status(201).json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.put('/api/mortgages/:id', async (req, res) => {
  try { await db.updateMortgage(req.params.id, req.body); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.delete('/api/mortgages/:id', async (req, res) => {
  try { await db.deleteMortgage(req.params.id); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// Incomes
planRouter.post('/api/incomes', async (req, res) => {
  try { await db.createIncome(req.body); res.status(201).json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.put('/api/incomes/:id', async (req, res) => {
  try { await db.updateIncome(req.params.id, req.body); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.delete('/api/incomes/:id', async (req, res) => {
  try { await db.deleteIncome(req.params.id); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// Timeline Milestones
planRouter.post('/api/milestones', async (req, res) => {
  try { res.status(201).json(await db.createMilestone(req.body)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.put('/api/milestones/:id', async (req, res) => {
  try { await db.updateMilestone(req.params.id, req.body); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
planRouter.delete('/api/milestones/:id', async (req, res) => {
  try { await db.deleteMilestone(req.params.id); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

planRouter.get('/assets', async (req, res) => {
  try {
    const permissionsConfig = await db.getPermissionsConfig();
    const activeRole = getActiveRole(req, permissionsConfig);
    const perms      = getRolePermissions(activeRole, permissionsConfig);
    const allPlans   = db.listPlans();

    const [
      { people },
      { addresses },
      { incomes },
      { project },
      { contracts },
      { periodic_expenses: periodicExpenses },
      mortgages,
      { properties: realEstate },
      { bank_accounts: bankAccounts },
    ] = await Promise.all([
      db.getPeople(),
      db.getAddresses(),
      db.getIncomes(),
      db.getTimeline(),
      db.getContracts(),
      db.getPeriodicExpenses(),
      db.getMortgages(),
      db.getRealEstate(),
      db.getBankAccounts(),
    ]);

    const addressMap = Object.fromEntries(addresses.map(a => [a.id, a]));

    const filteredPeople      = filterPeople(people, perms);
    const filteredIncomes     = filterIncomes(incomes, perms);
    const filteredContracts   = filterContracts(contracts.filter(c => c.direction !== 'income'), perms);
    const employmentContracts = contracts.filter(c => c.direction === 'income');
    const filteredEmployment  = perms.has('display_amounts')
      ? employmentContracts
      : employmentContracts.map(c => ({
          ...c,
          periods: c.periods.map(p => ({ start_date: p.start_date, end_date: p.end_date })),
        }));
    const filteredPeriodic     = filterPeriodicExpenses(periodicExpenses, perms);
    const filteredMortgages    = filterMortgages(mortgages, perms);
    const filteredBankAccounts = filterBankAccounts(bankAccounts, perms);

    const incomeMap = {};
    for (const inc of filteredIncomes) {
      if (!incomeMap[inc.person_id]) incomeMap[inc.person_id] = [];
      incomeMap[inc.person_id].push(inc);
    }

    const oidcUser = (req.oidc && req.oidc.isAuthenticated())
      ? { name: req.oidc.user.name, email: req.oidc.user.email, picture: req.oidc.user.picture }
      : null;

    const template = fs.readFileSync(path.join(__dirname, 'views', 'assets.html'), 'utf8');
    const html = template
      .replace('__PROJECT_JSON__',            JSON.stringify(project))
      .replace('__PEOPLE_JSON__',             JSON.stringify(filteredPeople))
      .replace('__ADDRESSES_JSON__',          JSON.stringify(addresses))
      .replace('__ADDRESS_MAP_JSON__',        JSON.stringify(addressMap))
      .replace('__INCOME_MAP_JSON__',         JSON.stringify(incomeMap))
      .replace('__INCOMES_JSON__',            JSON.stringify(filteredIncomes))
      .replace('__CONTRACTS_JSON__',          JSON.stringify(filteredContracts))
      .replace('__PERIODIC_EXPENSES_JSON__',  JSON.stringify(filteredPeriodic))
      .replace('__MORTGAGES_JSON__',          JSON.stringify(filteredMortgages))
      .replace('__REAL_ESTATE_JSON__',        JSON.stringify(realEstate))
      .replace('__BANK_ACCOUNTS_JSON__',      JSON.stringify(filteredBankAccounts))
      .replace('__EMPLOYMENT_JSON__',         JSON.stringify(filteredEmployment))
      .replace('__ROLE_JSON__',               JSON.stringify(activeRole))
      .replace('__PERMISSIONS_CONFIG_JSON__', JSON.stringify(permissionsConfig))
      .replace('__USER_JSON__',               JSON.stringify(oidcUser))
      .replace('__PLAN_JSON__',               JSON.stringify(req.activePlan))
      .replace('__ALL_PLANS_JSON__',          JSON.stringify(allPlans))
      .replace('__IS_PRODUCTION_JSON__',      JSON.stringify(process.env.NODE_ENV === 'production'));

    res.send(html);
  } catch (e) {
    console.error('Error rendering assets:', e);
    res.status(500).send(`<pre>Error: ${e.message}\n${e.stack}</pre>`);
  }
});

planRouter.get('/', async (req, res) => {
  try {
    const permissionsConfig = await db.getPermissionsConfig();
    const activeRole = getActiveRole(req, permissionsConfig);
    const perms      = getRolePermissions(activeRole, permissionsConfig);

    const allPlans = db.listPlans();

    const [
      { people },
      { addresses },
      { incomes },
      { project },
      { contracts },
      { periodic_expenses: periodicExpenses },
      mortgages,
      { properties: realEstate },
      { bank_accounts: bankAccounts },
    ] = await Promise.all([
      db.getPeople(),
      db.getAddresses(),
      db.getIncomes(),
      db.getTimeline(),
      db.getContracts(),
      db.getPeriodicExpenses(),
      db.getMortgages(),
      db.getRealEstate(),
      db.getBankAccounts(),
    ]);

    const addressMap = Object.fromEntries(addresses.map(a => [a.id, a]));

    const filteredPeople       = filterPeople(people, perms);
    const filteredIncomes      = filterIncomes(incomes, perms);
    const filteredContracts    = filterContracts(contracts.filter(c => c.direction !== 'income'), perms);
    const employmentContracts  = contracts.filter(c => c.direction === 'income');
    const filteredEmployment   = perms.has('display_amounts')
      ? employmentContracts
      : employmentContracts.map(c => ({
          ...c,
          periods: c.periods.map(p => ({ start_date: p.start_date, end_date: p.end_date })),
        }));
    const filteredPeriodic     = filterPeriodicExpenses(periodicExpenses, perms);
    const filteredMortgages    = filterMortgages(mortgages, perms);
    const filteredBankAccounts = filterBankAccounts(bankAccounts, perms);

    const incomeMap = {};
    for (const inc of filteredIncomes) {
      if (!incomeMap[inc.person_id]) incomeMap[inc.person_id] = [];
      incomeMap[inc.person_id].push(inc);
    }

    // Expose Auth0 user to the template
    const oidcUser = (req.oidc && req.oidc.isAuthenticated())
      ? { name: req.oidc.user.name, email: req.oidc.user.email, picture: req.oidc.user.picture }
      : null;

    const template = fs.readFileSync(path.join(__dirname, 'views', 'index.html'), 'utf8');
    const html = template
      .replace('__PROJECT_JSON__',            JSON.stringify(project))
      .replace('__PEOPLE_JSON__',             JSON.stringify(filteredPeople))
      .replace('__ADDRESS_MAP_JSON__',        JSON.stringify(addressMap))
      .replace('__INCOME_MAP_JSON__',         JSON.stringify(incomeMap))
      .replace('__CONTRACTS_JSON__',          JSON.stringify(filteredContracts))
      .replace('__PERIODIC_EXPENSES_JSON__',  JSON.stringify(filteredPeriodic))
      .replace('__MORTGAGES_JSON__',          JSON.stringify(filteredMortgages))
      .replace('__REAL_ESTATE_JSON__',        JSON.stringify(realEstate))
      .replace('__BANK_ACCOUNTS_JSON__',      JSON.stringify(filteredBankAccounts))
      .replace('__EMPLOYMENT_JSON__',         JSON.stringify(filteredEmployment))
      .replace('__ROLE_JSON__',               JSON.stringify(activeRole))
      .replace('__PERMISSIONS_CONFIG_JSON__', JSON.stringify(permissionsConfig))
      .replace('__USER_JSON__',               JSON.stringify(oidcUser))
      .replace('__PLAN_JSON__',               JSON.stringify(req.activePlan))
      .replace('__ALL_PLANS_JSON__',          JSON.stringify(allPlans))
      .replace('__IS_PRODUCTION_JSON__',      JSON.stringify(true));

    res.send(html);
  } catch (e) {
    console.error('Error rendering dashboard:', e);
    res.status(500).send(`<pre>Error: ${e.message}\n${e.stack}</pre>`);
  }
});

// ── Startup ───────────────────────────────────────────────────────────────────

if (require.main === module) {
  // Local execution: wait for DB init before opening the port
  _dbReady.then(() => {
    app.listen(PORT, () => {
      const env = process.env.NODE_ENV || 'development';
      console.log(`Dashboard running at http://localhost:${PORT} [${env}]`);
    });
  }).catch(err => {
    console.error('Fatal: database initialization failed:', err);
    process.exit(1);
  });
}

// Serverless entry point (Vercel) — module.exports is the Express app
module.exports = app;
