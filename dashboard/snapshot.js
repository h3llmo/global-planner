/**
 * snapshot.js — JavaScript snapshot computation engine.
 * Mirrors SnapshotSituation.py logic, reading from the shared SQLite DB.
 *
 * Key functions:
 *   computeMonthlySnapshot(planId, year, month)  → snapshot object
 *   computeAnnualSnapshot(planId, year)           → snapshot object
 *   generateAutoMonths(planId)                   → [{year,month},...] (changed months only)
 *   saveSnapshot(planId, snapshot)               → void (upserts to DB)
 *   generateSnapshots(planId, {mode, months})    → { generated, errors }
 */

'use strict';

const fs   = require('fs');
const path = require('path');
const { getDb, query: dbQuery, queryOne: dbQueryOne } = require('./db');

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------

async function _getPlanData(planId) {
  const db = await getDb();

  // Contracts
  const contracts = dbQuery(db,
    `SELECT id, title, category, category_i18n, direction, owner_id, property_id, consultant_relevant
     FROM contracts WHERE plan_id=? ORDER BY id`, [planId]);
  for (const c of contracts) {
    c.periods = dbQuery(db,
      'SELECT monthly_cost, gross_monthly, start_date, end_date FROM contract_periods WHERE plan_id=? AND contract_id=? ORDER BY id',
      [planId, c.id]);
  }

  // Virtual contracts (plan-level additions, treated as expense contracts with a simple single period)
  const vc = dbQuery(db,
    'SELECT id, title, category, direction, owner_id, monthly_cost, start_date, end_date FROM virtual_contracts WHERE plan_id=?',
    [planId]);
  for (const v of vc) {
    contracts.push({
      id: `vc-${v.id}`, title: v.title, category: v.category,
      direction: v.direction || 'expense', owner_id: v.owner_id,
      property_id: null, consultant_relevant: 1,
      periods: v.monthly_cost != null
        ? [{ monthly_cost: v.monthly_cost, gross_monthly: null, start_date: v.start_date, end_date: v.end_date }]
        : [],
    });
  }

  // Incomes
  const incomes = dbQuery(db,
    `SELECT id, person_id, year, avg_net_monthly_salary, avg_gross_monthly_salary,
       health_insurance, transportation_allowance, meal_vouchers,
       performance_bonus_gross, performance_bonus_net,
       end_of_year_bonus_gross, end_of_year_bonus_net, child_allowance
     FROM incomes WHERE plan_id=? ORDER BY person_id, year`, [planId]);
  for (const inc of incomes) {
    inc.consultant_relevant_benefits = dbQuery(db,
      'SELECT benefit_key FROM income_consultant_benefits WHERE income_id=?', [inc.id])
      .map(r => r.benefit_key);
  }

  // Virtual incomes
  const vi = dbQuery(db,
    'SELECT person_id, year, avg_net_monthly_salary FROM virtual_incomes WHERE plan_id=?', [planId]);
  incomes.push(...vi.map(v => ({
    id: null, person_id: v.person_id, year: v.year,
    avg_net_monthly_salary: v.avg_net_monthly_salary,
    avg_gross_monthly_salary: null, meal_vouchers: null, child_allowance: null,
    performance_bonus_gross: null, performance_bonus_net: null,
    end_of_year_bonus_gross: null, end_of_year_bonus_net: null,
    consultant_relevant_benefits: [],
  })));

  // Periodic expenses
  const periodicExpenses = dbQuery(db,
    `SELECT id, title, title_i18n, category, category_i18n, owner_id, property_id, consultant_relevant
     FROM periodic_expenses WHERE plan_id=?`, [planId]);
  for (const pe of periodicExpenses) {
    pe.payments = dbQuery(db,
      'SELECT label, amount FROM periodic_expense_payments WHERE plan_id=? AND expense_id=? ORDER BY id',
      [planId, pe.id]);
  }

  // Virtual periodic expenses
  const vpe = dbQuery(db,
    'SELECT id, title, category, owner_id, payments FROM virtual_periodic_expenses WHERE plan_id=?', [planId]);
  for (const v of vpe) {
    let payments = [];
    try { payments = JSON.parse(v.payments || '[]'); } catch {}
    periodicExpenses.push({
      id: `vpe-${v.id}`, title: v.title, category: v.category,
      owner_id: v.owner_id, consultant_relevant: 1, payments,
    });
  }

  return { contracts, incomes, periodicExpenses };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function _resolveActivePeriod(contract, year, month) {
  if (!contract.periods || contract.periods.length === 0) return null;
  const target = new Date(Date.UTC(year, month - 1, 1));
  for (const p of contract.periods) {
    const start = p.start_date ? new Date(p.start_date) : null;
    const end   = p.end_date   ? new Date(p.end_date)   : null;
    if ((!start || start <= target) && (!end || end >= target)) return p;
  }
  return null;
}

function _resolveIncome(incomes, personId, year) {
  const mine = incomes
    .filter(i => i.person_id === personId)
    .sort((a, b) => b.year - a.year);
  return mine.find(i => i.year === year) || mine.find(i => i.year <= year) || null;
}

// ---------------------------------------------------------------------------
// Monthly snapshot
// ---------------------------------------------------------------------------

async function computeMonthlySnapshot(planId, year, month) {
  const { contracts, incomes, periodicExpenses } = await _getPlanData(planId);

  const personIds = new Set([
    ...contracts.map(c => c.owner_id),
    ...incomes.map(i => i.person_id),
    ...periodicExpenses.map(e => e.owner_id),
  ].filter(Boolean));

  const round2 = n => Math.round(n * 100) / 100;
  const people = [];

  for (const pid of [...personIds].sort((a, b) => a - b)) {
    const inc = _resolveIncome(incomes, pid, year);
    let net_monthly_salary   = inc?.avg_net_monthly_salary   || 0;
    let gross_monthly_salary = inc?.avg_gross_monthly_salary || null;
    const meal_vouchers      = inc?.meal_vouchers            || 0;
    const child_allowance    = inc?.child_allowance          || 0;
    const crb                = inc?.consultant_relevant_benefits || [];

    // Override with active employment (income-direction) contract
    for (const c of contracts) {
      if (c.owner_id !== pid || c.direction !== 'income') continue;
      const p = _resolveActivePeriod(c, year, month);
      if (p) {
        net_monthly_salary = p.monthly_cost;
        if (p.gross_monthly != null) gross_monthly_salary = p.gross_monthly;
        break;
      }
    }

    const total_monthly_net = round2(net_monthly_salary + meal_vouchers + child_allowance);
    const income = { net_monthly_salary, meal_vouchers, child_allowance, total_monthly_net };
    if (gross_monthly_salary != null) income.gross_monthly_salary = gross_monthly_salary;
    if (crb.length) income.consultant_relevant_benefits = crb;

    // Active expense contracts
    const activeContracts = [];
    for (const c of contracts) {
      if (c.owner_id !== pid || c.direction === 'income') continue;
      const p = _resolveActivePeriod(c, year, month);
      if (!p) continue;
      const entry = { id: c.id, title: c.title, category: c.category, monthly_cost: p.monthly_cost };
      if (c.property_id != null)     entry.property_id   = c.property_id;
      if (c.category_i18n)           entry.category_i18n = c.category_i18n;
      if (c.consultant_relevant === 0 || c.consultant_relevant === false) entry.consultant_relevant = false;
      activeContracts.push(entry);
    }

    // Periodic expenses
    const activePeriodic = [];
    for (const pe of periodicExpenses) {
      if (pe.owner_id !== pid) continue;
      const annual_total = round2(pe.payments.reduce((s, p) => s + p.amount, 0));
      const monthly_avg  = round2(annual_total / 12);
      const entry = { id: pe.id, title: pe.title, category: pe.category, annual_total, monthly_avg };
      if (pe.property_id != null) entry.property_id   = pe.property_id;
      if (pe.title_i18n)          entry.title_i18n    = pe.title_i18n;
      if (pe.category_i18n)       entry.category_i18n = pe.category_i18n;
      if (pe.consultant_relevant === 0 || pe.consultant_relevant === false) entry.consultant_relevant = false;
      activePeriodic.push(entry);
    }

    const total_monthly_expenses = round2(
      activeContracts.reduce((s, c) => s + c.monthly_cost, 0) +
      activePeriodic.reduce((s, e)  => s + e.monthly_avg,  0)
    );
    people.push({ person_id: pid, income, contracts: activeContracts, periodic_expenses: activePeriodic, total_monthly_expenses });
  }

  const total_net_income = round2(people.reduce((s, p) => s + p.income.total_monthly_net, 0));
  const total_contracts  = round2(people.reduce((s, p) => s + p.contracts.reduce((t, c) => t + c.monthly_cost, 0), 0));
  const total_periodic   = round2(people.reduce((s, p) => s + p.periodic_expenses.reduce((t, e) => t + e.monthly_avg, 0), 0));
  const total_expenses   = round2(total_contracts + total_periodic);

  const MONTHS = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  return {
    year, month,
    label: `${MONTHS[month - 1]} ${year}`,
    generated_at: new Date().toISOString().replace(/\.\d{3}Z$/, '.000000Z'),
    people,
    summary: { total_net_income, total_contracts, total_periodic_avg: total_periodic, total_expenses, net_balance: round2(total_net_income - total_expenses) },
  };
}

// ---------------------------------------------------------------------------
// Annual snapshot
// ---------------------------------------------------------------------------

async function computeAnnualSnapshot(planId, year) {
  const { contracts, incomes, periodicExpenses } = await _getPlanData(planId);

  const personIds = new Set([
    ...contracts.map(c => c.owner_id),
    ...incomes.map(i => i.person_id),
    ...periodicExpenses.map(e => e.owner_id),
  ].filter(Boolean));

  const round2 = n => Math.round(n * 100) / 100;
  const people = [];

  for (const pid of [...personIds].sort((a, b) => a - b)) {
    const inc = _resolveIncome(incomes, pid, year);
    const base_monthly          = inc?.avg_net_monthly_salary   || 0;
    const base_gross_monthly    = inc?.avg_gross_monthly_salary  || null;
    const meal_vouchers         = inc?.meal_vouchers             || 0;
    const child_allowance       = inc?.child_allowance           || 0;
    const performance_bonus_net = inc?.performance_bonus_net     || null;
    const end_of_year_bonus_net = inc?.end_of_year_bonus_net     || null;
    const crb                   = inc?.consultant_relevant_benefits || [];

    // Sum salary across all 12 months (accounts for mid-year raises)
    let net_salary_annual   = 0;
    let gross_salary_annual = 0;
    for (let m = 1; m <= 12; m++) {
      let monthNet   = base_monthly;
      let monthGross = base_gross_monthly || 0;
      for (const c of contracts) {
        if (c.owner_id !== pid || c.direction !== 'income') continue;
        const p = _resolveActivePeriod(c, year, m);
        if (p) { monthNet = p.monthly_cost; if (p.gross_monthly != null) monthGross = p.gross_monthly; break; }
      }
      net_salary_annual   += monthNet;
      gross_salary_annual += monthGross;
    }
    net_salary_annual   = round2(net_salary_annual);
    gross_salary_annual = round2(gross_salary_annual);

    const meal_vouchers_annual   = round2(meal_vouchers * 12);
    const child_allowance_annual = round2(child_allowance * 12);
    let total_annual_net = round2(net_salary_annual + meal_vouchers_annual + child_allowance_annual);
    if (performance_bonus_net != null) total_annual_net = round2(total_annual_net + performance_bonus_net);
    if (end_of_year_bonus_net  != null) total_annual_net = round2(total_annual_net + end_of_year_bonus_net);

    const income = { net_salary_annual, gross_salary_annual, meal_vouchers_annual, child_allowance_annual, total_annual_net };
    if (performance_bonus_net != null) income.performance_bonus = performance_bonus_net;
    if (end_of_year_bonus_net != null) income.end_of_year_bonus = end_of_year_bonus_net;
    if (crb.length) income.consultant_relevant_benefits = crb;

    // Annual contract costs (sum of active months)
    const annualContracts = [];
    for (const c of contracts) {
      if (c.owner_id !== pid || c.direction === 'income') continue;
      let annual_cost  = 0;
      let months_active = 0;
      for (let m = 1; m <= 12; m++) {
        const p = _resolveActivePeriod(c, year, m);
        if (p) { annual_cost += p.monthly_cost; months_active++; }
      }
      if (months_active === 0) continue;
      annual_cost = round2(annual_cost);
      const entry = { id: c.id, title: c.title, category: c.category, annual_cost, months_active };
      if (c.property_id != null)    entry.property_id   = c.property_id;
      if (c.category_i18n)          entry.category_i18n = c.category_i18n;
      if (c.consultant_relevant === 0 || c.consultant_relevant === false) entry.consultant_relevant = false;
      annualContracts.push(entry);
    }

    // Annual periodic expenses
    const annualPeriodic = [];
    for (const pe of periodicExpenses) {
      if (pe.owner_id !== pid) continue;
      const annual_total = round2(pe.payments.reduce((s, p) => s + p.amount, 0));
      const entry = { id: pe.id, title: pe.title, category: pe.category, annual_total };
      if (pe.property_id != null) entry.property_id   = pe.property_id;
      if (pe.title_i18n)          entry.title_i18n    = pe.title_i18n;
      if (pe.category_i18n)       entry.category_i18n = pe.category_i18n;
      if (pe.consultant_relevant === 0 || pe.consultant_relevant === false) entry.consultant_relevant = false;
      annualPeriodic.push(entry);
    }

    const total_annual_expenses = round2(
      annualContracts.reduce((s, c) => s + c.annual_cost, 0) +
      annualPeriodic.reduce((s, e)  => s + e.annual_total, 0)
    );
    people.push({ person_id: pid, income, contracts: annualContracts, periodic_expenses: annualPeriodic, total_annual_expenses });
  }

  const total_annual_net_income = round2(people.reduce((s, p) => s + p.income.total_annual_net, 0));
  const total_annual_expenses   = round2(people.reduce((s, p) => s + p.total_annual_expenses, 0));

  return {
    year, type: 'annual',
    label: String(year),
    generated_at: new Date().toISOString().replace(/\.\d{3}Z$/, '.000000Z'),
    people,
    summary: { total_annual_net_income, total_annual_expenses, net_annual_balance: round2(total_annual_net_income - total_annual_expenses) },
  };
}

// ---------------------------------------------------------------------------
// Auto-detection of changed months
// ---------------------------------------------------------------------------

async function generateAutoMonths(planId) {
  const data = await _getPlanData(planId);
  const currentYear = new Date().getUTCFullYear();
  const endYear = currentYear + 5;
  let prevFingerprint = null;
  const result = [];

  for (let year = currentYear; year <= endYear; year++) {
    for (let month = 1; month <= 12; month++) {
      const fp = _fingerprint(data, year, month);
      if (fp !== prevFingerprint) {
        result.push({ year, month });
      }
      prevFingerprint = fp;
    }
  }
  return result;
}

function _fingerprint(data, year, month) {
  const { contracts, incomes, periodicExpenses } = data;
  const parts = [];

  const personIds = new Set([
    ...contracts.map(c => c.owner_id),
    ...incomes.map(i => i.person_id),
    ...periodicExpenses.map(e => e.owner_id),
  ].filter(Boolean));

  for (const pid of [...personIds].sort((a, b) => a - b)) {
    const inc = _resolveIncome(incomes, pid, year);
    let net = inc?.avg_net_monthly_salary || 0;
    for (const c of contracts) {
      if (c.owner_id !== pid || c.direction !== 'income') continue;
      const p = _resolveActivePeriod(c, year, month);
      if (p) { net = p.monthly_cost; break; }
    }
    parts.push(`I${pid}=${net}`);

    for (const c of contracts) {
      if (c.owner_id !== pid || c.direction === 'income') continue;
      const p = _resolveActivePeriod(c, year, month);
      if (p) parts.push(`C${c.id}=${p.monthly_cost}`);
    }
  }

  return parts.sort().join('|');
}

// ---------------------------------------------------------------------------
// Persist snapshot to DB (upsert via delete + insert)
// ---------------------------------------------------------------------------

async function saveSnapshot(planId, snapshot) {
  const db = await getDb();
  const content     = JSON.stringify(snapshot, null, 2);
  const generatedAt = snapshot.generated_at;
  const label       = snapshot.label;

  if (snapshot.type === 'annual') {
    db.run(
      `DELETE FROM snapshots WHERE plan_id=? AND type='annual' AND year=? AND month IS NULL`,
      [planId, snapshot.year]
    );
    db.run(
      `INSERT INTO snapshots (plan_id, type, year, month, label, generated_at, content)
       VALUES (?, 'annual', ?, NULL, ?, ?, ?)`,
      [planId, snapshot.year, label, generatedAt, content]
    );
  } else {
    db.run(
      `DELETE FROM snapshots WHERE plan_id=? AND type='monthly' AND year=? AND month=?`,
      [planId, snapshot.year, snapshot.month]
    );
    db.run(
      `INSERT INTO snapshots (plan_id, type, year, month, label, generated_at, content)
       VALUES (?, 'monthly', ?, ?, ?, ?, ?)`,
      [planId, snapshot.year, snapshot.month, label, generatedAt, content]
    );
  }

  // Persist to disk
  const dbPath = process.env.NODE_ENV === 'production' ? '/tmp/finance.db' : path.join(__dirname, 'finance.db');
  try { fs.writeFileSync(dbPath, Buffer.from(db.export())); } catch {}
}

// ---------------------------------------------------------------------------
// Generate snapshots (monthly + annual)
// ---------------------------------------------------------------------------

async function generateSnapshots(planId, { mode, months }) {
  const generated = [];
  const errors    = [];
  const currentYear = new Date().getUTCFullYear();

  // Determine monthly snapshots to generate
  let monthList = [];
  if (mode === 'auto') {
    monthList = await generateAutoMonths(planId);
  } else if (mode === 'manual' && Array.isArray(months)) {
    monthList = months.map(m => {
      const [y, mo] = m.split('-').map(Number);
      return { year: y, month: mo };
    }).filter(m => m.year && m.month);
  }

  for (const { year, month } of monthList) {
    try {
      const snap = await computeMonthlySnapshot(planId, year, month);
      await saveSnapshot(planId, snap);
      generated.push({ type: 'monthly', label: snap.label });
    } catch (e) {
      errors.push({ type: 'monthly', year, month, error: e.message });
    }
  }

  // Annual snapshots: always current year + 5
  for (let year = currentYear; year <= currentYear + 5; year++) {
    try {
      const snap = await computeAnnualSnapshot(planId, year);
      await saveSnapshot(planId, snap);
      generated.push({ type: 'annual', label: snap.label });
    } catch (e) {
      errors.push({ type: 'annual', year, error: e.message });
    }
  }

  return { generated, errors };
}

module.exports = { computeMonthlySnapshot, computeAnnualSnapshot, generateAutoMonths, saveSnapshot, generateSnapshots };
