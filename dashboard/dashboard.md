# dashboard

## First-time Setup Guide

This guide covers all third-party configuration needed to run the app locally and deploy to Vercel.

---

## 1. Auth0 — OpenID Connect Authentication

### Create an Auth0 Application

1. Go to [Auth0 Dashboard](https://manage.auth0.com) → **Applications → Create Application**
2. Name it (e.g. `global-planner`), choose **Regular Web Application**, click Create
3. Go to **Settings** tab and note down:
   - **Client ID**
   - **Client Secret**
   - **Domain** (e.g. `dev-xxxx.eu.auth0.com`)

### Configure Allowed URLs

In **Settings → Application URIs**:

| Field | Value |
|---|---|
| Allowed Callback URLs | `http://localhost:3000/callback, https://*.vercel.app/callback` |
| Allowed Logout URLs | `http://localhost:3000, https://*.vercel.app` |

### Grant Types (Advanced Settings)

In **Settings → Advanced Settings → Grant Types**, ensure **Authorization Code** is checked.

---

## 2. Environment Variables

### Local development

Copy `.env.example` → `.env` and fill in:

```env
AUTH0_CLIENT_ID=<from Auth0 app settings>
AUTH0_CLIENT_SECRET=<from Auth0 app settings>
AUTH0_ISSUER_BASE_URL=https://<your-domain>.eu.auth0.com
AUTH0_SESSION_SECRET=<generate: node -e "console.log(require('crypto').randomBytes(32).toString('hex'))">
BASE_URL=http://localhost:3000
```

Auth is **skipped entirely in development** (`NODE_ENV !== production`) — env vars are only needed for Vercel.

### Vercel production

In **Vercel → Project → Settings → Environment Variables**, add (all marked Sensitive):

| Variable | Where to find it |
|---|---|
| `AUTH0_CLIENT_ID` | Auth0 app → Settings |
| `AUTH0_CLIENT_SECRET` | Auth0 app → Settings |
| `AUTH0_ISSUER_BASE_URL` | Auth0 app → Settings → Domain |
| `AUTH0_SESSION_SECRET` | Generate locally, store securely |
| `BASE_URL` | Optional — auto-derived from `VERCEL_URL` if omitted |

> **Important:** After adding env vars, Vercel does **not** automatically redeploy. Push a new commit or manually trigger a redeployment for them to take effect.

---

## 3. Vercel — Deployment

### Connect GitHub repository

1. Go to [vercel.com](https://vercel.com) → **Add New Project**
2. Import the `h3llmo/global-planner` GitHub repository
3. Set **Root Directory** to `dashboard`
4. Framework Preset: **Express** (or Other)
5. Click **Deploy**

### Continuous deployment

Every push to `main` triggers an automatic production deployment. No manual steps needed after initial setup.

### `vercel.json` explained

```json
{
  "functions": {
    "server.js": { "includeFiles": "node_modules/sql.js/dist/**" }
  },
  "rewrites": [{ "source": "/(.*)", "destination": "/server.js" }],
  "env": { "NODE_ENV": "production" }
}
```

- `includeFiles` — bundles the `sql-wasm.wasm` binary (Vercel only auto-includes `.js` files)
- `rewrites` — routes all requests to the Express app
- `NODE_ENV=production` — disables dev-only features (snapshot generation, dotenv loading)

---

## 4. How Authentication Works

| State | Behaviour |
|---|---|
| Not logged in (production) | App accessible, forced to **public** role — role selector hidden, Login button shown |
| Logged in (production) | Role selector visible, user can switch roles |
| Development (localhost) | Auth0 bypassed entirely — role selector always visible |

### Automatic routes (provided by `express-openid-connect`)

| Route | Description |
|---|---|
| `GET /login` | Redirects to Auth0 Universal Login |
| `GET /logout` | Clears session and redirects back |
| `GET /callback` | Handles the OIDC redirect from Auth0 (automatic) |

### Note on `form_post` warning (localhost only)

When running locally over HTTP, `express-openid-connect` logs a warning about `response_mode: form_post`. This is expected and harmless. On Vercel (HTTPS) the warning does not appear.
