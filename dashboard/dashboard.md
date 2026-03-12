# dashboard

## Authentication (Auth0 OpenID Connect)

The dashboard uses [express-openid-connect](https://github.com/auth0/express-openid-connect) for authentication.

### How it works

| State | Behaviour |
|---|---|
| Not logged in | App is accessible, forced to **public** role (no amounts, no personal info) |
| Logged in | User can switch roles via the dropdown in the navbar |

Auth is **not required** to visit the site (`authRequired: false`). Auth0 just tells us *who* the user is; the existing role cookie determines *what they can see*.

### Automatic routes (provided by the library)
| Route | Description |
|---|---|
| `GET /login` | Redirects to Auth0 Universal Login page |
| `GET /logout` | Clears the session and redirects back |
| `GET /callback` | Handles the OIDC redirect from Auth0 (automatic) |

### Environment variables

| Variable | Description |
|---|---|
| `AUTH0_CLIENT_ID` | Auth0 application client ID |
| `AUTH0_ISSUER_BASE_URL` | Auth0 tenant URL (`https://<domain>.eu.auth0.com`) |
| `AUTH0_SESSION_SECRET` | Random 32-byte hex — generate with `node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"` |
| `BASE_URL` | Full URL of the app (e.g. `http://localhost:3000` or `https://your-app.vercel.app`). If omitted on Vercel, `VERCEL_URL` is used automatically. |

Copy `.env.example` → `.env` and fill in your values for local development.

### Auth0 console configuration

In **Auth0 → Applications → global-planner → Settings**, set:

- **Allowed Callback URLs**: `http://localhost:3000/callback, https://*.vercel.app/callback`
- **Allowed Logout URLs**: `http://localhost:3000, https://*.vercel.app`

### Vercel deployment

Set these 4 environment variables in **Vercel → Project → Settings → Environment Variables**:

```
AUTH0_CLIENT_ID
AUTH0_ISSUER_BASE_URL
AUTH0_SESSION_SECRET
BASE_URL   (optional — auto-derived from VERCEL_URL if omitted)
```

### Note on `form_post` warning (localhost only)
When running locally over HTTP, `express-openid-connect` logs a warning about `response_mode: form_post`. This is expected and harmless — login still works. On Vercel (HTTPS) the warning does not appear.
