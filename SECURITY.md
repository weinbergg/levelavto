# Security Notes

## Current safeguards
- Passwords are stored as PBKDF2-SHA256 hashes with per-user salt; comparisons use constant-time checks.
- Sessions are signed with `APP_SECRET` via Starlette SessionMiddleware; `user_id` is stored in the signed cookie.
- External service credentials (Telegram, email, Encar) are read from environment variables.

## Gaps / improvements
- Set `APP_SECRET` in environment; the default value is not safe for production.
- Configure SessionMiddleware cookie flags in production: `https_only=True`, `same_site="lax"/"strict"`, and a `max_age`.
- Add CSRF protection for POST forms (login/register/admin/account).
- Add rate limiting and lockout/backoff for auth endpoints.
- Keep mobile.de downloader credentials in ENV (`MOBILEDE_LOGIN`, `MOBILEDE_PASSWORD`).
- Add audit logging for admin changes.

## Future SMS login (note only)
- Requires a paid SMS gateway provider with credentials, sender ID, and per-SMS cost.
- Needs verification flow, rate limiting, and anti-abuse controls.
