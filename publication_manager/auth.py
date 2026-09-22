"""Signed session tokens, logout revocation, and login throttling.

The Streamlit UI keeps the logged-in role and username in ``st.query_params`` so a
browser refresh does not log the user out. Storing the role in plain text there lets
anyone type ``?role=admin`` and become admin, so the parameters are signed with an
HMAC and verified on every restore. Admin tokens are short-lived and logout revokes
them, because a copied URL is otherwise a bearer credential.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets as _secrets
import time

VALID_ROLES = {"admin", "faculty"}
DEFAULT_MAX_AGE_SECONDS = 12 * 60 * 60
ADMIN_MAX_AGE_SECONDS = 2 * 60 * 60
FACULTY_MAX_AGE_SECONDS = DEFAULT_MAX_AGE_SECONDS

_process_secret: str | None = None
_revoked: dict[str, float] = {}  # signature -> time after which it can be forgotten


def process_secret() -> str:
    """Random per-process fallback secret. Tokens signed with it die on restart."""
    global _process_secret
    if _process_secret is None:
        _process_secret = _secrets.token_hex(32)
    return _process_secret


def max_age_for_role(role: str) -> int:
    return ADMIN_MAX_AGE_SECONDS if role == "admin" else FACULTY_MAX_AGE_SECONDS


def _message(role: str, user: str, issued_at: int) -> bytes:
    return f"{role}\x1f{user}\x1f{issued_at}".encode("utf-8")


def sign_session(secret: str, role: str, user: str, issued_at: int | None = None) -> dict[str, str]:
    """Return the query parameters that represent a signed login session."""
    if role not in VALID_ROLES:
        raise ValueError(f"Unknown role: {role}")
    if not user or not user.strip():
        raise ValueError("User is required")
    ts = int(issued_at if issued_at is not None else time.time())
    sig = hmac.new(secret.encode("utf-8"), _message(role, user, ts), hashlib.sha256).hexdigest()
    return {"role": role, "user": user, "ts": str(ts), "sig": sig}


def verify_session(
    secret: str,
    params: dict[str, str | None],
    max_age_seconds: int | None = None,
    now: float | None = None,
) -> tuple[str, str] | None:
    """Return ``(role, user)`` if the parameters carry a valid, unexpired, unrevoked signature."""
    role = params.get("role")
    user = params.get("user")
    ts_raw = params.get("ts")
    sig = params.get("sig")
    if not role or not user or not ts_raw or not sig:
        return None
    if role not in VALID_ROLES:
        return None
    try:
        issued_at = int(ts_raw)
    except (TypeError, ValueError):
        return None
    current = now if now is not None else time.time()
    max_age = max_age_seconds if max_age_seconds is not None else max_age_for_role(role)
    if issued_at > current + 60 or current - issued_at > max_age:
        return None
    expected = hmac.new(secret.encode("utf-8"), _message(role, user, issued_at), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, str(sig)):
        return None
    if is_revoked(str(sig), now=current):
        return None
    return role, user


def revoke_session(params: dict[str, str | None], now: float | None = None) -> None:
    """Invalidate a token on logout so a copied URL stops working."""
    sig = params.get("sig")
    if not sig:
        return
    current = now if now is not None else time.time()
    _revoked[str(sig)] = current + DEFAULT_MAX_AGE_SECONDS
    for key in [k for k, expiry in _revoked.items() if expiry < current]:
        _revoked.pop(key, None)


def is_revoked(sig: str, now: float | None = None) -> bool:
    expiry = _revoked.get(sig)
    if expiry is None:
        return False
    current = now if now is not None else time.time()
    if expiry < current:
        _revoked.pop(sig, None)
        return False
    return True


class LoginThrottle:
    """Process-wide failed-login counter keyed by client (normally its IP).

    Session state is per browser tab, so a counter kept there resets whenever the
    attacker opens a new tab. This one does not.
    """

    def __init__(self, max_attempts: int = 5, window_seconds: int = 300, lockout_seconds: int = 300) -> None:
        self.max_attempts = max_attempts
        self.window_seconds = window_seconds
        self.lockout_seconds = lockout_seconds
        self._failures: dict[str, list[float]] = {}
        self._locked_until: dict[str, float] = {}

    def locked_for(self, key: str, now: float | None = None) -> int:
        """Seconds left on this client's lockout, or 0."""
        current = now if now is not None else time.time()
        until = self._locked_until.get(key, 0.0)
        if until <= current:
            self._locked_until.pop(key, None)
            return 0
        return int(until - current) + 1

    def record_failure(self, key: str, now: float | None = None) -> int:
        """Record a failed attempt. Returns attempts left before lockout (0 = now locked)."""
        current = now if now is not None else time.time()
        recent = [t for t in self._failures.get(key, []) if current - t < self.window_seconds]
        recent.append(current)
        self._failures[key] = recent
        if len(recent) >= self.max_attempts:
            self._locked_until[key] = current + self.lockout_seconds
            self._failures[key] = []
            return 0
        return self.max_attempts - len(recent)

    def reset(self, key: str) -> None:
        self._failures.pop(key, None)
        self._locked_until.pop(key, None)


ADMIN_THROTTLE = LoginThrottle()
