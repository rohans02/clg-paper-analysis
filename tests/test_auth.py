from __future__ import annotations

import pytest

from publication_manager.auth import (
    ADMIN_MAX_AGE_SECONDS,
    DEFAULT_MAX_AGE_SECONDS,
    FACULTY_MAX_AGE_SECONDS,
    LoginThrottle,
    process_secret,
    revoke_session,
    sign_session,
    verify_session,
)

SECRET = "unit-test-secret"


def test_sign_then_verify_roundtrip():
    params = sign_session(SECRET, "admin", "admin", issued_at=1_000_000)
    assert verify_session(SECRET, params, now=1_000_100) == ("admin", "admin")


def test_plain_role_and_user_without_signature_are_rejected():
    assert verify_session(SECRET, {"role": "admin", "user": "admin"}) is None


def test_tampered_role_is_rejected():
    params = sign_session(SECRET, "faculty", "Dr. A", issued_at=1_000_000)
    params["role"] = "admin"
    assert verify_session(SECRET, params, now=1_000_100) is None


def test_tampered_user_is_rejected():
    params = sign_session(SECRET, "faculty", "Dr. A", issued_at=1_000_000)
    params["user"] = "Dr. B"
    assert verify_session(SECRET, params, now=1_000_100) is None


def test_wrong_secret_is_rejected():
    params = sign_session(SECRET, "admin", "admin", issued_at=1_000_000)
    assert verify_session("other-secret", params, now=1_000_100) is None


def test_admin_token_expires_after_two_hours_but_faculty_lasts_twelve():
    admin = sign_session(SECRET, "admin", "admin", issued_at=1_000_000)
    faculty = sign_session(SECRET, "faculty", "Dr. A", issued_at=1_000_000)
    assert verify_session(SECRET, admin, now=1_000_000 + ADMIN_MAX_AGE_SECONDS + 1) is None
    assert verify_session(SECRET, faculty, now=1_000_000 + ADMIN_MAX_AGE_SECONDS + 1) is not None
    assert verify_session(SECRET, faculty, now=1_000_000 + FACULTY_MAX_AGE_SECONDS + 1) is None
    assert verify_session(SECRET, admin, now=1_000_000 + DEFAULT_MAX_AGE_SECONDS + 1) is None


def test_future_timestamp_is_rejected():
    params = sign_session(SECRET, "admin", "admin", issued_at=2_000_000)
    assert verify_session(SECRET, params, now=1_000_000) is None


def test_unknown_role_cannot_be_signed():
    with pytest.raises(ValueError):
        sign_session(SECRET, "superuser", "x")


def test_process_secret_is_stable_within_process():
    assert process_secret() == process_secret()
    assert len(process_secret()) == 64


def test_logout_revokes_token_so_copied_url_stops_working():
    params = sign_session(SECRET, "admin", "admin", issued_at=1_000_000)
    assert verify_session(SECRET, params, now=1_000_100) is not None
    revoke_session(params, now=1_000_200)
    assert verify_session(SECRET, params, now=1_000_300) is None
    # A fresh login (new timestamp, new signature) is unaffected.
    fresh = sign_session(SECRET, "admin", "admin", issued_at=1_000_400)
    assert verify_session(SECRET, fresh, now=1_000_500) is not None


def test_throttle_locks_after_max_attempts_and_releases_after_lockout():
    throttle = LoginThrottle(max_attempts=3, window_seconds=300, lockout_seconds=60)
    assert throttle.record_failure("1.2.3.4", now=100) == 2
    assert throttle.record_failure("1.2.3.4", now=101) == 1
    assert throttle.locked_for("1.2.3.4", now=102) == 0
    assert throttle.record_failure("1.2.3.4", now=102) == 0
    assert throttle.locked_for("1.2.3.4", now=110) > 0
    assert throttle.locked_for("1.2.3.4", now=170) == 0
    # A different client is not affected.
    assert throttle.locked_for("5.6.7.8", now=110) == 0


def test_throttle_reset_clears_state():
    throttle = LoginThrottle(max_attempts=2, window_seconds=300, lockout_seconds=60)
    throttle.record_failure("k", now=0)
    throttle.record_failure("k", now=1)
    assert throttle.locked_for("k", now=2) > 0
    throttle.reset("k")
    assert throttle.locked_for("k", now=2) == 0
