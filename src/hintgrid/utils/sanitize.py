"""Sanitization utilities for preventing secret leaks in logs and errors.

Provides functions to mask sensitive data (passwords, API keys) in
connection strings and error messages before they reach logs.
"""

from __future__ import annotations

import re


# Pattern to match password in PostgreSQL/generic DSN:
#   postgresql://user:PASSWORD@host:port/db
_DSN_PASSWORD_RE = re.compile(
    r"(?<=://)([^:]+):([^@]+)@"
)

# Replacement mask for hidden passwords
_PASSWORD_MASK = "***"


def sanitize_dsn(dsn: str) -> str:
    """Mask password in a database connection string (DSN).

    Replaces the password component in URIs like:
        postgresql://user:s3cr3t@host:5432/db
    with:
        postgresql://user:***@host:5432/db

    Safe to call on strings that don't contain a DSN (returned unchanged).
    """
    return _DSN_PASSWORD_RE.sub(r"\1:***@", dsn)


def sanitize_error(error: Exception | None) -> str:
    """Convert exception to string with sensitive data masked.

    Masks any DSN-like passwords found in the error message.
    Use this instead of str(error) when the error may contain
    connection strings or credentials.
    """
    if error is None:
        return ""
    return sanitize_dsn(str(error))
