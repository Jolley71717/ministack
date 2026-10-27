"""
Which AWS accounts and IAM identities exist, as far as MiniStack can tell.

AWS answers differently for an account that exists and one that does not:
Kinesis says AccessDeniedException for another account's stream ARN but
UnrecognizedClientException ("No account found for the given parameters")
when the account is not real, and a resource policy naming a principal that
does not exist is refused ("Invalid principal in policy document."). Any
12-digit access key is an account here, so an account exists once it is in
use: a request was signed for it, or MiniStack holds an identity, stream or
Organizations membership in it. The default account always exists.
"""

import os
import re

from ministack.core import responses

_ACCOUNT_RE = re.compile(r"^\d{12}$")
_IDENTITY_RE = re.compile(r"^arn:aws:iam::(\d{12}):(role|user)(/.*)$")


def _holds(store, account_id):
    return any(key[0] == account_id for key in list(getattr(store, "_data", {})))


def account_exists(account_id: str) -> bool:
    if not _ACCOUNT_RE.match(account_id or ""):
        return False
    if account_id in responses._accounts_seen:
        return True
    if account_id == os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000"):
        return True
    from ministack.services import iam, kinesis, organizations

    if _holds(iam._roles, account_id) or _holds(iam._users, account_id) or _holds(kinesis._streams, account_id):
        return True
    return any(key[1] == account_id for key in list(organizations._accounts._data))


def principal_exists(principal: str) -> bool:
    """Whether an AWS principal in a resource policy names something real: an
    account (``123456789012`` or ``arn:aws:iam::123456789012:root``), or an IAM
    role or user, by its exact ARN."""
    if _ACCOUNT_RE.match(principal):
        return account_exists(principal)
    m = re.match(r"^arn:aws:iam::(\d{12}):root$", principal)
    if m:
        return account_exists(m.group(1))
    m = _IDENTITY_RE.match(principal)
    if not m:
        return False
    from ministack.services import iam

    account_id, kind = m.group(1), m.group(2)
    store = iam._roles if kind == "role" else iam._users
    return any(key[0] == account_id and isinstance(rec, dict) and rec.get("Arn") == principal
               for key, rec in list(store._data.items()))


def reset():
    responses._accounts_seen.clear()
