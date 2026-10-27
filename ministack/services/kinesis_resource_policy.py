"""
Kinesis resource policies: validation and the stored (normalized) form.

PutResourcePolicy checks a policy document in a fixed order and answers
InvalidArgumentException with the text AWS uses at each step. The checks and
messages below were recorded against Kinesis in us-east-1:

1. size: 1..20000 UTF-8 bytes of the document as sent;
2. JSON: an object, else "This policy contains invalid Json";
3. syntax: Version, then Statement, then each statement's fields and types;
4. statements, one at a time: Sid characters, actions, principal kind, resource;
5. Sids unique across statements;
6. principals exist (checked by the caller, after this module).

Statement-level messages carry the line and column just past the token that
precedes the statement, as AWS's streaming JSON parser reports them.
GetResourcePolicy returns the document compact, in AWS's key order, with
one-element lists collapsed and AWS principals normalized, deduplicated and
sorted.
"""

import json
import re

MAX_POLICY_BYTES = 20000

# Actions a resource policy may grant, by resource type (case-insensitive).
STREAM_ACTIONS = frozenset(a.lower() for a in (
    "kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:GetRecords",
    "kinesis:GetShardIterator", "kinesis:ListShards", "kinesis:PutRecord", "kinesis:PutRecords",
))
CONSUMER_ACTIONS = frozenset(a.lower() for a in (
    "kinesis:DescribeStreamConsumer", "kinesis:SubscribeToShard",
))

_VERSIONS = ("2012-10-17", "2008-10-17")
_TOP_FIELDS = ("Version", "Id", "Statement")
_STATEMENT_FIELDS = ("Sid", "Effect", "Principal", "Action", "Resource", "Condition")
_PROHIBITED_FIELDS = ("NotPrincipal", "NotAction", "NotResource")
_SID_RE = re.compile(r"^[A-Za-z0-9]*$")
_CONDITION_OPERATORS = frozenset(op.lower() for op in (
    "StringEquals", "StringNotEquals", "StringEqualsIgnoreCase", "StringNotEqualsIgnoreCase",
    "StringLike", "StringNotLike", "NumericEquals", "NumericNotEquals", "NumericLessThan",
    "NumericLessThanEquals", "NumericGreaterThan", "NumericGreaterThanEquals", "DateEquals",
    "DateNotEquals", "DateLessThan", "DateLessThanEquals", "DateGreaterThan", "DateGreaterThanEquals",
    "Bool", "BinaryEquals", "IpAddress", "NotIpAddress", "ArnEquals", "ArnLike", "ArnNotEquals",
    "ArnNotLike", "Null",
))

EMPTY_PRINCIPALS = ("Invalid policy document: aws.auth.client.error.ARCInvalidArgumentException: "
                    "com.amazon.balsa.error.InvalidPolicyException: "
                    "The passed in policy has a statement with no principals!")


class PolicyError(Exception):
    """A document AWS refuses; ``str(exc)`` is the whole message."""


class _InvalidJson(Exception):
    pass


# ---------------------------------------------------------------------------
# A JSON reader that keeps offsets, so messages can name a line and column.
# ---------------------------------------------------------------------------

class _Node:
    __slots__ = ("kind", "value", "start", "end", "pre")

    def __init__(self, kind, value, start, end):
        self.kind = kind      # "object" | "array" | "string" | "number" | "literal"
        self.value = value    # object: list of (key, _Node); array: list of _Node; scalar: Python value
        self.start = start    # offset of the value's first character
        self.end = end        # offset just past the value
        self.pre = start      # offset just past the token before it (set by the container)

    def field(self, name):
        for key, node in self.value:
            if key == name:
                return node
        return None


_WS = " \t\n\r"
_NUMBER_RE = re.compile(r"-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?")


class _Reader:
    def __init__(self, text):
        self.text = text
        self.i = 0

    def _skip(self):
        while self.i < len(self.text) and self.text[self.i] in _WS:
            self.i += 1

    def document(self):
        node = self.value()
        self._skip()
        if self.i != len(self.text):
            raise _InvalidJson()
        return node

    def value(self):
        self._skip()
        if self.i >= len(self.text):
            raise _InvalidJson()
        c = self.text[self.i]
        if c == "{":
            return self._object()
        if c == "[":
            return self._array()
        if c == '"':
            start = self.i
            return _Node("string", self._string(), start, self.i)
        m = _NUMBER_RE.match(self.text, self.i)
        if m and m.end() > self.i:
            self.i = m.end()
            raw = m.group(0)
            return _Node("number", float(raw) if any(ch in raw for ch in ".eE") else int(raw), m.start(), m.end())
        for word, val in (("true", True), ("false", False), ("null", None)):
            if self.text.startswith(word, self.i):
                start, self.i = self.i, self.i + len(word)
                return _Node("literal", val, start, self.i)
        raise _InvalidJson()

    def _string(self):
        j = self.i + 1
        while j < len(self.text):
            ch = self.text[j]
            if ch == "\\":
                j += 2
                continue
            if ch == '"':
                break
            if ord(ch) < 0x20:
                raise _InvalidJson()
            j += 1
        else:
            raise _InvalidJson()
        try:
            s = json.loads(self.text[self.i:j + 1])
        except ValueError:
            raise _InvalidJson() from None
        self.i = j + 1
        return s

    def _object(self):
        start = self.i
        self.i += 1
        members = []
        self._skip()
        if self.text.startswith("}", self.i):
            self.i += 1
            return _Node("object", members, start, self.i)
        pre = start + 1
        while True:
            self._skip()
            if not self.text.startswith('"', self.i):
                raise _InvalidJson()
            key = self._string()
            self._skip()
            if not self.text.startswith(":", self.i):
                raise _InvalidJson()
            self.i += 1
            node = self.value()
            node.pre = pre
            members.append((key, node))
            pre = node.end
            self._skip()
            if self.text.startswith(",", self.i):
                self.i += 1
                continue
            if self.text.startswith("}", self.i):
                self.i += 1
                return _Node("object", members, start, self.i)
            raise _InvalidJson()

    def _array(self):
        start = self.i
        self.i += 1
        items = []
        self._skip()
        if self.text.startswith("]", self.i):
            self.i += 1
            return _Node("array", items, start, self.i)
        pre = start + 1
        while True:
            node = self.value()
            node.pre = pre
            items.append(node)
            pre = node.end
            self._skip()
            if self.text.startswith(",", self.i):
                self.i += 1
                continue
            if self.text.startswith("]", self.i):
                self.i += 1
                return _Node("array", items, start, self.i)
            raise _InvalidJson()


def _line_col(text, offset):
    """1-based line and column of *offset*; CRLF, CR and LF each end a line."""
    line, line_start, i = 1, 0, 0
    while i < offset:
        ch = text[i]
        if ch == "\r" or ch == "\n":
            if ch == "\r" and i + 1 < len(text) and text[i + 1] == "\n":
                i += 1
            line, line_start = line + 1, i + 1
        i += 1
    return line, offset - line_start + 1


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _syntax_error(text, offset):
    line, col = _line_col(text, offset)
    return PolicyError(f"Policy validation error: Syntax error at position ({line},{col})")


def _read_end(node):
    """Where the parser stands after reading *node* as a value it expected."""
    return node.start + 1 if node.kind in ("object", "array") else node.end


def _strings(text, node):
    """A string or list of strings, as a list."""
    if node.kind == "string":
        return [node.value]
    if node.kind == "array":
        for item in node.value:
            if item.kind != "string":
                raise _syntax_error(text, _read_end(item))
        return [item.value for item in node.value]
    raise _syntax_error(text, _read_end(node))


def _statement_syntax(text, stmt):
    """Field presence and types, in the order AWS checks them."""
    if stmt.kind != "object":
        raise PolicyError("Policy validation error: Could not parse the policy: Statement is not well formatted.")
    for key, node in stmt.value:
        if key in _PROHIBITED_FIELDS:
            raise PolicyError(f"Policy validation error: Has prohibited field {key}")
        if key not in _STATEMENT_FIELDS:
            # The parser stops on the unknown field's value, just past its first character.
            raise _syntax_error(text, node.start + 1)
    sid = stmt.field("Sid")
    if sid is not None and sid.kind != "string":
        raise _syntax_error(text, _read_end(sid))
    for name in ("Effect", "Principal", "Action", "Resource"):
        if stmt.field(name) is None:
            raise PolicyError(f"Policy validation error: Missing required field {name}")
    effect = stmt.field("Effect")
    if effect.kind != "string":
        raise _syntax_error(text, _read_end(effect))
    if effect.value not in ("Allow", "Deny"):
        raise PolicyError(f"Policy validation error: Invalid effect: {effect.value}")
    principal = stmt.field("Principal")
    if principal.kind == "string":
        if principal.value != "*":
            raise _syntax_error(text, principal.end)
    elif principal.kind == "object":
        for _key, value in principal.value:
            _strings(text, value)
    else:
        raise _syntax_error(text, _read_end(principal))
    actions = _strings(text, stmt.field("Action"))
    if not actions:
        raise PolicyError("Policy validation error: Missing required field Action cannot be empty!")
    for action in actions:
        if action != "*" and ":" not in action:
            raise PolicyError("Policy validation error: Could not parse vendor in action")
    _strings(text, stmt.field("Resource"))
    condition = stmt.field("Condition")
    if condition is not None:
        if condition.kind != "object":
            raise _syntax_error(text, _read_end(condition))
        for operator, keys in condition.value:
            base = operator.split(":", 1)[-1]
            base = base[:-len("IfExists")] if base.endswith("IfExists") else base
            if base.lower() not in _CONDITION_OPERATORS:
                raise PolicyError(f"Policy validation error: Invalid Condition type : {operator}")
            if keys.kind != "object":
                raise _syntax_error(text, _read_end(keys))


def _statement_rules(text, stmt, resource_arn, allowed_actions):
    """The per-statement rules; the first broken one is reported at the statement."""
    line, col = _line_col(text, stmt.pre)
    where = f"Policy validation error: Resource policy contains invalid statement at line {line}, column {col}: "
    sid = stmt.field("Sid")
    if sid is not None and not _SID_RE.match(sid.value):
        raise PolicyError(where + f"The following Statement Ids are invalid: {sid.value}, ")

    actions = _strings(text, stmt.field("Action"))
    if any(a != "*" and a.split(":", 1)[0].lower() != "kinesis" for a in actions):
        raise PolicyError(where + "Action field includes AWS services that are inconsistent with specified vendor")
    wildcards = [a for a in actions if "*" in a or "?" in a]
    if wildcards:
        raise PolicyError(where + f"The resource policy cannot contain invalid actions: [{', '.join(wildcards)}].")
    invalid = [a for a in actions if a.lower() not in allowed_actions]
    if invalid:
        names = ", ".join(f'"{a}"' for a in invalid)
        raise PolicyError(where + f"The following action names are invalid: {names}")

    principal = stmt.field("Principal")
    if principal.kind == "string":
        raise PolicyError(where + "The resource policy cannot contain the wildcard principal.")
    for kind, value in principal.value:
        if kind == "AWS":
            if "*" in _strings(text, value):
                raise PolicyError(where + "The resource policy cannot contain the wildcard principal.")
        else:
            raise PolicyError(where + f"The resource policy cannot contain {kind} principals.")

    resources = _strings(text, stmt.field("Resource"))
    if len(resources) != 1:
        raise PolicyError(where + "The resource policy must contain exactly one resource.")
    if resources[0] != resource_arn:
        raise PolicyError(where + "The resource policy's resource must be the same as the resource ARN.")


def validate(policy, resource_arn, is_consumer):
    """Check *policy* for *resource_arn*; returns the parsed document.

    Raises PolicyError with AWS's message. Principal existence is left to the
    caller, which knows the accounts and identities.
    """
    if not isinstance(policy, str):
        policy = ""
    size = len(policy.encode("utf-8"))
    if size == 0 or size > MAX_POLICY_BYTES:
        raise PolicyError("Policy validation error: Resource policy size should be greater than 0 "
                          "and less than 20000 bytes")
    try:
        root = _Reader(policy).document()
    except _InvalidJson:
        raise PolicyError("Policy validation error: This policy contains invalid Json") from None
    if root.kind != "object":
        raise PolicyError("Policy validation error: This policy contains invalid Json")

    version = root.field("Version")
    if version is None:
        raise PolicyError("Policy validation error: Missing required field Version")
    if version.kind != "string" or version.value not in _VERSIONS:
        raise PolicyError("Policy validation error: The policy must contain a valid version string")
    for key, _node in root.value:
        if key not in _TOP_FIELDS:
            raise PolicyError(f"Policy validation error: Unknown field {key}")
    statement = root.field("Statement")
    if statement is None:
        raise PolicyError("Policy validation error: Missing required field Statement")
    if statement.kind == "object":
        statements = [statement]
        statement.pre = statement.start + 1
    elif statement.kind == "array":
        statements = statement.value
        if not statements:
            raise PolicyError("Policy validation error: Could not parse the policy: Statement is empty!")
    else:
        raise PolicyError("Policy validation error: Could not parse the policy: Statement is not well formatted.")

    for stmt in statements:
        _statement_syntax(policy, stmt)
    allowed = CONSUMER_ACTIONS if is_consumer else STREAM_ACTIONS
    for stmt in statements:
        _statement_rules(policy, stmt, resource_arn, allowed)
    seen = set()
    for stmt in statements:
        sid = stmt.field("Sid")
        if sid is None or not sid.value:
            continue
        if sid.value in seen:
            line, col = _line_col(policy, stmt.pre)
            raise PolicyError("Policy validation error: Resource policy contains invalid statement at "
                              f"line {line}, column {col}: The Statement Ids in the policy are not unique")
        seen.add(sid.value)
    return root, statements


def aws_principals(statements):
    """Every AWS principal string the statements name, in order."""
    out = []
    for stmt in statements:
        for kind, value in stmt.field("Principal").value:
            if kind == "AWS":
                out.extend([value.value] if value.kind == "string" else [v.value for v in value.value])
    return out


def statement_has_no_principals(statements):
    for stmt in statements:
        principal = stmt.field("Principal")
        if principal.kind == "object" and (
                not principal.value or any(v.kind == "array" and not v.value for _k, v in principal.value)):
            return True
    return False


# ---------------------------------------------------------------------------
# Stored form
# ---------------------------------------------------------------------------

def _one_or_list(values):
    return values[0] if len(values) == 1 else values


def _principal_arn(value):
    return f"arn:aws:iam::{value}:root" if re.fullmatch(r"\d{12}", value) else value


def _condition_value(node):
    def scalar(n):
        if n.kind == "literal" and isinstance(n.value, bool):
            return "true" if n.value else "false"
        if n.kind == "number":
            return str(n.value)
        return n.value
    if node.kind == "array":
        return _one_or_list([scalar(n) for n in node.value])
    return scalar(node)


def normalize(root, statements):
    """The compact document GetResourcePolicy returns."""
    out_statements = []
    for stmt in statements:
        out = {}
        sid = stmt.field("Sid")
        if sid is not None:
            out["Sid"] = sid.value
        out["Effect"] = stmt.field("Effect").value
        principal = {}
        for kind, value in stmt.field("Principal").value:
            names = [value.value] if value.kind == "string" else [v.value for v in value.value]
            # Sorted as written, then a bare account id becomes its root ARN and repeats drop out.
            principal[kind] = _one_or_list(list(dict.fromkeys(_principal_arn(n) for n in sorted(set(names)))))
        out["Principal"] = principal
        action = stmt.field("Action")
        out["Action"] = _one_or_list([action.value] if action.kind == "string" else [a.value for a in action.value])
        resource = stmt.field("Resource")
        out["Resource"] = resource.value if resource.kind == "string" else resource.value[0].value
        condition = stmt.field("Condition")
        if condition is not None:
            out["Condition"] = {op: {k: _condition_value(v) for k, v in keys.value} for op, keys in condition.value}
        out_statements.append(out)
    doc = {"Version": root.field("Version").value}
    if root.field("Id") is not None:
        doc["Id"] = root.field("Id").value
    doc["Statement"] = out_statements
    return json.dumps(doc, separators=(",", ":"), ensure_ascii=False)
