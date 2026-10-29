# Backend contract v1

任务 WBR-01-20261024-001。2026-10-24。01 owns this contract, registry and scheduler. Human authorization in 00 includes WeChat switching and one-time permission decisions. This concrete interface supersedes the candidate for new backends; legacy Muse remains on WorkerState/Hook. No live deployment is implied.

## Python adapter interface (frozen v1)

03 owns `wechat_bridge/backends/codex.py`; 04 owns `wechat_bridge/backends/claude_code.py`; 01 owns `__init__.py`, `contract.py`, `registry.py`, `backend_state.py`, `dispatcher.py` and CLI/transport changes. Python >=3.8 remains supported.

Each platform module exports `create_backend(config: dict)` returning a single-execution driver. No imports from scheduler internals required. Config is the selected backend configuration below. Methods:

```python
backend.start(request: dict) -> None
backend.poll() -> list  # nonblocking drain, <=100 events; return within 1s
backend.respond_approval(approval_id: str, decision: str, action_digest: str) -> None
backend.cancel() -> None  # request only; confirmation arrives as event
backend.close() -> None  # <=30s cleanup, no successful-cancel inference
```

Start is bounded process setup (<=5s), not the generation loop. Methods raise on protocol/transport errors; public core treats uncertain side effects as unknown and never retries automatically. Driver owns subprocess pipes/limits and deadlines; core owns durable job, route, approvals and output. Each object handles exactly one execution/attempt. `cancel()` must remove pending provider input as well as interrupt active generation. `close()` must finish within 30 seconds and must not leave descendants knowingly running. The service keeps the scheduler thread non-daemon and waits 35s before reporting overrun; an overrun retains the process awaiting cleanup instead of silently abandoning children. Provider adapter cannot send WeChat, access context tokens, mutate public DB/outbox, or select another account.

Request fields:

```json
{
  "schema_version": 1,
  "execution_id": "job-opaque",
  "attempt_id": "attempt-opaque",
  "account_scope": "account-opaque",
  "sender_key": "owner-opaque",
  "backend_id": "codex",
  "backend_instance_ref": "local-codex-a",
  "conversation_key": "conversation-opaque",
  "session_id": null,
  "messages": [{"message_id":"123", "text":"synthetic task", "attachments":[]}],
  "cwd": "/absolute/isolated/workspace",
  "deadline_at": 1900001800,
  "execution_policy": {"approval":"explicit", "sandbox":"workspace-write"}
}
```

Attachments: `{path, status:"available"}` for real local regular files; legacy media failure is represented by `{status:"failed",count:N}`. No remote URL fetch or fake MIME/hash claims. Account scope derives from configured bot identity and authenticated owner; backend instance is required operator config, not inferred from model text. Session is explicit provider session ID returned by the prior successful attempt in this exact account/sender/backend/instance conversation; never `--last`/global continue. Inputs are frozen per start; later ordinary messages queue another execution. No `update_input` in v1.

Limits: event/provider approval IDs <=256 characters, provider session <=1024, event text/final <=64000, <=100 drained events per poll and <=10000 unique events per execution. Only one outstanding approval per execution is supported; providers must serialize requests or report unsupported concurrency.

Every event MUST include `event_id` (stable unique within attempt), `execution_id`, `attempt_id`, and `type`. Core ignores duplicate event IDs, rejects wrong identities and terminal late arrivals. Types:

- `started` / `progress`: optional `session_id`, bounded `text`; progress is diagnostic, not a lease renewal.
- `approval_requested`: `approval_id` (provider correlation), `action_summary` (<=2000 characters), `action_digest` (64 lowercase hex SHA256 of canonical actual action), optional `expires_at` (epoch seconds; core caps at 300s and job deadline). Adapter must bind its approval ID to the exact current action/digest and reject mismatches or duplicate answers. No secrets in summary; if safe complete summary is impossible, reject rather than approve an unspecified action.
- `final`: nonempty `text`, `session_id` if resumable; emitted ONLY after provider confirms successful final (not merely exit0/stream end). Core admits one final per execution. No streaming text treated as final.
- `failed`: bounded `error_code`, `side_effect_state` (`none|possible|confirmed|unknown`). Failure is not retried.
- `cancelled`: positive provider/process termination confirmation; side effects are not undone.
- `unknown`: disconnect/ambiguous termination/unsupported interaction; never rerun implicitly.

Provider user-question interactions unsupported by a particular adapter must produce a clear `failed`/`unknown` event, never be converted to permission grants. Approval reply method receives the ORIGINAL provider ID, `allow` or `deny`, and the exact stored digest. Only explicit one-time allow is sent; no persistent-policy grants. A decision delivery exception is unknown, never retried. `respond_approval()` returning is not final completion.

## Configuration / registry

Opt-in file `state/backends.json`, schema_version=1:

```json
{
  "schema_version":1,
  "account_scope":"separate-test-bot",
  "default_backend":"codex",
  "legacy_state_dir":null,
  "job_timeout_seconds":1800,
  "approval_timeout_seconds":300,
  "backends":{
    "codex":{"instance_id":"local-codex-a", "command":["/absolute/path/codex"], "cwd":"/absolute/isolated/workspace", "execution_policy":{"approval":"explicit","sandbox":"workspace-write"}},
    "claude_code":{"instance_id":"collaborator-cc-a", "command":["/absolute/path/claude"], "cwd":"/absolute/isolated/workspace", "execution_policy":{"approval":"explicit"}}
  }
}
```

`command` is argv, never shell text. Adapter may use provider-specific extra fields (document in own line). CLI preflight validates schema, bounded timeouts, cwd/executable, adapter factory and authenticated WeChat identity; provider auth status is adapter-specific, no token export. Default invocation without this file preserves legacy behavior. `muse` may be enabled only with explicit `legacy_state_dir` matching Hook state; no adapter factory required for legacy.

01 will register factory imports lazily; missing adapter is a visible preflight failure. 03/04 do not edit public registry. 00 integrates fixed commits; worktrees do not auto-sync. Contract is available before shared implementation so platform drivers can develop against dicts and test fakes.

## Commands and durable state

Exact commands (no authorization from ordinary text):

- `/backend` lists current/available backends; `/backend codex`, `/backend claude_code`, `/backend muse` switches only when idle.
- `/approve <public-approval-id> allow` or `deny`; binds logged-in account, sender, backend instance, execution, attempt, provider ID, action digest and expiry. Public ID is unguessable. Ordinary `好` is task text.
- `/cancel <execution-id>` requests cancellation; acknowledgement is separate from actual confirmation.
- `/status` reports current job/state. Malformed reserved commands are consumed with help, not forwarded as model authorization.

New managed incoming directory is isolated from legacy `inbox/`, so Muse Hook cannot claim new-backend/control input. Public scheduler serializes per sender and freezes backend/instance/route for every execution. Busy includes queued work, waiting approval, unresolved unknown, and pending delivery. Legacy switching also checks Hook claims and legacy inbox/outbox. Config changes with existing managed state are rejected; use a fresh independent bot/state for changed identity/profile.

`state/backend_jobs.sqlite3` owns jobs, inputs, approvals, routes, session maps, and publication intent. SQLite transactions and single scheduler lock are short operation ownership; no worker lease is held during generation/approval waiting. Attempt fence is durable, distinct from ownership. Existing Muse 90/570 limits are unchanged; no fake refresh. Provider execution deadline is independent and bounded; waiting approval holds the durable job/sender order, not a short worker lease.

State: queued → running → waiting_approval → running → succeeded/failed/cancelled/unknown. Cancel request waits for positive confirmation. Restart: prior running/waiting/cancelling become unknown, approvals invalidated, no auto-allow/replay/resume. Queued jobs that never reached dispatch can start. Unknown blocks switching/new dispatch for sender until explicit local operator reconciliation; user-facing status reports this boundary.

Outputs use stable `client_key`, distinct public IDs and `backend_v1` metadata. They are never eligible for legacy automatic merge. Core stores an output intent before pending publication; publishing crash is uncertain and not silently regenerated. Existing sender continues normal chunks/retry accounting. Admission, API acceptance and phone receipt remain separate facts. Completed input receipts survive inbox deletion/restart. No state/cursor/receipt rollback.

## Validation and recovery

Required offline cases: account/instance/session isolation, busy switch, frozen batch, duplicate inputs/events/finals, wrong attempt, approval digest/identity/expiry/restart/replay, cancel confirmation/late final, lost stream, publish crash, legacy nonmerge, legacy regression, missing binary/config/auth diagnostics. Runtime and real-phone tests remain separately labeled.

No irreversible data migration: opt-in adds independent managed state; old inbox/queues/ledgers stay intact. To disable, stop scheduler and transport, preserve DB and managed inbox; resolve active/unknown jobs and drain outputs first. Never move managed input into legacy or delete receipts to replay it. Revert code alone does not migrate pending jobs.
