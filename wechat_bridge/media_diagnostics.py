"""Optional metadata only; invalid diagnostics never invalidate useful input."""
import json

MAX_ITEMS = 16
MAX_SCAN = 32
MAX_BYTES = 4096
MAX_DOWNLOADED_BYTES = (100 << 20) + 33
SOURCES = frozenset(("param", "full_url", "unknown"))
REASONS = frozenset((
    "ok", "request_or_http_error", "http_body_read_failed", "ciphertext_too_large",
    "empty_body", "ciphertext_not_block_aligned", "decrypt_no_candidate",
    "decrypt_ambiguous", "plaintext_too_large", "image_invalid",
    "image_resource_limit", "image_validation_timeout", "write_failed", "unknown",
))


def sanitize_media_diagnostics(value):
    if (type(value) is not dict or type(value.get("version")) is not int
            or value["version"] != 1 or type(value.get("items")) is not list
            or type(value.get("truncated")) is not bool):
        return None
    result = {"version": 1, "items": [],
              "truncated": value["truncated"] or len(value["items"]) > MAX_SCAN}
    seen = {}
    for item in value["items"][:MAX_SCAN]:
        if type(item) is not dict:
            result["truncated"] = True
            continue
        index, source = item.get("item_index"), item.get("source")
        outcome, reason = item.get("outcome"), item.get("reason")
        size = item.get("downloaded_bytes")
        valid = (type(index) is int and 0 <= index <= 999
                 and type(source) is str and len(source) <= 8 and source in SOURCES
                 and type(outcome) is str and len(outcome) <= 7 and outcome in ("success", "failed")
                 and type(reason) is str and len(reason) <= 40 and reason in REASONS
                 and ((outcome == "success") == (reason == "ok"))
                 and "downloaded_bytes" in item
                 and (size is None or type(size) is int and 0 <= size <= MAX_DOWNLOADED_BYTES))
        if (not valid or len(result["items"]) >= MAX_ITEMS
                or source in seen.get(index, ()) or len(seen.get(index, ())) >= 2):
            result["truncated"] = True
            continue
        clean = {"item_index": index, "source": source, "outcome": outcome,
                 "downloaded_bytes": size, "reason": reason}
        result["items"].append(clean)
        if len(json.dumps(result, separators=(",", ":")).encode("utf-8")) > MAX_BYTES:
            result["items"].pop()
            result["truncated"] = True
            continue
        seen.setdefault(index, set()).add(source)
    return result
