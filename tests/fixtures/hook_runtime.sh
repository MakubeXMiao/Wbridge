#!/usr/bin/env bash
# Offline protocol fixture. Not a Muse runtime implementation or platform proof.
_emit() {
  "${WECHAT_BRIDGE_PYTHON:-python3}" -c 'import json,sys; print(json.dumps({"decision":sys.argv[1],"reason":sys.argv[2],"payload":json.loads(sys.argv[3])},ensure_ascii=False))' "$1" "$2" "$3"
}
silent() { _emit silent "$1" "$2"; }
log() { :; }
wake() {
  _emit wake "$1" "$2"
  [ "${FAKE_WAKE_FAIL:-0}" != 1 ]
}
