#!/usr/bin/env bash

BASE_URL="${1:-${BASE_URL:-http://localhost:8090}}"
SMOKE_EMAIL="${SMOKE_EMAIL:-tech@test.com}"
SMOKE_PASSWORD="${SMOKE_PASSWORD:-123456}"
SMOKE_RECEIVER_EMAIL="${SMOKE_RECEIVER_EMAIL:-tech@test.com}"
SMOKE_SENDER_EMAIL="${SMOKE_SENDER_EMAIL:-photo@test.com}"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { printf "${GREEN}✅ PASS${NC} %s\n" "$*"; }
fail() { printf "${RED}❌ FAIL${NC} %s\n" "$*" >&2; return 1; }
info() { printf "${YELLOW}ℹ️  ${NC} %s\n" "$*"; }

request() {
  curl --noproxy '*' --fail-with-body --silent --show-error \
    --connect-timeout 3 --max-time 20 "$@"
}

json_get() {
  local expression="$1"
  python3 -c '
import json, sys
data = json.load(sys.stdin)
try:
    value = eval(
        sys.argv[1],
        {"__builtins__": {}, "any": any, "str": str},
        {"data": data},
    )
    print("" if value is None else value)
except (KeyError, IndexError, TypeError, AttributeError):
    pass
' "$expression"
}

login() {
  local email="$1"
  local password="${2:-$SMOKE_PASSWORD}"
  local response
  response=$(request -X POST "$BASE_URL/api/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"email\":\"$email\",\"password\":\"$password\"}") || return 1
  printf '%s' "$response" | json_get 'data["data"]["tokens"]["accessToken"]'
}

require_token() {
  local email="$1"
  local token
  token=$(login "$email") || true
  if [ -z "$token" ]; then
    fail "账号 $email 登录失败。可先执行：curl -X POST $BASE_URL/api/dev/seed"
    return 1
  fi
  printf '%s' "$token"
}

wait_for_json_value() {
  local attempts="$1"
  local expected="$2"
  local command="$3"
  local value=""
  local i
  for ((i=1; i<=attempts; i++)); do
    value=$(eval "$command") || true
    [ "$value" = "$expected" ] && { printf '%s' "$value"; return 0; }
    sleep 1
  done
  printf '%s' "$value"
  return 1
}
