#!/usr/bin/env bash

# This matrix must be run outside sandboxed execution environments.
# The real Boss/LisM path depends on live provider DNS/network access, and
# sandboxed runs can fail with transport-level DNS resolution errors that do
# not reproduce in a normal shell.
#
# NOTE: This matrix execution can take tens of minutes or hours to complete.
# It is recommended to use low-frequency polling to monitor progress.
#
# Audit discipline:
# - Do not wait for the full matrix to finish before inspecting results.
# - As soon as one mode finishes its runs, immediately audit that mode's
#   samples/logs/summary slice.
# - If that mode already shows aborted/failed behavior or obviously unhealthy
#   state transitions, stop treating the remaining matrix time as meaningful
#   signal and investigate the failure path immediately.
# - In other words: mode-level completion is an audit checkpoint; do not
#   "quietly wait for the rest" when the finished mode is already broken.


set -euo pipefail

SCRIPT_DIR="$(
  cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1
  pwd
)"
AGENT_DIR="$(
  cd "$SCRIPT_DIR/.." >/dev/null 2>&1
  pwd
)"
REPO_ROOT="$(
  cd "$AGENT_DIR/../.." >/dev/null 2>&1
  pwd
)"
ENV_LOADER="$REPO_ROOT/load-env.sh"
ENV_FILE="$REPO_ROOT/.env"
PREPARE_SCRIPT="$AGENT_DIR/scripts/boss_ab_matrix.sh"
BIN_PATH="$AGENT_DIR/target/debug/morgo"

DEFAULT_OUT_DIR="${TMPDIR:-/tmp}/rustagent-boss-lism-matrix-$(date +%Y%m%d-%H%M%S)"
DEFAULT_MODEL="${RUST_AGENT_AB_MODEL:-gpt-5-mini-2025-08-07}"
DEFAULT_TIMEOUT_SECS="${RUST_AGENT_BOSS_TASK_TIMEOUT_SECS:-600}"
DEFAULT_PROGRAM_TIMEOUT_SECS="${RUST_AGENT_PROGRAM_TIMEOUT_SECS:-1100}"
MAX_SCRIPT_RUNTIME_SECS=1200
DEFAULT_PLAN="3x3"
DEFAULT_SINGLE_MODE="all_on"
SCRIPT_START_SECONDS="$SECONDS"

usage() {
  cat <<EOF
Usage:
  bash RustAgent/Agent/tests/run_boss_lism_matrix.sh [options]

Required env bootstrap:
  source $ENV_LOADER $ENV_FILE

Execution requirement:
  run this script in a normal host shell, not inside a sandboxed executor
  sandboxed runs can fail with provider DNS/transport errors before the
  matrix reaches real tool dispatch

Examples:
  bash RustAgent/Agent/tests/run_boss_lism_matrix.sh --cases u6-u9 --plan 3x3
  bash RustAgent/Agent/tests/run_boss_lism_matrix.sh --cases u7-u9 --plan 2x2
  bash RustAgent/Agent/tests/run_boss_lism_matrix.sh --cases u8 --plan 3plus3
  bash RustAgent/Agent/tests/run_boss_lism_matrix.sh --cases u7,u9 --plan single --mode boss_on_only
  bash RustAgent/Agent/tests/run_boss_lism_matrix.sh --cases all --plan 3x3 --out /tmp/rustagent-full
  bash RustAgent/Agent/tests/run_boss_lism_matrix.sh --summary-only /tmp/rustagent-full

Options:
  --cases VALUE         all | u6 | u6,u7,u9 | u6-u9. Default: all
  --plan VALUE          3x3 | 3plus3 | 2x2 | 2plus2 | single. Default: $DEFAULT_PLAN
  --mode VALUE          all_off | boss_on_only | all_on. Only for --plan single.
  --shared-memory-enabled
                        Enable shared step memory in the launched runtime.
  --st                  Enable /st mode in the launched runtime.
  --runs N              Runs per mode. Default: 3. For single, default 1.
  --out DIR             Output root. Default: $DEFAULT_OUT_DIR
  --model MODEL         Override model written into generated config.
  --timeout SECONDS     --boss-task-timeout-secs. Default: $DEFAULT_TIMEOUT_SECS
  --program-timeout SECONDS
                        Max wall time for each morgo invocation. Default: $DEFAULT_PROGRAM_TIMEOUT_SECS
                        This is capped by the remaining script-wide $MAX_SCRIPT_RUNTIME_SECS second budget.
  --prepare-only        Only generate usecases/configs, do not run.
  --summary-only DIR    Skip execution, summarize an existing output directory.
  --list-cases          Print supported use cases and exit.
  --check-semantics     Verify mode mapping semantics and exit.
  --help                Show this help.

Plan semantics:
  3x3:
    all_off      = boss force-off  + worker force-off
    boss_on_only = boss force-on   + worker force-off
    all_on       = boss force-on   + worker force-on
  3plus3:
    all_off x 3 + all_on x 3
  2x2:
    boss_on_only x 2 + all_on x 2
  2plus2:
    all_off x 2 + all_on x 2
  single:
    one mode only, controlled by --mode

Notes:
  - Run this matrix outside sandboxed execution; provider DNS/network access must be real.
  - The whole script has a hard runtime budget of $MAX_SCRIPT_RUNTIME_SECS seconds.
  - This script automatically generates isolated cache keys per output root + use case + mode.
  - For u6-u10 build tasks, it also rewrites target paths per mode/run to avoid artifact reuse.
  - Final summary deduplicates records by run_id because aborted samples may be appended twice.
  - Operational rule: once any mode finishes, audit that mode immediately before
    deciding whether waiting for the rest of the matrix is still worthwhile.
EOF
}

all_usecases() {
  cat <<'EOF'
u1_security_beta_runtime
u2_memory_backpressure_contract
u3_token_efficiency_rollout
u4_boss_workflow_and_lism
u5_gap_audit_and_roadmap
u6_frontend_agent_site
u7_python_boss_lism_demo
u8_multistage_tools_memory_token_report
u9_lism_jsonl_analyzer_tool
u10_state_decision_runtime_validator
EOF
}

print_cases() {
  nl -ba <(all_usecases) | sed 's/^[[:space:]]*//'
}

die() {
  echo "error: $*" >&2
  exit 1
}

remaining_runtime_secs() {
  local elapsed=$((SECONDS - SCRIPT_START_SECONDS))
  local remaining=$((MAX_SCRIPT_RUNTIME_SECS - elapsed))
  if [ "$remaining" -lt 0 ]; then
    remaining=0
  fi
  printf '%s\n' "$remaining"
}

assert_runtime_budget() {
  local remaining
  remaining="$(remaining_runtime_secs)"
  if [ "$remaining" -le 0 ]; then
    echo "script runtime timeout after ${MAX_SCRIPT_RUNTIME_SECS}s" >&2
    exit 124
  fi
}

start_script_watchdog() {
  (
    sleep "$MAX_SCRIPT_RUNTIME_SECS"
    echo "script runtime timeout after ${MAX_SCRIPT_RUNTIME_SECS}s; terminating pid $$" >&2
    pkill -TERM -P "$$" 2>/dev/null || true
    kill -TERM "$$" 2>/dev/null || true
    sleep 2
    pkill -KILL -P "$$" 2>/dev/null || true
    kill -KILL "$$" 2>/dev/null || true
  ) &
  SCRIPT_WATCHDOG_PID="$!"
  trap 'kill "$SCRIPT_WATCHDOG_PID" 2>/dev/null || true' EXIT
}

cap_program_timeout() {
  local requested="$1"
  local remaining
  remaining="$(remaining_runtime_secs)"

  if [ "$remaining" -le 0 ]; then
    echo "script runtime timeout after ${MAX_SCRIPT_RUNTIME_SECS}s" >&2
    exit 124
  fi
  if [ -z "$requested" ] || [ "$requested" -le 0 ]; then
    printf '%s\n' "$remaining"
    return
  fi
  if [ "$requested" -gt "$remaining" ]; then
    printf '%s\n' "$remaining"
    return
  fi
  printf '%s\n' "$requested"
}

ensure_binary() {
  assert_runtime_budget
  if [ ! -x "$BIN_PATH" ]; then
    echo "binary missing at $BIN_PATH; building morgo" >&2
    run_with_program_timeout "$(remaining_runtime_secs)" cargo build --manifest-path "$AGENT_DIR/Cargo.toml" --bin morgo
    return
  fi
  if [ -n "$(find "$AGENT_DIR/src" "$AGENT_DIR/Cargo.toml" -newer "$BIN_PATH" -print -quit)" ]; then
    echo "binary at $BIN_PATH is older than source; rebuilding morgo" >&2
    run_with_program_timeout "$(remaining_runtime_secs)" cargo build --manifest-path "$AGENT_DIR/Cargo.toml" --bin morgo
  fi
}

expand_cases() {
  local spec="$1"
  if [ "$spec" = "all" ]; then
    all_usecases
    return 0
  fi

  python3 - "$spec" <<'PY'
import sys

spec = sys.argv[1]
all_cases = [
    "u1_security_beta_runtime",
    "u2_memory_backpressure_contract",
    "u3_token_efficiency_rollout",
    "u4_boss_workflow_and_lism",
    "u5_gap_audit_and_roadmap",
    "u6_frontend_agent_site",
    "u7_python_boss_lism_demo",
    "u8_multistage_tools_memory_token_report",
    "u9_lism_jsonl_analyzer_tool",
    "u10_state_decision_runtime_validator",
]
by_prefix = {case.split("_", 1)[0]: case for case in all_cases}
selected = []

def add_prefix(prefix: str):
    if prefix not in by_prefix:
        raise SystemExit(f"unknown case prefix: {prefix}")
    case = by_prefix[prefix]
    if case not in selected:
        selected.append(case)

for chunk in [part.strip() for part in spec.split(",") if part.strip()]:
    if "-" in chunk and chunk.startswith("u"):
        left, right = chunk.split("-", 1)
        if left not in by_prefix or right not in by_prefix:
            raise SystemExit(f"unknown range: {chunk}")
        start = int(left[1:])
        end = int(right[1:])
        if start > end:
            raise SystemExit(f"descending range is not supported: {chunk}")
        for value in range(start, end + 1):
            add_prefix(f"u{value}")
        continue
    if chunk.startswith("u") and "_" not in chunk:
        add_prefix(chunk)
        continue
    if chunk in all_cases:
        if chunk not in selected:
            selected.append(chunk)
        continue
    raise SystemExit(f"unknown case selector: {chunk}")

for case in selected:
    print(case)
PY
}

read_lines_into_array() {
  local __resultvar="$1"
  local __input
  __input="$(cat)"
  eval "$__resultvar=()"
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    eval "$__resultvar+=(\"\$line\")"
  done <<EOF
$__input
EOF
}

prepare_dirs() {
  local out_dir="$1"
  mkdir -p \
    "$out_dir/tasks" \
    "$out_dir/configs" \
    "$out_dir/reports" \
    "$out_dir/reports/memory"
}

mode_sequence_for_plan() {
  local plan="$1"
  local single_mode="$2"
  case "$plan" in
    3x3)
      printf '%s\n' all_off boss_on_only all_on
      ;;
    3plus3)
      printf '%s\n' all_off all_on
      ;;
    2x2)
      printf '%s\n' boss_on_only all_on
      ;;
    2plus2)
      printf '%s\n' all_off all_on
      ;;
    single)
      printf '%s\n' "$single_mode"
      ;;
    *)
      die "unsupported plan: $plan"
      ;;
  esac
}

source_mode_for_label() {
  local mode="$1"
  case "$mode" in
    all_off)
      # all_off is the production baseline; only the LisM policies change.
      printf 'on\n'
      ;;
    boss_on_only|all_on)
      printf 'on\n'
      ;;
    *)
      die "unsupported mode label: $mode"
      ;;
  esac
}

check_mode_semantics() {
  [ "$(source_mode_for_label all_off)" = "on" ] || die "all_off must use the production-like config family"
  [ "$(boss_policy_for_label all_off)" = "force-off" ] || die "all_off must force boss LisM off"
  [ "$(worker_policy_for_label all_off)" = "force-off" ] || die "all_off must force worker LisM off"
  [ "$(source_mode_for_label boss_on_only)" = "on" ] || die "boss_on_only must use the production-like config family"
  [ "$(source_mode_for_label all_on)" = "on" ] || die "all_on must use the production-like config family"
  echo "mode semantics check passed"
}

boss_policy_for_label() {
  local mode="$1"
  case "$mode" in
    all_off)
      printf 'force-off\n'
      ;;
    boss_on_only|all_on)
      printf 'force-on\n'
      ;;
    *)
      die "unsupported mode label: $mode"
      ;;
  esac
}

worker_policy_for_label() {
  local mode="$1"
  case "$mode" in
    all_off|boss_on_only)
      printf 'force-off\n'
      ;;
    all_on)
      printf 'force-on\n'
      ;;
    *)
      die "unsupported mode label: $mode"
      ;;
  esac
}

make_mode_config() {
  local out_dir="$1"
  local usecase="$2"
  local mode="$3"
  local run_tag="$4"
  local src_mode="$5"
  local src="$out_dir/config/${usecase}-${src_mode}/.claude"
  local dst="$out_dir/configs/${usecase}-${mode}/.claude"
  local cache_key="boss-lism-${run_tag}-${usecase}-${mode}"

  mkdir -p "$dst"
  cp "$src/workspace-capability.json" "$dst/workspace-capability.json"
  python3 - "$src/models.toml" "$dst/models.toml" "$cache_key" <<'PY'
import pathlib
import re
import sys

src, dst, cache_key = sys.argv[1:]
text = pathlib.Path(src).read_text(encoding="utf-8")
text = re.sub(
    r'prompt_cache_key = "[^"]*"',
    f'prompt_cache_key = "{cache_key}"',
    text,
)
pathlib.Path(dst).write_text(text, encoding="utf-8")
PY
}

rewrite_task_for_run() {
  local out_dir="$1"
  local usecase="$2"
  local mode="$3"
  local run_id="$4"
  local src="$out_dir/usecases/${usecase}.txt"
  local dst="$out_dir/tasks/${usecase}-${mode}-run${run_id}.txt"
  local base=""
  local target=""

  case "$usecase" in
    u6_frontend_agent_site)
      base="$out_dir/morgo/agent-site"
      target="$out_dir/morgo/agent-site-${mode}-run${run_id}"
      ;;
    u7_python_boss_lism_demo)
      base="$out_dir/morgo/python-boss-lism-demo"
      target="$out_dir/morgo/python-boss-lism-demo-${mode}-run${run_id}"
      ;;
    u8_multistage_tools_memory_token_report)
      base="$out_dir/morgo/reports/multistage-tools-memory-token-report.md"
      target="$out_dir/morgo/reports/multistage-tools-memory-token-report-${mode}-run${run_id}.md"
      ;;
    u9_lism_jsonl_analyzer_tool)
      base="$out_dir/morgo/lism-jsonl-analyzer"
      target="$out_dir/morgo/lism-jsonl-analyzer-${mode}-run${run_id}"
      ;;
    u10_state_decision_runtime_validator)
      base="$out_dir/morgo/state-decision-validator"
      target="$out_dir/morgo/state-decision-validator-${mode}-run${run_id}"
      ;;
  esac

  if [ -n "$base" ]; then
    sed "s#${base}#${target}#g" "$src" >"$dst"
  else
    cp "$src" "$dst"
  fi

  case "$usecase" in
    u6_frontend_agent_site)
    cat >>"$dst" <<EOF

\`\`\`stage_execution_contract
{
  "review_mode": "independent_review",
  "task_profile": "code_change",
  "requires_source_evidence": false,
  "declared_artifacts": [
    {
      "ref_id": "artifact:u6:target_dir",
      "path": "$target",
      "kind": "directory",
      "required_actions": ["create", "write"],
      "required_evidence": ["artifact:u6:target_dir", "$target", "directory"]
    },
    {
      "ref_id": "artifact:u6:index",
      "path": "$target/index.html",
      "kind": "file",
      "required_actions": ["create", "write"],
      "required_evidence": ["artifact:u6:index", "$target/index.html", "file"]
    },
    {
      "ref_id": "artifact:u6:readme",
      "path": "$target/README.md",
      "kind": "file",
      "required_actions": ["create", "write"],
      "required_evidence": ["artifact:u6:readme", "$target/README.md", "file"]
    }
  ],
  "required_actions": ["create", "write"],
  "required_evidence": [
    "artifact:u6:target_dir",
    "artifact:u6:index",
    "artifact:u6:readme",
    "$target",
    "$target/index.html",
    "$target/README.md"
  ]
}
\`\`\`
EOF
      ;;
    u9_lism_jsonl_analyzer_tool)
    cat >>"$dst" <<EOF

\`\`\`stage_execution_contract
{
  "review_mode": "independent_review",
  "task_profile": "code_change",
  "requires_source_evidence": false,
  "declared_artifacts": [
    {
      "ref_id": "artifact:u9:target_dir",
      "path": "$target",
      "kind": "directory",
      "required_actions": ["create", "write"],
      "required_evidence": ["artifact:u9:target_dir", "$target", "directory"]
    },
    {
      "ref_id": "artifact:u9:analyzer",
      "path": "$target/analyze.py",
      "kind": "file",
      "required_actions": ["create", "write"],
      "required_evidence": ["artifact:u9:analyzer", "$target/analyze.py", "file"]
    },
    {
      "ref_id": "artifact:u9:report",
      "path": "$target/report.md",
      "kind": "file",
      "required_actions": ["create", "write"],
      "required_evidence": ["artifact:u9:report", "$target/report.md", "file"]
    }
  ],
  "tests": [
    {
      "name": "u9_analyzer_runtime",
      "required_actions": ["run_test"],
      "required_evidence": ["runtime_test_passed"]
    }
  ],
  "required_actions": ["create", "write", "run_test"],
  "required_evidence": [
    "artifact:u9:target_dir",
    "artifact:u9:analyzer",
    "artifact:u9:report",
    "$target",
    "$target/analyze.py",
    "$target/report.md",
    "runtime_test_passed"
  ]
}
\`\`\`
EOF
      ;;
  esac

  printf '%s\n' "$dst"
}

run_one() {
  local out_dir="$1"
  local usecase="$2"
  local mode="$3"
  local run_id="$4"
  local run_tag="$5"
  local timeout_secs="$6"
  local program_timeout_secs="$7"
  local st_enabled="$8"

  local src_mode
  local boss_policy
  local worker_policy
  local task_file
  local config_root
  local sample_file
  local log_file
  local api_log_file
  local memory_report_file

  src_mode="$(source_mode_for_label "$mode")"
  boss_policy="$(boss_policy_for_label "$mode")"
  worker_policy="$(worker_policy_for_label "$mode")"
  program_timeout_secs="$(cap_program_timeout "$program_timeout_secs")"

  make_mode_config "$out_dir" "$usecase" "$mode" "$run_tag" "$src_mode"
  task_file="$(rewrite_task_for_run "$out_dir" "$usecase" "$mode" "$run_id")"

  config_root="$out_dir/configs/${usecase}-${mode}/.claude"
  sample_file="$out_dir/samples/${usecase}-${mode}.jsonl"
  log_file="$out_dir/logs/${usecase}-${mode}-run${run_id}.log"
  api_log_file="$out_dir/api_logs/${usecase}-${mode}-run${run_id}.jsonl"
  memory_report_file="$out_dir/reports/memory/${usecase}-${mode}-run${run_id}.tsv"

  echo "=== START $usecase $mode run$run_id $(date '+%F %T') ==="
  (
    export RUST_AGENT_CONFIG_ROOT="$config_root"
    export RUST_AGENT_API_CALL_LOG="$api_log_file"
    export RUST_AGENT_MEMORY_REPORT_FILE="$memory_report_file"
    cmd=(
      "$BIN_PATH"
      --lism-policy "$boss_policy"
      --worker-lism-policy "$worker_policy"
      --lism-ab-sample "$sample_file"
      --boss-task "$(cat "$task_file")"
      --boss-task-timeout-secs "$timeout_secs"
    )
    if [ "$shared_memory_enabled" = "true" ]; then
      cmd+=(--shared-memory-enabled)
    fi
    if [ "$st_enabled" = "true" ]; then
      cmd+=(--st)
    fi
    run_with_program_timeout "$program_timeout_secs" "${cmd[@]}"
  ) >"$log_file" 2>&1
  if [ "$usecase" = "u3_token_efficiency_rollout" ]; then
    case "$mode" in
      all_off)
        cp -f "$api_log_file" "$out_dir/api_logs/u3_token_efficiency_rollout-off-run${run_id}.jsonl"
        ;;
      all_on)
        cp -f "$api_log_file" "$out_dir/api_logs/u3_token_efficiency_rollout-on-run${run_id}.jsonl"
        ;;
    esac
  fi
  echo "=== END $usecase $mode run$run_id $(date '+%F %T') ==="
}

process_tree_pids() {
  local root_pid="$1"
  local all_pids=("$root_pid")
  local frontier=("$root_pid")
  local next=()
  local parent_pid
  local child_pid

  while [ "${#frontier[@]}" -gt 0 ]; do
    next=()
    for parent_pid in "${frontier[@]}"; do
      while IFS= read -r child_pid; do
        [ -n "$child_pid" ] || continue
        all_pids+=("$child_pid")
        next+=("$child_pid")
      done < <(pgrep -P "$parent_pid" 2>/dev/null || true)
    done
    frontier=("${next[@]}")
  done

  printf '%s\n' "${all_pids[@]}"
}

sample_process_tree_rss_kb() {
  local root_pid="$1"
  local rss_kb=0
  local pid
  local value

  while IFS= read -r pid; do
    [ -n "$pid" ] || continue
    value="$(ps -o rss= -p "$pid" 2>/dev/null | tr -d '[:space:]' || true)"
    if [ -n "$value" ]; then
      rss_kb=$((rss_kb + value))
    fi
  done < <(process_tree_pids "$root_pid")

  printf '%s\n' "$rss_kb"
}

write_memory_sample() {
  local report_file="$1"
  local root_pid="$2"
  local started_seconds="$3"
  local elapsed=$((SECONDS - started_seconds))
  local rss_kb

  rss_kb="$(sample_process_tree_rss_kb "$root_pid")"
  printf '%s\t%s\n' "$elapsed" "$rss_kb" >>"$report_file"
}

run_with_program_timeout() {
  local timeout_secs="$1"
  shift

  if [ -z "$timeout_secs" ]; then
    "$@"
    return
  fi
  if [ "$timeout_secs" -le 0 ]; then
    echo "program timeout before command start" >&2
    return 124
  fi

  "$@" &
  local child_pid="$!"
  local deadline=$((SECONDS + timeout_secs))
  local memory_report_file="${RUST_AGENT_MEMORY_REPORT_FILE:-}"
  local memory_started_seconds="$SECONDS"

  if [ -n "$memory_report_file" ]; then
    mkdir -p "$(dirname "$memory_report_file")"
    printf 'elapsed_seconds\trss_kb\n' >"$memory_report_file"
    write_memory_sample "$memory_report_file" "$child_pid" "$memory_started_seconds"
  fi

  while kill -0 "$child_pid" 2>/dev/null; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      if [ -n "$memory_report_file" ]; then
        write_memory_sample "$memory_report_file" "$child_pid" "$memory_started_seconds"
      fi
      echo "program timeout after ${timeout_secs}s; terminating pid $child_pid" >&2
      kill "$child_pid" 2>/dev/null || true
      sleep 2
      if kill -0 "$child_pid" 2>/dev/null; then
        kill -KILL "$child_pid" 2>/dev/null || true
      fi
      wait "$child_pid" 2>/dev/null || true
      return 124
    fi
    sleep 1
    if [ -n "$memory_report_file" ]; then
      write_memory_sample "$memory_report_file" "$child_pid" "$memory_started_seconds"
    fi
  done

  if [ -n "$memory_report_file" ]; then
    write_memory_sample "$memory_report_file" "$child_pid" "$memory_started_seconds"
  fi

  set +e
  wait "$child_pid"
  local status=$?
  set -e
  return "$status"
}

summarize_out_dir() {
  local out_dir="$1"
  local report_path="$out_dir/reports/matrix-summary.md"

  python3 - "$out_dir" "$report_path" <<'PY'
import json
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

out_dir = Path(sys.argv[1])
report_path = Path(sys.argv[2])
samples_dir = out_dir / "samples"
memory_dir = out_dir / "reports" / "memory"

def load_unique(path: Path):
    records = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        obj = json.loads(raw)
        records.setdefault(obj["run_id"], obj)
    return list(records.values())

grouped = defaultdict(dict)
for path in sorted(samples_dir.glob("*.jsonl")):
    stem = path.stem
    matched = None
    for mode in ("all_off", "boss_on_only", "all_on"):
        marker = f"-{mode}"
        if stem.endswith(marker):
            matched = (stem[:-len(marker)], mode)
            break
    if not matched:
        continue
    usecase, mode = matched
    grouped[usecase][mode] = load_unique(path)

def avg(records, field):
    return sum(record.get(field, 0) for record in records) / len(records) if records else 0.0

def load_memory_stats(path: Path):
    if not path.exists():
        return None
    values = []
    for raw in path.read_text(encoding="utf-8").splitlines()[1:]:
        raw = raw.strip()
        if not raw:
            continue
        parts = raw.split("\t")
        if len(parts) != 2:
            continue
        try:
            rss_kb = int(parts[1])
        except ValueError:
            continue
        if rss_kb > 0:
            values.append(rss_kb)
    if not values:
        return None
    return {
        "min_kb": min(values),
        "max_kb": max(values),
        "avg_kb": sum(values) / len(values),
        "samples": len(values),
    }

def fmt_mb(kb):
    return "{:.1f} MB".format(kb / 1024)

memory_stats = {}
if memory_dir.exists():
    for path in sorted(memory_dir.glob("*.tsv")):
        stem = path.stem
        matched = None
        for mode in ("all_off", "boss_on_only", "all_on"):
            marker = f"-{mode}-run"
            if marker in stem:
                usecase, run_id = stem.split(marker, 1)
                matched = (usecase, mode, run_id)
                break
        if not matched:
            continue
        stats = load_memory_stats(path)
        if stats:
            memory_stats[matched] = stats

lines = []
lines.append("# Boss LisM Matrix Summary")
lines.append("")
lines.append(f"output_dir: `{out_dir}`")
lines.append("")
lines.append("| use case | mode | unique runs | completed | completion | avg cost | avg input | avg uncached input | avg output | avg hydration | avg tool dispatch | avg ref writes | avg missing refs | dominant context tier | dominant typed path signal |")
lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|")

for usecase in sorted(grouped):
    for mode in ("all_off", "boss_on_only", "all_on"):
        records = grouped[usecase].get(mode)
        if not records:
            continue
        completed = sum(1 for record in records if record.get("outcome") == "completed")
        ctx = Counter(record.get("context_tier", "n/a") for record in records)
        dominant_ctx = ctx.most_common(1)[0][0] if ctx else "n/a"
        typed = Counter(record.get("typed_path_signal", "n/a") for record in records)
        dominant_typed = typed.most_common(1)[0][0] if typed else "n/a"
        lines.append(
            "| `{}` | `{}` | {} | {} | {:.2f} | {} | {} | {} | {} | {:.2f} | {:.2f} | {:.2f} | {:.2f} | `{}` | `{}` |".format(
                usecase,
                mode,
                len(records),
                completed,
                completed / len(records),
                int(round(avg(records, "cost_micros_usd"))),
                int(round(avg(records, "total_input_tokens"))),
                int(round(avg(records, "total_uncached_input_tokens"))),
                int(round(avg(records, "total_output_tokens"))),
                avg(records, "hydration_count"),
                avg(records, "tool_dispatch_count"),
                avg(records, "tool_dispatch_ref_write_count"),
                avg(records, "hydration_ref_missing"),
                dominant_ctx,
                dominant_typed,
            )
        )

lines.append("")
lines.append("## Memory Pressure Report")
lines.append("")
lines.append("| use case | mode | runs sampled | lowest memory | highest memory | average memory | samples |")
lines.append("|---|---|---:|---:|---:|---:|---:|")

for usecase in sorted(grouped):
    for mode in ("all_off", "boss_on_only", "all_on"):
        records = grouped[usecase].get(mode)
        if not records:
            continue
        mode_stats = []
        for script_run_id, record in enumerate(records, start=1):
            stats = memory_stats.get((usecase, mode, str(script_run_id)))
            if stats:
                mode_stats.append(stats)
        if not mode_stats:
            lines.append(f"| `{usecase}` | `{mode}` | 0 | n/a | n/a | n/a | 0 |")
            continue
        min_kb = min(stats["min_kb"] for stats in mode_stats)
        max_kb = max(stats["max_kb"] for stats in mode_stats)
        sample_count = sum(stats["samples"] for stats in mode_stats)
        weighted_avg_kb = sum(stats["avg_kb"] * stats["samples"] for stats in mode_stats) / sample_count
        lines.append(
            "| `{}` | `{}` | {} | {} | {} | {} | {} |".format(
                usecase,
                mode,
                len(mode_stats),
                fmt_mb(min_kb),
                fmt_mb(max_kb),
                fmt_mb(weighted_avg_kb),
                sample_count,
            )
        )

lines.append("")
lines.append("## Run Details")
lines.append("")

for usecase in sorted(grouped):
    lines.append(f"### `{usecase}`")
    lines.append("")
    for mode in ("all_off", "boss_on_only", "all_on"):
        records = grouped[usecase].get(mode)
        if not records:
            continue
        lines.append(f"- `{mode}`")
        for script_run_id, record in enumerate(records, start=1):
            run_id = str(record.get("run_id"))
            stats = memory_stats.get((usecase, mode, str(script_run_id)))
            memory_detail = " memory_min=`n/a` memory_max=`n/a` memory_avg=`n/a`"
            if stats:
                memory_detail = " memory_min=`{}` memory_max=`{}` memory_avg=`{}`".format(
                    fmt_mb(stats["min_kb"]),
                    fmt_mb(stats["max_kb"]),
                    fmt_mb(stats["avg_kb"]),
                )
            lines.append(
                "  - run_id=`{}` outcome=`{}` cost={} input={} uncached={} output={} hydration={} tool_dispatch={} ref_writes={} missing_refs={} context_tier=`{}` typed_path_signal=`{}` fallback_tier=`{}`{}".format(
                    run_id,
                    record.get("outcome"),
                    record.get("cost_micros_usd", 0),
                    record.get("total_input_tokens", 0),
                    record.get("total_uncached_input_tokens", 0),
                    record.get("total_output_tokens", 0),
                    record.get("hydration_count", 0),
                    record.get("tool_dispatch_count", 0),
                    record.get("tool_dispatch_ref_write_count", 0),
                    record.get("hydration_ref_missing", 0),
                    record.get("context_tier"),
                    record.get("typed_path_signal"),
                    record.get("fallback_tier"),
                    memory_detail,
                )
            )
    lines.append("")

report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("\n".join(lines))
PY
}

prepare_run_root() {
  local out_dir="$1"
  local model="$2"
  prepare_dirs "$out_dir"
  run_with_program_timeout "$(remaining_runtime_secs)" \
    env RUST_AGENT_AB_MODEL="$model" bash "$PREPARE_SCRIPT" prepare "$out_dir" >/dev/null
}

cases_spec="all"
plan="$DEFAULT_PLAN"
single_mode="$DEFAULT_SINGLE_MODE"
runs=""
out_dir="$DEFAULT_OUT_DIR"
model="$DEFAULT_MODEL"
timeout_secs="$DEFAULT_TIMEOUT_SECS"
program_timeout_secs="$DEFAULT_PROGRAM_TIMEOUT_SECS"
shared_memory_enabled="false"
st_enabled="false"
prepare_only="false"
summary_only=""
list_cases="false"

while [ $# -gt 0 ]; do
  case "$1" in
    --cases)
      cases_spec="${2:-}"
      shift 2
      ;;
    --plan)
      plan="${2:-}"
      shift 2
      ;;
    --mode)
      single_mode="${2:-}"
      shift 2
      ;;
    --shared-memory-enabled)
      shared_memory_enabled="true"
      shift
      ;;
    --st)
      st_enabled="true"
      shift
      ;;
    --runs)
      runs="${2:-}"
      shift 2
      ;;
    --out)
      out_dir="${2:-}"
      shift 2
      ;;
    --model)
      model="${2:-}"
      shift 2
      ;;
    --timeout)
      timeout_secs="${2:-}"
      shift 2
      ;;
    --program-timeout)
      program_timeout_secs="${2:-}"
      shift 2
      ;;
    --prepare-only)
      prepare_only="true"
      shift
      ;;
    --summary-only)
      summary_only="${2:-}"
      shift 2
      ;;
    --list-cases)
      list_cases="true"
      shift
      ;;
    --check-semantics)
      check_mode_semantics
      exit 0
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

start_script_watchdog

if [ "$list_cases" = "true" ]; then
  print_cases
  exit 0
fi

if [ -n "$summary_only" ]; then
  summarize_out_dir "$summary_only"
  exit 0
fi

if [ ! -f "$ENV_LOADER" ]; then
  die "missing env loader: $ENV_LOADER"
fi
if [ ! -f "$ENV_FILE" ]; then
  die "missing env file: $ENV_FILE"
fi

# shellcheck disable=SC1090
source "$ENV_LOADER" "$ENV_FILE" >/dev/null
[ -n "${OPENAI_API_KEY:-}" ] || die "OPENAI_API_KEY is not set after sourcing $ENV_LOADER $ENV_FILE"

case "$plan" in
  3x3|3plus3|2x2|2plus2|single)
    ;;
  *)
    die "unsupported --plan: $plan"
    ;;
esac

case "$single_mode" in
  all_off|boss_on_only|all_on)
    ;;
  *)
    die "unsupported --mode: $single_mode"
    ;;
esac

if [ -z "$runs" ]; then
  if [ "$plan" = "single" ]; then
    runs="1"
  elif [ "$plan" = "2x2" ] || [ "$plan" = "2plus2" ]; then
    runs="2"
  else
    runs="3"
  fi
fi

assert_runtime_budget
ensure_binary
prepare_run_root "$out_dir" "$model"

if [ "$prepare_only" = "true" ]; then
  echo "Prepared: $out_dir"
  exit 0
fi

read_lines_into_array selected_cases <<EOF
$(expand_cases "$cases_spec")
EOF
[ "${#selected_cases[@]}" -gt 0 ] || die "no cases selected"

run_tag="$(basename "$out_dir")"

echo "Output root: $out_dir"
echo "Cases: ${selected_cases[*]}"
echo "Plan: $plan"
echo "Runs per mode: $runs"
echo "Model: $model"
echo "Timeout secs: $timeout_secs"
echo "Program timeout secs: $program_timeout_secs"
echo "Script max runtime secs: $MAX_SCRIPT_RUNTIME_SECS"
echo

read_lines_into_array mode_sequence <<EOF
$(mode_sequence_for_plan "$plan" "$single_mode")
EOF

for usecase in "${selected_cases[@]}"; do
  for mode in "${mode_sequence[@]}"; do
    for run_id in $(seq 1 "$runs"); do
      assert_runtime_budget
      run_one "$out_dir" "$usecase" "$mode" "$run_id" "$run_tag" "$timeout_secs" "$program_timeout_secs" "$st_enabled"
    done
  done
done

echo
echo "=== SUMMARY ==="
summarize_out_dir "$out_dir"
