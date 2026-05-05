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
DEFAULT_TIMEOUT_SECS="${RUST_AGENT_BOSS_TASK_TIMEOUT_SECS:-300}"
DEFAULT_PLAN="3x3"
DEFAULT_SINGLE_MODE="all_on"

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
  --runs N              Runs per mode. Default: 3. For single, default 1.
  --out DIR             Output root. Default: $DEFAULT_OUT_DIR
  --model MODEL         Override model written into generated config.
  --timeout SECONDS     --boss-task-timeout-secs. Default: $DEFAULT_TIMEOUT_SECS
  --prepare-only        Only generate usecases/configs, do not run.
  --summary-only DIR    Skip execution, summarize an existing output directory.
  --list-cases          Print supported use cases and exit.
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

ensure_binary() {
  if [ ! -x "$BIN_PATH" ]; then
    echo "binary missing at $BIN_PATH; building morgo" >&2
    cargo build --manifest-path "$AGENT_DIR/Cargo.toml" --bin morgo
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
    "$out_dir/reports"
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
      printf 'off\n'
      ;;
    boss_on_only|all_on)
      printf 'on\n'
      ;;
    *)
      die "unsupported mode label: $mode"
      ;;
  esac
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

  printf '%s\n' "$dst"
}

run_one() {
  local out_dir="$1"
  local usecase="$2"
  local mode="$3"
  local run_id="$4"
  local run_tag="$5"
  local timeout_secs="$6"

  local src_mode
  local boss_policy
  local worker_policy
  local task_file
  local config_root
  local sample_file
  local log_file
  local api_log_file

  src_mode="$(source_mode_for_label "$mode")"
  boss_policy="$(boss_policy_for_label "$mode")"
  worker_policy="$(worker_policy_for_label "$mode")"

  make_mode_config "$out_dir" "$usecase" "$mode" "$run_tag" "$src_mode"
  task_file="$(rewrite_task_for_run "$out_dir" "$usecase" "$mode" "$run_id")"

  config_root="$out_dir/configs/${usecase}-${mode}/.claude"
  sample_file="$out_dir/samples/${usecase}-${mode}.jsonl"
  log_file="$out_dir/logs/${usecase}-${mode}-run${run_id}.log"
  api_log_file="$out_dir/api_logs/${usecase}-${mode}-run${run_id}.jsonl"

  echo "=== START $usecase $mode run$run_id $(date '+%F %T') ==="
  (
    export RUST_AGENT_CONFIG_ROOT="$config_root"
    export RUST_AGENT_API_CALL_LOG="$api_log_file"
    "$BIN_PATH" \
      --lism-policy "$boss_policy" \
      --worker-lism-policy "$worker_policy" \
      --lism-ab-sample "$sample_file" \
      --boss-task "$(cat "$task_file")" \
      --boss-task-timeout-secs "$timeout_secs"
  ) >"$log_file" 2>&1
  echo "=== END $usecase $mode run$run_id $(date '+%F %T') ==="
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
        for record in records:
            lines.append(
                "  - run_id=`{}` outcome=`{}` cost={} input={} uncached={} output={} hydration={} tool_dispatch={} ref_writes={} missing_refs={} context_tier=`{}` typed_path_signal=`{}` fallback_tier=`{}`".format(
                    record.get("run_id"),
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
  RUST_AGENT_AB_MODEL="$model" bash "$PREPARE_SCRIPT" prepare "$out_dir" >/dev/null
}

cases_spec="all"
plan="$DEFAULT_PLAN"
single_mode="$DEFAULT_SINGLE_MODE"
runs=""
out_dir="$DEFAULT_OUT_DIR"
model="$DEFAULT_MODEL"
timeout_secs="$DEFAULT_TIMEOUT_SECS"
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
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

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
echo

read_lines_into_array mode_sequence <<EOF
$(mode_sequence_for_plan "$plan" "$single_mode")
EOF

for usecase in "${selected_cases[@]}"; do
  for mode in "${mode_sequence[@]}"; do
    for run_id in $(seq 1 "$runs"); do
      run_one "$out_dir" "$usecase" "$mode" "$run_id" "$run_tag" "$timeout_secs"
    done
  done
done

echo
echo "=== SUMMARY ==="
summarize_out_dir "$out_dir"
