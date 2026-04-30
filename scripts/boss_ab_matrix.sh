#!/usr/bin/env bash

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
DOCS_DIR="$REPO_ROOT/RustAgent/docs"
ENV_LOADER="$REPO_ROOT/load-env.sh"
BIN_PATH="$AGENT_DIR/target/debug/rust-agent"
DEFAULT_OUT_DIR="${TMPDIR:-/tmp}/rustagent-boss-ab-$(date +%Y%m%d-%H%M%S)"
MODEL_ID="${RUST_AGENT_AB_MODEL:-gpt-5-mini-2025-08-07}"

usage() {
  cat <<'EOF'
Usage:
  boss_ab_matrix.sh prepare [out_dir]
  boss_ab_matrix.sh run [out_dir]
  boss_ab_matrix.sh report [out_dir]
  boss_ab_matrix.sh all [out_dir]

Environment:
  RUST_AGENT_AB_MODEL   Override the model in generated models.toml.
EOF
}

subcommand="${1:-}"
out_dir="${2:-$DEFAULT_OUT_DIR}"

if [ -z "$subcommand" ]; then
  usage
  exit 1
fi

ensure_binary() {
  if [ ! -x "$BIN_PATH" ]; then
    echo "binary missing at $BIN_PATH; building rust-agent" >&2
    cargo build --manifest-path "$AGENT_DIR/Cargo.toml" --bin rust-agent
  fi
}

prepare_dirs() {
  mkdir -p \
    "$out_dir/usecases" \
    "$out_dir/config" \
    "$out_dir/samples" \
    "$out_dir/logs" \
    "$out_dir/reports"
}

write_models_toml() {
  local config_root="$1"
  local cache_key="$2"
  mkdir -p "$config_root"
  cat >"$config_root/models.toml" <<EOF
active = "default"

[profiles.default]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
chat_completions_path = "/v1/chat/completions"
model = "$MODEL_ID"
auth_strategy = "bearer_api_key"
api_key_env = "OPENAI_API_KEY"
request_timeout_ms = 30000
stream_timeout_ms = 60000
retry_max_attempts = 1
retry_initial_backoff_ms = 0
retry_max_backoff_ms = 0
max_tokens_param = "max_completion_tokens"
prompt_cache_key = "$cache_key"
prompt_cache_retention = "in_memory"

[profiles.worker-override]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
chat_completions_path = "/v1/chat/completions"
model = "$MODEL_ID"
auth_strategy = "bearer_api_key"
api_key_env = "OPENAI_API_KEY"
request_timeout_ms = 30000
stream_timeout_ms = 60000
retry_max_attempts = 1
retry_initial_backoff_ms = 0
retry_max_backoff_ms = 0
max_tokens_param = "max_completion_tokens"
prompt_cache_key = "$cache_key"
prompt_cache_retention = "in_memory"
EOF
}

append_task_footer() {
  local file="$1"
  cat >>"$file" <<'EOF'

输出要求：
- 只做只读分析，不修改文件，不提出 patch。
- 使用中文输出。
- 输出 4 个小段：现状、主要风险、证据来源、下一步建议。
- 若需要补充核验，可读取引用文件，但最终只返回简洁报告。
EOF
}

write_usecase_security() {
  local file="$out_dir/usecases/u1_security_beta_runtime.txt"
  cat >"$file" <<'EOF'
真实 /boss A/B use case 1：审计 beta 安全 runtime contract。

任务目标：
- 基于下面的真实仓库材料，判断当前 beta 安全口径是否自洽。
- 重点看：workspace capability、PendingApproval、shell sandbox/backend、filesystem policy v1、MCP governance 当前边界。
- 不要改代码，不要写 patch，只输出审计结论。

关键材料摘录：
EOF
  sed -n '1,110p' "$DOCS_DIR/11-security-safety.md" >>"$file"
  cat >>"$file" <<'EOF'

参考实现路径：
- RustAgent/Agent/src/security/workspace_capability.rs
- RustAgent/Agent/src/security/approval_protocol.rs
- RustAgent/Agent/src/tool/builtin/bash/mod.rs
- RustAgent/Agent/src/tool/builtin/bash/sandbox.rs
EOF
  append_task_footer "$file"
}

write_usecase_memory() {
  local file="$out_dir/usecases/u2_memory_backpressure_contract.txt"
  cat >"$file" <<'EOF'
真实 /boss A/B use case 2：审计 bash 输出限界与内存背压合同。

任务目标：
- 判断文档对 bash clamped output、head/tail truncation、并发读取防死锁、以及未完成系统级内存水位治理的描述是否准确。
- 只做只读审计与摘要。

关键材料摘录：
EOF
  sed -n '1,220p' "$DOCS_DIR/29-memory-backpressure-and-resource-limits.md" >>"$file"
  cat >>"$file" <<'EOF'

实现摘录一：tool/builtin/bash/clamped_reader.rs
EOF
  sed -n '1,60p' "$AGENT_DIR/src/tool/builtin/bash/clamped_reader.rs" >>"$file"
  cat >>"$file" <<'EOF'

实现摘录二：tool/builtin/bash/sandbox.rs
EOF
  sed -n '1,60p' "$AGENT_DIR/src/tool/builtin/bash/sandbox.rs" >>"$file"
  cat >>"$file" <<'EOF'

实现摘录三：tool/builtin/bash/mod.rs
EOF
  sed -n '1,60p' "$AGENT_DIR/src/tool/builtin/bash/mod.rs" >>"$file"
  append_task_footer "$file"
}

write_usecase_efficiency() {
  local file="$out_dir/usecases/u3_token_efficiency_rollout.txt"
  cat >"$file" <<'EOF'
真实 /boss A/B use case 3：审计 LisM token 效率、KV cache 与 rollout 口径。

任务目标：
- 基于真实文档，提炼当前 Less-is-More 的主目标、cache 设计约束、projection 风险与 rollout 判据。
- 只读输出，不改文件。

关键材料摘录：
EOF
  sed -n '1,210p' "$DOCS_DIR/31-token-efficiency-cost-performance.md" >>"$file"
  append_task_footer "$file"
}

write_usecase_boss() {
  local file="$out_dir/usecases/u4_boss_workflow_and_lism.txt"
  cat >"$file" <<'EOF'
真实 /boss A/B use case 4：总结 Boss workflow、prompt 分层与 T26/T27 当前真实边界。

任务目标：
- 从真实 Boss 文档中总结：当前 production seam、observability、cache boundary、StateFrame-First 风险与反制。
- 输出面向产品测试的简洁判断。

关键材料摘录：
EOF
  sed -n '191,245p' "$DOCS_DIR/30-boss-mode-and-dual-agent-workflow.md" >>"$file"
  printf '\n' >>"$file"
  sed -n '334,396p' "$DOCS_DIR/30-boss-mode-and-dual-agent-workflow.md" >>"$file"
  append_task_footer "$file"
}

write_usecase_roadmap() {
  local file="$out_dir/usecases/u5_gap_audit_and_roadmap.txt"
  cat >"$file" <<'EOF'
真实 /boss A/B use case 5：综合 gap audit 与 roadmap，给出当前真实产品测试主线判断。

任务目标：
- 综合 full design gap audit 与 roadmap，回答当前主 blocker、已关账基线、以及 `/boss + real skill + MCP` 的下一步。
- 只做只读总结。

关键材料摘录一：full design implementation gap audit
EOF
  sed -n '1,130p' "$DOCS_DIR/33-full-design-implementation-gap-audit.md" >>"$file"
  cat >>"$file" <<'EOF'

关键材料摘录二：future gaps and roadmap
EOF
  sed -n '44,110p' "$DOCS_DIR/14-progress-gap-roadmap.md" >>"$file"
  printf '\n' >>"$file"
  sed -n '184,214p' "$DOCS_DIR/14-progress-gap-roadmap.md" >>"$file"
  append_task_footer "$file"
}

generate_usecases() {
  write_usecase_security
  write_usecase_memory
  write_usecase_efficiency
  write_usecase_boss
  write_usecase_roadmap
}

generate_configs() {
  local usecase
  for usecase_path in "$out_dir"/usecases/*.txt; do
    usecase="$(basename "$usecase_path" .txt)"
    write_models_toml "$out_dir/config/${usecase}-off/.claude" "boss-ab-${usecase}-off"
    write_models_toml "$out_dir/config/${usecase}-on/.claude" "boss-ab-${usecase}-on"
  done
}

write_manifest() {
  {
    echo "output_dir=$out_dir"
    echo "model=$MODEL_ID"
    echo "generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo
    for usecase_path in "$out_dir"/usecases/*.txt; do
      printf '%s\t' "$(basename "$usecase_path")"
      wc -w -c "$usecase_path" | awk '{print "words="$1"\tchars="$2}'
    done
  } >"$out_dir/reports/manifest.tsv"
}

prepare() {
  prepare_dirs
  generate_usecases
  generate_configs
  write_manifest
  echo "Prepared use cases under $out_dir"
  cat "$out_dir/reports/manifest.tsv"
}

run_one() {
  local usecase="$1"
  local arm="$2"
  local iteration="$3"
  local policy
  local config_root
  local sample_file
  local log_file
  local task_file
  local objective

  policy="force-$arm"
  config_root="$out_dir/config/${usecase}-${arm}/.claude"
  sample_file="$out_dir/samples/${usecase}.jsonl"
  log_file="$out_dir/logs/${usecase}-${arm}-run${iteration}.log"
  task_file="$out_dir/usecases/${usecase}.txt"
  objective="$(cat "$task_file")"

  echo "=== $usecase | $arm | run $iteration ==="
  (
    export RUST_AGENT_CONFIG_ROOT="$config_root"
    "$BIN_PATH" \
      --lism-policy "$policy" \
      --lism-ab-sample "$sample_file" \
      --boss-task "$objective"
  ) | tee "$log_file"
}

run_matrix() {
  if [ ! -f "$ENV_LOADER" ]; then
    echo "missing env loader: $ENV_LOADER" >&2
    exit 1
  fi
  # shellcheck disable=SC1090
  source "$ENV_LOADER" >/dev/null
  if [ -z "${OPENAI_API_KEY:-}" ]; then
    echo "OPENAI_API_KEY is not set after sourcing $ENV_LOADER" >&2
    exit 1
  fi
  ensure_binary
  prepare_dirs
  if [ ! -f "$out_dir/reports/manifest.tsv" ]; then
    prepare
  fi

  local usecase
  for usecase_path in "$out_dir"/usecases/*.txt; do
    usecase="$(basename "$usecase_path" .txt)"
    for i in 1 2 3; do
      run_one "$usecase" off "$i"
      sleep 1
    done
    for i in 1 2 3; do
      run_one "$usecase" on "$i"
      sleep 1
    done
  done
}

report() {
  ensure_binary
  prepare_dirs
  if [ -f "$out_dir/reports/manifest.tsv" ]; then
    echo "=== Manifest ==="
    cat "$out_dir/reports/manifest.tsv"
    echo
  fi

  : >"$out_dir/reports/combined_samples.jsonl"
  local usecase
  for sample_file in "$out_dir"/samples/*.jsonl; do
    [ -e "$sample_file" ] || continue
    usecase="$(basename "$sample_file" .jsonl)"
    cat "$sample_file" >>"$out_dir/reports/combined_samples.jsonl"
    {
      echo "=== $usecase ==="
      "$BIN_PATH" --lism-ab-summarize "$sample_file"
      echo
      "$BIN_PATH" --lism-ab-conclude "$sample_file"
      echo
      echo "Records:"
      jq -r '[.run_id,.lism_enabled,.total_input_tokens,.total_output_tokens,.cache_read_tokens,.cache_write_tokens,.cache_hit_ratio,.cost_micros_usd] | @tsv' "$sample_file"
      echo
    } | tee "$out_dir/reports/${usecase}.txt"
  done

  if [ -s "$out_dir/reports/combined_samples.jsonl" ]; then
    {
      echo "=== Combined Summary ==="
      "$BIN_PATH" --lism-ab-summarize "$out_dir/reports/combined_samples.jsonl"
      echo
      "$BIN_PATH" --lism-ab-conclude "$out_dir/reports/combined_samples.jsonl"
      echo
      echo "=== Combined Records ==="
      jq -r '[.run_id,.lism_enabled,.total_input_tokens,.total_output_tokens,.cache_read_tokens,.cache_write_tokens,.cache_hit_ratio,.cost_micros_usd] | @tsv' "$out_dir/reports/combined_samples.jsonl"
    } | tee "$out_dir/reports/combined.txt"
  fi
}

case "$subcommand" in
  prepare)
    prepare
    ;;
  run)
    run_matrix
    ;;
  report)
    report
    ;;
  all)
    prepare
    run_matrix
    report
    ;;
  *)
    usage
    exit 1
    ;;
esac
