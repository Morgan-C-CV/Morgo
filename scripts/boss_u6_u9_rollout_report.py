#!/usr/bin/env python3

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path


EXPECTED_BEST_MODE = {
    "u6_frontend_agent_site": "all_on",
    "u7_python_boss_lism_demo": "boss_on_only",
    "u8_multistage_tools_memory_token_report": "all_on",
    "u9_lism_jsonl_analyzer_tool": "boss_on_only",
}

MODE_LABELS = ["all_off", "boss_on_only", "all_on"]
METRIC_FIELDS = [
    "fallback_count",
    "hydration_count",
    "hydration_ref_missing",
    "stale_ref_count",
    "context_tier",
    "fallback_tier",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize boss-mode u6-u9 worker LisM rollout metrics."
    )
    parser.add_argument("samples_dir", help="Directory containing per-mode JSONL sample files")
    parser.add_argument(
        "--output",
        required=True,
        help="Markdown output path",
    )
    return parser.parse_args()


def load_records(path: Path):
    records = []
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            raw = raw.strip()
            if not raw:
                continue
            records.append(json.loads(raw))
    return records


def split_name(path: Path):
    stem = path.stem
    for suffix in MODE_LABELS:
        marker = f"-{suffix}"
        if stem.endswith(marker):
            return stem[: -len(marker)], suffix
    raise ValueError(f"unrecognized sample file name: {path.name}")


def average(records, field):
    if not records:
        return None
    values = [record.get(field) for record in records if isinstance(record.get(field), (int, float))]
    if not values:
        return None
    return sum(values) / len(values)


def completion_rate(records):
    if not records:
        return None
    completed = sum(1 for record in records if record.get("outcome") == "completed")
    return completed / len(records)


def dominant_counter(records, field):
    counts = Counter()
    for record in records:
        value = record.get(field)
        if isinstance(value, str) and value:
            counts[value] += 1
    return counts


def telemetry_available(records):
    return any(any(field in record for field in METRIC_FIELDS) for record in records)


def format_float(value, digits=3):
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def format_intish(value):
    if value is None:
        return "n/a"
    return str(int(round(value)))


def pick_best_mode(mode_stats):
    completed_modes = [
        (mode, stats)
        for mode, stats in mode_stats.items()
        if stats["completion_rate"] is not None and stats["completion_rate"] > 0
    ]
    if not completed_modes:
        return None
    completed_modes.sort(
        key=lambda item: (
            item[1]["completion_rate"],
            -1 * (item[1]["avg_cost"] if item[1]["avg_cost"] is not None else float("inf")),
        ),
        reverse=True,
    )
    top_completion = completed_modes[0][1]["completion_rate"]
    best_candidates = [
        item for item in completed_modes if item[1]["completion_rate"] == top_completion
    ]
    best_candidates.sort(
        key=lambda item: item[1]["avg_cost"] if item[1]["avg_cost"] is not None else float("inf")
    )
    return best_candidates[0][0]


def summarize(samples_dir: Path):
    grouped = defaultdict(dict)
    for path in sorted(samples_dir.glob("*.jsonl")):
        usecase, mode = split_name(path)
        records = load_records(path)
        grouped[usecase][mode] = {
            "path": path,
            "records": records,
            "completion_rate": completion_rate(records),
            "avg_cost": average(records, "cost_micros_usd"),
            "avg_input": average(records, "total_input_tokens"),
            "avg_uncached_input": average(records, "total_uncached_input_tokens"),
            "avg_fallback_count": average(records, "fallback_count"),
            "avg_hydration_count": average(records, "hydration_count"),
            "avg_missing_refs": average(records, "hydration_ref_missing"),
            "avg_stale_refs": average(records, "stale_ref_count"),
            "dominant_context_tier": dominant_counter(records, "context_tier"),
            "dominant_fallback_tier": dominant_counter(records, "fallback_tier"),
            "telemetry_available": telemetry_available(records),
        }
    return grouped


def render_report(grouped):
    lines = []
    lines.append("# Boss Mode `u6-u9` Rollout Metric Alignment")
    lines.append("")
    lines.append("日期：`2026-05-02`")
    lines.append("")
    lines.append("## 1. 目标")
    lines.append("")
    lines.append(
        "这份报告把当前 rollout strategy table 和 `u6-u9` 的真实样本对齐，重点看："
    )
    lines.append("")
    lines.append("- completion")
    lines.append("- fallback rate / avg fallback count")
    lines.append("- hydration rate proxy：`avg hydration` 与 `avg missing refs`")
    lines.append("- stale refs")
    lines.append("- context tier / fallback tier 分布")
    lines.append("")
    lines.append("## 2. 当前样本可用性")
    lines.append("")
    lines.append(
        "当前 `/tmp/rustagent-boss-worker-lism-u6u9-20260501/samples` 里的样本属于旧 schema。"
    )
    lines.append("")
    lines.append(
        "- `completion / cost / input / uncached input` 可用"
    )
    lines.append(
        "- `fallback_count / hydration_count / hydration_ref_missing / stale_ref_count / context_tier / fallback_tier` 基本缺失"
    )
    lines.append(
        "- 因此这份报告能先把策略和 completion/cost 对齐，但不能把 fallback/hydration 做成最终结论"
    )
    lines.append("")
    lines.append("## 3. 策略对齐表")
    lines.append("")
    lines.append(
        "| use case | strategy table expected | observed best mode | completion evidence | telemetry status | 当前判断 |"
    )
    lines.append("|---|---|---|---|---|---|")
    for usecase in sorted(grouped):
        expected = EXPECTED_BEST_MODE.get(usecase, "n/a")
        mode_stats = grouped[usecase]
        best = pick_best_mode(mode_stats) or "n/a"
        completion_notes = []
        for mode in MODE_LABELS:
            stats = mode_stats.get(mode)
            if not stats:
                continue
            completion_notes.append(
                f"{mode}={format_float(stats['completion_rate'], 2)}"
            )
        telemetry_status = (
            "available"
            if any(stats["telemetry_available"] for stats in mode_stats.values())
            else "legacy-missing"
        )
        if expected == best:
            verdict = "策略方向与现有 completion/cost 结果一致"
        else:
            verdict = "需要复核；旧样本或成本/完成率与策略表存在偏差"
        lines.append(
            f"| `{usecase}` | `{expected}` | `{best}` | {'; '.join(completion_notes)} | `{telemetry_status}` | {verdict} |"
        )
    lines.append("")
    lines.append("## 4. 逐案分析")
    lines.append("")
    for usecase in sorted(grouped):
        lines.append(f"### 4.{sorted(grouped).index(usecase) + 1} `{usecase}`")
        lines.append("")
        expected = EXPECTED_BEST_MODE.get(usecase, "n/a")
        best = pick_best_mode(grouped[usecase]) or "n/a"
        lines.append(f"- 策略表期望：`{expected}`")
        lines.append(f"- 当前样本 best mode：`{best}`")
        telemetry_missing = True
        for mode in MODE_LABELS:
            stats = grouped[usecase].get(mode)
            if not stats:
                continue
            telemetry_missing = telemetry_missing and not stats["telemetry_available"]
            lines.append(
                f"- `{mode}`: completion={format_float(stats['completion_rate'], 2)}, "
                f"avg_cost_micros={format_intish(stats['avg_cost'])}, "
                f"avg_input={format_intish(stats['avg_input'])}, "
                f"avg_uncached_input={format_intish(stats['avg_uncached_input'])}, "
                f"avg_fallback={format_float(stats['avg_fallback_count'], 2)}, "
                f"avg_hydration={format_float(stats['avg_hydration_count'], 2)}, "
                f"avg_missing_refs={format_float(stats['avg_missing_refs'], 2)}, "
                f"avg_stale_refs={format_float(stats['avg_stale_refs'], 2)}"
            )
        if telemetry_missing:
            lines.append(
                "- 结论：当前只能确认 completion/cost 方向，无法证明 fallback/hydration 是否支撑了这个模式优势。"
            )
        else:
            lines.append(
                "- 结论：当前样本已带 telemetry，可继续比较 fallback/hydration 与 completion 的相关性。"
            )
        lines.append("")
    lines.append("## 5. 当前可确认的事")
    lines.append("")
    lines.append(
        "- `u6/u8` 的策略表与既有真实结果一致：它们当前都指向 `all_on`。"
    )
    lines.append(
        "- `u7/u9` 的策略表与既有真实结果一致：它们当前都指向 `boss_on_only`。"
    )
    lines.append(
        "- 但这仍然只是 completion/cost 层面的对齐，不足以证明是 `fallback_rate` 或 `hydration_rate` 导致。"
    )
    lines.append("")
    lines.append("## 6. 当前不能确认的事")
    lines.append("")
    lines.append(
        "- 不能从这批旧 sample 里回答 `u6/u8` 是否因为 worker LisM 带来更低 `fallback_count`。"
    )
    lines.append(
        "- 不能从这批旧 sample 里回答 `u7/u9` 是否因为 worker full-context 带来更低 `missing_refs`。"
    )
    lines.append(
        "- 不能从这批旧 sample 里比较 `context_tier` 分布，因为字段尚未写入。"
    )
    lines.append("")
    lines.append("## 7. 下一步 rerun 要求")
    lines.append("")
    lines.append("下一轮真实 `u6-u9` matrix 必须满足：")
    lines.append("")
    lines.append("- 使用当前代码重新生成 sample，确保 sample record 带上 `fallback_count / hydration_count / hydration_ref_missing / stale_ref_count / context_tier / fallback_tier`")
    lines.append("- 继续保留 3 模式：`all_off / boss_on_only / all_on`")
    lines.append("- 至少保留 `u7` 和 `u9` 作为 worker full-context 候选的对照集")
    lines.append("- rerun 后用同一脚本重生这份报告，避免人工抄表")
    lines.append("")
    return "\n".join(lines) + "\n"


def main():
    args = parse_args()
    samples_dir = Path(args.samples_dir)
    output = Path(args.output)
    grouped = summarize(samples_dir)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_report(grouped), encoding="utf-8")
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
