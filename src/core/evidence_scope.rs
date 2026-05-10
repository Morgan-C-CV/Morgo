fn clean_path_like(value: &str) -> String {
    let mut cleaned = value.trim();
    for prefix in [
        "hydrated_context: file_snippet:",
        "hydrated_context: artifact:",
        "file_snippet:",
        "read:",
        "verification:",
        "write:",
        "artifact:",
    ] {
        if let Some(rest) = cleaned.strip_prefix(prefix) {
            cleaned = rest.trim();
            break;
        }
    }
    for marker in [
        " source=",
        " match_reason=",
        " trace=",
        " excerpt=",
        " status=",
        " kind=",
    ] {
        if let Some((path, _)) = cleaned.split_once(marker) {
            cleaned = path.trim();
            break;
        }
    }
    let cleaned = cleaned
        .trim_matches(|ch: char| matches!(ch, '"' | '\'' | '`' | ',' | ';'))
        .replace('\\', "/");
    let mut parts = Vec::new();
    for part in cleaned.split('/') {
        match part {
            "" | "." => {}
            ".." => parts.push(part),
            _ => parts.push(part),
        }
    }
    let normalized = if cleaned.starts_with('/') {
        format!("/{}", parts.join("/"))
    } else {
        parts.join("/")
    };
    normalized.trim_end_matches('/').to_string()
}

fn has_path_boundary_before(value: &str, start: usize) -> bool {
    start == 0 || value[..start].ends_with('/')
}

fn has_path_boundary_after(value: &str, end: usize) -> bool {
    end == value.len() || value[end..].starts_with('/')
}

fn boundary_contains_path(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return false;
    }
    let mut search_start = 0;
    while let Some(offset) = haystack[search_start..].find(needle) {
        let start = search_start + offset;
        let end = start + needle.len();
        if has_path_boundary_before(haystack, start) && has_path_boundary_after(haystack, end) {
            return true;
        }
        search_start = end;
    }
    false
}

pub(crate) fn evidence_path_scope_matches(candidate: &str, target: &str) -> bool {
    let candidate = clean_path_like(candidate);
    let target = clean_path_like(target);
    if candidate.is_empty() || target.is_empty() {
        return false;
    }
    candidate == target
        || boundary_contains_path(&candidate, &target)
        || boundary_contains_path(&target, &candidate)
}

pub(crate) fn matching_target_scope<'a>(
    candidate: &str,
    target_paths: &'a [String],
) -> Option<&'a str> {
    let candidate = clean_path_like(candidate);
    target_paths
        .iter()
        .map(String::as_str)
        .filter(|target| evidence_path_scope_matches(&candidate, target))
        .max_by_key(|target| {
            let target = clean_path_like(target);
            (candidate == target, target.len())
        })
}

fn prefixed_evidence_path(value: &str, prefix: &str) -> Option<String> {
    value
        .trim()
        .strip_prefix(prefix)
        .map(clean_path_like)
        .filter(|path| !path.is_empty())
}

fn evidence_ref_kind_and_path(value: &str) -> Option<(&'static str, String)> {
    let trimmed = value.trim();
    if let Some(path) = prefixed_evidence_path(trimmed, "read:") {
        return Some(("read", path));
    }
    if let Some(path) = prefixed_evidence_path(trimmed, "verification:") {
        return Some(("verification", path));
    }
    if let Some(path) = prefixed_evidence_path(trimmed, "write:") {
        return Some(("write", path));
    }
    if let Some(path) = prefixed_evidence_path(trimmed, "file_snippet:") {
        return Some(("file_snippet", path));
    }
    if let Some(path) = prefixed_evidence_path(trimmed, "hydrated_context: file_snippet:") {
        return Some(("file_snippet", path));
    }
    if let Some(path) = prefixed_evidence_path(trimmed, "artifact:") {
        return Some(("artifact", path));
    }
    None
}

pub(crate) fn evidence_ref_matches_anchor_scope(
    evidence_ref: &str,
    expected_kind: &str,
    target: &str,
) -> bool {
    let Some((kind, path)) = evidence_ref_kind_and_path(evidence_ref) else {
        return false;
    };
    let kind_matches = kind == expected_kind || (expected_kind == "read" && kind == "file_snippet");
    kind_matches && evidence_path_scope_matches(&path, target)
}

pub(crate) fn evidence_refs_have_anchor_scope(
    evidence_refs: &[String],
    expected_kind: &str,
    target: &str,
) -> bool {
    evidence_refs
        .iter()
        .any(|evidence_ref| evidence_ref_matches_anchor_scope(evidence_ref, expected_kind, target))
}

pub(crate) fn evidence_ref_mentions_scope(evidence_ref: &str, target: &str) -> bool {
    evidence_ref_kind_and_path(evidence_ref)
        .map(|(_, path)| evidence_path_scope_matches(&path, target))
        .unwrap_or_else(|| {
            let evidence_ref = clean_path_like(evidence_ref);
            let target = clean_path_like(target);
            !target.is_empty()
                && (evidence_ref.contains(&target)
                    || evidence_path_scope_matches(&evidence_ref, &target))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_matcher_closes_absolute_and_repo_relative_paths() {
        assert!(evidence_ref_matches_anchor_scope(
            "read:/Users/example/repo/RustAgent/Agent/src/core/state_frame_projection.rs",
            "read",
            "RustAgent/Agent/src/core/state_frame_projection.rs"
        ));
        assert!(evidence_ref_matches_anchor_scope(
            "hydrated_context: file_snippet:/Users/example/repo/RustAgent/Agent/src/core/state_frame_projection.rs source=tool:Read",
            "read",
            "RustAgent/Agent/src/core/state_frame_projection.rs"
        ));
        assert!(evidence_ref_matches_anchor_scope(
            "file_snippet:src/core/state_frame_projection.rs",
            "read",
            "/Users/example/repo/src/core/state_frame_projection.rs"
        ));
    }

    #[test]
    fn matching_target_scope_prefers_more_specific_file_targets() {
        let targets = vec![
            "/tmp/example-site".to_string(),
            "/tmp/example-site/README.md".to_string(),
        ];
        assert_eq!(
            matching_target_scope("/tmp/example-site/README.md", &targets),
            Some("/tmp/example-site/README.md")
        );
    }
}
