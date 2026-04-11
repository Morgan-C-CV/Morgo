pub fn normalized_command_variants(command: &str) -> Vec<String> {
    let trimmed = command.trim();
    let mut variants = vec![trimmed.to_string()];

    let mut tokens = trimmed.split_whitespace().peekable();
    while let Some(token) = tokens.peek().copied() {
        if token.contains('=') && !token.starts_with("./") && !token.starts_with('/') {
            tokens.next();
            let rest = tokens.clone().collect::<Vec<_>>().join(" ");
            if !rest.is_empty() {
                variants.push(rest.clone());
            }
            continue;
        }
        if matches!(token, "env" | "timeout" | "command") {
            tokens.next();
            if token == "timeout" {
                let _ = tokens.next();
            }
            let rest = tokens.clone().collect::<Vec<_>>().join(" ");
            if !rest.is_empty() {
                variants.push(rest.clone());
            }
            continue;
        }
        break;
    }

    variants.sort();
    variants.dedup();
    variants
}

pub fn command_matches_rule(command: &str, rule: &str) -> bool {
    if rule == command {
        return true;
    }
    if let Some(prefix) = rule.strip_suffix('*') {
        return command.starts_with(prefix);
    }
    command.contains(rule)
}
