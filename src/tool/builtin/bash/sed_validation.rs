#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SedSafety {
    NotSed,
    SafeReadOnly,
    SafeInPlace,
    Unsafe(String),
}

pub fn analyze_sed_safety(command: &str) -> SedSafety {
    let trimmed = command.trim();
    if !trimmed.starts_with("sed") {
        return SedSafety::NotSed;
    }

    let tokens = trimmed.split_whitespace().collect::<Vec<_>>();
    let in_place = tokens
        .iter()
        .any(|token| *token == "-i" || token.starts_with("-i"));
    let expr = tokens
        .windows(2)
        .find(|window| window[0] == "-e")
        .map(|window| window[1])
        .or_else(|| tokens.iter().copied().find(|token| token.starts_with("s/")));

    let Some(expr) = expr else {
        return SedSafety::Unsafe("sed command is missing an expression".into());
    };

    if expr.contains('e') || expr.contains('w') {
        return SedSafety::Unsafe("sed expression uses shell execution or file writes".into());
    }

    if in_place {
        SedSafety::SafeInPlace
    } else {
        SedSafety::SafeReadOnly
    }
}

pub fn is_safe_sed(command: &str) -> bool {
    !matches!(analyze_sed_safety(command), SedSafety::Unsafe(_))
}
