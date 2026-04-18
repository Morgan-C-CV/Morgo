#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BashScan {
    pub words: Vec<String>,
    pub operators: Vec<ShellOperator>,
    pub has_command_substitution: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellOperator {
    Pipe,
    AndIf,
    OrIf,
    Sequence,
    RedirectWrite,
    RedirectAppend,
    RedirectRead,
    HereDoc,
    Background,
    CommandSubstitution,
    BacktickSubstitution,
}

impl ShellOperator {
    pub fn display(self) -> &'static str {
        match self {
            Self::Pipe => "|",
            Self::AndIf => "&&",
            Self::OrIf => "||",
            Self::Sequence => ";",
            Self::RedirectWrite => ">",
            Self::RedirectAppend => ">>",
            Self::RedirectRead => "<",
            Self::HereDoc => "<<",
            Self::Background => "&",
            Self::CommandSubstitution => "$(",
            Self::BacktickSubstitution => "`",
        }
    }

    pub fn reason_code(self) -> &'static str {
        match self {
            Self::Pipe => "shell_operator.pipe",
            Self::AndIf => "shell_operator.and_if",
            Self::OrIf => "shell_operator.or_if",
            Self::Sequence => "shell_operator.sequence",
            Self::RedirectWrite => "shell_operator.redirect_write",
            Self::RedirectAppend => "shell_operator.redirect_append",
            Self::RedirectRead => "shell_operator.redirect_read",
            Self::HereDoc => "shell_operator.heredoc",
            Self::Background => "shell_operator.background",
            Self::CommandSubstitution => "command_substitution",
            Self::BacktickSubstitution => "command_substitution.backtick",
        }
    }

    pub fn is_redirection(self) -> bool {
        matches!(
            self,
            Self::RedirectWrite | Self::RedirectAppend | Self::RedirectRead | Self::HereDoc
        )
    }
}

pub fn scan_bash_command(command: &str) -> BashScan {
    let mut words = Vec::new();
    let mut operators = Vec::new();
    let mut current = String::new();
    let mut chars = command.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;

    while let Some(ch) = chars.next() {
        if in_single {
            if ch == '\'' {
                in_single = false;
            } else {
                current.push(ch);
            }
            continue;
        }

        if ch == '\\' {
            if let Some(next) = chars.next() {
                current.push(next);
            }
            continue;
        }

        if in_double {
            match ch {
                '"' => in_double = false,
                '$' if chars.peek() == Some(&'(') => {
                    chars.next();
                    push_word(&mut words, &mut current);
                    operators.push(ShellOperator::CommandSubstitution);
                    skip_command_substitution(&mut chars);
                }
                '`' => {
                    push_word(&mut words, &mut current);
                    operators.push(ShellOperator::BacktickSubstitution);
                    skip_backtick_substitution(&mut chars);
                }
                _ => current.push(ch),
            }
            continue;
        }

        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            c if c.is_whitespace() => push_word(&mut words, &mut current),
            '$' if chars.peek() == Some(&'(') => {
                chars.next();
                push_word(&mut words, &mut current);
                operators.push(ShellOperator::CommandSubstitution);
                skip_command_substitution(&mut chars);
            }
            '`' => {
                push_word(&mut words, &mut current);
                operators.push(ShellOperator::BacktickSubstitution);
                skip_backtick_substitution(&mut chars);
            }
            '|' => {
                push_word(&mut words, &mut current);
                if chars.peek() == Some(&'|') {
                    chars.next();
                    operators.push(ShellOperator::OrIf);
                } else {
                    operators.push(ShellOperator::Pipe);
                }
            }
            '&' => {
                push_word(&mut words, &mut current);
                if chars.peek() == Some(&'&') {
                    chars.next();
                    operators.push(ShellOperator::AndIf);
                } else {
                    operators.push(ShellOperator::Background);
                }
            }
            ';' => {
                push_word(&mut words, &mut current);
                operators.push(ShellOperator::Sequence);
            }
            '>' => {
                push_word(&mut words, &mut current);
                if chars.peek() == Some(&'>') {
                    chars.next();
                    operators.push(ShellOperator::RedirectAppend);
                } else {
                    operators.push(ShellOperator::RedirectWrite);
                }
            }
            '<' => {
                push_word(&mut words, &mut current);
                if chars.peek() == Some(&'<') {
                    chars.next();
                    operators.push(ShellOperator::HereDoc);
                } else {
                    operators.push(ShellOperator::RedirectRead);
                }
            }
            _ => current.push(ch),
        }
    }

    push_word(&mut words, &mut current);
    BashScan {
        words,
        has_command_substitution: operators.iter().any(|operator| {
            matches!(
                operator,
                ShellOperator::CommandSubstitution | ShellOperator::BacktickSubstitution
            )
        }),
        operators,
    }
}

fn push_word(words: &mut Vec<String>, current: &mut String) {
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        words.push(trimmed.to_string());
    }
    current.clear();
}

fn skip_command_substitution<I>(chars: &mut std::iter::Peekable<I>)
where
    I: Iterator<Item = char>,
{
    let mut depth = 1usize;
    let mut in_single = false;
    let mut in_double = false;
    while let Some(ch) = chars.next() {
        if in_single {
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        if ch == '\\' {
            chars.next();
            continue;
        }
        if in_double {
            match ch {
                '"' => in_double = false,
                '$' if chars.peek() == Some(&'(') => {
                    chars.next();
                    depth += 1;
                }
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            continue;
        }
        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '$' if chars.peek() == Some(&'(') => {
                chars.next();
                depth += 1;
            }
            ')' => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            _ => {}
        }
    }
}

fn skip_backtick_substitution<I>(chars: &mut std::iter::Peekable<I>)
where
    I: Iterator<Item = char>,
{
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            chars.next();
            continue;
        }
        if ch == '`' {
            break;
        }
    }
}
