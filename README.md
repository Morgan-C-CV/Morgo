# Morgo Agent

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Morgo Agent** is a high-performance, developer-centric agentic runtime built in Rust. It serves as the core execution engine for the Morgo coding assistant ecosystem, featuring a beautiful terminal TUI, safe filesystem/command sandboxing, Model Context Protocol (MCP) tool integration, and a sophisticated Boss-Worker task coordinator.

---

## ✨ Features

* **Terminal UI (TUI)**: A responsive terminal interface featuring dynamic autocomplete suggestions, command history, and clear visual task progress.
* **Boss-Worker Architecture**: An autonomous execution paradigm where the **Boss** plans steps and the **Worker** executes them, equipped with self-repair loops and state memory.
* **Model Context Protocol (MCP)**: Native integration for connecting to external tool servers dynamically.
* **Secure Sandbox**: Flexible permissions control with default, plan-only, and accept-edits policies, safeguarding your local filesystem and terminal execution.
* **Observability & Logging**: Integrated cost tracking, telemetry metrics, and runtime audit trails.

---

## 🛠️ Getting Started

### 1. Install the Release Binary
Install the latest GitHub release binary:
```bash
curl -fsSL https://raw.githubusercontent.com/Morgan-C-CV/Morgo/main/scripts/install.sh | sh
```

The installer downloads the right binary for your OS/CPU and installs it to
`~/.local/bin/morgo` by default. If `morgo` is not found after installation, add
this to your shell profile:
```bash
export PATH="$HOME/.local/bin:$PATH"
```

For zsh users, you can make that permanent with:
```bash
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

You can verify the binary is available with:
```bash
which morgo
```

### 2. Configure Provider Credentials
Morgo needs an LLM provider API key before it can answer inside the TUI. On
first interactive startup, run:

```bash
morgo
```

If no provider has been configured yet, Morgo opens a setup guide that asks for
the provider, model, and API key. The guide writes:

* `~/.morgo/models.toml`: model profiles and the active profile.
* `~/.morgo/env`: local environment variables loaded before Morgo starts.

The OpenAI setup default model is `gpt-5.5`. You can inspect the available
profiles from inside the TUI with:

```text
/model list
```

If your network requires a proxy, use either environment variables in
`~/.morgo/env`:

```bash
RUST_AGENT_PROXY_URL="http://127.0.0.1:7890"
RUST_AGENT_NO_PROXY="localhost,127.0.0.1,::1"
```

or uncomment the profile-level proxy fields in `~/.morgo/models.toml`:

```toml
[profiles.openai]
proxy_url = "http://127.0.0.1:7890"
no_proxy = "localhost,127.0.0.1,::1"
```

This is the recommended fix when OpenAI requests fail with errors such as
`tls handshake eof`.

For one-off usage, export the key directly in your shell:
```bash
export OPENAI_API_KEY="your-api-key"
```

Or use OpenRouter / Anthropic:
```bash
export OPENROUTER_API_KEY="your-api-key"
export ANTHROPIC_API_KEY="your-api-key"
```

When running from this repository, you can also keep credentials in a `.env`
file at the repository root:
```bash
cp .env.example .env
# Edit .env to set your API keys (e.g., Gemini, OpenRouter, Anthropic, etc.)
```

Then load it before starting Morgo:
```bash
cd /path/to/Morgo
source ./load-env.sh
```

For OpenAI-compatible providers, you can explicitly select the provider and
model with:
```bash
export RUST_AGENT_PROVIDER_ID=openai
export RUST_AGENT_PROVIDER_API_KEY="$OPENAI_API_KEY"
export RUST_AGENT_PROVIDER_BASE_URL="https://api.openai.com"
export RUST_AGENT_PROVIDER_MODEL="gpt-5.5"
```

### 3. Start the Interactive TUI
To start the Morgo interactive TUI, run:
```bash
morgo
```

Running `morgo` without extra arguments starts the interactive terminal UI by
default.

### 4. Run from Source
If you are developing Morgo locally, use the Rust toolchain and run:
```bash
cd RustAgent/Agent
cargo run --bin morgo
```

The published binary name is `morgo`. If you want the capitalized `Morgo`
command on a case-sensitive shell, create an installation alias or symlink that
points to the same binary.

### 5. Publishing a Release
Create and push a release tag:
```bash
git tag morgo-v0.1.0
git push origin morgo-v0.1.0
```

GitHub Actions will build and attach release archives for:
* `aarch64-apple-darwin`
* `x86_64-apple-darwin`
* `x86_64-unknown-linux-gnu`

---

## 📂 Project Structure

* `src/bootstrap/`: Runtime startup, environment detection, and session initialization.
* `src/coordinator/`: Core agent decision-making loop (BossCoordinator).
* `src/core/`: Housekeeping, diagnostics, and workspace utilities.
* `src/security/`: Filesystem access policies and tool validation schemas.
* `src/task/`: Local process, shell executor, and background task manager.
* `src/tool/`: Multi-source tool registry and calling conventions.
* `src/plugins/`: Extension loader and MCP server configurations.

---

## 🤝 Contributing

Contributions are welcome! Please feel free to open issues or submit pull requests.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.
