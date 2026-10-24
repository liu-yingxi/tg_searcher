//! CLI-level regression tests: configuration loading, env expansion and
//! validation. These never contact Telegram.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn temp_dir() -> tempfile::TempDir {
    let base = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp");
    fs::create_dir_all(&base).unwrap();
    tempfile::Builder::new()
        .prefix("cli-test-")
        .tempdir_in(base)
        .unwrap()
}

fn write_config(dir: &Path, token: &str, admin: &str) -> PathBuf {
    let path = dir.join("searcher.yaml");
    let yaml = format!(
        r#"
common:
  name: cli_test
  runtime_dir: "{runtime}"
  api_id: 12345
  api_hash: testhash
sessions:
  - name: alice
    phone: '+15551234567'
backends:
  - id: pub
    use_session: alice
frontends:
  - type: bot
    id: public
    use_backend: pub
    config:
      admin_id: {admin}
      bot_token: "{token}"
      page_len: 5
      no_redis: true
"#,
        runtime = dir.join("data").to_string_lossy().replace('\\', "/"),
    );
    fs::write(&path, yaml).unwrap();
    path
}

fn run(args: &[&str], envs: &[(&str, &str)]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tg-searcher"));
    command.args(args);
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().unwrap()
}

#[test]
fn check_valid_configuration() {
    let temp = temp_dir();
    let path = write_config(temp.path(), "123:test-token", "42");
    let output = run(&["-f", path.to_str().unwrap(), "--check"], &[]);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("configuration OK"));
    assert!(stdout.contains("threshold=0.50"));
}

#[test]
fn missing_token_env_fails_clearly() {
    let temp = temp_dir();
    let path = write_config(temp.path(), "${TGS_MISSING_TOKEN_XYZ}", "42");
    let output = run(&["-f", path.to_str().unwrap(), "--check"], &[]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("TGS_MISSING_TOKEN_XYZ") && stderr.contains("not set"),
        "stderr was: {stderr}"
    );
}

#[test]
fn env_interpolation_and_threshold_override() {
    let temp = temp_dir();
    let path = write_config(temp.path(), "${TGS_CLI_TOKEN}", "${TGS_CLI_ADMIN}");
    let output = run(
        &["-f", path.to_str().unwrap(), "--check"],
        &[
            ("TGS_CLI_TOKEN", "123:abc"),
            ("TGS_CLI_ADMIN", "42"),
            ("TG_SEARCH_FUZZY_THRESHOLD", "0.77"),
        ],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("threshold=0.77"), "stdout was: {stdout}");
}

#[test]
fn unknown_option_is_rejected() {
    let output = run(&["--definitely-not-an-option"], &[]);
    assert!(!output.status.success());
}

#[test]
fn invalid_threshold_env_is_clamped() {
    let temp = temp_dir();
    let path = write_config(temp.path(), "123:abc", "42");
    let output = run(
        &["-f", path.to_str().unwrap(), "--check"],
        &[("TG_SEARCH_FUZZY_THRESHOLD", "9.9")],
    );
    // The override clamps to 1.0 rather than failing.
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("threshold=1.00"));
}
