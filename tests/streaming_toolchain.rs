//! The generated streaming callers, through the real toolchains.
//!
//! `generate_streaming.rs` asserts the individual emission decisions as
//! substrings. That cannot catch output that reads correctly and does not
//! compile, or that compiles and decodes a payload with the wrong alphabet, so
//! the shapes the samples do not carry are built and run here: a required wait
//! hint, and a `base64url` payload beside a `base64` one.

mod toolchain;

use std::fs;
use std::path::PathBuf;

use nexgen::generator::GenerationMode;
use nexgen::nexgen_config::NexgenConfig;
use nexgen::{GenerateRequest, generate_to_file};
use toolchain::{Target, Workspace, command, prepare_go_module, python_interpreter, run};

/// One payload on each alphabet, a required wait hint, and a long poll, so one
/// contract covers every shape the committed sample leaves out.
const CONTRACT: &str = r##"
nexusrpc: "1.0.0"
services:
  StreamService:
    fqn: example.streams.v1.StreamService
    operations:
      append:
        fqn: append
        input: { $ref: "#/$defs/AppendInput" }
        output: { $ref: "#/$defs/AppendOutput" }
      read:
        fqn: read
        x-nexus-long-poll:
          wait-field: waitMs
          result-field: records
        input: { $ref: "#/$defs/ReadInput" }
        output: { $ref: "#/$defs/ReadOutput" }
$defs:
  AppendInput:
    type: object
    properties:
      padded: { type: string, contentEncoding: base64, x-nexus-payload: true }
      urlsafe: { type: string, contentEncoding: base64url, x-nexus-payload: true }
    additionalProperties: false
  AppendOutput:
    type: object
    properties:
      echoed: { type: string, contentEncoding: base64url, x-nexus-payload: true }
    additionalProperties: false
  ReadInput:
    type: object
    properties:
      waitMs: { type: integer }
    required: [waitMs]
    additionalProperties: false
  ReadOutput:
    type: object
    properties:
      records: { type: array, items: { type: string } }
    additionalProperties: false
"##;

/// An operation that hands back a stream reference and a read that takes one,
/// so the emitted converter is run against the SDK type it builds.
const STREAM_REF_CONTRACT: &str = r##"
nexusrpc: "1.0.0"
services:
  ScoresService:
    fqn: example.scores.v1.ScoresService
    operations:
      startGame:
        fqn: startGame
        input: { $ref: "#/$defs/GameRequest" }
        output: { $ref: "#/$defs/StreamRef" }
      read:
        fqn: read
        input: { $ref: "#/$defs/ReadInput" }
        output: { $ref: "#/$defs/ReadOutput" }
$defs:
  GameRequest:
    type: object
    properties:
      gameId: { type: string }
    required: [gameId]
    additionalProperties: false
  StreamRef:
    type: object
    x-nexus-stream-ref: true
    properties:
      owner: { type: string, enum: [workflow, activity, standalone] }
      workflow_id: { type: string }
      run_id: { type: string }
      activity_id: { type: string }
      stream_id: { type: string }
      topic: { type: string }
    required: [owner, topic]
    additionalProperties: false
  ReadInput:
    type: object
    properties:
      stream: { $ref: "#/$defs/StreamRef" }
      afterToken: { type: string }
    required: [stream]
    additionalProperties: false
  ReadOutput:
    type: object
    properties:
      records: { type: array, items: { type: string } }
    additionalProperties: false
"##;

fn write_contract(workspace: &Workspace) -> PathBuf {
    let path = workspace.root().join("streams.nexusrpc.yaml");
    fs::write(&path, CONTRACT).expect("write the contract");
    path
}

/// Generates with the caller enabled, which the shared workspace helper does
/// not do: its request is the definitions-only one the conformance drivers use.
fn generate_client(workspace: &Workspace, target: Target, dir: &str) -> PathBuf {
    let contract = write_contract(workspace);
    generate_client_from(workspace, target, dir, contract)
}

/// Generates `dir` from a contract already written into the workspace.
fn generate_client_from(
    workspace: &Workspace,
    target: Target,
    dir: &str,
    contract: PathBuf,
) -> PathBuf {
    let output_path = workspace.package_path(target, dir);
    fs::create_dir_all(output_path.parent().expect("a package parent"))
        .expect("create the package parent");
    generate_to_file(&GenerateRequest {
        config: NexgenConfig {
            mode: GenerationMode::DefinitionsOnly,
            client: true,
            ..Default::default()
        },
        language: target.language(),
        input_paths: vec![contract],
        support_paths: Vec::new(),
        descriptor_paths: Vec::new(),
        output_path: output_path.clone(),
        format: false,
        java_package_name: None,
        ts_date_time_types: Default::default(),
    })
    .expect("generate the caller");
    output_path
}

/// The walker's alphabet choice in Go, exercised in the generated package so it
/// can reach the unexported walker.
const GO_DRIVER: &str = r#"package streams

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestEachAlphabetRoundTrips(t *testing.T) {
	raw := []byte{250, 251, 252, 253, 254, 255}
	padded := base64.StdEncoding.EncodeToString(raw)
	urlsafe := base64.RawURLEncoding.EncodeToString(raw)
	if padded == urlsafe {
		t.Fatal("the alphabets must differ for this to prove anything")
	}
	body, err := json.Marshal(map[string]any{"padded": padded, "urlsafe": urlsafe})
	if err != nil {
		t.Fatal(err)
	}
	var seen [][]byte
	rewritten, err := applyPayloadCodec(
		context.Background(),
		body,
		appendInputPayloadSites,
		func(_ context.Context, payloads [][]byte) ([][]byte, error) {
			seen = payloads
			return payloads, nil
		},
	)
	if err != nil {
		t.Fatalf("the walker refused a payload the contract declares: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("want two payloads, got %d", len(seen))
	}
	for index, payload := range seen {
		if !bytes.Equal(payload, raw) {
			t.Fatalf("payload %d decoded to %v, want %v", index, payload, raw)
		}
	}
	var back map[string]string
	if err := json.Unmarshal(rewritten, &back); err != nil {
		t.Fatal(err)
	}
	if back["padded"] != padded || back["urlsafe"] != urlsafe {
		t.Fatalf("re-encoded to %v, want the wire it came from", back)
	}
}

func TestARetryableStatusIsTold(t *testing.T) {
	for status, want := range map[int]bool{429: true, 503: true, 400: false} {
		err := &HTTPStatusError{URL: "u", StatusCode: status}
		if err.Retryable() != want {
			t.Fatalf("status %d: retryable %v, want %v", status, err.Retryable(), want)
		}
	}
}
"#;

#[test]
fn the_generated_go_caller_compiles_and_walks_each_alphabet() {
    let workspace = Workspace::new("streaming-go-toolchain");
    let root = prepare_go_module(&workspace).expect("prepare the go module");
    let package = generate_client(&workspace, Target::Go, "streams");
    fs::write(package.join("alphabet_test.go"), GO_DRIVER).expect("write the driver");

    run(command("go")
        .current_dir(&root)
        .env("GOFLAGS", "-mod=mod")
        .args(["vet", "./..."]))
    .expect_ok("go vet over the generated streaming caller");
    run(command("go")
        .current_dir(&root)
        .env("GOFLAGS", "-mod=mod")
        .args(["test", "./..."]))
    .expect_ok("go test over the generated streaming caller");
}

/// The walker's alphabet choice, exercised rather than read.
///
/// The two alphabets render the same bytes as different strings, so a walker
/// that assumed one would either fail to decode or hand the codec bytes the
/// contract never carried.
const PYTHON_DRIVER: &str = r#"
import asyncio
import base64
import pathlib
import re
import sys

source = pathlib.Path(sys.argv[1]).read_text()
# The walker is self-contained; the model imports below it need a runtime the
# generated caller does not exercise here.
body = re.sub(r"from \.models import \([^)]*\)", "", source, count=1)
body = body.split("class StreamServiceHTTPClient")[0]
namespace = {}
exec(compile(body, "client.py", "exec"), namespace)

raw = bytes(range(250, 256))
padded = base64.b64encode(raw).decode()
urlsafe = base64.urlsafe_b64encode(raw).decode().rstrip("=")
assert padded != urlsafe, "the alphabets must differ for this to prove anything"

wire = {"padded": padded, "urlsafe": urlsafe}
seen = []


async def passthrough(payloads):
    seen.extend(payloads)
    return payloads


asyncio.run(
    namespace["_apply_codec"](
        wire, namespace["_APPEND_INPUT_PAYLOAD_SITES"], passthrough
    )
)
assert seen == [raw, raw], seen
assert wire == {"padded": padded, "urlsafe": urlsafe}, wire

status = namespace["HTTPStatusError"]
assert status("u", 503, "").retryable
assert status("u", 429, "").retryable
assert not status("u", 400, "").retryable
print("ok")
"#;

#[test]
fn the_generated_python_caller_runs_each_alphabet() {
    let workspace = Workspace::new("streaming-python-toolchain");
    let package = generate_client(&workspace, Target::Python, "streams");

    let driver = workspace.root().join("drive.py");
    fs::write(&driver, PYTHON_DRIVER).expect("write the driver");

    let interpreter = python_interpreter();
    run(command(&interpreter.to_string_lossy())
        .arg("-m")
        .arg("compileall")
        .arg("-q")
        .arg(&package))
    .expect_ok("compile the generated streaming caller");
    let output = run(command(&interpreter.to_string_lossy())
        .arg(&driver)
        .arg(package.join("client.py")))
    .expect_ok("run the generated payload walker");
    assert!(output.contains("ok"), "{output}");
}

/// Runs the emitted stream-reference converter against the type it builds.
///
/// The generated module imports `temporalio.streams.StreamRef` by name, which
/// the SDK release the sample environment installs does not carry yet, so the
/// driver stands one in with the members the contract declares. What is
/// checked is the emitted code's contract with that type: built by keyword
/// from the wire members, read back by attribute, and handed through unchanged
/// where a wire dataclass would have been copied.
const PYTHON_STREAM_REF_DRIVER: &str = r#"
import dataclasses
import importlib
import pathlib
import sys
import types

package = pathlib.Path(sys.argv[1])
sys.path.insert(0, str(package.parent))


@dataclasses.dataclass(frozen=True)
class StreamRef:
    owner: str
    topic: str
    workflow_id: str | None = None
    run_id: str | None = None
    activity_id: str | None = None
    stream_id: str | None = None


import temporalio

streams = types.ModuleType("temporalio.streams")
streams.StreamRef = StreamRef
sys.modules["temporalio.streams"] = streams
temporalio.streams = streams

models = importlib.import_module(f"{package.name}.models")
assert models.StreamRef is StreamRef, models.StreamRef

ref = StreamRef(owner="workflow", topic="scores", workflow_id="game-1")
request = models.ReadInput(stream=ref, after_token="t1")
converter = models._ReadInputTransferTypeConverter()
wire = converter.to_transfer_type(request)
assert wire == {
    "stream": {"owner": "workflow", "workflow_id": "game-1", "topic": "scores"},
    "afterToken": "t1",
}, wire
back = converter.from_transfer_type(wire, models.ReadInput)
assert type(back.stream) is StreamRef, type(back.stream)
assert back.stream == ref, back.stream

# The reference alone crosses as an operation result.
alone = models._StreamRefTransferTypeConverter()
assert alone.from_transfer_type(alone.to_transfer_type(ref), StreamRef) == ref
print("ok")
"#;

#[test]
fn the_generated_python_stream_reference_round_trips_through_the_sdk_type() {
    let workspace = Workspace::new("streaming-python-stream-ref-toolchain");
    let contract = workspace.root().join("scores.nexusrpc.yaml");
    fs::write(&contract, STREAM_REF_CONTRACT).expect("write the contract");
    let package = generate_client_from(&workspace, Target::Python, "scores", contract);
    let driver = workspace.root().join("drive_stream_ref.py");
    fs::write(&driver, PYTHON_STREAM_REF_DRIVER).expect("write the driver");

    let interpreter = python_interpreter();
    run(command(&interpreter.to_string_lossy())
        .arg("-m")
        .arg("compileall")
        .arg("-q")
        .arg(&package))
    .expect_ok("compile the generated stream reference module");
    let output = run(command(&interpreter.to_string_lossy())
        .arg(&driver)
        .arg(&package))
    .expect_ok("round-trip the stream reference");
    assert!(output.contains("ok"), "{output}");
}
