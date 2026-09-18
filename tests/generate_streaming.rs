//! The streaming annotations and the HTTP callers they drive.
//!
//! The committed `samples/schemas/streams.nexusrpc.yaml` output is the golden
//! for the whole shape; these assert the individual decisions, including the
//! cases that sample does not carry (an operation with no annotations, a
//! required wait hint).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use nexgen::generator::GenerationMode;
use nexgen::language::Language;
use nexgen::nexgen_config::NexgenConfig;
use nexgen::{GenerateRequest, generate_to_file};

const STREAM_CONTRACT: &str = r##"
nexusrpc: "1.0.0"
services:
  StreamService:
    fqn: example.streams.v1.StreamService
    description: Append records and read them back.
    operations:
      append:
        fqn: append
        description: Append one batch of records.
        input: { $ref: "#/$defs/AppendInput" }
        output: { $ref: "#/$defs/AppendOutput" }
      read:
        fqn: read
        description: Read the records after the caller's token.
        x-nexus-long-poll:
          wait-field: waitMs
          result-field: records
        input: { $ref: "#/$defs/ReadInput" }
        output: { $ref: "#/$defs/ReadOutput" }
$defs:
  AppendInput:
    type: object
    properties:
      stream: { type: string }
      payloads:
        type: array
        items: { type: string, contentEncoding: base64 }
        x-nexus-payload: true
    required: [stream]
    additionalProperties: false
  AppendOutput:
    type: object
    properties:
      cursor: { type: string, x-nexus-cursor: StreamCursor }
    additionalProperties: false
  ReadInput:
    type: object
    properties:
      stream: { type: string }
      afterToken: { type: string, x-nexus-cursor: StreamCursor }
      waitMs: { type: integer }
    required: [stream]
    additionalProperties: false
  RecordWire:
    type: object
    properties:
      token: { type: string, x-nexus-cursor: StreamCursor }
      frame: { type: string, contentEncoding: base64, x-nexus-payload: true }
    required: [token, frame]
    additionalProperties: false
  ReadOutput:
    type: object
    properties:
      records:
        type: array
        items: { $ref: "#/$defs/RecordWire" }
      nextToken: { type: string, x-nexus-cursor: StreamCursor }
    additionalProperties: false
"##;

/// The same service with no streaming annotations, to pin what the caller does
/// *not* carry.
const PLAIN_CONTRACT: &str = r##"
nexusrpc: "1.0.0"
services:
  PlainService:
    fqn: example.plain.v1.PlainService
    operations:
      ping:
        fqn: ping
        input: { $ref: "#/$defs/PingInput" }
        output: { $ref: "#/$defs/PingOutput" }
$defs:
  PingInput:
    type: object
    properties:
      note: { type: string }
    additionalProperties: false
  PingOutput:
    type: object
    properties:
      note: { type: string }
    additionalProperties: false
"##;

/// A required wait hint, which a target holds by value rather than behind an
/// optional.
const REQUIRED_WAIT_CONTRACT: &str = r##"
nexusrpc: "1.0.0"
services:
  StreamService:
    fqn: example.streams.v1.StreamService
    operations:
      read:
        fqn: read
        x-nexus-long-poll:
          wait-field: waitMs
          result-field: records
        input: { $ref: "#/$defs/ReadInput" }
        output: { $ref: "#/$defs/ReadOutput" }
$defs:
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

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_output_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let ordinal = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("nexgen-{label}-{nanos}-{ordinal}"))
}

/// Generates one contract and answers with the emitted files by name.
fn generate(
    language: Language,
    contract: &str,
    client: bool,
    label: &str,
) -> (PathBuf, Vec<(String, String)>) {
    let temp_dir = unique_output_path(label);
    let input_dir = temp_dir.join("input");
    fs::create_dir_all(&input_dir).unwrap();
    let input_path = input_dir.join("streams.nexusrpc.yaml");
    fs::write(&input_path, contract).unwrap();
    let output_path = temp_dir.join("streams");
    generate_to_file(&GenerateRequest {
        config: NexgenConfig {
            mode: GenerationMode::DefinitionsOnly,
            client,
            ..Default::default()
        },
        language,
        input_paths: vec![input_path],
        support_paths: Vec::new(),
        descriptor_paths: Vec::new(),
        output_path: output_path.clone(),
        format: false,
        java_package_name: None,
        ts_date_time_types: Default::default(),
    })
    .unwrap();
    (temp_dir, read_files(&output_path))
}

fn read_files(root: &Path) -> Vec<(String, String)> {
    let mut files = Vec::new();
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            files.push((
                path.file_name().unwrap().to_string_lossy().into_owned(),
                fs::read_to_string(&path).unwrap(),
            ));
        }
    }
    files.sort();
    files
}

fn file<'a>(files: &'a [(String, String)], name: &str) -> &'a str {
    files
        .iter()
        .find(|(file_name, _)| file_name == name)
        .map(|(_, contents)| contents.as_str())
        .unwrap_or_else(|| {
            panic!(
                "{name} should be generated; got {:?}",
                files.iter().map(|(name, _)| name).collect::<Vec<_>>()
            )
        })
}

fn has_file(files: &[(String, String)], name: &str) -> bool {
    files.iter().any(|(file_name, _)| file_name == name)
}

#[test]
fn python_declares_the_cursor_type_without_a_client() {
    let (temp_dir, files) = generate(
        Language::Python,
        STREAM_CONTRACT,
        false,
        "streaming-python-models",
    );
    let models = file(&files, "models.py");
    assert!(
        models.contains("StreamCursor = typing.NewType(\"StreamCursor\", str)"),
        "{models}"
    );
    assert!(
        models.contains("opaque resume token issued by the endpoint"),
        "{models}"
    );
    assert!(
        models.contains("resume strictly after the record it names"),
        "{models}"
    );
    // One declaration, however many members name it.
    assert_eq!(models.matches("typing.NewType(\"StreamCursor\"").count(), 1);
    // The cursor is a model-layer declaration, so it does not wait on a client.
    assert!(!has_file(&files, "client.py"));
    assert!(file(&files, "__init__.py").contains("StreamCursor"));
    fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn python_wire_models_keep_the_cursor_members_as_strings() {
    let (temp_dir, files) = generate(
        Language::Python,
        STREAM_CONTRACT,
        false,
        "streaming-python-wire",
    );
    let models = file(&files, "models.py");
    assert!(
        models.contains("after_token: str | None = None"),
        "{models}"
    );
    assert!(models.contains("next_token: str | None = None"), "{models}");
    fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn python_client_applies_the_codec_on_the_marked_members_only() {
    let (temp_dir, files) = generate(
        Language::Python,
        STREAM_CONTRACT,
        true,
        "streaming-python-client",
    );
    let client = file(&files, "client.py");
    assert!(
        client.contains("_APPEND_INPUT_PAYLOAD_SITES: tuple[_PayloadSteps, ...] = (\n    (\"payloads\", None,),\n)"),
        "{client}"
    );
    assert!(
        client.contains(
            "_READ_OUTPUT_PAYLOAD_SITES: tuple[_PayloadSteps, ...] = (\n    (\"records\", None, \"frame\",),\n)"
        ),
        "{client}"
    );
    // Encode on the way out of the marked input, decode on the way in.
    assert!(
        client.contains("wire, _APPEND_INPUT_PAYLOAD_SITES, self._codec.encode"),
        "{client}"
    );
    assert!(
        client.contains("answer, _READ_OUTPUT_PAYLOAD_SITES, self._codec.decode"),
        "{client}"
    );
    // `ReadInput` and `AppendOutput` carry no payload, so neither gets a table
    // or a codec call.
    assert!(!client.contains("_READ_INPUT_PAYLOAD_SITES"), "{client}");
    assert!(!client.contains("_APPEND_OUTPUT_PAYLOAD_SITES"), "{client}");
    // No codec configured means pass-through.
    assert_eq!(client.matches("if self._codec is not None:").count(), 2);
    fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn python_client_posts_to_the_wire_operation_name() {
    let (temp_dir, files) = generate(
        Language::Python,
        STREAM_CONTRACT,
        true,
        "streaming-python-path",
    );
    let client = file(&files, "client.py");
    assert!(
        client.contains("class StreamServiceHttpClient:"),
        "{client}"
    );
    assert!(client.contains("f\"{self._base_url}/append\""), "{client}");
    assert!(client.contains("f\"{self._base_url}/read\""), "{client}");
    assert!(
        client.contains("async def append(\n        self,\n        request: AppendInput,\n    ) -> AppendOutput:"),
        "{client}"
    );
    fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn python_client_loops_the_long_poll_operation_and_keeps_the_single_shot() {
    let (temp_dir, files) = generate(
        Language::Python,
        STREAM_CONTRACT,
        true,
        "streaming-python-poll",
    );
    let client = file(&files, "client.py");
    assert!(client.contains("async def read_until_records("), "{client}");
    assert!(client.contains("deadline: float,"), "{client}");
    assert!(
        client.contains("wait_ms=max(1, int(remaining * 1000))"),
        "{client}"
    );
    assert!(client.contains("if answer.records:"), "{client}");
    // The single-shot call is what the loop drives, and stays callable.
    assert!(
        client.contains("answer = await self.read(attempt)"),
        "{client}"
    );
    assert!(client.contains("async def read("), "{client}");
    // `append` declares no long-poll contract, so it gets no loop.
    assert!(!client.contains("append_until"), "{client}");
    fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn python_client_without_annotations_carries_no_codec_or_loop() {
    let (temp_dir, files) = generate(
        Language::Python,
        PLAIN_CONTRACT,
        true,
        "streaming-python-plain",
    );
    let client = file(&files, "client.py");
    assert!(client.contains("class PlainServiceHttpClient:"), "{client}");
    assert!(!client.contains("PayloadCodec"), "{client}");
    assert!(!client.contains("_apply_codec"), "{client}");
    assert!(!client.contains("import base64"), "{client}");
    assert!(!client.contains("import time"), "{client}");
    assert!(!client.contains("_until_"), "{client}");
    fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn go_declares_the_cursor_type_without_a_client() {
    let (temp_dir, files) = generate(Language::Go, STREAM_CONTRACT, false, "streaming-go-models");
    let models = file(&files, "streams.go");
    assert!(models.contains("type StreamCursor string"), "{models}");
    assert!(
        models.contains("// StreamCursor is an opaque resume token issued by the endpoint."),
        "{models}"
    );
    assert_eq!(models.matches("type StreamCursor string").count(), 1);
    assert!(!has_file(&files, "client.go"));
    fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn go_wire_models_keep_the_cursor_members_as_strings() {
    let (temp_dir, files) = generate(Language::Go, STREAM_CONTRACT, false, "streaming-go-wire");
    let models = file(&files, "streams.go");
    assert!(
        models.contains("AfterToken *string `json:\"afterToken,omitempty\"`"),
        "{models}"
    );
    fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn go_client_applies_the_codec_on_the_marked_members_only() {
    let (temp_dir, files) = generate(Language::Go, STREAM_CONTRACT, true, "streaming-go-client");
    let client = file(&files, "client.go");
    assert!(
        client.contains(
            "var appendInputPayloadSites = [][]payloadStep{\n\t{{member: \"payloads\"}, {each: true}},\n}"
        ),
        "{client}"
    );
    assert!(
        client.contains(
            "var readOutputPayloadSites = [][]payloadStep{\n\t{{member: \"records\"}, {each: true}, {member: \"frame\"}},\n}"
        ),
        "{client}"
    );
    assert!(
        client.contains("applyPayloadCodec(ctx, body, appendInputPayloadSites, c.codec.Encode)"),
        "{client}"
    );
    assert!(
        client.contains("applyPayloadCodec(ctx, raw, readOutputPayloadSites, c.codec.Decode)"),
        "{client}"
    );
    assert!(!client.contains("readInputPayloadSites"), "{client}");
    assert_eq!(client.matches("if c.codec != nil {").count(), 2);
    fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn go_client_posts_to_the_wire_operation_name() {
    let (temp_dir, files) = generate(Language::Go, STREAM_CONTRACT, true, "streaming-go-path");
    let client = file(&files, "client.go");
    assert!(
        client.contains("type StreamServiceHTTPClient struct"),
        "{client}"
    );
    assert!(
        client.contains("func NewStreamServiceHTTPClient(baseURL string, options *StreamServiceHTTPClientOptions)"),
        "{client}"
    );
    assert!(client.contains("c.post(ctx, \"append\", body)"), "{client}");
    assert!(client.contains("c.post(ctx, \"read\", body)"), "{client}");
    assert!(
        client.contains(
            "func (c *StreamServiceHTTPClient) Append(ctx context.Context, request AppendInput) (AppendOutput, error)"
        ),
        "{client}"
    );
    fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn go_client_loops_the_long_poll_operation_through_a_pointer_wait_hint() {
    let (temp_dir, files) = generate(Language::Go, STREAM_CONTRACT, true, "streaming-go-poll");
    let client = file(&files, "client.go");
    assert!(
        client.contains(
            "func (c *StreamServiceHTTPClient) ReadUntilRecords(ctx context.Context, request ReadInput, deadline time.Duration) (ReadOutput, error)"
        ),
        "{client}"
    );
    // An optional wait hint is a pointer field, so the attempt takes its address.
    assert!(client.contains("attempt.WaitMs = &wait"), "{client}");
    assert!(client.contains("if len(answer.Records) > 0 {"), "{client}");
    assert!(
        client.contains("answer, err := c.Read(ctx, attempt)"),
        "{client}"
    );
    assert!(!client.contains("AppendUntil"), "{client}");
    fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn go_client_assigns_a_required_wait_hint_by_value() {
    let (temp_dir, files) = generate(
        Language::Go,
        REQUIRED_WAIT_CONTRACT,
        true,
        "streaming-go-required-wait",
    );
    let client = file(&files, "client.go");
    assert!(client.contains("attempt.WaitMs = wait"), "{client}");
    assert!(!client.contains("attempt.WaitMs = &wait"), "{client}");
    fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn go_client_without_annotations_carries_no_codec_or_loop() {
    let (temp_dir, files) = generate(Language::Go, PLAIN_CONTRACT, true, "streaming-go-plain");
    let client = file(&files, "client.go");
    assert!(
        client.contains("type PlainServiceHTTPClient struct"),
        "{client}"
    );
    assert!(!client.contains("PayloadCodec"), "{client}");
    assert!(!client.contains("applyPayloadCodec"), "{client}");
    assert!(!client.contains("encoding/base64"), "{client}");
    assert!(!client.contains("\"time\""), "{client}");
    assert!(!client.contains("Until"), "{client}");
    fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn the_other_targets_ignore_the_streaming_annotations() {
    // The annotations are always accepted, so one contract stays portable across
    // every target. Only the two targets with a caller act on them.
    for language in [Language::TypeScript, Language::Java] {
        let temp_dir = unique_output_path("streaming-other-targets");
        let input_dir = temp_dir.join("input");
        fs::create_dir_all(&input_dir).unwrap();
        let input_path = input_dir.join("streams.nexusrpc.yaml");
        fs::write(&input_path, STREAM_CONTRACT).unwrap();
        let output_path = temp_dir.join("streams");
        generate_to_file(&GenerateRequest {
            config: NexgenConfig::default(),
            language,
            input_paths: vec![input_path],
            support_paths: Vec::new(),
            descriptor_paths: Vec::new(),
            output_path: output_path.clone(),
            format: false,
            java_package_name: (language == Language::Java)
                .then(|| "json_schema.streams".to_string()),
            ts_date_time_types: Default::default(),
        })
        .unwrap_or_else(|error| panic!("{language:?} should generate: {error}"));
        for (name, contents) in read_files(&output_path) {
            assert!(
                !contents.contains("x-nexus-"),
                "{language:?} {name} leaked an annotation keyword"
            );
        }
        fs::remove_dir_all(temp_dir).unwrap();
    }
}
