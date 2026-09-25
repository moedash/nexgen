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

/// Generates two input files into one output, the way a closure of them is
/// generated, and answers with the emitted files by name.
fn generate_pair(
    language: Language,
    contracts: &[(&str, &str)],
    client: bool,
    label: &str,
) -> (PathBuf, Vec<(String, String)>) {
    let temp_dir = unique_output_path(label);
    let input_dir = temp_dir.join("input");
    fs::create_dir_all(&input_dir).unwrap();
    let mut input_paths = Vec::new();
    for (name, contract) in contracts {
        let input_path = input_dir.join(name);
        fs::write(&input_path, contract).unwrap();
        input_paths.push(input_path);
    }
    let output_path = temp_dir.join("streams");
    generate_to_file(&GenerateRequest {
        config: NexgenConfig {
            mode: GenerationMode::DefinitionsOnly,
            client,
            ..Default::default()
        },
        language,
        input_paths,
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

/// Generates one contract and answers with the loader's refusal, if it made one.
fn refusal(contract: &str, label: &str) -> String {
    let temp_dir = unique_output_path(label);
    let input_dir = temp_dir.join("input");
    fs::create_dir_all(&input_dir).unwrap();
    let input_path = input_dir.join("streams.nexusrpc.yaml");
    fs::write(&input_path, contract).unwrap();
    let outcome = generate_to_file(&GenerateRequest {
        config: NexgenConfig {
            mode: GenerationMode::DefinitionsOnly,
            client: true,
            ..Default::default()
        },
        language: Language::Go,
        input_paths: vec![input_path],
        support_paths: Vec::new(),
        descriptor_paths: Vec::new(),
        output_path: temp_dir.join("streams"),
        format: false,
        java_package_name: None,
        ts_date_time_types: Default::default(),
    });
    let message = match outcome {
        Ok(()) => panic!("the loader accepted a contract it cannot lower"),
        Err(error) => error.to_string(),
    };
    fs::remove_dir_all(&temp_dir).ok();
    message
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
        client.contains(
            "_APPEND_INPUT_PAYLOAD_SITES: tuple[_PayloadSite, ...] = (\n    _PayloadSite((\"payloads\", None,), False),\n)"
        ),
        "{client}"
    );
    assert!(
        client.contains(
            "_READ_OUTPUT_PAYLOAD_SITES: tuple[_PayloadSite, ...] = (\n    _PayloadSite((\"records\", None, \"frame\",), False),\n)"
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
    assert!(client.contains("wait_ms=int(wait * 1000)"), "{client}");
    assert!(client.contains("if answer.records:"), "{client}");
    // The single-shot call is what the loop drives, and stays callable.
    assert!(
        client.contains("answer = await self.read(attempt)"),
        "{client}"
    );
    // The ask is clamped to the transport, and an empty answer that came back
    // faster than the wait it asked for is paced.
    assert!(
        client.contains("wait = self._poll_budget(end - time.monotonic())"),
        "{client}"
    );
    assert!(
        client.contains("min(remaining, self._timeout * 0.9)"),
        "{client}"
    );
    assert!(
        client.contains("if time.monotonic() - started < wait / 2:"),
        "{client}"
    );
    assert!(
        client.contains("except HTTPStatusError as error:"),
        "{client}"
    );
    assert!(client.contains("if not error.retryable"), "{client}");
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
    // Go flattens the closure into one package, so the token type lands in the
    // file whose census spans it rather than beside each file's models.
    let definitions = file(&files, "definitions.go");
    assert!(
        definitions.contains("type StreamCursor string"),
        "{definitions}"
    );
    assert!(
        definitions.contains("// StreamCursor is an opaque resume token issued by the endpoint."),
        "{definitions}"
    );
    assert_eq!(definitions.matches("type StreamCursor string").count(), 1);
    assert!(!file(&files, "streams.go").contains("type StreamCursor string"));
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
            "var appendInputPayloadSites = []payloadSite{\n\t{steps: []payloadStep{{member: \"payloads\"}, {each: true}}, encoding: base64.StdEncoding},\n}"
        ),
        "{client}"
    );
    assert!(
        client.contains(
            "var readOutputPayloadSites = []payloadSite{\n\t{steps: []payloadStep{{member: \"records\"}, {each: true}, {member: \"frame\"}}, encoding: base64.StdEncoding},\n}"
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
    assert!(
        client.contains("attempt.WaitMs = &milliseconds"),
        "{client}"
    );
    assert!(client.contains("if len(answer.Records) > 0 {"), "{client}");
    assert!(
        client.contains("next, err := c.Read(ctx, attempt)"),
        "{client}"
    );
    // The ask is clamped to the transport, and an empty answer that came back
    // faster than the wait it asked for is paced.
    assert!(
        client.contains("wait := c.pollBudget(time.Until(end))"),
        "{client}"
    );
    assert!(
        client.contains("if time.Since(started) < wait/2 {"),
        "{client}"
    );
    assert!(
        client.contains("backoff = longPollNextBackoff(backoff)"),
        "{client}"
    );
    assert!(
        client.contains("if !errors.As(err, &status) || !status.Retryable()"),
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
    assert!(client.contains("attempt.WaitMs = milliseconds"), "{client}");
    assert!(
        !client.contains("attempt.WaitMs = &milliseconds"),
        "{client}"
    );
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

/// A long-poll operation whose output is a map, which has no member to name.
const MAP_OUTPUT_CONTRACT: &str = r##"
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
    additionalProperties: { type: string }
"##;

/// A payload marked on a map's value schema, which the site walk never reaches.
const UNREACHABLE_PAYLOAD_CONTRACT: &str = r##"
nexusrpc: "1.0.0"
services:
  StreamService:
    fqn: example.streams.v1.StreamService
    operations:
      append:
        fqn: append
        input: { $ref: "#/$defs/AppendInput" }
$defs:
  AppendInput:
    type: object
    additionalProperties:
      type: string
      contentEncoding: base64
      x-nexus-payload: true
"##;

/// A cursor on an array element, which is a position the model's own facts
/// never report: an inline object is hoisted into a model of its own and keeps
/// its direct properties, but an element schema has no member to be.
const NESTED_CURSOR_CONTRACT: &str = r##"
nexusrpc: "1.0.0"
services:
  StreamService:
    fqn: example.streams.v1.StreamService
    operations:
      read:
        fqn: read
        input: { $ref: "#/$defs/ReadInput" }
$defs:
  ReadInput:
    type: object
    properties:
      tokens:
        type: array
        items: { type: string, x-nexus-cursor: PageCursor }
    additionalProperties: false
"##;

/// One payload on each alphabet, so a walker that assumes one is caught.
const MIXED_ENCODING_CONTRACT: &str = r##"
nexusrpc: "1.0.0"
services:
  StreamService:
    fqn: example.streams.v1.StreamService
    operations:
      append:
        fqn: append
        input: { $ref: "#/$defs/AppendInput" }
        output: { $ref: "#/$defs/AppendOutput" }
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
"##;

#[test]
fn a_long_poll_needs_a_properties_shaped_model() {
    let message = refusal(MAP_OUTPUT_CONTRACT, "streaming-map-output");
    assert!(
        message.contains("requires a `properties`-shaped output model"),
        "{message}"
    );
}

#[test]
fn a_payload_the_walk_cannot_reach_is_refused() {
    let message = refusal(UNREACHABLE_PAYLOAD_CONTRACT, "streaming-unreachable");
    assert!(
        message.contains("additionalProperties: `x-nexus-payload`"),
        "{message}"
    );
    assert!(message.contains("cannot address it"), "{message}");
}

#[test]
fn a_cursor_off_a_direct_property_is_refused() {
    let message = refusal(NESTED_CURSOR_CONTRACT, "streaming-nested-cursor");
    assert!(
        message.contains("properties.tokens.items: `x-nexus-cursor`"),
        "{message}"
    );
}

#[test]
fn the_go_walker_uses_each_site_own_alphabet() {
    let (temp_dir, files) = generate(
        Language::Go,
        MIXED_ENCODING_CONTRACT,
        true,
        "streaming-go-alphabets",
    );
    let client = file(&files, "client.go");
    assert!(
        client
            .contains("{steps: []payloadStep{{member: \"padded\"}}, encoding: base64.StdEncoding}"),
        "{client}"
    );
    assert!(
        client.contains(
            "{steps: []payloadStep{{member: \"urlsafe\"}}, encoding: base64.RawURLEncoding}"
        ),
        "{client}"
    );
    assert!(
        client.contains(
            "{steps: []payloadStep{{member: \"echoed\"}}, encoding: base64.RawURLEncoding}"
        ),
        "{client}"
    );
    // The walker reads the alphabet off the slot rather than naming one.
    assert!(
        client.contains("slot.encoding.DecodeString(encoded)"),
        "{client}"
    );
    assert!(
        client.contains("slot.encoding.EncodeToString(transformed[index])"),
        "{client}"
    );
    assert!(
        !client.contains("base64.StdEncoding.DecodeString"),
        "{client}"
    );
    fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn the_python_walker_uses_each_site_own_alphabet() {
    let (temp_dir, files) = generate(
        Language::Python,
        MIXED_ENCODING_CONTRACT,
        true,
        "streaming-python-alphabets",
    );
    let client = file(&files, "client.py");
    assert!(
        client.contains("_PayloadSite((\"padded\",), False)"),
        "{client}"
    );
    assert!(
        client.contains("_PayloadSite((\"urlsafe\",), True)"),
        "{client}"
    );
    assert!(
        client.contains("_PayloadSite((\"echoed\",), True)"),
        "{client}"
    );
    assert!(
        client.contains("_decode_payload(encoded, slot.url_safe)"),
        "{client}"
    );
    assert!(
        client.contains("_encode_payload(payload, slot.url_safe)"),
        "{client}"
    );
    fs::remove_dir_all(temp_dir).unwrap();
}

/// A second file naming the same token type. Go flattens the closure into one
/// package, so a per-file declaration would be a redeclaration.
const SECOND_CURSOR_CONTRACT: &str = r##"
nexusrpc: "1.0.0"
services:
  AuditService:
    fqn: example.audit.v1.AuditService
    operations:
      tail:
        fqn: tail
        input: { $ref: "#/$defs/TailInput" }
$defs:
  TailInput:
    type: object
    properties:
      afterToken: { type: string, x-nexus-cursor: StreamCursor }
    additionalProperties: false
"##;

#[test]
fn two_files_naming_one_token_type_declare_it_once() {
    let (temp_dir, files) = generate_pair(
        Language::Go,
        &[
            ("streams.nexusrpc.yaml", STREAM_CONTRACT),
            ("audit.nexusrpc.yaml", SECOND_CURSOR_CONTRACT),
        ],
        false,
        "streaming-go-two-files",
    );
    let declarations: usize = files
        .iter()
        .map(|(_, contents)| contents.matches("type StreamCursor string").count())
        .sum();
    assert_eq!(
        declarations,
        1,
        "one declaration across the flat package; got {:?}",
        files.iter().map(|(name, _)| name).collect::<Vec<_>>()
    );
    fs::remove_dir_all(temp_dir).unwrap();
}
