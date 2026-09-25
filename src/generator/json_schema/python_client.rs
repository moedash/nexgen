//! The generated Python HTTP caller (`--client`).
//!
//! This is a different caller from the `{Service}Client` the NativeApi surface
//! emits: that one starts a Nexus operation from inside a workflow, while this
//! one is for a process outside the worker posting to the Nexus HTTP ingress.
//! It carries no dependency beyond the standard library and the models module
//! next to it, because a stream producer is often not a Temporal worker at all.

use heck::ToShoutySnakeCase;
use serde_json::Value;

use crate::generator::json_schema::client::{
    ClientLongPoll, ClientModel, ClientOperation, ClientPlan, ClientService,
};
use crate::generator::json_schema::python::converter_class_name;
use crate::generator::python::{python_field_name, render_generated_file_header};
use crate::json_schema::content_encoding::Encoding;
use crate::json_schema::streaming::PayloadStep;

/// The file the caller is emitted to, next to the models it uses.
pub(in crate::generator) const CLIENT_MODULE: &str = "client.py";

fn literal(value: &str) -> String {
    serde_json::to_string(value).expect("a Rust string is always JSON-serializable")
}

/// The emitted attribute for a wire member: the `x-py-name` override if the
/// member carries one, otherwise the snake-cased wire name. This mirrors the
/// models module's own rule, so the caller reads the field the models declare.
fn member_attribute(member: Option<&Value>, wire_name: &str) -> String {
    member
        .and_then(|member| member.get("x-py-name"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| python_field_name(wire_name))
}

fn client_class_name(service: &ClientService) -> String {
    format!("{}HttpClient", service.name)
}

/// The looping method's name. Deriving it from the result member keeps the
/// contract visible at the call site: an operation whose emptiness test is
/// `records` reads as `read_until_records`.
fn long_poll_method_name(
    operation: &ClientOperation,
    long_poll: &ClientLongPoll,
    output: &ClientModel,
) -> String {
    format!(
        "{}_until_{}",
        operation.name,
        member_attribute(
            output.member(&long_poll.result_member),
            &long_poll.result_member
        )
    )
}

/// The constant holding one model's payload sites. It is keyed by the model, not
/// by the side of the call, so a model used as both an input and an output
/// carries one table.
fn payload_sites_constant(model: &ClientModel) -> String {
    format!("_{}_PAYLOAD_SITES", model.model_name.to_shouty_snake_case())
}

fn render_payload_steps(output: &mut String, steps: &[PayloadStep]) {
    output.push('(');
    for (index, step) in steps.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        match step {
            PayloadStep::Member(name) => output.push_str(&literal(name)),
            PayloadStep::Each => output.push_str("None"),
        }
    }
    // A one-step site would otherwise render as a parenthesized string.
    output.push_str(",)");
}

/// Renders the whole module, or `None` when the module declares no service.
pub(in crate::generator) fn render_client_module(plan: &ClientPlan) -> Option<String> {
    if plan.is_empty() {
        return None;
    }
    let mut output = String::new();
    render_generated_file_header(&mut output);
    output.push('\n');
    render_imports(&mut output, plan);
    render_model_imports(&mut output, plan);
    if plan.has_payloads() {
        render_codec_runtime(&mut output);
    }
    render_post_helper(&mut output);
    if plan.has_long_poll() {
        render_long_poll_runtime(&mut output);
    }
    for service in &plan.services {
        render_payload_site_constants(&mut output, service);
    }
    for service in &plan.services {
        // Per service: a service with nothing marked gets no codec knob even
        // when a sibling service in the same module has one.
        let codec = service.operations.iter().any(ClientOperation::has_payloads);
        render_service_client(&mut output, service, codec);
    }
    Some(output)
}

fn render_imports(output: &mut String, plan: &ClientPlan) {
    output.push_str("import asyncio\n");
    if plan.has_payloads() {
        output.push_str("import base64\n");
        output.push_str("import collections.abc\n");
    }
    if plan.has_long_poll() {
        output.push_str("import dataclasses\n");
    }
    output.push_str("import json\n");
    if plan.has_long_poll() {
        output.push_str("import time\n");
    }
    output.push_str("import typing\n");
    output.push_str("import urllib.error\n");
    output.push_str("import urllib.request\n\n");
}

fn render_model_imports(output: &mut String, plan: &ClientPlan) {
    let mut names = std::collections::BTreeSet::new();
    for service in &plan.services {
        for operation in &service.operations {
            for model in [operation.input.as_ref(), operation.output.as_ref()]
                .into_iter()
                .flatten()
            {
                names.insert(model.type_name.clone());
                names.insert(converter_class_name(&model.model_name));
            }
        }
    }
    if names.is_empty() {
        return;
    }
    // The converters are the models module's own private classes. A generated
    // caller is versioned with the models it sits beside, so reaching for them
    // is a package-internal call rather than a dependency on private API.
    output.push_str("from .models import (\n");
    for name in &names {
        output.push_str("    ");
        output.push_str(name);
        output.push_str(",\n");
    }
    output.push_str(")\n\n\n");
}

fn render_codec_runtime(output: &mut String) {
    output.push_str(
        r#"_PayloadSteps = tuple[str | None, ...]
"""One wire member chain from a model's root to a payload. `None` stands for
every element of the array reached so far.
"""


class _PayloadSite(typing.NamedTuple):
    """One site's step chain and the alphabet its wire string is written in.

    The two base64 alphabets are not interchangeable, so a site carries its own
    rather than the walker assuming one.
    """

    steps: _PayloadSteps
    url_safe: bool


@typing.runtime_checkable
class PayloadCodec(typing.Protocol):
    """Transforms the payload bytes an operation carries.

    A caller applies it before the send and after the receive, so a payload
    never travels in the form the process holds it in. Both directions take and
    answer with a list of the same length, in order.
    """

    async def encode(self, payloads: list[bytes]) -> list[bytes]: ...

    async def decode(self, payloads: list[bytes]) -> list[bytes]: ...


class _Slot(typing.NamedTuple):
    container: typing.Any
    key: typing.Any
    url_safe: bool


def _payload_slots(
    node: typing.Any, steps: _PayloadSteps, url_safe: bool
) -> list[_Slot]:
    if not steps:
        return []
    head, rest = steps[0], steps[1:]
    if head is None:
        if not isinstance(node, list):
            return []
        entries = typing.cast("list[typing.Any]", node)
        if not rest:
            return [_Slot(entries, index, url_safe) for index in range(len(entries))]
        return [
            slot for entry in entries for slot in _payload_slots(entry, rest, url_safe)
        ]
    if not isinstance(node, dict):
        return []
    members = typing.cast("dict[str, typing.Any]", node)
    if head not in members:
        return []
    if not rest:
        return [_Slot(members, head, url_safe)]
    return _payload_slots(members[head], rest, url_safe)


def _decode_payload(encoded: str, url_safe: bool) -> bytes:
    if url_safe:
        # The wire form is unpadded, which `urlsafe_b64decode` will not take.
        return base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
    return base64.b64decode(encoded, validate=True)


def _encode_payload(payload: bytes, url_safe: bool) -> str:
    if url_safe:
        return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    return base64.b64encode(payload).decode("ascii")


async def _apply_codec(
    wire: typing.Any,
    sites: tuple[_PayloadSite, ...],
    transform: collections.abc.Callable[
        [list[bytes]], collections.abc.Awaitable[list[bytes]]
    ],
) -> None:
    """Runs every payload the sites reach through the codec, in one call.

    The codec sees one list per request, so whatever it does (a key fetch, a
    round trip) is paid once for the whole batch. Each site is decoded and
    re-encoded in its own alphabet, so a member the contract wrote as base64url
    goes back on the wire as base64url.
    """
    slots = [
        slot
        for site in sites
        for slot in _payload_slots(wire, site.steps, site.url_safe)
    ]
    if not slots:
        return
    payloads: list[bytes] = []
    for slot in slots:
        encoded = slot.container[slot.key]
        if not isinstance(encoded, str):
            raise ValueError(f"payload at {slot.key!r} is not a base64 string")
        payloads.append(_decode_payload(encoded, slot.url_safe))
    transformed = await transform(payloads)
    if len(transformed) != len(payloads):
        raise ValueError(
            f"codec answered with {len(transformed)} payloads for {len(payloads)}"
        )
    for slot, payload in zip(slots, transformed):
        slot.container[slot.key] = _encode_payload(payload, slot.url_safe)


"#,
    );
}

fn render_long_poll_runtime(output: &mut String) {
    output.push_str(
        r#"_LONG_POLL_MIN_BACKOFF = 0.05
_LONG_POLL_MAX_BACKOFF = 2.0


async def _long_poll_pause(backoff: float, end: float) -> None:
    """Waits out a backoff without running past the read's own deadline."""
    remaining = end - time.monotonic()
    delay = min(backoff, remaining)
    if delay > 0:
        await asyncio.sleep(delay)


def _long_poll_next_backoff(backoff: float) -> float:
    return min(backoff * 2, _LONG_POLL_MAX_BACKOFF)


"#,
    );
}

fn render_poll_budget_method(output: &mut String) {
    output.push_str(
        r#"
    def _poll_budget(self, remaining: float) -> float:
        """How long one attempt may ask the endpoint to park, in seconds.

        Asking for longer than the transport will wait is a socket timeout
        dressed as a long poll, so the ask leaves the answer room to come back
        inside this client's own timeout.
        """
        return max(0.001, min(remaining, self._timeout * 0.9))
"#,
    );
}

fn render_post_helper(output: &mut String) {
    output.push_str(
        r#"class HTTPStatusError(RuntimeError):
    """A non-2xx answer from the ingress.

    It carries the status so a caller can tell a refusal apart from an answer
    another attempt could still get.
    """

    def __init__(self, url: str, status: int, detail: str) -> None:
        super().__init__(f"{url} failed ({status}): {detail}")
        self.url = url
        self.status = status
        self.detail = detail

    @property
    def retryable(self) -> bool:
        """Whether the same request could answer differently.

        Too many requests and the server-side failures are the endpoint's own
        transient conditions; every other status is about this request, and
        repeating it changes nothing.
        """
        return self.status == 429 or self.status >= 500


def _post(
    url: str, body: bytes, headers: dict[str, str], timeout: float
) -> bytes:
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return typing.cast("bytes", response.read())
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")
        raise HTTPStatusError(url, error.code, detail) from error


"#,
    );
}

fn render_payload_site_constants(output: &mut String, service: &ClientService) {
    let mut rendered = std::collections::BTreeSet::new();
    for operation in &service.operations {
        for model in [operation.input.as_ref(), operation.output.as_ref()]
            .into_iter()
            .flatten()
        {
            if model.payload_sites().is_empty() {
                continue;
            }
            let name = payload_sites_constant(model);
            if !rendered.insert(name.clone()) {
                continue;
            }
            output.push_str(&name);
            output.push_str(": tuple[_PayloadSite, ...] = (\n");
            for site in model.payload_sites() {
                output.push_str("    _PayloadSite(");
                render_payload_steps(output, &site.steps);
                output.push_str(", ");
                output.push_str(match site.encoding {
                    Encoding::Base64 => "False",
                    Encoding::Base64Url => "True",
                });
                output.push_str("),\n");
            }
            output.push_str(")\n\n\n");
        }
    }
}

fn render_service_client(output: &mut String, service: &ClientService, codec: bool) {
    let class_name = client_class_name(service);
    output.push_str("class ");
    output.push_str(&class_name);
    output.push_str(":\n");
    let mut paragraphs = vec![
        service
            .doc
            .clone()
            .unwrap_or_else(|| format!("An HTTP caller for the {} service.", service.wire_name)),
        "Every method posts to `{base_url}/{operation}`, so `base_url` is the service's \
         address on the Nexus HTTP ingress, up to but not including the operation name."
            .to_string(),
    ];
    if service.deprecated {
        paragraphs.push("This service is deprecated.".to_string());
    }
    render_docstring(output, "    ", &paragraphs);
    output.push_str("\n    def __init__(\n");
    output.push_str("        self,\n");
    output.push_str("        base_url: str,\n");
    output.push_str("        *,\n");
    // A contract with no marked payload gets no codec knob: there would be
    // nothing for it to act on, and the protocol would not be declared.
    if codec {
        output.push_str("        codec: PayloadCodec | None = None,\n");
    }
    output.push_str("        headers: dict[str, str] | None = None,\n");
    output.push_str("        timeout: float = 30.0,\n");
    output.push_str("    ) -> None:\n");
    // Annotated explicitly: a target's strict type checker asks for it on a class
    // it cannot prove closed.
    output.push_str("        self._base_url: str = base_url.rstrip(\"/\")\n");
    if codec {
        output.push_str("        self._codec: PayloadCodec | None = codec\n");
    }
    output.push_str("        self._headers: dict[str, str] = dict(headers or {})\n");
    output.push_str("        self._timeout: float = timeout\n");

    if service
        .operations
        .iter()
        .any(|operation| operation.long_poll.is_some())
    {
        render_poll_budget_method(output);
    }

    for operation in &service.operations {
        output.push('\n');
        render_operation_method(output, operation);
        if let Some(long_poll) = &operation.long_poll
            && let Some(input) = &operation.input
            && let Some(model_output) = &operation.output
        {
            output.push('\n');
            render_long_poll_method(output, operation, long_poll, input, model_output);
        }
    }
    output.push('\n');
}

fn render_operation_method(output: &mut String, operation: &ClientOperation) {
    let return_annotation = operation
        .output
        .as_ref()
        .map(|model| model.type_name.clone())
        .unwrap_or_else(|| "None".to_string());
    output.push_str("    async def ");
    output.push_str(&operation.name);
    output.push_str("(\n        self,\n");
    if let Some(input) = &operation.input {
        output.push_str("        request: ");
        output.push_str(&input.type_name);
        output.push_str(",\n");
    }
    output.push_str("    ) -> ");
    output.push_str(&return_annotation);
    output.push_str(":\n");
    let mut lines = Vec::new();
    if let Some(doc) = &operation.doc {
        lines.push(doc.clone());
    }
    if operation.deprecated {
        lines.push("This operation is deprecated.".to_string());
    }
    if !lines.is_empty() {
        render_docstring(output, "        ", &lines);
    }

    match &operation.input {
        Some(input) => {
            output.push_str("        wire: typing.Any = ");
            output.push_str(&converter_class_name(&input.model_name));
            output.push_str("().to_transfer_type(request)\n");
            if !input.payload_sites().is_empty() {
                output.push_str("        if self._codec is not None:\n");
                output.push_str("            await _apply_codec(\n");
                output.push_str("                wire, ");
                output.push_str(&payload_sites_constant(input));
                output.push_str(", self._codec.encode\n");
                output.push_str("            )\n");
            }
            output.push_str("        body = json.dumps(wire).encode()\n");
        }
        None => output.push_str("        body = b\"{}\"\n"),
    }
    output.push_str(if operation.output.is_some() {
        "        raw = await asyncio.to_thread(\n"
    } else {
        "        await asyncio.to_thread(\n"
    });
    output.push_str("            _post,\n");
    output.push_str("            f\"{self._base_url}/");
    output.push_str(&operation.wire_name);
    output.push_str("\",\n");
    output.push_str("            body,\n");
    output.push_str("            self._headers,\n");
    output.push_str("            self._timeout,\n");
    output.push_str("        )\n");

    let Some(model_output) = &operation.output else {
        return;
    };
    // An operation that declares an output and answers with nothing has not
    // answered. Reading it as the all-defaults value hands a long poll a zero
    // cursor, which resumes the read from the beginning.
    output.push_str("        if not raw:\n");
    output.push_str("            raise ValueError(\n                ");
    output.push_str(&literal(&format!(
        "{} answered with an empty body",
        operation.wire_name
    )));
    output.push_str("\n            )\n");
    output.push_str("        answer: typing.Any = json.loads(raw)\n");
    if !model_output.payload_sites().is_empty() {
        output.push_str("        if self._codec is not None:\n");
        output.push_str("            await _apply_codec(\n");
        output.push_str("                answer, ");
        output.push_str(&payload_sites_constant(model_output));
        output.push_str(", self._codec.decode\n");
        output.push_str("            )\n");
    }
    output.push_str("        return ");
    output.push_str(&converter_class_name(&model_output.model_name));
    output.push_str("().from_transfer_type(answer, ");
    output.push_str(&model_output.type_name);
    output.push_str(")\n");
}

fn render_long_poll_method(
    output: &mut String,
    operation: &ClientOperation,
    long_poll: &ClientLongPoll,
    input: &ClientModel,
    model_output: &ClientModel,
) {
    let wait_attribute =
        member_attribute(input.member(&long_poll.wait_member), &long_poll.wait_member);
    let result_attribute = member_attribute(
        model_output.member(&long_poll.result_member),
        &long_poll.result_member,
    );
    output.push_str("    async def ");
    output.push_str(&long_poll_method_name(operation, long_poll, model_output));
    output.push_str("(\n        self,\n        request: ");
    output.push_str(&input.type_name);
    output.push_str(",\n        *,\n        deadline: float,\n    ) -> ");
    output.push_str(&model_output.type_name);
    output.push_str(":\n");
    render_docstring(
        output,
        "        ",
        &[
            format!(
                "Calls `{}` until it answers with something, or until `deadline`.",
                operation.name
            ),
            format!(
                "Each attempt sets `{wait_attribute}` from the budget left, so the endpoint \
                 parks for the caller instead of the caller spinning. The first answer whose \
                 `{result_attribute}` is non-empty is returned. When the deadline passes first, \
                 the last answer is returned, which still carries the caller's resume position.",
            ),
            "`deadline` is the whole budget in seconds. The single-shot method stays \
             available for a caller that wants one attempt."
                .to_string(),
        ],
    );
    output.push_str("        end = time.monotonic() + deadline\n");
    output.push_str("        answer: ");
    output.push_str(&model_output.type_name);
    output.push_str(" | None = None\n");
    output.push_str("        backoff = _LONG_POLL_MIN_BACKOFF\n");
    output.push_str("        while True:\n");
    output.push_str("            wait = self._poll_budget(end - time.monotonic())\n");
    output.push_str("            attempt = dataclasses.replace(\n");
    output.push_str("                request, ");
    output.push_str(&wait_attribute);
    output.push_str("=int(wait * 1000)\n");
    output.push_str("            )\n");
    output.push_str("            started = time.monotonic()\n");
    output.push_str("            try:\n");
    output.push_str("                answer = await self.");
    output.push_str(&operation.name);
    output.push_str("(attempt)\n");
    output.push_str("            except HTTPStatusError as error:\n");
    output.push_str(
        "                if not error.retryable or time.monotonic() >= end:\n                    raise\n",
    );
    output.push_str("                await _long_poll_pause(backoff, end)\n");
    output.push_str("                backoff = _long_poll_next_backoff(backoff)\n");
    output.push_str("                continue\n");
    output.push_str("            if answer.");
    output.push_str(&result_attribute);
    output.push_str(":\n");
    output.push_str("                return answer\n");
    output.push_str("            if time.monotonic() >= end:\n");
    output.push_str("                return answer\n");
    // An endpoint that answers empty without parking for the wait it was given
    // would otherwise be asked again as fast as the network allows.
    output.push_str("            if time.monotonic() - started < wait / 2:\n");
    output.push_str("                await _long_poll_pause(backoff, end)\n");
    output.push_str("                backoff = _long_poll_next_backoff(backoff)\n");
    output.push_str("            else:\n");
    output.push_str("                backoff = _LONG_POLL_MIN_BACKOFF\n");
}


/// A docstring with one paragraph per entry. The shared Python docstring writer
/// takes a single summary plus tagged sections, which is the wrong shape for
/// prose paragraphs.
fn render_docstring(output: &mut String, indent: &str, paragraphs: &[String]) {
    let paragraphs = paragraphs
        .iter()
        .map(|paragraph| paragraph.trim())
        .filter(|paragraph| !paragraph.is_empty())
        .collect::<Vec<_>>();
    if paragraphs.is_empty() {
        return;
    }
    let lines = paragraphs
        .iter()
        .map(|paragraph| wrap(paragraph, 88 - indent.len() - 6))
        .collect::<Vec<_>>();
    output.push_str(indent);
    output.push_str("\"\"\"");
    // A docstring that fits on one line closes on it, which is what every
    // Python formatter would rewrite it to anyway.
    if let [only] = lines.as_slice()
        && let [line] = only.as_slice()
    {
        output.push_str(line);
        output.push_str("\"\"\"\n");
        return;
    }
    for (index, paragraph) in lines.iter().enumerate() {
        if index > 0 {
            output.push('\n');
        }
        for (line_index, line) in paragraph.iter().enumerate() {
            if index > 0 || line_index > 0 {
                output.push_str(indent);
            }
            output.push_str(line);
            output.push('\n');
        }
    }
    output.push_str(indent);
    output.push_str("\"\"\"\n");
}

fn wrap(text: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        if !current.is_empty() && current.len() + 1 + word.len() > width {
            lines.push(std::mem::take(&mut current));
        }
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(word);
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines
}
