//! The generated Go HTTP caller (`--client`).
//!
//! This is a different caller from the `{Service}Client` the NativeApi surface
//! emits: that one starts a Nexus operation from inside a workflow, while this
//! one is for a process outside the worker posting to the Nexus HTTP ingress.
//! It depends on the standard library only, because a stream producer is often
//! not a Temporal worker at all.

use heck::ToLowerCamelCase;
use serde_json::Value;

use crate::generator::go::{
    GENERATED_HEADER, go_field_name, render_go_doc_comment as render_wrapped_go_doc_comment,
};
use crate::generator::json_schema::client::{
    ClientLongPoll, ClientModel, ClientOperation, ClientPlan, ClientService,
};
use crate::json_schema::content_encoding::Encoding;
use crate::json_schema::streaming::PayloadStep;

/// The file the caller is emitted to, in the same flat package as the models.
pub(in crate::generator) const CLIENT_FILE: &str = "client.go";

fn literal(value: &str) -> String {
    serde_json::to_string(value).expect("a Rust string is always JSON-serializable")
}

/// The emitted field for a wire member: the `x-go-name` override if the member
/// carries one, otherwise the PascalCased wire name. This mirrors the models
/// file's own rule, so the caller reads the field the structs declare.
fn member_field(member: Option<&Value>, wire_name: &str) -> String {
    member
        .and_then(|member| member.get("x-go-name"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| go_field_name(wire_name))
}

fn client_type_name(service: &ClientService) -> String {
    format!("{}HTTPClient", service.name)
}

/// The looping method's name. Deriving it from the result member keeps the
/// contract visible at the call site: an operation whose emptiness test is
/// `records` reads as `ReadUntilRecords`.
fn long_poll_method_name(
    operation: &ClientOperation,
    long_poll: &ClientLongPoll,
    output: &ClientModel,
) -> String {
    format!(
        "{}Until{}",
        operation.name,
        member_field(
            output.member(&long_poll.result_member),
            &long_poll.result_member
        )
    )
}

/// The `encoding/base64` value for one site's alphabet.
fn go_base64_encoding(encoding: Encoding) -> &'static str {
    match encoding {
        Encoding::Base64 => "base64.StdEncoding",
        Encoding::Base64Url => "base64.RawURLEncoding",
    }
}

/// The package-level variable holding one model's payload sites.
fn payload_sites_variable(model: &ClientModel) -> String {
    format!("{}PayloadSites", model.model_name.to_lower_camel_case())
}

/// Renders the whole file, or `None` when the module declares no service.
pub(in crate::generator) fn render_client_file(
    package_name: &str,
    plan: &ClientPlan,
) -> Option<String> {
    if plan.is_empty() {
        return None;
    }
    let mut output = String::new();
    output.push_str(GENERATED_HEADER);
    output.push_str("\n\npackage ");
    output.push_str(package_name);
    output.push_str("\n\n");
    render_imports(&mut output, plan);
    if plan.has_payloads() {
        render_codec_runtime(&mut output);
    }
    for service in &plan.services {
        render_payload_site_variables(&mut output, service);
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
    output.push_str("import (\n");
    output.push_str("\t\"bytes\"\n");
    output.push_str("\t\"context\"\n");
    if plan.has_payloads() {
        output.push_str("\t\"encoding/base64\"\n");
    }
    output.push_str("\t\"encoding/json\"\n");
    output.push_str("\t\"fmt\"\n");
    output.push_str("\t\"io\"\n");
    output.push_str("\t\"net/http\"\n");
    output.push_str("\t\"strings\"\n");
    if plan.has_long_poll() {
        output.push_str("\t\"time\"\n");
    }
    output.push_str(")\n\n");
}

fn render_codec_runtime(output: &mut String) {
    output.push_str(
        r#"// PayloadCodec transforms the payload bytes an operation carries. A caller
// applies it before the send and after the receive, so a payload never travels
// in the form the process holds it in. Both directions take and answer with a
// slice of the same length, in order.
type PayloadCodec interface {
	Encode(ctx context.Context, payloads [][]byte) ([][]byte, error)
	Decode(ctx context.Context, payloads [][]byte) ([][]byte, error)
}

// payloadStep is one step from a model's root towards a payload: a wire member,
// or every element of the array reached so far.
type payloadStep struct {
	member string
	each   bool
}

// payloadSite is one site's step chain together with the alphabet its wire
// string is written in. The two base64 alphabets are not interchangeable, so a
// site carries its own rather than the walker assuming one.
type payloadSite struct {
	steps    []payloadStep
	encoding *base64.Encoding
}

// payloadSlot is a place in the decoded request or response body holding one
// payload, addressed so it can be read and written back.
type payloadSlot struct {
	object   map[string]any
	key      string
	array    []any
	index    int
	encoding *base64.Encoding
}

func (s payloadSlot) get() any {
	if s.object != nil {
		return s.object[s.key]
	}
	return s.array[s.index]
}

func (s payloadSlot) set(value any) {
	if s.object != nil {
		s.object[s.key] = value
		return
	}
	s.array[s.index] = value
}

func collectPayloadSlots(
	node any, steps []payloadStep, encoding *base64.Encoding, out *[]payloadSlot,
) {
	if len(steps) == 0 {
		return
	}
	head, rest := steps[0], steps[1:]
	if head.each {
		entries, ok := node.([]any)
		if !ok {
			return
		}
		for index := range entries {
			if len(rest) == 0 {
				*out = append(*out, payloadSlot{array: entries, index: index, encoding: encoding})
				continue
			}
			collectPayloadSlots(entries[index], rest, encoding, out)
		}
		return
	}
	members, ok := node.(map[string]any)
	if !ok {
		return
	}
	if _, present := members[head.member]; !present {
		return
	}
	if len(rest) == 0 {
		*out = append(*out, payloadSlot{object: members, key: head.member, encoding: encoding})
		return
	}
	collectPayloadSlots(members[head.member], rest, encoding, out)
}

// applyPayloadCodec runs every payload the sites reach through transform, in one
// call, and answers with the rewritten body.
//
// The codec sees one slice per request, so whatever it does (a key fetch, a
// round trip) is paid once for the whole batch. Each site is decoded and
// re-encoded in its own alphabet, so a member the contract wrote as base64url
// goes back on the wire as base64url. A body whose sites reach nothing is
// returned untouched, so an operation that happens to carry no payload costs
// nothing beyond the walk.
func applyPayloadCodec(
	ctx context.Context,
	body []byte,
	sites []payloadSite,
	transform func(context.Context, [][]byte) ([][]byte, error),
) ([]byte, error) {
	var node any
	if err := json.Unmarshal(body, &node); err != nil {
		return nil, fmt.Errorf("decode body for the payload codec: %w", err)
	}
	var slots []payloadSlot
	for _, site := range sites {
		collectPayloadSlots(node, site.steps, site.encoding, &slots)
	}
	if len(slots) == 0 {
		return body, nil
	}
	payloads := make([][]byte, 0, len(slots))
	for _, slot := range slots {
		encoded, ok := slot.get().(string)
		if !ok {
			return nil, fmt.Errorf("payload at %q is not a base64 string", slot.key)
		}
		decoded, err := slot.encoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode payload at %q: %w", slot.key, err)
		}
		payloads = append(payloads, decoded)
	}
	transformed, err := transform(ctx, payloads)
	if err != nil {
		return nil, err
	}
	if len(transformed) != len(payloads) {
		return nil, fmt.Errorf(
			"codec answered with %d payloads for %d", len(transformed), len(payloads),
		)
	}
	for index, slot := range slots {
		slot.set(slot.encoding.EncodeToString(transformed[index]))
	}
	rewritten, err := json.Marshal(node)
	if err != nil {
		return nil, fmt.Errorf("re-encode body after the payload codec: %w", err)
	}
	return rewritten, nil
}

"#,
    );
}

fn render_payload_site_variables(output: &mut String, service: &ClientService) {
    let mut rendered = std::collections::BTreeSet::new();
    for operation in &service.operations {
        for model in [operation.input.as_ref(), operation.output.as_ref()]
            .into_iter()
            .flatten()
        {
            if model.payload_sites().is_empty() {
                continue;
            }
            let name = payload_sites_variable(model);
            if !rendered.insert(name.clone()) {
                continue;
            }
            render_wrapped_go_doc_comment(
                output,
                "",
                &format!(
                    "{name} addresses every payload the codec owns inside a {}.",
                    model.type_name
                ),
            );
            output.push_str("var ");
            output.push_str(&name);
            output.push_str(" = []payloadSite{\n");
            for site in model.payload_sites() {
                output.push_str("\t{steps: []payloadStep{");
                for (index, step) in site.steps.iter().enumerate() {
                    if index > 0 {
                        output.push_str(", ");
                    }
                    match step {
                        PayloadStep::Member(member) => {
                            output.push_str("{member: ");
                            output.push_str(&literal(member));
                            output.push('}');
                        }
                        PayloadStep::Each => output.push_str("{each: true}"),
                    }
                }
                output.push_str("}, encoding: ");
                output.push_str(go_base64_encoding(site.encoding));
                output.push_str("},\n");
            }
            output.push_str("}\n\n");
        }
    }
}

fn render_service_client(output: &mut String, service: &ClientService, codec: bool) {
    let type_name = client_type_name(service);
    let options_name = format!("{type_name}Options");

    let mut summary = vec![format!(
        "{type_name} is an HTTP caller for the {} service.",
        service.wire_name
    )];
    if let Some(doc) = &service.doc {
        summary.push(doc.clone());
    }
    summary.push(
        "Every method posts to baseURL/<operation>, so baseURL is the service's address on \
         the Nexus HTTP ingress, up to but not including the operation name."
            .to_string(),
    );
    if service.deprecated {
        summary.push("Deprecated: this service is deprecated.".to_string());
    }
    render_wrapped_go_doc_comment(output, "", &summary.join(" "));
    output.push_str("type ");
    output.push_str(&type_name);
    output.push_str(" struct {\n");
    output.push_str("\tbaseURL string\n");
    output.push_str("\thttpClient *http.Client\n");
    // A contract with no marked payload gets no codec knob: there would be
    // nothing for it to act on, and the interface would not be declared.
    if codec {
        output.push_str("\tcodec PayloadCodec\n");
    }
    output.push_str("\theader http.Header\n");
    output.push_str("}\n\n");

    render_wrapped_go_doc_comment(
        output,
        "",
        &format!("{options_name} configures a {type_name}. Every member is optional."),
    );
    output.push_str("type ");
    output.push_str(&options_name);
    output.push_str(" struct {\n");
    render_wrapped_go_doc_comment(
        output,
        "\t",
        "HTTPClient issues the requests. A nil value uses http.DefaultClient.",
    );
    output.push_str("\tHTTPClient *http.Client\n");
    if codec {
        render_wrapped_go_doc_comment(
            output,
            "\t",
            "Codec transforms the payloads the operations carry. A nil value passes them \
             through untouched.",
        );
        output.push_str("\tCodec PayloadCodec\n");
    }
    render_wrapped_go_doc_comment(output, "\t", "Header is added to every request.");
    output.push_str("\tHeader http.Header\n");
    output.push_str("}\n\n");

    render_wrapped_go_doc_comment(
        output,
        "",
        &format!(
            "New{type_name} constructs a {type_name} posting to baseURL. A nil options is \
             the same as a zero one."
        ),
    );
    output.push_str("func New");
    output.push_str(&type_name);
    output.push_str("(baseURL string, options *");
    output.push_str(&options_name);
    output.push_str(") *");
    output.push_str(&type_name);
    output.push_str(" {\n");
    output.push_str("\tif options == nil {\n\t\toptions = &");
    output.push_str(&options_name);
    output.push_str("{}\n\t}\n");
    output.push_str("\thttpClient := options.HTTPClient\n");
    output.push_str("\tif httpClient == nil {\n\t\thttpClient = http.DefaultClient\n\t}\n");
    output.push_str("\treturn &");
    output.push_str(&type_name);
    output.push_str("{\n");
    output.push_str("\t\tbaseURL: strings.TrimRight(baseURL, \"/\"),\n");
    output.push_str("\t\thttpClient: httpClient,\n");
    if codec {
        output.push_str("\t\tcodec: options.Codec,\n");
    }
    output.push_str("\t\theader: options.Header,\n");
    output.push_str("\t}\n}\n\n");

    render_post_method(output, &type_name);

    for operation in &service.operations {
        render_operation_method(output, &type_name, operation);
        if let Some(long_poll) = &operation.long_poll
            && let Some(input) = &operation.input
            && let Some(model_output) = &operation.output
        {
            render_long_poll_method(
                output,
                &type_name,
                operation,
                long_poll,
                input,
                model_output,
            );
        }
    }
}

fn render_post_method(output: &mut String, type_name: &str) {
    output.push_str("func (c *");
    output.push_str(type_name);
    output.push_str(
        r#") post(ctx context.Context, operation string, body []byte) ([]byte, error) {
	url := c.baseURL + "/" + operation
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for name, values := range c.header {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("%s failed (%d): %s", url, response.StatusCode, raw)
	}
	return raw, nil
}

"#,
    );
}

fn render_operation_method(output: &mut String, type_name: &str, operation: &ClientOperation) {
    let mut summary = vec![format!(
        "{} posts one {} call.",
        operation.name, operation.wire_name
    )];
    if let Some(doc) = &operation.doc {
        summary.push(doc.clone());
    }
    if operation.deprecated {
        summary.push("Deprecated: this operation is deprecated.".to_string());
    }
    render_wrapped_go_doc_comment(output, "", &summary.join(" "));

    output.push_str("func (c *");
    output.push_str(type_name);
    output.push_str(") ");
    output.push_str(&operation.name);
    output.push_str("(ctx context.Context");
    if let Some(input) = &operation.input {
        output.push_str(", request ");
        output.push_str(&input.type_name);
    }
    output.push_str(") (");
    match &operation.output {
        Some(model_output) => {
            output.push_str(&model_output.type_name);
            output.push_str(", error) {\n\tvar answer ");
            output.push_str(&model_output.type_name);
            output.push('\n');
        }
        None => output.push_str("error) {\n"),
    }
    let bail = |output: &mut String, indent: &str| {
        output.push_str(indent);
        if operation.output.is_some() {
            output.push_str("return answer, err\n");
        } else {
            output.push_str("return err\n");
        }
    };

    match &operation.input {
        Some(input) => {
            output.push_str("\tbody, err := json.Marshal(request)\n");
            output.push_str("\tif err != nil {\n");
            bail(output, "\t\t");
            output.push_str("\t}\n");
            if !input.payload_sites().is_empty() {
                output.push_str("\tif c.codec != nil {\n");
                output.push_str("\t\tbody, err = applyPayloadCodec(ctx, body, ");
                output.push_str(&payload_sites_variable(input));
                output.push_str(", c.codec.Encode)\n");
                output.push_str("\t\tif err != nil {\n");
                bail(output, "\t\t\t");
                output.push_str("\t\t}\n");
                output.push_str("\t}\n");
            }
        }
        None => output.push_str("\tbody := []byte(\"{}\")\n"),
    }

    output.push_str("\traw, err := c.post(ctx, ");
    output.push_str(&literal(&operation.wire_name));
    output.push_str(", body)\n");
    output.push_str("\tif err != nil {\n");
    bail(output, "\t\t");
    output.push_str("\t}\n");

    let Some(model_output) = &operation.output else {
        output.push_str("\t_ = raw\n\treturn nil\n}\n\n");
        return;
    };
    // An operation may answer with an empty body; the models treat that as the
    // all-defaults value rather than a decode failure.
    output.push_str("\tif len(raw) == 0 {\n\t\traw = []byte(\"{}\")\n\t}\n");
    if !model_output.payload_sites().is_empty() {
        output.push_str("\tif c.codec != nil {\n");
        output.push_str("\t\traw, err = applyPayloadCodec(ctx, raw, ");
        output.push_str(&payload_sites_variable(model_output));
        output.push_str(", c.codec.Decode)\n");
        output.push_str("\t\tif err != nil {\n");
        bail(output, "\t\t\t");
        output.push_str("\t\t}\n");
        output.push_str("\t}\n");
    }
    output.push_str("\tif err := json.Unmarshal(raw, &answer); err != nil {\n");
    output.push_str("\t\treturn answer, err\n\t}\n");
    output.push_str("\treturn answer, nil\n}\n\n");
}

fn render_long_poll_method(
    output: &mut String,
    type_name: &str,
    operation: &ClientOperation,
    long_poll: &ClientLongPoll,
    input: &ClientModel,
    model_output: &ClientModel,
) {
    let method = long_poll_method_name(operation, long_poll, model_output);
    let wait_field = member_field(input.member(&long_poll.wait_member), &long_poll.wait_member);
    let result_field = member_field(
        model_output.member(&long_poll.result_member),
        &long_poll.result_member,
    );
    let wait_required = input.member_required(&long_poll.wait_member);
    render_wrapped_go_doc_comment(
        output,
        "",
        &format!(
            "{method} calls {} until it answers with something, or until the deadline. Each \
             attempt sets {wait_field} from the budget left, so the endpoint parks for the \
             caller instead of the caller spinning. The first answer whose {result_field} is \
             non-empty is returned. When the deadline passes first, the last answer is \
             returned, which still carries the caller's resume position. The single-shot \
             method stays available for a caller that wants one attempt.",
            operation.name
        ),
    );
    output.push_str("func (c *");
    output.push_str(type_name);
    output.push_str(") ");
    output.push_str(&method);
    output.push_str("(ctx context.Context, request ");
    output.push_str(&input.type_name);
    output.push_str(", deadline time.Duration) (");
    output.push_str(&model_output.type_name);
    output.push_str(", error) {\n");
    output.push_str("\tend := time.Now().Add(deadline)\n");
    output.push_str("\tfor {\n");
    output.push_str("\t\twait := time.Until(end).Milliseconds()\n");
    output.push_str("\t\tif wait < 1 {\n\t\t\twait = 1\n\t\t}\n");
    output.push_str("\t\tattempt := request\n");
    output.push_str("\t\tattempt.");
    output.push_str(&wait_field);
    if wait_required {
        output.push_str(" = wait\n");
    } else {
        output.push_str(" = &wait\n");
    }
    output.push_str("\t\tanswer, err := c.");
    output.push_str(&operation.name);
    output.push_str("(ctx, attempt)\n");
    output.push_str("\t\tif err != nil {\n\t\t\treturn answer, err\n\t\t}\n");
    output.push_str("\t\tif len(answer.");
    output.push_str(&result_field);
    output.push_str(") > 0 {\n\t\t\treturn answer, nil\n\t\t}\n");
    output.push_str("\t\tif !time.Now().Before(end) {\n\t\t\treturn answer, nil\n\t\t}\n");
    output.push_str("\t}\n}\n\n");
}
