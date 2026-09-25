//! The streaming annotations (`x-nexus-cursor`, `x-nexus-payload`,
//! `x-nexus-long-poll`) and the language-neutral analysis a generated caller
//! needs from them.
//!
//! The three keywords are declared here so the loader that validates them and
//! the emitters that honor them share one vocabulary. Everything in this module
//! reads authored JSON Schema and yields wire-level facts (member names, array
//! hops); no target-language policy lives here.
//!
//! See `specs/json-schema/features/streaming.md` for the authoritative rules.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::json_schema::content_encoding::Encoding;

/// Marks a `type: string` node as an opaque resume token. The value names the
/// emitted token type, so every node naming the same type shares it.
pub const CURSOR_KEYWORD: &str = "x-nexus-cursor";

/// Marks a bytes-materialized node as a payload the caller's codec owns.
pub const PAYLOAD_KEYWORD: &str = "x-nexus-payload";

/// Marks an `operations:` entry as long-pollable.
pub const LONG_POLL_KEYWORD: &str = "x-nexus-long-poll";

/// The `x-nexus-long-poll` member naming the input's wait-hint member.
pub const LONG_POLL_WAIT_MEMBER: &str = "wait-field";

/// The `x-nexus-long-poll` member naming the output's emptiness-test member.
pub const LONG_POLL_RESULT_MEMBER: &str = "result-field";

/// The two rules a caller must not break. They are generator-owned rather than
/// copied from the authored `description`: a contract that words them
/// differently would still have to mean this.
const CURSOR_RULES: [&str; 2] = [
    "The value is never parsed, compared or constructed by a caller; only the endpoint that issued it can interpret it.",
    "Pass one back to resume strictly after the record it names, so that record is never delivered twice.",
];

/// The documentation an emitted token type carries.
///
/// The lead sentence names the type so it satisfies Go's godoc convention and
/// still reads correctly as a Python docstring. `paragraph_separator` is the
/// target's paragraph break: a space for a target whose doc comment is one
/// wrapped paragraph, `"\n\n"` for one that keeps paragraphs.
pub fn cursor_doc(name: &str, paragraph_separator: &str) -> String {
    format!(
        "{name} is an opaque resume token issued by the endpoint.{paragraph_separator}{}{paragraph_separator}{}",
        CURSOR_RULES[0], CURSOR_RULES[1]
    )
}

/// The token type a schema node declares itself a cursor for.
pub fn cursor_name(schema: &Value) -> Option<&str> {
    schema.get(CURSOR_KEYWORD)?.as_str()
}

/// The materialized encoding a node's wire string carries, if any.
pub fn node_encoding(schema: &Value) -> Option<Encoding> {
    Encoding::from_name(schema.get("contentEncoding")?.as_str()?)
}

/// Whether a schema node declares its bytes codec-owned.
pub fn payload_marked(schema: &Value) -> bool {
    schema
        .get(PAYLOAD_KEYWORD)
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// One step from a model's root towards a payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadStep {
    /// Descend into a wire member. The name is the authored JSON member, which
    /// an emitter maps to its own identifier.
    Member(String),
    /// Iterate an array.
    Each,
}

/// A place inside one model where the codec applies. The value reached by
/// walking `steps` is always a single bytes value: a marked array of payloads
/// contributes a trailing [`PayloadStep::Each`] rather than a batch flag, so a
/// caller can treat every site the same way.
///
/// `encoding` is the site's own `contentEncoding`, carried because the two
/// alphabets are not interchangeable: a caller that decoded a `base64url`
/// member with the standard alphabet would hand the codec the wrong bytes, or
/// fail on the first `-` in a token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadSite {
    pub steps: Vec<PayloadStep>,
    pub encoding: Encoding,
}

/// What the streaming annotations say about one model.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelStreamingFacts {
    /// Every payload site in the model, in a deterministic order.
    pub payload_sites: Vec<PayloadSite>,
    /// The model's own top-level cursor members, as `(wire member, token type)`.
    /// Used for documentation; nested cursors are not reported because a caller
    /// reads them through the nested model's own facts.
    pub cursor_members: Vec<(String, String)>,
}

impl ModelStreamingFacts {
    pub fn is_empty(&self) -> bool {
        self.payload_sites.is_empty() && self.cursor_members.is_empty()
    }
}

/// Resolves a `$ref` string to the referenced model's schema.
pub type RefResolver<'a> = dyn Fn(&str) -> Option<&'a Value> + 'a;

/// Collects every token type declared anywhere in `schemas`.
///
/// The walk is over raw authored schema, so a model no operation references
/// still contributes its type: the type is part of the contract's vocabulary,
/// not of one operation's signature. It stays a deep walk although the loader
/// admits the keyword on a model's direct property only, so the two are not
/// coupled through this function.
pub fn cursor_type_names<'a>(schemas: impl IntoIterator<Item = &'a Value>) -> Vec<String> {
    let mut names = BTreeSet::new();
    for schema in schemas {
        collect_cursor_names(schema, &mut names);
    }
    names.into_iter().collect()
}

fn collect_cursor_names(schema: &Value, names: &mut BTreeSet<String>) {
    if let Some(name) = cursor_name(schema) {
        names.insert(name.to_string());
    }
    match schema {
        Value::Object(members) => {
            for value in members.values() {
                collect_cursor_names(value, names);
            }
        }
        Value::Array(entries) => {
            for entry in entries {
                collect_cursor_names(entry, names);
            }
        }
        _ => {}
    }
}

/// Analyses one model, following `$ref`s through `resolve`.
///
/// `model_full_name` seeds the cycle guard. A recursive model stops the walk at
/// the repeat rather than unrolling it, so a self-referential type contributes
/// only the payload sites reachable without revisiting itself. A caller that
/// needs the deeper sites walks its own data.
pub fn model_facts<'a>(
    model_full_name: &str,
    schema: &'a Value,
    resolve: &RefResolver<'a>,
) -> ModelStreamingFacts {
    let mut facts = ModelStreamingFacts::default();
    let mut visiting = BTreeSet::from([model_full_name.to_string()]);
    collect_payload_sites(schema, resolve, &mut Vec::new(), &mut visiting, &mut facts);
    facts.cursor_members = top_level_cursor_members(schema, resolve);
    facts
}

/// Analyses every model once, keyed by full name.
pub fn model_facts_by_name<'a>(
    models: impl IntoIterator<Item = (&'a str, &'a Value)>,
    resolve: &RefResolver<'a>,
) -> BTreeMap<String, ModelStreamingFacts> {
    models
        .into_iter()
        .map(|(full_name, schema)| {
            (
                full_name.to_string(),
                model_facts(full_name, schema, resolve),
            )
        })
        .collect()
}

fn top_level_cursor_members<'a>(
    schema: &'a Value,
    resolve: &RefResolver<'a>,
) -> Vec<(String, String)> {
    let Some(properties) = follow_ref(schema, resolve)
        .and_then(|schema| schema.get("properties"))
        .and_then(Value::as_object)
    else {
        return Vec::new();
    };
    properties
        .iter()
        .filter_map(|(member, property)| {
            cursor_name(property).map(|name| (member.clone(), name.to_string()))
        })
        .collect()
}

/// Resolves a node that is a bare `$ref` to its target, and passes any other
/// node through.
fn follow_ref<'a>(schema: &'a Value, resolve: &RefResolver<'a>) -> Option<&'a Value> {
    match schema.get("$ref").and_then(Value::as_str) {
        Some(reference) => resolve(reference),
        None => Some(schema),
    }
}

fn collect_payload_sites<'a>(
    schema: &'a Value,
    resolve: &RefResolver<'a>,
    steps: &mut Vec<PayloadStep>,
    visiting: &mut BTreeSet<String>,
    facts: &mut ModelStreamingFacts,
) {
    if payload_marked(schema) {
        let mut site_steps = steps.clone();
        // A marked array carries a batch of payloads. Normalizing it to a
        // trailing element hop keeps every site's terminal a single value.
        let encoded = if schema.get("type").and_then(Value::as_str) == Some("array") {
            site_steps.push(PayloadStep::Each);
            schema.get("items")
        } else {
            Some(schema)
        };
        facts.payload_sites.push(PayloadSite {
            steps: site_steps,
            encoding: encoded.and_then(node_encoding).unwrap_or(Encoding::Base64),
        });
        return;
    }

    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        let full_name = ref_full_name(reference);
        if !visiting.insert(full_name.to_string()) {
            return;
        }
        if let Some(target) = resolve(reference) {
            collect_payload_sites(target, resolve, steps, visiting, facts);
        }
        visiting.remove(full_name);
        return;
    }

    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (member, property) in properties {
            steps.push(PayloadStep::Member(member.clone()));
            collect_payload_sites(property, resolve, steps, visiting, facts);
            steps.pop();
        }
    }
    if let Some(items) = schema.get("items") {
        steps.push(PayloadStep::Each);
        collect_payload_sites(items, resolve, steps, visiting, facts);
        steps.pop();
    }
    // `oneOf` branches share the parent's position, so a payload inside one
    // branch is reached by the same steps as the branch itself.
    if let Some(branches) = schema.get("oneOf").and_then(Value::as_array) {
        for branch in branches {
            collect_payload_sites(branch, resolve, steps, visiting, facts);
        }
    }
}

/// The annotated positions inside one model's raw schema that the site walk
/// and the cursor report cannot act on, as authored paths.
///
/// [`collect_payload_sites`] addresses a payload through `properties`, `items`
/// and `oneOf` only, and [`top_level_cursor_members`] reports a model's direct
/// properties only. A marking anywhere else parses, validates and then does
/// nothing, which for a payload means bytes meant for the codec travel in the
/// clear. Reporting the positions lets the loader refuse the contract.
///
/// The walk does not follow `$ref`: a referenced model is checked as itself.
pub fn unaddressable_annotations(schema: &Value) -> Vec<String> {
    let mut found = Vec::new();
    walk_addressability(schema, "", Reach::ROOT, &mut found);
    found
}

/// What the two readers can still see from one position in a model's schema.
#[derive(Debug, Clone, Copy)]
struct Reach {
    /// `collect_payload_sites` still descends here.
    payload: bool,
    /// This node is the model root, so its own `properties` are the direct
    /// properties `top_level_cursor_members` reads.
    model_root: bool,
    /// `top_level_cursor_members` would report a cursor on this node.
    cursor: bool,
}

impl Reach {
    const ROOT: Self = Self {
        payload: true,
        model_root: true,
        cursor: true,
    };

    /// A position the site walk reaches but the cursor report does not.
    fn payload_only(self) -> Self {
        Self {
            payload: self.payload,
            model_root: false,
            cursor: false,
        }
    }

    /// A position neither reader descends into.
    const INERT: Self = Self {
        payload: false,
        model_root: false,
        cursor: false,
    };
}

fn walk_addressability(schema: &Value, path: &str, reach: Reach, found: &mut Vec<String>) {
    let Value::Object(members) = schema else {
        return;
    };
    if payload_marked(schema) {
        if !reach.payload {
            found.push(format!("{}: `{PAYLOAD_KEYWORD}`", position(path)));
        }
        // The site walk stops at a marked node, so nothing below it is a site
        // of its own and nothing below it is reported here either.
        return;
    }
    if cursor_name(schema).is_some() && !reach.cursor {
        found.push(format!("{}: `{CURSOR_KEYWORD}`", position(path)));
    }
    if let Some(properties) = members.get("properties").and_then(Value::as_object) {
        let child_reach = Reach {
            payload: reach.payload,
            model_root: false,
            cursor: reach.model_root,
        };
        for (member, property) in properties {
            let child = format!("{path}.properties.{member}");
            walk_addressability(property, &child, child_reach, found);
        }
    }
    if let Some(items) = members.get("items") {
        let child = format!("{path}.items");
        walk_addressability(items, &child, reach.payload_only(), found);
    }
    if let Some(branches) = members.get("oneOf").and_then(Value::as_array) {
        for (index, branch) in branches.iter().enumerate() {
            let child = format!("{path}.oneOf[{index}]");
            walk_addressability(branch, &child, reach.payload_only(), found);
        }
    }
    // Everything below here is a position neither reader descends into, so an
    // annotation inside one is inert.
    for keyword in ["additionalProperties", "not", "if", "then", "else"] {
        if let Some(value) = members.get(keyword) {
            let child = format!("{path}.{keyword}");
            walk_addressability(value, &child, Reach::INERT, found);
        }
    }
    for keyword in ["allOf", "anyOf", "prefixItems"] {
        if let Some(branches) = members.get(keyword).and_then(Value::as_array) {
            for (index, branch) in branches.iter().enumerate() {
                let child = format!("{path}.{keyword}[{index}]");
                walk_addressability(branch, &child, Reach::INERT, found);
            }
        }
    }
    if let Some(patterned) = members.get("patternProperties").and_then(Value::as_object) {
        for (pattern, property) in patterned {
            let child = format!("{path}.patternProperties.{pattern}");
            walk_addressability(property, &child, Reach::INERT, found);
        }
    }
}

fn position(path: &str) -> String {
    if path.is_empty() {
        "the model root".to_string()
    } else {
        path.trim_start_matches('.').to_string()
    }
}

/// The model identity inside a `#/$defs/<name>` reference. A cross-file
/// reference keeps its file part, which is enough to tell two models apart.
pub fn ref_full_name(reference: &str) -> &str {
    reference
        .rsplit_once("#/$defs/")
        .map(|(_, name)| name)
        .unwrap_or(reference)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn resolver<'a>(
        models: &'a BTreeMap<String, Value>,
    ) -> impl Fn(&str) -> Option<&'a Value> + 'a {
        move |reference: &str| models.get(ref_full_name(reference))
    }

    #[test]
    fn normalizes_a_marked_array_to_an_element_hop() {
        let schema = json!({
            "type": "object",
            "properties": {
                "payloads": {
                    "type": "array",
                    "x-nexus-payload": true,
                    "items": { "type": "string", "contentEncoding": "base64" },
                },
            },
        });
        let models = BTreeMap::new();
        let facts = model_facts("AppendInput", &schema, &resolver(&models));
        assert_eq!(
            facts.payload_sites,
            vec![PayloadSite {
                steps: vec![
                    PayloadStep::Member("payloads".to_string()),
                    PayloadStep::Each
                ],
                encoding: Encoding::Base64,
            }]
        );
    }

    #[test]
    fn reaches_a_payload_behind_an_array_of_referenced_models() {
        let record = json!({
            "type": "object",
            "properties": {
                "token": { "type": "string", "x-nexus-cursor": "StreamCursor" },
                "frame": {
                    "type": "string",
                    "contentEncoding": "base64",
                    "x-nexus-payload": true,
                },
            },
        });
        let models = BTreeMap::from([("RecordWire".to_string(), record)]);
        let schema = json!({
            "type": "object",
            "properties": {
                "records": {
                    "type": "array",
                    "items": { "$ref": "#/$defs/RecordWire" },
                },
                "next_token": { "type": "string", "x-nexus-cursor": "StreamCursor" },
            },
        });
        let facts = model_facts("ReadOutput", &schema, &resolver(&models));
        assert_eq!(
            facts.payload_sites,
            vec![PayloadSite {
                steps: vec![
                    PayloadStep::Member("records".to_string()),
                    PayloadStep::Each,
                    PayloadStep::Member("frame".to_string()),
                ],
                encoding: Encoding::Base64,
            }]
        );
        assert_eq!(
            facts.cursor_members,
            vec![("next_token".to_string(), "StreamCursor".to_string())]
        );
    }

    #[test]
    fn stops_a_recursive_model_at_the_repeat() {
        let node = json!({
            "type": "object",
            "properties": {
                "blob": {
                    "type": "string",
                    "contentEncoding": "base64",
                    "x-nexus-payload": true,
                },
                "child": { "$ref": "#/$defs/Node" },
            },
        });
        let models = BTreeMap::from([("Node".to_string(), node.clone())]);
        let facts = model_facts("Node", &node, &resolver(&models));
        assert_eq!(
            facts.payload_sites,
            vec![PayloadSite {
                steps: vec![PayloadStep::Member("blob".to_string())],
                encoding: Encoding::Base64,
            }]
        );
    }

    #[test]
    fn carries_each_site_own_encoding() {
        let schema = json!({
            "type": "object",
            "properties": {
                "padded": {
                    "type": "string",
                    "contentEncoding": "base64",
                    "x-nexus-payload": true,
                },
                "urlsafe": {
                    "type": "string",
                    "contentEncoding": "base64url",
                    "x-nexus-payload": true,
                },
                "batch": {
                    "type": "array",
                    "x-nexus-payload": true,
                    "items": { "type": "string", "contentEncoding": "base64url" },
                },
            },
        });
        let models = BTreeMap::new();
        let facts = model_facts("Mixed", &schema, &resolver(&models));
        let encodings: Vec<_> = facts
            .payload_sites
            .iter()
            .map(|site| (site.steps.clone(), site.encoding))
            .collect();
        assert_eq!(
            encodings,
            vec![
                (
                    vec![PayloadStep::Member("padded".to_string())],
                    Encoding::Base64
                ),
                (
                    vec![PayloadStep::Member("urlsafe".to_string())],
                    Encoding::Base64Url
                ),
                (
                    vec![PayloadStep::Member("batch".to_string()), PayloadStep::Each],
                    Encoding::Base64Url
                ),
            ]
        );
    }

    #[test]
    fn reports_a_payload_the_walk_cannot_address() {
        let schema = json!({
            "type": "object",
            "additionalProperties": {
                "type": "string",
                "contentEncoding": "base64",
                "x-nexus-payload": true,
            },
        });
        assert_eq!(
            unaddressable_annotations(&schema),
            vec!["additionalProperties: `x-nexus-payload`".to_string()]
        );
    }

    #[test]
    fn reports_a_cursor_below_a_model_direct_property() {
        let schema = json!({
            "type": "object",
            "properties": {
                "fine": { "type": "string", "x-nexus-cursor": "StreamCursor" },
                "nested": {
                    "type": "object",
                    "properties": {
                        "deep": { "type": "string", "x-nexus-cursor": "PageCursor" },
                    },
                },
            },
        });
        assert_eq!(
            unaddressable_annotations(&schema),
            vec!["properties.nested.properties.deep: `x-nexus-cursor`".to_string()]
        );
    }

    #[test]
    fn accepts_every_position_the_walk_does_reach() {
        let schema = json!({
            "type": "object",
            "properties": {
                "token": { "type": "string", "x-nexus-cursor": "StreamCursor" },
                "frames": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "contentEncoding": "base64",
                        "x-nexus-payload": true,
                    },
                },
            },
        });
        assert!(unaddressable_annotations(&schema).is_empty());
    }

    #[test]
    fn collects_every_declared_token_type_once() {
        let schema = json!({
            "type": "object",
            "properties": {
                "token": { "type": "string", "x-nexus-cursor": "StreamCursor" },
                "nested": {
                    "type": "object",
                    "properties": {
                        "other": { "type": "string", "x-nexus-cursor": "PageCursor" },
                        "same": { "type": "string", "x-nexus-cursor": "StreamCursor" },
                    },
                },
            },
        });
        assert_eq!(
            cursor_type_names([&schema]),
            vec!["PageCursor".to_string(), "StreamCursor".to_string()]
        );
    }
}
