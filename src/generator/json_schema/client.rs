//! The language-neutral plan an HTTP caller emitter works from.
//!
//! An emitted caller needs three things per operation that the planned IR does
//! not hold together: the target's type name for the input and output models,
//! where the codec applies inside each of them, and the long-poll member names.
//! This module assembles those once so the Python and Go emitters differ only
//! in how they render them.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::json_schema::streaming::{self, ModelStreamingFacts, PayloadSite};
use crate::language::Language;
use crate::planning::{PlannedFamily, PlannedJsonType, PlannedSpec};
use crate::spec::{ExternalTypeSpec, OperationSpec, ServiceSpec, TypeSpec};

/// One operation's input or output.
#[derive(Debug, Clone)]
pub(in crate::generator) struct ClientModel {
    /// The emitted type name in the target language.
    pub(in crate::generator) type_name: String,
    /// The model's name as the JSON backend knows it, used to derive sibling
    /// identifiers such as a converter class.
    pub(in crate::generator) model_name: String,
    /// The model's authored schema, for the member lookups an emitter needs.
    pub(in crate::generator) schema: Value,
    pub(in crate::generator) facts: ModelStreamingFacts,
}

impl ClientModel {
    /// The authored schema of one top-level member, when the model declares it.
    pub(in crate::generator) fn member(&self, wire_name: &str) -> Option<&Value> {
        self.schema.get("properties")?.get(wire_name)
    }

    pub(in crate::generator) fn payload_sites(&self) -> &[PayloadSite] {
        &self.facts.payload_sites
    }
}

/// The two member names a long-poll loop drives.
#[derive(Debug, Clone)]
pub(in crate::generator) struct ClientLongPoll {
    pub(in crate::generator) wait_member: String,
    pub(in crate::generator) result_member: String,
}

#[derive(Debug, Clone)]
pub(in crate::generator) struct ClientOperation {
    pub(in crate::generator) name: String,
    /// The operation name on the wire, which is also the path segment a caller
    /// posts to.
    pub(in crate::generator) wire_name: String,
    pub(in crate::generator) doc: Option<String>,
    pub(in crate::generator) deprecated: bool,
    pub(in crate::generator) input: Option<ClientModel>,
    pub(in crate::generator) output: Option<ClientModel>,
    pub(in crate::generator) long_poll: Option<ClientLongPoll>,
}

impl ClientOperation {
    /// Whether either side of the call carries codec-owned payloads.
    pub(in crate::generator) fn has_payloads(&self) -> bool {
        [self.input.as_ref(), self.output.as_ref()]
            .into_iter()
            .flatten()
            .any(|model| !model.payload_sites().is_empty())
    }
}

#[derive(Debug, Clone)]
pub(in crate::generator) struct ClientService {
    pub(in crate::generator) name: String,
    pub(in crate::generator) wire_name: String,
    pub(in crate::generator) doc: Option<String>,
    pub(in crate::generator) deprecated: bool,
    pub(in crate::generator) operations: Vec<ClientOperation>,
}

#[derive(Debug, Clone, Default)]
pub(in crate::generator) struct ClientPlan {
    pub(in crate::generator) services: Vec<ClientService>,
}

impl ClientPlan {
    pub(in crate::generator) fn is_empty(&self) -> bool {
        self.services.is_empty()
    }

    fn operations(&self) -> impl Iterator<Item = &ClientOperation> {
        self.services
            .iter()
            .flat_map(|service| service.operations.iter())
    }

    /// Whether any operation carries codec-owned payloads. This decides whether
    /// the emitted caller carries the codec machinery at all.
    pub(in crate::generator) fn has_payloads(&self) -> bool {
        self.operations().any(ClientOperation::has_payloads)
    }

    /// Whether any operation declares a long-poll contract.
    pub(in crate::generator) fn has_long_poll(&self) -> bool {
        self.operations()
            .any(|operation| operation.long_poll.is_some())
    }
}

/// How a target names the services and operations it emits. The two
/// derivations differ per target (verbatim `x-<lang>-name`, then that target's
/// casing), so they stay with the target rather than here.
pub(in crate::generator) struct ClientNaming<'a> {
    pub(in crate::generator) service: &'a dyn Fn(&ServiceSpec<PlannedFamily>) -> String,
    pub(in crate::generator) operation: &'a dyn Fn(&OperationSpec<PlannedFamily>) -> String,
}

/// Assembles the plan for one module.
///
/// `resolved_names` maps a model's full name to the identifier the target
/// emits for it, which is the JSON backend's own resolution with
/// `x-<lang>-name` applied. A model missing from the map keeps its planned
/// name, matching what the backends fall back to.
pub(in crate::generator) fn build_client_plan(
    api_plan: &PlannedSpec,
    models: &[PlannedJsonType],
    resolved_names: &BTreeMap<String, String>,
    language: Language,
    naming: &ClientNaming<'_>,
) -> ClientPlan {
    let schemas: BTreeMap<&str, &Value> = models
        .iter()
        .map(|model| (model.full_name.as_str(), &model.schema))
        .collect();
    let resolve = |reference: &str| -> Option<&Value> {
        schemas
            .get(streaming::ref_full_name(reference))
            .or_else(|| schemas.get(reference))
            .copied()
    };
    let facts: BTreeMap<&str, ModelStreamingFacts> = models
        .iter()
        .map(|model| {
            (
                model.full_name.as_str(),
                streaming::model_facts(&model.full_name, &model.schema, &resolve),
            )
        })
        .collect();

    let client_model = |type_spec: Option<&TypeSpec<PlannedFamily>>| {
        let TypeSpec::External(ExternalTypeSpec::Json(json_type)) = type_spec? else {
            return None;
        };
        Some(ClientModel {
            type_name: resolved_names
                .get(&json_type.full_name)
                .cloned()
                .unwrap_or_else(|| json_type.model_name.clone()),
            model_name: json_type.model_name.clone(),
            schema: json_type.schema.clone(),
            facts: facts
                .get(json_type.full_name.as_str())
                .cloned()
                .unwrap_or_default(),
        })
    };

    ClientPlan {
        services: api_plan
            .services
            .iter()
            .map(|service| ClientService {
                name: (naming.service)(service),
                wire_name: service.wire_name.clone(),
                doc: service.doc.for_language(language).map(str::to_string),
                deprecated: service.deprecated,
                operations: service
                    .operations
                    .iter()
                    .map(|operation| ClientOperation {
                        name: (naming.operation)(operation),
                        wire_name: operation.wire_name.clone(),
                        doc: operation.doc.for_language(language).map(str::to_string),
                        deprecated: operation.deprecated,
                        input: client_model(operation.input.as_ref()),
                        output: client_model(operation.output.as_ref()),
                        long_poll: operation
                            .long_poll
                            .as_ref()
                            .map(|long_poll| ClientLongPoll {
                                wait_member: long_poll.wait_field.clone(),
                                result_member: long_poll.result_field.clone(),
                            }),
                    })
                    .collect(),
            })
            .collect(),
    }
}
