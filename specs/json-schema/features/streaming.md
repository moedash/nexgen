# Streaming annotations

Source: not JSON Schema. Five generator extensions, in the same
`x-`-prefixed family as [[properties]]'s `x-<lang>-name`.

A stream endpoint is a request/response contract whose callers do five
things the schema grammar cannot state. They carry an opaque resume
token they must never parse. They carry payload bytes that a codec owns
before the bytes leave the process. They poll one operation until it
answers. They hand a stream itself across an operation, as an input or a
result, in a form the SDK on the other side opens directly. And they bind
a stream's identity once and call the operations as methods on it.
Without a way to say those five things, every language SDK writes the
same wrapper around the generated bindings by hand, which is the state
the annotations remove.

| Keyword | Position | Value |
|---|---|---|
| `x-nexus-cursor` | a `type: string` schema node | the emitted token type's name |
| `x-nexus-payload` | a bytes-materialized node, or an array of them | `true` |
| `x-nexus-long-poll` | an `operations:` entry | `{wait-field, result-field}` |
| `x-nexus-stream-ref` | a `$defs` model | `true` |
| `x-nexus-handle` | a `services:` entry | emitted handle type names to ordered wire-member lists |

## `x-nexus-cursor`

The value names an emitted type, so it must match
`^[A-Z][a-zA-Z\d]+$`. Every node naming the same type shares one
declaration: a token read from one member is accepted wherever another
member names the same type, which is what makes a resume work across two
operations.

The declaring node must be `type: string`. `contentEncoding` on the same
node is rejected, because a token is never decoded, and
`x-nexus-payload` on the same node is rejected, because a token is not a
payload.

Emitted as a distinct type rather than an alias, so a caller holding a
token cannot pass a bare string into its place: Python
`typing.NewType`, Go a named string type. Go and Python emit it today;
the other targets ignore it.

**The wire models keep the plain string.** A generated model's member
is unchanged by the annotation, so the JSON on the wire and every
target's strict type checker see what they saw before. The token type is
for the code a caller writes around the models.

Its documentation is generator-owned, not copied from the authored
`description`: the three rules (opaque, never parsed, resume strictly
after the record it names) are what the annotation means, and a contract
that worded them differently would still have to mean this.

## `x-nexus-payload`

Marks a node whose bytes belong to the caller's codec. The node must
materialize to bytes, so it carries [[contentEncoding]] `base64` or
`base64url`, either directly or on its `items`. A marked array is a
batch: it contributes one codec site per element.

A generated caller applies the codec to the **serialized** form, not to
the model. The wire member is base64 at that point, so the caller
base64-decodes each site, hands the codec one list of bytes for the
whole request, and writes the results back. One shared walker covers
every site, driven by a table of member chains; there is no generated
code per site. A caller with no codec configured passes the bytes
through and never walks.

The walk follows `$ref` and stops at a repeat, so a recursive model
contributes only the sites reachable without revisiting itself.

## `x-nexus-long-poll`

```yaml
read:
  x-nexus-long-poll:
    wait-field: waitMs
    result-field: records
```

Both members name **wire** members of the operation's input and output,
not emitted identifiers, because the authored contract is the only thing
both ends of a call agree on. An emitter resolves them through its own
`x-<lang>-name` mapping.

The loader checks that both members exist and that `wait-field` is
`type: integer` (milliseconds) and `result-field` is `type: array`.
Pinning both keeps every target's emitted loop one fixed shape: one
numeric assignment and one length test. Without the check, a typo lands
as a type error inside generated caller code rather than against the
contract that caused it.

The single-shot method stays. The loop is an extra method named after
the result member (`read` plus `records` reads as `read_until_records`
and `ReadUntilRecords`), so the contract is visible at the call site.

## `x-nexus-stream-ref`

```yaml
$defs:
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
```

Marks a `$defs` model as the stream reference: the value an operation
takes or returns to hand a stream to its caller, naming the stream's
owner and topic and never a cursor or a store. The model's members are
the wire form, so every target still has a type to hold and a Go or
TypeScript caller sees an ordinary object. A target whose stream SDK
already has a reference type emits that type in the model's place, so an
operation result is the value the SDK's handle accessor opens, with no
copy in between.

The keyword sits on a model. The reference is a type the contract shares
between operations, the way a token type is; a member typed by the model
carries it through its `$ref`, and an operation whose `input` or `output`
is the model carries it whole. An inline object is hoisted into a model
of its own before the marker is read, so a marker on one names the
hoisted model (`ReadInputStream` for a `stream` member of `ReadInput`);
a `$defs` entry is how the reference gets the name the SDK type has.

The model must be `type: object` with `properties` and
`additionalProperties: false`. The SDK type is built from the wire
members by keyword and read back by attribute, which needs the members
declared, and a member the reference does not know could only be
dropped, which a closed object refuses instead. The member names are the
SDK type's field names, resolved through `x-py-name` like any other; the
loader does not check them against the SDK, so a mismatch is a type
error in the generated module rather than a load failure.

Python emits `StreamRef: typing.TypeAlias = temporalio.streams.StreamRef`
in place of the dataclass and imports `temporalio.streams`. The model's
converter is emitted as for any model and owns the wire form in both
directions. The SDK type's own JSON encoding is the same members, so an
operation whose input or output is the reference serializes the same way
whether the SDK or the generated converter does it, with one condition:
the SDK writes `null` for an unset member, and an optional member that is
not declared nullable refuses an explicit `null` ([[nullability]]), so a
marked model declares its optional members
`oneOf: [{type: string}, {type: "null"}]`. Go, TypeScript, Java and .NET
emit the model unchanged.

## `x-nexus-handle`

```yaml
services:
  StreamService:
    x-nexus-handle:
      StreamHandle: [workflowId, stream]
      StreamProducer: [workflowId, stream, producerId, attempt]
```

Maps an emitted handle type name to an ordered key set of **wire**
members of the operations' inputs, resolved through each target's
`x-<lang>-name` mapping like the long-poll members. An operation joins a
handle when its input declares every key member; the handle carries it as
a method with those members bound and the rest of the input as
parameters. An operation missing a key stays off that handle. Membership
is not exclusive: an operation whose input covers two key sets is a
method on both handles.

A handle whose key set extends another's is constructible from it by the
keys it adds, so `handle.producer(producer_id, attempt)` falls out of the
two sets above rather than being declared. Every handle is also
constructible from the flat caller with all of its keys. The constructor
is named after the handle: a trailing `Handle` is dropped (`StreamHandle`
is built by `stream`), and from a parent handle the parent's own base is
dropped as a prefix when something is left (`StreamProducer` from
`StreamHandle` is `producer`; from the flat caller it is
`stream_producer`). Each target cases the result its own way.

A key member that is optional on the wire may be bound absent, and the
handle sends what it holds. Binding is construction-time only; a handle
carries no other state. The flat caller stays emitted, the handles are a
projection over it, and nothing about the wire, the handler, or a caller
that ignores the keyword changes. A long-poll loop method appears on the
handle like any other operation method, minus the wait member the loop
sets.

The loader refuses what an emitter could not decide: a handle no
operation joins, two handles over one key set, a key member the joining
operations declare differently (a handle holds one value per key and
sends it on every call), a key with one admissible value (`const`), a
handle named like a model or a token type in the same file, and a
constructor name that collides with an operation or with another
constructor on the same object.

Python emits one class per handle after the caller: `__init__` takes the
caller and the keys, each operation method takes the free members as
keyword parameters and posts through the caller, and constructors are
methods on the caller and on parent handles. Go has one request struct
per operation, so a handle method takes it whole and overwrites the bound
fields before posting, which the doc comment says; constructors are
methods on the caller and on parent handles. Both sit behind `--client`
with the callers they project; the other targets accept the keyword and
emit nothing for it.

## Rejection

The keywords go through the loader's exact allowlist
(`schema_extra_keyword_is_known` and the `operations:` entry's own pair)
rather than being tolerated as unknown, per **P7**. They are **not**
behind the `advanced` feature: a gated keyword would make one contract
parse under one build of the binary and fail under another, which is
worse than a slightly wider authored vocabulary. A target that does not
act on an annotation still accepts it, so one contract stays portable.

## Ownership

- The keyword strings, the payload-site walk, the token documentation
  and the position rules live in `src/json_schema/streaming.rs`, which
  is language-neutral.
- The Python stream-reference alias is `render_stream_ref_alias` in
  `src/generator/json_schema/python.rs`, next to the model emitter it
  replaces for a marked model.
- The handle projection is planned in `src/generator/json_schema/client.rs`
  (`plan_handles`) from the key sets the loader lowered onto
  `ServiceSpec::handles`, and rendered by the `render_handle*` functions
  of the two caller emitters.
- `src/generator/json_schema/client.rs` assembles the per-operation plan
  a caller emitter needs.
- The rendering decisions live with their target, in
  `src/generator/json_schema/{python,go}_client.rs`.

## See also

- [[contentEncoding]] — the bytes materialization `x-nexus-payload`
  requires.
- [[properties]] — the `x-<lang>-name` family these keywords join, and
  the member-identifier algorithm an emitter resolves them through.
- [[services]] — the `operations:` entry `x-nexus-long-poll` sits on.
- [[PRINCIPLES.md]] — **P1** (polyglot wire), **P7** (strict schema
  validation).
