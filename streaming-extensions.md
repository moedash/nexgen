# Streaming extensions

A proposal, not a merge request. Five extensions that let a stream
endpoint's contract say what its callers need, so every language SDK stops
writing the same wrapper by hand.

The motivation is concrete. Temporal's stream endpoint ships today as a
hand-written Python provider: five dataclasses, a service class, a handler,
and a raw HTTP caller that posts JSON. Moving the models and the service
definition onto a `nexusrpc.yaml` contract removes most of it. What is left
in the wrapper is the requirements list below.

## The keywords

| Keyword | Position | Value |
|---|---|---|
| `x-nexus-cursor` | a model's direct `type: string` property | the emitted token type's name |
| `x-nexus-payload` | a bytes node the walk can address, or an array of them | `true` |
| `x-nexus-long-poll` | an `operations:` entry | `{wait-field, result-field}` |
| `x-nexus-stream-ref` | a `$defs` model | `true` |
| `x-nexus-handle` | a `services:` entry | emitted handle type names to their key member lists |

```yaml
services:
  StreamService:
    operations:
      read:
        x-nexus-long-poll:
          wait-field: waitMs
          result-field: records
        input: { $ref: "#/$defs/ReadInput" }
        output: { $ref: "#/$defs/ReadOutput" }
      startGame:
        input: { $ref: "#/$defs/GameRequest" }
        output: { $ref: "#/$defs/StreamRef" }
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
  ReadInput:
    properties:
      stream: { $ref: "#/$defs/StreamRef" }
      afterToken: { type: string, x-nexus-cursor: StreamCursor }
      waitMs: { type: integer }
  AppendInput:
    properties:
      stream: { $ref: "#/$defs/StreamRef" }
      payloads:
        type: array
        x-nexus-payload: true
        items: { type: string, contentEncoding: base64 }
```

`x-` keywords rather than a `format:` value or a naming convention, because
that is what this loader's design points at. Its keyword allowlist is exact
and per-position, so a new keyword is one arm in
`schema_extra_keyword_is_known` plus its own validation, and the value then
round-trips into the model's schema untouched for an emitter to read. A new
`format:` value would instead mean two const arrays, a pinned RE2-safe
regex, a corpus, an `allOf` intersection row and emitted checks in four
backends, for a keyword that asserts nothing about the string. The
`x-<lang>-name` family already establishes the spelling.

`x-nexus-cursor` takes a **name**, not `true`. The stream contract names the
same token concept in four places across four models; a boolean would emit
four incompatible types and a resume could not cross an operation.

The long-poll members name **wire** members, not emitted identifiers,
because the authored contract is the only thing both ends of a call agree
on. Each emitter resolves them through its own `x-<lang>-name` mapping.

`x-nexus-stream-ref` sits on a **model**. A stream reference is a type
the contract shares between operations, the way the token type is: the
read and the append take it in place of an owner and a topic spelled out,
and an operation that starts a stream returns it. A member carries it
through its `$ref`; an operation whose `input` or `output` is the model
carries it whole; an inline object marked as one is hoisted into a model
first, as every inline object is, and the hoisted model carries the
marker. The members are the reference's wire form, so a target with no
stream SDK still has a type to hold. The model has to be a closed object
with members, because the SDK type is built from them by keyword.

## What each target emits

**Cursor type, Go and Python, always.** Python
`StreamCursor = typing.NewType("StreamCursor", str)`, Go
`type StreamCursor string`, each with a generator-owned doc comment stating
the three rules: opaque, never parsed, resume strictly after the record it
names. It is a distinct type, not an alias, so a caller holding a token
cannot pass a bare string into its place. It lands in the model layer and is
exported, so it is available with or without a generated caller. The other
three targets ignore the keyword.

**The wire models do not move.** A cursor member stays `str` / `*string`.
See open question 1. Because the emitted type is referenced by nothing but its
own doc comment today, the keyword is admitted on a model's direct property
only: anywhere else it declared a type and registered no member, which read as
two behaviours by position.

**Stream reference, Python, always.** A marked model is emitted as
`StreamRef: typing.TypeAlias = temporalio.streams.StreamRef` in place of
its dataclass, with `temporalio.streams` imported, so the value an
operation returns is the one `client.get_stream_handle()` opens. The
model's converter is emitted as usual and owns the wire form: it builds
the SDK type from the wire members by keyword and reads them back by
attribute. The other four targets emit the model unchanged; Go has no
stream SDK to swap in yet, and `type StreamRef struct` is what a Go
caller holds.

**HTTP callers, behind a new `--client` flag on the `go` and `python`
subcommands.** One `{Service}HttpClient` / `{Service}HTTPClient` per
service, one method per operation, typed with the generated models, posting
to `{base_url}/{operation}`. Python is stdlib only (`urllib` inside
`asyncio.to_thread`, which is what the shipping provider already does) and
serializes through the models' own transfer-type converters. Go is stdlib
only over `net/http` and serializes through the models' own
`MarshalJSON`/`UnmarshalJSON`, so the generated validation runs on both
sides.

This is a different caller from the `{Service}Client` the NativeApi surface
already emits. That one starts a Nexus operation from inside a workflow.
A stream producer or consumer is a process outside the worker talking to the
HTTP ingress, which is why the shipping provider posts raw JSON today. The
TypeScript emitter is untouched.

**Codec.** A caller is constructed with an optional codec (Python an object
with async `encode`/`decode` over `list[bytes]`, Go an interface with the
same shape). Encode runs before the send and decode after the receive, on
exactly the marked members. No codec configured is pass-through, and a
service with nothing marked gets no codec knob at all.

The codec runs over the **serialized** form rather than the model. The wire
member is base64 there, so a caller decodes each site, hands the codec one
list for the whole request, and writes the results back through a table of
member chains. One shared walker covers every site in every operation
instead of generated code per site, and the emitted caller shrinks to a
table and two calls.

**Long poll.** An extra method per marked operation, named after the result
member: `read_until_records` and `ReadUntilRecords`. It sets the wait member
from the budget left, returns the first answer whose result member is
non-empty, and returns the last answer when the deadline passes first, so a
caller can still read its resume token. The single-shot method stays.

## The `advanced` feature

The `--client` flag is behind `advanced`; the five annotations are not.

The flag follows the repo's own rule: `advanced` is the CLI surface README.md
does not document, and the existing `{Service}Client` emitters are already
reachable only through the `advanced`-gated `--native-api`. A proposal should
not widen the published binary's documented surface on its own.

The annotations must not be gated. A gated keyword would make one contract
parse under one build of the binary and fail under another, which is worse
than a slightly wider authored vocabulary. A target that does not act on an
annotation still accepts it, so one contract stays portable.

## The handle projection

The keywords above still leave the largest piece of every wrapper
hand-written: the object that binds a stream's identity once and exposes the
operations as methods. The shipping Python provider writes it as
`NexusStreamHandle` and `NexusProducer`; every other language would write the
same two classes again. A fifth keyword names that projection:

```yaml
services:
  TemporalStreams:
    x-nexus-handle:
      StreamHandle: [workflow_id, run_id]
      StreamProducer: [workflow_id, run_id, topic, producer_id, attempt]
```

The rules:

- The value maps an emitted type name to an ordered key set of **wire** member
  names, resolved through each emitter's `x-<lang>-name` mapping, the same
  rule as the long-poll members.
- An operation joins a handle when its input carries every key member. The
  generated method drops those members from its signature and injects the
  bound values; the rest of the signature stays. An operation missing a key
  member stays off that handle.
- Membership is not exclusive. An operation whose input carries the key
  members of two handles joins both, and its method is emitted on both. There
  is no "which handle wins": an `append` that carries the producer keys is
  reachable from a `StreamHandle` with the rest of its arguments spelled out,
  and from a `StreamProducer` with them bound. Two handles declaring the same
  key set are a contract error, because they would emit the same projection
  twice under two names.
- A handle whose key set extends another's is constructible from it, so
  `handle.producer(topic, producer_id, attempt)` falls out of the key sets
  rather than being declared. The constructor is named after the handle: a
  trailing `Handle` is dropped (`StreamHandle` is built by `stream`), and
  from a parent the parent's own base is dropped as a prefix when something
  is left (`StreamProducer` from `StreamHandle` is `producer`; from the flat
  client it stays `stream_producer`). Each target cases the result.
- A key member that is optional on the wire may be bound absent, and the
  handle sends what it holds. Absent is a bound value, not an unbound key: a
  handle that binds `run_id` absent is a different handle from one that binds
  it, and both are the same handle *type*. Membership is decided on the key
  names the input declares, so an optional key member counts as carried
  whether or not any particular call fills it. Binding is construction-time
  only; a handle carries no other state, so the endpoint stays as stateless as
  the flat calls leave it.
- Handle names land in the emitted-model namespace, beside the model names and
  the cursor type names, and take whatever collision check that namespace
  grows. Today it has none, which is the last bullet under "What is not done".
- The flat client stays emitted, and handles are a projection over it:
  nothing about the wire, the handler, or a caller that ignores the keyword
  changes. A long-poll loop method appears on the handle like any other
  operation method.

**What each target emits.** Python: one class per handle after the caller,
whose `__init__` takes the caller and the keys, whose operation methods take
the free members as keyword parameters and post through the caller, and
whose constructors sit on the caller and on parent handles. Go: one struct
per handle; because Go has one request struct per operation, a handle
method takes it whole and overwrites the bound fields before posting, which
its doc comment says, rather than declaring a struct per handle and
operation. Both ride behind `--client`. The other targets accept the keyword
and emit nothing for it.

**What the loader refuses.** A handle no operation joins; two handles over
one key set; a key the joining operations declare differently (a handle
holds one value per key and sends it on every call); a key with one
admissible value; a handle named like a model or a token type in the file;
a constructor name that collides with an operation or with another
constructor on the same object.

The open questions below still decide what a generated handle method may
touch: the cursor's place in the wire model and the serialization entry
point.

## Open questions

**1. Should the wire model's member carry the cursor type?** Still open.
Today it does not: a cursor member stays `str` in Python and `*string` in
Go, and the token type is for the caller's own variables. Typing the member
is the stronger guarantee, but it costs a `typing.cast` on every assignment
inside the generated converters (and basedpyright runs with `--warnings` in
this repo's gate), a `string(...)` conversion on every Go validation path,
and it moves five targets' committed output. The wire is unaffected either
way, which is why this reads as a preference rather than a correctness
question. Your call.

**2. How far should a codec's reach be, and what about an envelope the
contract does not describe?**
The implementation batches: one `encode` call per request covering every
marked site, so a codec pays for a key fetch or a round trip once. A
per-member call would be simpler to reason about and worse under load. More
importantly, the stream contract is asymmetric: an append carries serialized
payloads, and a read answers with record frames that *wrap* one. Marking
both members works, but the codec then sees two different shapes, because
the real boundary on the read side is inside a frame the contract does not
describe. Should an annotation be able to point inside a member, should the
codec be per-member so the two directions can differ, or is a
contract-specific codec the caller's problem, as it is here?

**3. Does a generated caller need a supported serialization entry point?**
The Python caller imports `models.py`'s private
`_<Model>TransferTypeConverter` classes. They are the only thing that turns
a model into the wire form with the base64 and the `x-py-name` mapping
applied, and a generated caller is versioned with the models beside it, so
this is package-internal rather than a dependency on private API. It is
still the one place the new emitter reaches somewhere it was not invited.
The TypeScript backend already exports a per-model converter accessor. Should
the Python backend do the same, or should a caller keep reaching inside?

## What is not done

- Only Go and Python emit anything. Java, TypeScript and .NET accept the
  annotations and ignore them.
- Each generated module carries its own copy of the codec walker. A
  multi-module contract duplicates it; `definitions.py` / `definitions.go`
  show where a shared one would go.
- `__init__.py` does not re-export the Python caller. It is imported as
  `from <package>.client import ...`.
- The looping method's name is derived, so a contract with an operation
  already called `read_until_records` would collide. Nothing checks that.
- The emitted loop retries a 429 or a 5xx with a bounded backoff and paces an
  endpoint that answers empty faster than the wait it was given. It does not
  retry a transport error, which is the case a caller's own client settings
  already cover.
- A handle key is a top-level member of the input. A member of a nested
  model (`stream.workflow_id`) cannot be a key; the stream reference binds
  the whole `stream` member instead.
- Go handle methods overwrite the bound fields of the request struct rather
  than dropping them from the signature.
- The cursor type name is not checked against the model names it shares a
  namespace with.
- A stream reference's members are not checked against the SDK type's
  fields. A member the SDK does not know is a type error in the generated
  module, caught by the type checker rather than by the loader. Only
  Python swaps the SDK type in; the shipping `temporal_streams.nexusrpc.yaml`
  on sdk-python stays stock-parsable, so its `StreamRef` model is not
  marked and the front maps the wire model onto the SDK type by hand.
