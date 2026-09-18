# Streaming extensions

A proposal, not a merge request. Three extensions that let a stream
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
| `x-nexus-cursor` | a `type: string` node | the emitted token type's name |
| `x-nexus-payload` | a bytes node, or an array of them | `true` |
| `x-nexus-long-poll` | an `operations:` entry | `{wait-field, result-field}` |

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
$defs:
  ReadInput:
    properties:
      afterToken: { type: string, x-nexus-cursor: StreamCursor }
      waitMs: { type: integer }
  AppendInput:
    properties:
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
See open question 1.

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

The `--client` flag is behind `advanced`; the three annotations are not.

The flag follows the repo's own rule: `advanced` is the CLI surface README.md
does not document, and the existing `{Service}Client` emitters are already
reachable only through the `advanced`-gated `--native-api`. A proposal should
not widen the published binary's documented surface on its own.

The annotations must not be gated. A gated keyword would make one contract
parse under one build of the binary and fail under another, which is worse
than a slightly wider authored vocabulary. A target that does not act on an
annotation still accepts it, so one contract stays portable.

## Open questions

**1. Should the wire model's member carry the cursor type?**
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
- The cursor type name is not checked against the model names it shares a
  namespace with.
