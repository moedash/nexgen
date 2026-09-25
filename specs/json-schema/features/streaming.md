# Streaming annotations

Source: not JSON Schema. Three generator extensions, in the same
`x-`-prefixed family as [[properties]]'s `x-<lang>-name`.

A stream endpoint is a request/response contract whose callers do three
things the schema grammar cannot state. They carry an opaque resume
token they must never parse. They carry payload bytes that a codec owns
before the bytes leave the process. And they poll one operation until it
answers. Without a way to say those three things, every language SDK
writes the same wrapper around the generated bindings by hand, which is
the state the annotations remove.

| Keyword | Position | Value |
|---|---|---|
| `x-nexus-cursor` | a `type: string` schema node | the emitted token type's name |
| `x-nexus-payload` | a bytes-materialized node, or an array of them | `true` |
| `x-nexus-long-poll` | an `operations:` entry | `{wait-field, result-field}` |

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

## Rejection

The keywords go through the loader's exact allowlist
(`schema_extra_keyword_is_known` and the `operations:` entry's own pair)
rather than being tolerated as unknown, per **P7**. They are **not**
behind the `advanced` feature: a gated keyword would make one contract
parse under one build of the binary and fail under another, which is
worse than a slightly wider authored vocabulary. A target that does not
act on an annotation still accepts it, so one contract stays portable.

## Ownership

- The keyword strings, the payload-site walk and the token
  documentation live in `src/json_schema/streaming.rs`, which is
  language-neutral.
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
