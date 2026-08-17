# Relay

Relay is the inference **data plane** for the Oxy platform. It normalizes a
request, translates it for one upstream provider, streams the result back,
propagates cancellation, and reports what was technically consumed.

It is not a product. It has no customers, no accounts, no console and no
billing. Oxy is the single control plane for all of that
([ADR 0005][adr0005], [ADR 0006][adr0006]); Relay executes what Oxy has already
authorized and reports what it measured.

Tracked as workstream 13 of [OxyHQ/oxy#972][epic]. `Relay` is a working name
until naming review completes ([ADR 0011][adr0011]).

---

## The boundary, in one paragraph

Relay owns request normalization, provider adapters, routing **execution**,
streaming, cancellation, model deployments, provider health and technical
metering. Relay never owns accounts, organizations, projects, members,
applications, credentials, balances, a billing ledger or a customer console. It
stores Oxy identifiers as **immutable, opaque strings** and never as records it
may create, edit or delete. Authorization, attribution, scope checks and spend
reservation all happen **in Oxy, before a request reaches Relay** — Relay does
not re-derive them, and an envelope that does not carry them is refused.

If a change would put an Oxy-owned concept in this repository, the change is
wrong, not the boundary. `AGENTS.md` states the rules a reviewer applies.

## Layout

```
cmd/relay/                      the binary: env config, wiring, graceful drain
internal/contract/              Go types for @oxyhq/contracts' inference module
  descriptor.json               GENERATED from the published package
  descriptor_test.go            the drift gate
  drift_test.go                 the gate's positive control
  fixtures_test.go              wire fixtures for the Zod round-trip
internal/edgeauth/              Ed25519 verification of the Oxy edge's signature
internal/httpapi/               the Oxy-facing HTTP surface
internal/inventory/             deployment inventory and its configuration snapshot store
internal/provider/              the Adapter interface, registry and error vocabulary
  conformance/                  the suite every adapter must pass
  openaicompat/                 the ported OpenAI Chat Completions adapter
  anthropic/                    the ported Anthropic Messages API adapter
internal/providercost/          what a request cost Relay upstream; never a customer amount
internal/relay/                 the executor: routing, failover, framing, usage reports
internal/rotation/              per-deployment circuit breakers and health scoring
internal/sse/                   SSE decoding (upstream) and encoding (downstream)
tools/contract/                 Node tooling that derives and checks the contract
configs/inventory.example.json  an illustrative inventory snapshot
configs/provider-rates.example.json  illustrative upstream rate cards
```

Layers depend downward only: `httpapi → relay → {inventory, rotation,
providercost, provider} → contract`, with `sse` as a leaf. `contract` imports
nothing of Relay's, which is what lets the drift gate compare it against the
published package with nothing in between — and specifically it cannot reach
`providercost`, which is asserted rather than reviewed
(`TestTheContractCannotReachAnAmount`).

**Go** is the implementation language, as the epic prefers for a
high-concurrency streaming data plane. Nothing here argued against it: a
per-request goroutine with a cancellable `context.Context` threaded to the
upstream HTTP call *is* the cancellation design, rather than something layered
on top of it.

## The contract is not re-invented here

`@oxyhq/contracts@0.30.0` (contract version 1.2.0) is the wire contract, and the Go types in
`internal/contract` are hand-written against it. Hand-writing is only safe
because two independent gates fail when the two sides diverge.

**1. A generated descriptor, compared field by field.**
`tools/contract/generate.mjs` imports every module under the published package's
`inference/` directory and emits `internal/contract/descriptor.json`: every
shape, field, optionality, kind, enum member, literal value and string
constraint. The shape list is **discovered, not listed**, so a shape added
upstream appears on the next regeneration.
`internal/contract/descriptor_test.go` then requires that:

- every published shape is either implemented in Go or recorded in an exact,
  reason-carrying `notApplicable` list (an exact count, so a shape cannot be
  excused by appending a line);
- every implemented shape's Go JSON field set **equals** the contract's — no
  extra field, no missing field;
- optionality matches (optional ⇔ absentable and `omitempty`, required ⇔ neither);
- enum members match exactly, in both directions;
- the regexes Relay enforces are character-identical to the published ones;
- `ContractVersion` equals the published `INFERENCE_CONTRACT_VERSION`.

CI regenerates the descriptor and fails on any diff, which catches a hand-edit
and a version bump nobody re-derived. The bump to `0.28.0` is what that gate is
for: it brought 25 shapes from two new modules — account billing, entitlements
and reconciliation — every one of which is a control-plane concept this
repository is forbidden to hold, so all 25 are recorded not-applicable with the
reason naming the owner, and `expectedNotApplicableCount` moved in the same
change.

The bump to `0.30.0` is what it looks like when a version brings work rather
than exemptions. Five shapes arrived and the gate named all five: `authorizedRoute`
is **implemented**, because it is embedded in the request envelope and is the
subject of the failover section below; `sha256Digest` and the three
`aliaModelRelease` shapes are recorded not-applicable, because a signed model
release manifest is Oxy's publishing pipeline and the data plane is handed the
resulting reference. It also changed two shapes Relay already implements — a
`refusal` content part, and the optional route list on the request — and the
descriptor test named each one as a field or enum member Go was missing rather
than leaving them to be noticed at runtime.

### The `refusal` content part, and which dialect can carry one

1.2.0's `refusal` variant is a REQUEST-side shape: it lets a customer replay a
conversation in which the assistant declined. It is content and not a failure —
the request succeeded and the provider billed for it — so it is normalized like
any other part and never converted into an error.

The two dialects answer differently, and the difference is stated rather than
smoothed over:

- **`openaicompat` carries it, on the MESSAGE.** This protocol keeps an
  assistant's refusal in the message's own `refusal` field, which is also where
  the adapter already READS one from on the way back. The contract models it as a
  content part, so `translateMessage` lifts it out of the content list and onto
  the field. Two refusals in one message are refused rather than joined: the field
  holds one string, and concatenating them would invent a refusal the model never
  produced.
- **`anthropic` refuses it, with the real reason.** The messages api has no
  refusal block; a refusal arrives as ordinary `text` with
  `stop_reason: "refusal"`. Replaying one as text would tell the model that a
  refusal was the assistant's own prose, and it would answer a different
  conversation while the request reported success. So it is refused with
  `unsupported_modality` and the reason names `stop_reason`, because "this
  protocol has nowhere to put it" and "this protocol keeps it somewhere else" are
  different answers and only one is a request the customer can fix.

Neither adapter invents a field, which is the rule that decided both.

**What it catches:** a field renamed, added, removed, or flipped between
required and optional; a scalar's type changed; a reference repointed; a version
literal changed; a variant added to a discriminated union; an enum member added
or removed. `drift_test.go` proves this rather than asserting it — it perturbs
the descriptor in each of those eleven ways and requires the comparator to
report every one, after first confirming the unperturbed comparison is clean.

**2. Relay's own output, parsed by the real Zod schemas.**
Structure is not values. `go test ./internal/contract/...` marshals one fixture
per wire shape using the same Go types the server uses — with **every optional
field populated**, because an optional field that drifted is invisible in a
minimal fixture — and `tools/contract/validate.mjs` parses each with the
published schema itself. It also feeds six deliberately invalid fixtures that
the schemas must **reject**, and fails if it saw no fixtures at all: a validator
with a broken schema lookup would otherwise report the same clean run.

Regenerate after a version bump:

```bash
cd tools/contract && npm ci && npm run generate
```

## The Oxy-facing surface

```
POST /internal/v1/inference    signed envelope in, normalized event stream out
GET  /internal/v1/health       signed; the customer-safe provider projection
GET  /livez                    unsigned liveness; no provider or route detail
```

An HTTP status answers exactly one question: was this a well-formed envelope
from the Oxy edge? Once the answer is yes the response is `200` and an event
stream, and every outcome after that — including a refusal — arrives as the
stream's terminal `error` event. The alternative cannot be made to work: a
request that fails after two hundred tokens has already sent `200`.

Frames are named. `event: stream_event` carries a contract stream event;
`event: usage_report` carries the technical usage record. The framing is
Relay's own — the contract specifies shapes and says nothing about transport.

**Envelope versioning is a hard gate.** `schemaVersion` is read before anything
else is interpreted; an unrecognised version is refused whole. A version is
never inferred from the presence of a field. Conversely, **unknown fields are
tolerated**: the contract states that adding an optional field is additive, so a
strict decoder would turn every additive Oxy change into an outage here.

**Edge authentication is Ed25519 over the exact body** — Relay holds only public
keys, so it cannot construct an envelope it would itself accept. This is a
decision Oxy has not made; the reasoning, and why it is not an HMAC, is in
`internal/edgeauth`. It is deliberately one small file to replace.

## Cancellation

A client disconnect cancels the upstream provider call. The proof is split in
two, and each half is mutation-tested — the mutation was applied, the test was
observed to fail, and the file was restored:

| Link | Test | Mutation that must break it |
|---|---|---|
| client → executor | `internal/httpapi`: `TestClientDisconnectCancelsExecution` | `Execute(context.Background(), …)` instead of `r.Context()` |
| adapter → upstream | `internal/provider/conformance`: "a client disconnect cancels the upstream call" | `http.NewRequestWithContext(context.Background(), …)` in the adapter |

Both compare against a **control run that is not cancelled**, because "the
upstream saw its caller go away" is also what a request that simply finished
reports. The cancelled run must show the upstream observing the disconnect
*before* it wrote every chunk; the control must show it never observing one and
writing all of them.

A cancelled request still produces a usage report with the units measured up to
the cut, and settles as `cancelled`. A partial stream is a settlement case, so
an adapter that returned nothing on cancellation would make an exact refund
impossible.

## Same-model failover

When a deployment fails in a way another one could survive, the same **model
revision** is retried somewhere else. Never a different model: the platform
forbids serving weights the customer did not ask for, and a fallback that
crossed models would look exactly like a success.

**That distinction is structural, not a rule someone remembers.** A reference
resolves to an `inventory.RouteSet`: one model reference, and the endpoints that
serve it. The reference is stored **once, for the whole set**, and an
`inventory.Endpoint` carries none of its own — so a candidate that names
different weights is not a case that has to be excluded, it is a value that
cannot be constructed. `RouteSet.Candidates()` is the only place a route is
built from an inventory, and it stamps the set's single reference onto every
one. Two guards sit on top of that shape, and both are mutation-tested:
`TestAnEndpointCannotCarryItsOwnModelReference` fails if a future change gives
an endpoint its own model, and the emitter refuses to announce a switch whose
origin and destination references differ.

The `route_switch` event it emits is deployment-scoped and cannot be anything
else: the fields that describe a substitution — `requestedModelId`,
`fromModelReference`, `toModelReference`, `authorizedByPolicy` — are not set,
and the function that builds the event takes no argument from which they could
be.

**A switch is only possible while nothing has been streamed.** Once output has
reached the customer, retrying elsewhere would deliver the beginning of one
answer and the whole of another, so the emitter refuses — and because the
executor asks the emitter rather than keeping its own copy of the rule, there is
one place that knows it. That is also why the `route_switch` event **precedes**
the `start` event: the switch really did happen before anything was streamed,
and the contract specifies event shapes without specifying their order.

**A switch is announced at the attempt that replaces the failed one**, not at
the moment of failure — the replacement's own breaker may refuse it, and
announcing early would tell a customer their request moved somewhere it never
went, and put a switch on the receipt that never happened.

**What is never retried elsewhere:** a request the provider could not express (a
refusal about the request, identical everywhere — retrying would make what a
request *means* depend on which route happened to be healthy), a content filter,
a cancellation, and any failure no adapter classified. One function decides,
`provider.AttributableCategory`, and the circuit breakers read the same one, so
the two can never drift apart.

### The authorized route list, which is what authorizes failover

**Contract 1.2.0 answers the question this section used to be about.**
`inferenceRequestSchema.authorizedRoutes` is an ordered list of routes the Oxy
edge has ALREADY authorized for one request — deployment, model reference,
provider, regions — primary first. Oxy filters candidate deployments against the
customer's routing policy, which it can do because it holds the policy, and sends
the RESULT.

That makes failover "take the next entry", and it means **no policy semantics
exist in Go at all**. `provider` and `regions` on an entry describe what Oxy
permitted; Relay does not filter on them, because re-deriving admissibility from
them would rebuild the enforcement engine the list exists to remove, in the
language with no schema to check it. A route outside the customer's policy is
unreachable because it is **absent**, not because Relay declined it — which is
the difference between a guarantee and a check somebody can forget.

Three properties, each with a test that fails without it:

- **A deployment the list does not name is never attempted**, even when the
  inventory holds it and it is healthy.
- **A `cross_model` entry never becomes a candidate.** Relay carries the variant
  — the published union has two, and the descriptor gate requires both — but
  `Candidates` filters to `same_model`, so the `route_switch` emitter still has
  no argument from which it could describe a substitution. See "Explicitly out of
  scope".
- **The list's order is the tie-break.** Which routes are permitted is Oxy's;
  which of the permitted ones is tried first when their breakers are equally
  healthy is Relay's routing execution, so health ordering still applies and the
  list's order decides the rest.

A list naming only deployments this build cannot reach is a `service_unavailable`
that NAMES them, because "no route" and "no route I was allowed to use" send an
operator to different places — the catalogue in one case, the snapshot in the
other.

### The default is still OFF for a request with no list, and that is deliberate

`authorizedRoutes` is **optional** in the contract, so an edge that has not been
taught to send one sends none, and for those requests nothing has changed: the
envelope still carries a routing policy **reference** and none of
`routingFallbackPolicySchema`'s `disabled` / `sameModelDeployment` or
`routingPolicySchema`'s `allowedRegions` / `deniedRegions`. Relay still cannot
tell a customer who asked for failover from one who switched it off, so a
reference resolves to its **declared primary deployment and nowhere else**, and
health ordering is withheld with it.

**Turning the default on would be wrong now for a sharper reason than before.** A
missing list is no longer indistinguishable from "no policy" — it is a
distinguishable state that means *this edge has not told me*, and the honest
response to it is the same conservative one. It would also mask the rollout:
failover switches on per customer, automatically, as soon as Oxy starts sending
lists, and a global flag set in the meantime would make it impossible to tell
whether the lists are arriving at all.

`RELAY_ASSUME_FAILOVER_AUTHORIZED=<reason>:<YYYY-MM-DD>` still turns it on for
requests that carry no list, and is now best read as a TRANSITIONAL
acknowledgement for an edge that does not send them yet. It is deliberately
awkward: it states that every caller of this process has a routing policy
permitting same-model failover across every deployment in its inventory, which is
true of a first-party canary and of nothing else. An empty value, a bare `true`,
or a reason with no date either leave the default in place or refuse to start —
never enable it. A request that carries a list ignores it entirely: the list is
the authorization, and it is per request rather than per process.

## Circuit breakers and health scoring

One breaker and one health score per **deployment**. The unit is the deployment
rather than the provider because a provider is usually several deployments in
several regions, and taking all of them out because one is failing throws away
the capacity failover exists to use.

| State | Meaning |
|---|---|
| `closed` | admitting requests |
| `open` | out of rotation until its cooldown expires |
| `half_open` | the cooldown expired; one real request at a time decides its fate |

**What trips one:** only a failure attributable to the deployment — the upstream
refusing, timing out, rate limiting, exhausting a quota, or rejecting *Relay's
own* credential. Three consecutive ones open it, and the count is consecutive
rather than a rate because a rate needs a window and a window needs a traffic
assumption.

**What must never trip one:** a request the provider cannot express, a content
filter, a client that hung up. Those fail identically everywhere, so counting
them against a deployment would let one customer's malformed traffic take a
healthy route out of rotation for everybody — a denial of service with extra
steps. `Permit.NotAttributable` is how a caller says so rather than defaulting
into it, and it is mutation-tested from both directions.

**What probes it back in:** a cooldown, then **one real customer request**. Not
a burst — half-open admits exactly one trial at a time, because everything that
arrives the moment a cooldown expires is a thundering herd onto the provider
that just stopped failing. And not a synthetic probe: a synthetic probe proves
the provider answers some *other* request than the one it is failing, and Relay
would be paying for it. A successful trial closes the breaker; a failed one
reopens it with a doubled cooldown, capped, so a long outage is still retried
within a bounded time.

The **health score** is an exponential moving average of attributable outcomes,
and it orders candidates: admitting breakers first, healthier before flakier,
the inventory's declared order as the tie-break. A deployment nothing has routed
to scores 1 — assuming the worst would sort it permanently last, and it would
never receive the traffic that would prove otherwise.

When every deployment of a model is out of rotation the request is refused with
`deployment_unavailable`, carrying a retry hint that is **the moment the
earliest breaker will admit its next trial** rather than a number chosen to look
reasonable.

## Configuration snapshots

Relay's configuration arrives as a file the control plane publishes. If that
pipeline stops, the data plane must not stop serving — and must not start
pretending it knows things it no longer knows. Those are two requirements, and
`inventory.Store` keeps them apart.

**A failed reload changes nothing.** The last good snapshot stays installed,
whole: a half-parsed inventory is never swapped in, so there is no state where
some references resolve and others silently vanish.

**What a stale snapshot may serve: any pinned reference, at any age.** The
mapping from immutable weights to a provider's model id cannot go stale, and a
pinned request is served or refused, never substituted — so the customer is told
exactly which weights answered, as always.

**What it may not serve: the choice of a current revision.** Which revision an
unpinned reference resolves to is Oxy's decision and it is the one thing in the
file that decays. Past the horizon (`RELAY_INVENTORY_MAX_AGE`, default one hour)
an unpinned reference is refused with `service_unavailable`, retryable, naming
the age and saying that a revision-pinned reference is still served. Guessing
instead would serve weights Oxy may have replaced hours ago on a decision nobody
made.

**Prices do not enter this** — Relay holds none — which removes the hardest half
of the usual stale-configuration problem. The only thing that decays here is a
routing choice, and it degrades rather than breaking.

**One requirement this places on the publisher:** staleness is measured from the
snapshot's own `issuedAt`, not from when Relay last read the file. That is the
only measure that survives the failure that matters — a publisher that has
stopped running leaves a perfectly readable file on disk, and re-reading it
every thirty seconds would report it fresh forever. So the snapshot must be
**re-issued on a cadence shorter than the horizon even when nothing has
changed**. An unchanged snapshot with an old `issuedAt` is indistinguishable,
from here, from a control plane that has stopped publishing, and is treated as
one.

`GET /internal/v1/health` reports the snapshot id, its age, the horizon, whether
unpinned references are still being resolved, and the last reload failure with
no filesystem path in it.

## Provider cost

What the upstream will invoice Relay for a request. It is an **operator**
number, and this is the only package in the repository that holds an amount of
money at all.

It is deliberately not the contract's money type. ADR 0006 gives Oxy every
customer-facing amount and Relay its own upstream cost; `internal/contract` has
no money type and must not acquire one, so the two cannot be confused by
reaching for the same struct. Nothing here appears in any produced shape: the
stream events, the usage report and the error body have no field it could
occupy, and the descriptor gate fails on any field added to them that the
contract does not have. The containment check is the same amount in two places —
present in the operator log, absent from every byte the customer receives — with
a control proving a non-zero cost was measured, so "no cost in the response"
cannot be what an unpriced request also reports.

**A failed failover attempt is off the customer's receipt and on Relay's cost.**
The customer never received that output, so charging for it would be wrong; the
provider invoices for it regardless, so dropping it would leave Relay
reconciling against a number short by exactly its own failover traffic. That
asymmetry is why this is a separate measurement rather than a field on the usage
report.

**An unknown cost is not a zero cost.** A deployment with no rate card, or a
unit a card does not price, produces a measurement that says so and names the
unpriced units. Summing unknowns as zero yields a reconciliation that looks
complete and is quietly short by exactly the traffic nobody priced.

Rate cards are optional (`RELAY_PROVIDER_RATES_PATH`), live in their own file
read by their own package, and are keyed by deployment id. Amounts are integers
in 1e-12 of the currency's major unit — the same scale as the published
contract's money type, so an operator reconciling an invoice against the ledger
is comparing like with like.

## The provider adapter interface

```go
type Adapter interface {
	Provider() contract.ProviderSlug
	Translate(request *contract.Request, route Route) (*Call, error)
	Stream(ctx context.Context, call *Call, out Emitter) (Outcome, error)
	Health(ctx context.Context) Health
}
```

- **`Provider`** names the slug every event and usage record attributes work to.
  It comes from the adapter, not its registration site, so a mis-registration
  cannot mislabel a receipt.
- **`Translate`** is pure. A request the provider cannot express is refused
  before anything is spent upstream, and a pure translation is testable with no
  network — which is what makes covering that refusal cheap.
- **`Stream`** is execution, streaming, cancellation and usage measurement
  together, because they share one lifetime: the units are only correct if they
  come from the same read loop that saw the last frame before the cut.
- **`Health`** must be answerable without a customer request, so it cannot be
  folded into `Stream`.

What an adapter deliberately does **not** do: allocate ids, assign sequence
numbers, decide terminality, emit `done`/`error`/`route_switch`, resolve a model
reference to an upstream model id, or apply routing policy. Those are one
implementation in the executor, not one per provider. Adapters report semantic
content through `Emitter`, which stamps `requestId`, `sequence` and
`schemaVersion` itself — removing the whole class of bug where one provider's
events are unattributable or repeat a sequence.

## The ported adapters

Two protocols are implemented, and the second one exists to test the first one's
abstraction rather than to add a provider.

### `openaicompat` — OpenAI Chat Completions

A port of Alia's `openai` provider
(`packages/api/src/internal/providers/lib/providers/openai.ts`).

**Why that one.** Seven of Alia's adapters — openai, together, xai, cerebras,
hyperbolic, digitalocean, openrouter — are byte-identical apart from a base URL
and the word in their error string, because they all speak the OpenAI Chat
Completions protocol. Porting the *protocol* rather than a provider makes the
next six a `Config` and a conformance registration.
`TestOneProtocolServesSeveralProviders` runs the full suite under three more
slugs to keep that claim honest.

**What the port deliberately changes.** Alia's `proxy()` returned the upstream's
raw stream to its caller — no normalization, no usage, no cancellation, no error
classification. It also never sent `stream_options.include_usage`, so **a
streamed request reported no usage at all**; a faithful port of that would be a
billing hole. And it substituted `temperature: 0.7` / `max_tokens: 8192` when the
caller set none, which silently changes every request nobody configured.

### `anthropic` — the Messages API

A port of Alia's `anthropic` provider
(`.../providers/anthropic.ts`), and the answer to a question one adapter cannot
settle: whether `provider.Adapter` describes a provider or describes the first
one written against it.

**Why that one.** It disagrees with chat completions on every axis the interface
names — named SSE events instead of one repeated frame closed by `[DONE]`;
indexed content **blocks** whose kind is declared once in the event that opens
them; reasoning as a block type rather than a field; usage split across two
events with a **cumulative** output count; a failure that can arrive *inside* the
stream after a 200; `x-api-key` with a mandatory `anthropic-version` instead of a
bearer token; the system prompt hoisted out of the message list; a tool result
carried as a user message; and `max_tokens` **required**. A second
OpenAI-compatible provider would have exercised none of that.

Its usage fields also nest the *other way round*, which is the finding with money
attached: `input_tokens` **excludes** cached tokens where an OpenAI-compatible
`prompt_tokens` includes them, while `output_tokens` includes reasoning exactly
as `completion_tokens` does. So one of the two normalising subtractions the
contract's partition needs applies here and **the other must not** — and an
adapter written by copying the first one would under-report every cached request.

**What the port deliberately changes.** Alia's conversion read only a text delta
and `message_stop`: tool calls, reasoning, stop reasons and the whole of `usage`
were dropped, so a request that called a tool produced no tool call downstream
and every request reported no usage at all. It also defaulted `max_tokens: 8192`
and `temperature: 0.7`, and forced `stream: true`.

**What it refuses rather than inventing.** `max_tokens` is required upstream and
optional in the contract, so a request that omits `maxOutputTokens` is refused
with the field named. Choosing a ceiling here — or per deployment, which only
moves the invention into a config file — would truncate an answer the customer
asked to be unbounded and report success. That is item 14 below.

**No live provider call has been made from this repository.** There are no
provider credentials here, in the tests, or in CI. Both adapters are exercised
against a fake upstream that speaks the real wire format, including its habit of
echoing the request's credential header back inside an error message.

### What the second adapter changed

The `Adapter` interface itself did not change: `Provider`/`Translate`/`Stream`/
`Health`, `Call`, `Route` and `Outcome` all held. Three things around it did, and
each was a gap rather than a preference:

- **`Emitter` has nowhere to put provider-opaque block metadata.** A thinking
  block's `signature` is what makes multi-turn tool use with reasoning work, and
  no contract stream event has a field for it. The adapter reads it so it cannot
  be mistaken for output, and drops it. Item 17.
- **The conformance suite could only be told about one refusal**, which was an
  accident of the first adapter having exactly one. It now takes a list.
- **Credential redaction could not be left to the contract's pattern.** It is
  keyed to bearer-token shapes; against `x-api-key: <value>` it matches the
  marker and not the value, so redacting *removes the evidence and keeps the
  credential*. `provider.RedactSecret` removes the adapter's own key by exact
  match first. Item 18.

## The conformance harness

`internal/provider/conformance` is the suite an adapter must pass. An author
supplies five things — how to build the adapter, how to start a fake upstream
speaking that provider's **real** wire format, the route it serves, the requests
the provider genuinely cannot express, and what its fake upstream physically
consumed and produced — and gets back:

slug validity and stability · event framing (one `start` first, monotonic
sequences, `requestId` and `schemaVersion` on every event, exactly one terminal)
· a revision-pinned resolved model · the same normalized shape from a
non-streamed upstream · **units that partition the request**, on both read paths
· a provider that reports no usage settling as an estimate · tool calls a client
can reassemble · a transient throttle classified retryable · an exhausted
account classified non-retryable · **a refused PLATFORM credential classified
non-retryable and still attributable** · **a failure that arrives after the
response started** · **the configured credential never reaching the customer**, with a
control asserting the upstream actually echoed it AND that the customer still
receives the upstream's diagnostic rather than losing it to the contract's
refusal · one refusal per class, each
spending nothing upstream and naming the field at fault · cancellation, with its
control · health with and without a credential.

The suite drives the adapter through the **real executor**, because an adapter is
only correct in the shape it is actually used.

**What the second adapter changed here, and which half of it was general.** Six
changes, and the distinction matters:

| Addition | General, or the suite having been OpenAI-shaped? |
|---|---|
| Units partition the request (`StreamedUsage`) | **General.** It is the contract's own rule, and it caught the double-charge on the *first* adapter when that adapter's subtraction was removed — the suite had no check that would have. |
| A failure arriving mid-stream, after a 200 | **General.** Both protocols can do it; neither adapter handled it before, and `openaicompat` was reporting a truncated answer as a completed one. |
| A refused platform credential (`provider_credential_invalid`) | **General**, and newly expressible: the code landed in `@oxyhq/contracts@0.28.0` while this branch was open. Both adapters were reporting it as retryable. |
| A list of refusals rather than one | **The suite was OpenAI-shaped.** One slot fit because the first adapter had exactly one refusal class. |
| `maxOutputTokens` populated in the fixture | **The suite was OpenAI-shaped**, in the sense that a minimal fixture only passes for a provider that requires nothing the contract makes optional. Populating optional fields is this repository's own rule anyway. |
| "an exhausted quota on the *same status*" | **Was OpenAI-specific prose.** The invariant is that an adapter tells a throttle from an exhausted account; that they share a status is one provider's habit. Wording only. |

## Running it

Everything comes from the environment and one inventory file. There is no
unauthenticated mode, not even for local development: a bypass that exists is a
bypass that ships.

| Variable | Required | Meaning |
|---|---|---|
| `RELAY_INVENTORY_PATH` | yes | deployment inventory snapshot (see `configs/inventory.example.json`) |
| `RELAY_EDGE_PUBLIC_KEYS` | yes | `kid:base64,…` Ed25519 **public** keys; not secret |
| `RELAY_PROVIDER_OPENAI_API_KEY` | no | absent ⇒ the provider reports `unconfigured` |
| `RELAY_PROVIDER_OPENAI_BASE_URL` | no | default `https://api.openai.com/v1` |
| `RELAY_PROVIDER_ANTHROPIC_API_KEY` | no | absent ⇒ the provider reports `unconfigured` |
| `RELAY_PROVIDER_ANTHROPIC_BASE_URL` | no | default `https://api.anthropic.com/v1` |
| `RELAY_PROVIDER_RATES_PATH` | no | upstream rate cards; absent ⇒ provider cost is not measured |
| `RELAY_INVENTORY_MAX_AGE` | no | staleness horizon for unpinned resolution, default `1h` |
| `RELAY_INVENTORY_RELOAD_INTERVAL` | no | default `30s` |
| `RELAY_ASSUME_FAILOVER_AUTHORIZED` | no | `<reason>:<YYYY-MM-DD>`; absent ⇒ no failover, see above |
| `RELAY_BREAKER_FAILURES_TO_OPEN` | no | default `3` |
| `RELAY_BREAKER_COOLDOWN` | no | default `5s` |
| `RELAY_BREAKER_MAX_COOLDOWN` | no | default `2m` |
| `RELAY_BREAKER_SUCCESSES_TO_CLOSE` | no | default `1` |
| `RELAY_ADDR` | no | default `:8080` |
| `RELAY_EDGE_MAX_SKEW` | no | default `5m` |
| `RELAY_MAX_ENVELOPE_BYTES` | no | default `16777216` |

```bash
go build ./... && go vet ./... && go test -race ./...
golangci-lint run ./...
cd tools/contract && npm ci && npm run generate && npm run validate
```

## Explicitly out of scope

Named here so nobody assumes otherwise. None of these is stubbed; each is simply
absent, and the code refuses rather than pretending.

- **Cross-model fallback.** Contract 1.2.0 made it EXPRESSIBLE — an
  `authorizedRoute` entry may say `substitution: "cross_model"` with the literal
  `authorizedByPolicy: true` — and Relay carries the variant, round-trips it and
  has the published schema accept it. It never SELECTS one: `Candidates` filters
  to `same_model`, so a cross-model route is not excluded downstream, it never
  becomes a candidate.
  **Enabling it is a separate change and a deliberate one**, because it has to
  revisit two mutation-tested structural guarantees rather than relax a check:
  `TestAnEndpointCannotCarryItsOwnModelReference`, and the emitter's refusal to
  announce a switch whose origin and destination references differ. The
  `route_switch` event Relay builds is deployment-scoped by construction and the
  function that builds it takes no argument that could make it a substitution —
  that is the property a cross-model build would have to give up, and it should be
  reviewed as that rather than arriving inside a dependency bump.
- **Failover without authorization.** Same-model failover is built and tested. A
  request carrying an `authorizedRoutes` list is authorized by the list; a request
  without one falls back to the declared primary unless an operator has set the
  transitional acknowledgement. See the two failover sections above.
- **Reconciliation of provider cost against provider invoices.** Relay measures
  what each request cost it upstream; matching that against what a provider
  actually billed is a finance process with no home in a data plane.
- **Oxy-hosted open-weight serving (vLLM/SGLang) and any GPU scheduler.** The
  epic says not to block the first API-only launch on a scheduler.
- **BYOK.** The provider-connection shapes are recorded not-applicable.
- **Modalities other than text.** Embeddings, images, audio and rerank are
  refused with `unsupported_modality` rather than mistranslated.
- **Routing-profile targets**, for the contract reason below.
- **Replay protection beyond the signature time window.** Relay keeps no nonce
  cache; the edge owns request idempotency.

## What Oxy still has to decide

These surfaced while implementing against the contract, which has never been
implemented against before. Each is a real gap, not a preference.

1. **The envelope carries a routing policy *reference*, not a snapshot.**
   `inferenceRequestSchema.routingPolicy` is `{routingPolicyId, policyVersion}`.
   But ADR 0006 assigns *routing execution* to Relay and ADR 0010 says the
   envelope carries "the resolved routing policy snapshot and its version". As
   published, Relay **cannot** enforce provider allowlists, region residency,
   zero-data-retention, licence constraints or price ceilings — it has no
   values to enforce. Either the envelope must carry the snapshot, or the ADRs
   should say plainly that Oxy enforces all of it and Relay only executes.
   Item 11 is the sharpest instance of this and the one that blocks working
   code.
2. **A `routing_profile` target is unresolvable.** `routingTargetSchema` lets a
   request say "choose one for me", but the candidate list lives in the Oxy
   catalogue's `routingProfileSchema` and is not in the envelope. This build
   refuses such a request with `invalid_request` and `param:
   target.routingProfile` rather than picking a model, which would be exactly
   the silent substitution the platform forbids. Either Oxy resolves the profile
   before forwarding (in which case, why send the profile kind?), or the snapshot
   travels with the envelope.
3. **`requestId`'s owner is stated two different ways.** `identifiers.ts` says
   "a request id generated by the data plane", but it is *required* inside
   `attribution` on the inbound envelope, so it cannot be. ADR 0010 has the edge
   allocating it at step 1. Relay implements the ADR's reading — Oxy allocates
   `requestId`, Relay allocates `generationId` — and the comment should be
   corrected.
4. **No reservation or deadline in the envelope.** ADR 0010's `InferenceEnvelope
   v1` lists `reservation {reservationId, ceiling, priceVersion}` and `deadline`;
   `inferenceRequestSchema` has neither. So Relay cannot enforce a spend ceiling
   or an execution deadline, and `normalizedUsageReportSchema` carries no
   `reservationId` (nor an `idempotencyKey`, though `usageReceiptSchema` has
   one) — Oxy must correlate settlement by `requestId` alone. Workable, but it
   should be a stated decision.
5. **`cached_input_tokens` and `reasoning_tokens` are not defined as subsets or
   siblings.** *Answered by OxyHQ/oxy#1019: the units **partition** a request,
   which is what Relay already reported and what the ledger's arithmetic already
   assumed.* Two things follow that the answer does not cover, and the second
   adapter is where both surfaced.

   First, **the normalising subtraction is per provider and is not the same
   subtraction**. An OpenAI-compatible `prompt_tokens` includes its cached
   tokens; Anthropic's `input_tokens` is documented as excluding both of its
   cache counts, with the prompt total being
   `cache_read + cache_creation + input_tokens`. Copying the first adapter's
   arithmetic into the second would under-report every cached request. Both
   halves are now a conformance check rather than a comment.

   Second, **there is no unit for a cache WRITE**. Anthropic reports
   `cache_creation_input_tokens` separately and prices it at 1.25× to 2× the base
   input rate, against 0.1× for a read. Relay folds writes into `input_tokens`,
   because the alternative — reporting them as `cached_input_tokens` — would
   price the most expensive input tokens in the request at the cheapest rate on
   the card. The units still partition the request; what is lost is the premium,
   on Relay's own cost side. A `cache_write_input_tokens` unit would close it.
6. **The closed error set has no non-retryable platform-side failure.**
   *Answered by OxyHQ/oxy#1019, which added `provider_credential_invalid`
   (non-retryable), published in `@oxyhq/contracts@0.28.0` and adopted here.*
   Both adapters now report an upstream refusing the PLATFORM's credential under
   that code, with category `authentication`. The two halves pull in opposite
   directions on purpose: the code is non-retryable so a client stops hammering a
   request that cannot succeed, and the category is attributable so the breaker
   still takes the route out of rotation and a same-model failover to a
   deployment holding a DIFFERENT credential is still allowed. It is a
   conformance check, so the next adapter inherits it. Getting there also
   surfaced item 19, which is the larger of the two findings.

   **The neighbouring gap is closed too.** An upstream refusing to *bill* the
   platform is the same class of failure — only an operator can act, no retry
   helps — and reporting it as `quota_exceeded` was correct about retryability
   and wrong about whose account is exhausted, which reads as actionable while
   the action does nothing. `provider_billing_refused` landed in
   `@oxyhq/contracts@0.29.0`; Anthropic's 402 `billing_error` and an
   OpenAI-compatible `insufficient_quota` both map to it, and the conformance
   suite refuses any code that names the CUSTOMER's money for that scenario.
7. **Nothing specifies how Relay authenticates the edge.** See
   `internal/edgeauth` for what Relay implements and why it follows ADR 0012's
   asymmetric reasoning rather than a shared secret.
8. **The deployment descriptor has no upstream model identifier.**
   `modelDeploymentSchema` cannot express what a provider calls a model, so that
   mapping lives in Relay's inventory. The same descriptor also carries
   `availabilityScope`, `commercialPermission` and `priceVersionId` — Oxy
   commercial decisions under ADR 0006 — so the shape currently has two owners
   and no stated direction of exchange.
9. **Nothing says who picks the current revision of an unpinned reference.** The
   contract says Oxy chooses it, but the envelope carries no resolution and the
   `start` event must report a revision-pinned reference — so in practice Relay
   chooses. It does so from an explicit `current` flag in the inventory.
10. **Several produced shapes are not `.strict()`.** The stream events, the
    usage report and the error body all allow unknown keys, so a field Relay
    emitted by mistake is silently stripped at Oxy's parse rather than caught.
    The request's `client` block *is* strict, and that strictness is what makes
    its privacy rule enforceable — the same argument applies to the rest.
11. **ANSWERED in contract 1.2.0 — the customer's own switch for same-model
    failover now reaches the data plane, as a result rather than as a policy.**
    This item asked for the routing policy snapshot to travel, and noted the
    alternative: "Oxy should state that it resolves the deployment as well as the
    model, and send one." That is what `inferenceRequestSchema.authorizedRoutes`
    does, and it is the better of the two answers — a snapshot would have put
    provider allowlists, residency and price ceilings into Go, where a second
    enforcement engine would have to agree with Oxy's forever. A pre-authorized,
    ordered list needs no policy semantics here at all.
    Two halves of the original question remain open, and they are smaller:
    **(a)** the field is optional, so a request without one is still a customer
    Relay cannot read a failover preference for — see "The default is still OFF";
    **(b)** an entry Oxy authorized that this build's inventory does not hold is
    answered with `service_unavailable` naming the deployment, which is the
    exchange-direction question item 8 already asks for, now with a concrete
    symptom.
12. **The contract specifies event shapes and not their order.** Relay emits
    `route_switch` *before* `start`, because the only switch it can safely
    perform is one where nothing has been streamed yet, and saying so in order
    is the truthful framing. The alternative reading — that `route_switch`
    amends a `start` already sent — is only expressible for a switch that
    happens mid-stream, which would duplicate output. If any consumer assumes
    `start` is always the first event, that assumption should become a stated
    rule in the contract rather than an implicit one.
13. **A model-scope `route_switch` cannot be constructed for a pinned request.**
    `routeSwitchDetail.requestedModelId` is the *unpinned* model line the
    customer asked for, so a request that pinned a revision has no value that
    satisfies the field. That is consistent with never substituting pinned
    weights, but it means cross-model fallback is expressible only for unpinned
    requests, which is worth stating rather than discovering.

The five below came from implementing the SECOND adapter. They are the ones a
single implementation could not have found, because each is a place where the
contract fits one provider's shape and not another's.

14. **`maxOutputTokens` is optional, and at least one provider requires it.**
    The Anthropic Messages API rejects a request with no `max_tokens`. The
    contract makes the field optional and says nothing about what its absence
    means, so an adapter has three options and two of them are wrong: invent a
    ceiling (silently truncates an answer the caller asked to be unbounded, and
    reports success), take one from the deployment descriptor (the same
    invention, moved to a config file nobody reads), or refuse. This build
    refuses, with `invalid_request` and `param: maxOutputTokens`, which means a
    request that is valid under the contract and served by one provider is
    refused by another. Either the contract should require the field, or
    `modelDeploymentSchema` should carry a per-deployment output ceiling and the
    contract should say that an absent value means it — but it should say which.

15. **There is no unit for cache-write tokens.** See item 5. A provider that
    prices a cache write at a premium and a cache read at a tenth of the input
    rate cannot be metered exactly against the published unit list.

16. **`refusal` had no finish reason of its own.** *Closed in
    `@oxyhq/contracts@0.29.0`.* The Messages API stops with
    `stop_reason: "refusal"` when the model declines; the contract's finish
    reasons ended at `content_filter`, so Relay had to report a filter acting
    where the model had declined — different things to a customer deciding
    whether to rephrase, and a distinction the delta channels already carried.
    `refusal` is now a finish reason and this adapter emits it.

17. **No stream event can carry provider-opaque block metadata.** An
    extended-thinking response returns a `signature` per thinking block, and the
    provider REQUIRES those blocks back, unmodified, on the next turn of a
    tool-use conversation. `streamDeltaEvent` has `channel` and `text` and
    nothing that could hold it, and the request side has no content-part type
    for a thinking block either — so the round trip is not expressible in either
    direction, and multi-turn tool use with reasoning cannot be served through
    this contract at all. Relay reads the signature so it cannot be mistaken for
    output, and drops it. This is a design decision rather than an oversight to
    patch: an opaque per-block blob crossing the boundary needs a home nobody has
    chosen yet.

18. **`safeErrorTextSchema`'s credential pattern was bearer-shaped, and
    redacting against it made a leak worse.** *Closed in
    `@oxyhq/contracts@0.29.0`.* The old pattern refused `authorization:`,
    `bearer <token>`, `api_key=` and `sk-…`. An upstream echoing
    `{x-api-key: <value>}` matched the **marker** and not the **value**, so
    redacting the match produced `{x-[redacted] <value>}` — which no longer
    tripped the refinement and was therefore *accepted* with the credential
    intact. The rewrite is four independent signals, one of which is a
    placeholder standing beside a surviving opaque value: the residue of exactly
    that span redaction.

    **Two things this repository has to keep doing, and they are the reason the
    item stays here rather than being deleted.** First, `SafeErrorText` no
    longer redacts a span — it withholds the whole message or none of it, since
    a span redaction is now both wrong and *refused*. Second, the published
    refinement is a last-resort refusal and says so: it cannot see a credential
    with no marker, no issued-token prefix and no placeholder beside it, because
    refusing those bytes means refusing request ids. `provider.RedactSecret`,
    applied by the adapter that still holds the bytes it sent, is the control —
    and it earns its place twice over. Where the pattern *does* recognise the
    shape, redacting the value is what keeps the customer's diagnostic instead
    of losing the whole message to the refusal; where it does not, redaction is
    the only thing between the credential and the customer.
    `internal/contract`'s fixture table pins both halves against the published
    schema itself, including a string that carries a secret and is accepted.

19. **A published version number did not identify the contract it names, and
    nothing gates that.** While this adapter was being written,
    `@oxyhq/contracts` on `main` and `@oxyhq/contracts@0.27.0` on npm had
    *different contents under the same version*: `main`'s `errors.ts` carried
    `provider_credential_invalid` and the published tarball did not, because
    #1019 merged after 0.27.0 shipped and did not bump the version. The
    immediate effect here was a code that could not be adopted; the lasting one
    is that "which 0.27.0 do you have" had no answer.

    **No in-repo consumer could have seen it.** Everything inside the monorepo
    resolves `workspace:*` and therefore reads `main`'s source; this repository
    is the first consumer that installs the published artefact, so the two
    copies had never been compared before. That is also why the fix belongs
    upstream rather than here.

    OxyHQ/oxy#1025 bumped and published `0.28.0`, closing the drift, and this
    build is generated against it. What is still open is the gate: nothing
    fails when a change to `packages/contracts/src` merges without a version
    bump, so the same divergence can reopen silently on any later PR. A CI check
    comparing the working tree's contract source against the published tarball
    for the version in `package.json` would answer it once.

[epic]: https://github.com/OxyHQ/oxy/issues/972
[adr0005]: https://github.com/OxyHQ/sdk/blob/main/docs/adr/0005-oxy-is-the-single-control-plane.md
[adr0006]: https://github.com/OxyHQ/sdk/blob/main/docs/adr/0006-oxy-relay-boundary.md
[adr0011]: https://github.com/OxyHQ/sdk/blob/main/docs/adr/0011-inference-data-plane-name.md
