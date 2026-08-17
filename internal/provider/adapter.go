// Package provider defines the contract every upstream adapter satisfies.
//
// This interface is the point of the data plane. Everything else in Relay is
// plumbing around it: the HTTP surface exists to hand an adapter a normalized
// request, and the executor exists to turn what an adapter reports into a
// stream and a usage record. Adding a provider must therefore mean writing one
// implementation of Adapter and one fake upstream for the conformance harness —
// never touching the executor, the stream framing or the receipt shape.
package provider

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/OxyHQ/Relay/internal/contract"
)

// Adapter translates the normalized inference contract into one upstream
// provider's wire protocol and back.
//
// Four methods, and each exists because the concern it names has a different
// lifetime from the others:
//
//   - Provider names the slug every event and usage record attributes the work
//     to. It comes from the adapter rather than from its registration site so a
//     mis-registration cannot mislabel a receipt.
//   - Translate is pure. A request this provider cannot express must be refused
//     BEFORE anything is spent upstream, and a pure translation is testable
//     without a network, which is what makes that refusal cheap to cover.
//   - Stream is execution, streaming, cancellation and usage measurement
//     together, because they share one lifetime: the units measured are only
//     correct if they come from the same read loop that saw the last frame
//     before a cancellation. A separate Usage() method would invite being
//     called on a stream that ended differently from the one it measured.
//   - Health must be answerable without a customer request — it feeds the
//     internal status surface — so it cannot be folded into Stream. It is
//     deliberately NOT what returns a deployment to rotation: that is one real
//     customer request through a half-open breaker (internal/rotation), because
//     a probe of the provider's model listing proves it answers some other
//     request than the one it is failing.
//
// Everything an adapter does NOT do is as deliberate. It does not allocate
// request or generation ids, assign sequence numbers, decide terminality, emit
// the done or error event, resolve a model reference to an upstream model id,
// or apply routing policy. Those are one implementation in the executor rather
// than one per provider, which is the difference between adding a provider and
// re-deriving the contract each time.
type Adapter interface {
	Provider() contract.ProviderSlug
	Translate(request *contract.Request, route Route) (*Call, error)
	Stream(ctx context.Context, call *Call, out Emitter) (Outcome, error)
	Health(ctx context.Context) Health
}

// Route is what the deployment inventory resolved for this request: which
// concrete endpoint serves it, and what the upstream calls the model.
//
// UpstreamModelID has no representation anywhere in the published contract —
// the catalogue's deployment descriptor carries no field for a provider's own
// model identifier. The mapping is therefore Relay's, and it lives in the
// inventory rather than inside each adapter so two adapters for one provider
// cannot disagree about it.
type Route struct {
	DeploymentID    contract.DeploymentID
	Provider        contract.ProviderSlug
	ModelReference  contract.ModelReference
	UpstreamModelID string
	Region          contract.Region
}

// Call is a translated, ready-to-send upstream request.
//
// It carries no credential. The adapter holds its own, applies it when it
// builds the HTTP request, and nothing that could be logged, echoed into an
// error or written to a usage record ever holds one.
type Call struct {
	Route Route
	// Method and URL are recorded so a failure can name the endpoint that
	// failed without reconstructing it from adapter internals.
	Method string
	URL    string
	// Body is the upstream request body, already encoded.
	Body []byte
	// Header carries only non-secret headers. The adapter adds authentication
	// at send time.
	Header http.Header
	// Stream records whether the customer asked for a streamed response, since
	// providers express it in the body and Relay has to know without re-reading
	// it.
	Stream bool
}

// Outcome is what an adapter measured, and it is returned even when Stream
// fails.
//
// It is a value rather than a pointer for exactly that reason: a partial stream
// is a settlement case, so an adapter that returned nothing on cancellation
// would make an exact refund impossible, and a nil pointer is the easiest way
// to accidentally return nothing.
type Outcome struct {
	// Units measured so far. Each unit appears at most once, as a total.
	Units []contract.UsageQuantity
	// UsageSource distinguishes what the provider reported from what Relay
	// counted from what it had to reconstruct. An estimate indistinguishable
	// from a reported number is one nobody can reconcile later.
	UsageSource contract.UsageSource
	// FinishReason is why generation stopped, as the provider described it.
	FinishReason contract.FinishReason
	// TimeToFirstToken is zero when no output was produced.
	TimeToFirstToken time.Duration
}

// CountRequest records the one unit an adapter measures without the provider's
// help: that this request was served.
//
// ## The shape it exists to prevent
//
// `normalizedUsageReportSchema` refuses a `completed` report with an empty unit
// list — "a completed request consumed something" — and the published policy for
// one is refuse-and-release: the report is rejected and the hold is released. So
// a provider that streams a full answer and omits its usage block, which several
// do, produces a request that ran, cost Relay money upstream, and cannot be
// settled at all.
//
// Both adapters' `normalizeUsage` already open with `requests: 1` for exactly
// this reason, but they are only reached when a usage block ARRIVES. This is the
// other path.
//
// ## Why it is called at the CLEAN end of a stream and nowhere else
//
// `requests: 1` is a count of the call, not an estimate of the model's work, so
// it is honest to report — but only for a request that was actually served.
// Adding it earlier would make a FAILED attempt carry a unit, and
// `relay.outcomeFor` derives `partial` from "has units" and `failed` from "has
// none": a failed request would start settling as a partial one, billing a
// customer for output they never received. The unit therefore attaches once, at
// the point the adapter knows the stream finished cleanly.
//
// Existing units are left alone: a provider that reported its own counts has
// already included `requests`, and this must not double it.
func CountRequest(units []contract.UsageQuantity) []contract.UsageQuantity {
	for _, quantity := range units {
		if quantity.Unit == contract.UnitRequests {
			return units
		}
	}
	return append(units, contract.UsageQuantity{Unit: contract.UnitRequests, Quantity: 1})
}

// HealthStatus is the coarse state of an adapter's upstream.
type HealthStatus string

const (
	// HealthOK means the upstream answered a probe.
	HealthOK HealthStatus = "ok"
	// HealthDegraded means the upstream answered, but not well enough to route
	// to by preference.
	HealthDegraded HealthStatus = "degraded"
	// HealthUnavailable means the probe failed.
	HealthUnavailable HealthStatus = "unavailable"
	// HealthUnconfigured means no credential is configured for this adapter.
	// It is a distinct state from unavailable on purpose: an operator reading
	// "unavailable" goes looking at the provider, and the answer is here.
	HealthUnconfigured HealthStatus = "unconfigured"
)

// Health is the customer-safe projection of an adapter's state. It carries no
// credential, no upstream URL and no internal route id.
type Health struct {
	Provider  contract.ProviderSlug `json:"provider"`
	Status    HealthStatus          `json:"status"`
	CheckedAt contract.Timestamp    `json:"checkedAt"`
	LatencyMs *int                  `json:"latencyMs,omitempty"`
	// Detail is a short operator-facing note. It goes through the contract's
	// own redaction, because a probe failure is one of the places an upstream
	// echoes a credential back.
	Detail string `json:"detail,omitempty"`
}

// Emitter is how an adapter reports semantic output.
//
// It exists so no adapter can get the framing wrong. `requestId`, `sequence`
// and `schemaVersion` are stamped here, once, rather than by each adapter —
// which removes the entire class of bug where one provider's events are
// unattributable or arrive with a repeated sequence. Terminal events are
// deliberately absent: `done`, `error` and `route_switch` are the executor's,
// because an adapter does not know whether a failure ends the request or is
// about to be retried elsewhere.
type Emitter interface {
	// Start reports what was actually resolved. Exactly one Start per stream,
	// and it must precede every other event.
	Start(resolved contract.ModelReference, at time.Time) error
	// Delta reports a chunk of output on one channel.
	Delta(outputIndex int, channel contract.DeltaChannel, text string) error
	// ToolCall reports a tool call being streamed.
	ToolCall(call ToolCallDelta) error
	// Usage reports metered units for the request so far. Units only — a cost
	// quoted by the data plane would be a second, unauthoritative answer to a
	// question the ledger already owns.
	Usage(units []contract.UsageQuantity, source contract.UsageSource) error
}

// ToolCallDelta is one increment of a streamed tool call.
type ToolCallDelta struct {
	ID string
	// Name is set on the first increment of a call.
	Name string
	// ArgumentsDelta accumulates; it is not valid JSON until Complete.
	ArgumentsDelta string
	Complete       bool
}

// Registry holds the adapters this process can route to.
//
// It keys on the adapter's OWN slug rather than on a name supplied at
// registration, so a typo cannot produce a provider that serves requests under
// one name and reports usage under another.
type Registry struct {
	adapters map[contract.ProviderSlug]Adapter
}

// NewRegistry builds a registry, refusing duplicates and invalid slugs.
func NewRegistry(adapters ...Adapter) (*Registry, error) {
	registry := &Registry{adapters: make(map[contract.ProviderSlug]Adapter, len(adapters))}
	for _, adapter := range adapters {
		slug := adapter.Provider()
		if !slug.Valid() {
			return nil, fmt.Errorf("provider: %T reports slug %q, which is not a provider slug", adapter, slug)
		}
		if _, duplicate := registry.adapters[slug]; duplicate {
			return nil, fmt.Errorf("provider: two adapters claim the slug %q", slug)
		}
		registry.adapters[slug] = adapter
	}
	return registry, nil
}

// Lookup returns the adapter serving a provider slug.
func (r *Registry) Lookup(slug contract.ProviderSlug) (Adapter, bool) {
	adapter, found := r.adapters[slug]
	return adapter, found
}

// All returns every registered adapter, for the health surface.
func (r *Registry) All() []Adapter {
	all := make([]Adapter, 0, len(r.adapters))
	for _, adapter := range r.adapters {
		all = append(all, adapter)
	}
	return all
}
