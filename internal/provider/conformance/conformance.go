// Package conformance is the test suite every provider adapter must pass.
//
// It exists so that adding a provider is a fixture rather than a rewrite. An
// adapter author supplies four things — how to build the adapter, how to start
// a fake upstream speaking that provider's REAL wire format, one request the
// provider genuinely cannot express, and the route it serves — and gets back
// every invariant the platform depends on: event ordering, attribution,
// terminality, usage normalization, error classification, credential
// containment, cancellation and health.
//
// The suite deliberately drives the adapter through the real executor rather
// than calling its methods directly. An adapter is only correct in the shape it
// is actually used, and the framing, terminality and usage-report rules it has
// to satisfy live in the executor: testing the adapter alone would test a
// configuration nothing runs.
package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OxyHQ/Relay/internal/contract"
	"github.com/OxyHQ/Relay/internal/inventory"
	"github.com/OxyHQ/Relay/internal/provider"
	"github.com/OxyHQ/Relay/internal/relay"
	"github.com/OxyHQ/Relay/internal/rotation"
)

// Scenario names a behaviour the fake upstream must be able to perform. Each
// one corresponds to something a real provider does, and an adapter that cannot
// be driven through all of them has an untested branch on a path that costs
// money.
type Scenario string

const (
	// ScenarioStreaming streams several chunks and then reports usage.
	ScenarioStreaming Scenario = "streaming"
	// ScenarioSlowStream streams with a delay between chunks, so a caller can
	// cancel mid-stream.
	ScenarioSlowStream Scenario = "slow_stream"
	// ScenarioNoUsage streams output and never reports usage, which some
	// providers do and every settlement has to survive.
	ScenarioNoUsage Scenario = "no_usage"
	// ScenarioToolCalls streams a tool call in fragments.
	ScenarioToolCalls Scenario = "tool_calls"
	// ScenarioNonStreaming answers with a single complete response.
	ScenarioNonStreaming Scenario = "non_streaming"
	// ScenarioMidStreamError writes some output and then fails, without ever
	// having sent a failing HTTP status: the status was 200 and the failure
	// arrived inside the stream. Every streaming protocol has a spelling for
	// this — an `event: error` frame, an error object among the data frames —
	// and an adapter that treats it as the end of a normal stream reports a
	// request that failed as one that completed.
	ScenarioMidStreamError Scenario = "mid_stream_error"
	// ScenarioRateLimited refuses with a transient rate limit.
	ScenarioRateLimited Scenario = "rate_limited"
	// ScenarioQuotaExhausted refuses with an account-level exhaustion of the
	// PLATFORM's account with the provider — the failure a human raises rather
	// than one that clears on its own.
	//
	// Which status carries it is the provider's business and not this suite's:
	// OpenAI-compatible providers send it as a 429 indistinguishable from a
	// burst limit, Anthropic sends a 402 that no rate limit uses. What the
	// suite requires either way is that the adapter tells the two apart, since
	// they are opposite answers to "should this be retried".
	ScenarioQuotaExhausted Scenario = "quota_exhausted"
	// ScenarioCredentialRefused refuses the PLATFORM's own credential — the
	// upstream saying "this key is not valid", about a key no customer has ever
	// seen. It is a different failure from every other one here: nothing the
	// client does fixes it, and nothing about the request is wrong.
	ScenarioCredentialRefused Scenario = "credential_refused"
	// ScenarioCredentialEchoed refuses with an error whose text echoes the
	// credential the caller sent. Providers really do this, and it is the
	// single most likely way an upstream key reaches a customer.
	ScenarioCredentialEchoed Scenario = "credential_echoed"
)

// Upstream is a running fake provider.
type Upstream struct {
	// URL is the base URL the adapter should be pointed at.
	URL string
	// TotalChunks is how many output chunks this scenario writes when it runs
	// to completion. Zero for scenarios that produce no output.
	TotalChunks int
	// Written reports how many chunks were actually written.
	Written func() int
	// CancelledAfterChunks reports how many chunks had been written when the
	// upstream observed its caller disconnect, or -1 if it never did.
	//
	// This is what makes the cancellation test a proof rather than an
	// assertion: the observation is made by the upstream, about its own
	// connection, and it is compared against the number a completed request
	// would produce.
	CancelledAfterChunks func() int
	// RequestCount reports how many HTTP requests the upstream received.
	RequestCount func() int
}

// Refusal is a request one provider genuinely cannot express, and how the
// adapter must refuse it.
//
// It is a LIST on Subject rather than a single value because a provider has as
// many refusal classes as it has, and covering one of them was an accident of
// the first adapter having exactly one. A protocol that cannot do embeddings
// and a protocol that requires a parameter the contract makes optional are two
// different refusals with two different fields at fault, and an adapter that
// gets the second one wrong invents a value on the customer's behalf.
type Refusal struct {
	// Name identifies the case in test output.
	Name string
	// Request is refused before anything is sent upstream.
	Request *contract.Request
	// Code is the contract code the customer sees. It must be non-retryable:
	// a request the provider cannot express cannot succeed on a retry.
	Code contract.ErrorCode
	// Param is the request field at fault, named in full. A refusal that names
	// no field tells the customer their request is wrong and not which part of
	// it, so the suite requires one.
	Param string
}

// StreamedUsage is what the fake upstream PHYSICALLY consumed and produced in
// the streaming and non-streaming scenarios, stated as whole-request totals
// rather than in the provider's own fields.
//
// It is declared this way because the units the contract meters in PARTITION a
// request (OxyHQ/oxy#1019): `cached_input_tokens` is disjoint from
// `input_tokens`, and `reasoning_tokens` from `output_tokens`, so a price
// applied to every reported unit sums to exactly the request. Providers do not
// agree on which of their own numbers nest — an OpenAI-compatible
// `prompt_tokens` includes its cached tokens while an Anthropic `input_tokens`
// excludes them — so the normalising arithmetic is different per adapter and
// the invariant is the same for all of them.
//
// Getting it wrong is silent and financial in both directions: a nested report
// forwarded unchanged charges the cached and reasoning tokens twice, and a
// subtraction applied where the provider had already excluded them charges for
// less than was served.
type StreamedUsage struct {
	// PromptTokens is every input token the request consumed, cached or not.
	PromptTokens int
	// CachedPromptTokens is how many of PromptTokens were served from the
	// provider's prompt cache.
	CachedPromptTokens int
	// OutputTokens is every token generated, reasoning included.
	OutputTokens int
	// ReasoningTokens is how many of OutputTokens were reasoning.
	ReasoningTokens int
}

// Subject is what an adapter author supplies.
type Subject struct {
	// Name identifies the adapter in test output.
	Name string
	// Provider is the slug the adapter reports.
	Provider contract.ProviderSlug
	// ModelReference is the revision-pinned reference the fixture route serves.
	ModelReference contract.ModelReference
	// UpstreamModelID is what the fake upstream expects to be called.
	UpstreamModelID string
	// StreamedUsage is what the fake upstream consumed and produced, in totals
	// the contract's units must partition.
	StreamedUsage StreamedUsage
	// APIKey is the credential the adapter is configured with. The suite
	// asserts this exact string never appears in anything a customer receives,
	// so it must be the one NewAdapter uses.
	APIKey string
	// NewAdapter builds the adapter under test, pointed at a fake upstream.
	NewAdapter func(t *testing.T, upstreamURL string) provider.Adapter
	// NewUnconfigured builds the adapter with no credential.
	NewUnconfigured func(t *testing.T) provider.Adapter
	// StartUpstream starts a fake upstream for a scenario. It must speak the
	// provider's real wire format: a fake that speaks the normalized contract
	// would test nothing, because translation is the part most likely to be
	// wrong.
	StartUpstream func(t *testing.T, scenario Scenario) *Upstream
	// Refusals are the requests this provider genuinely cannot express. At
	// least one is required: an adapter that refuses nothing has either not
	// looked, or is translating something it should be refusing.
	Refusals func() []Refusal
}

// Run executes the suite.
func Run(t *testing.T, subject Subject) {
	t.Helper()
	requireSubject(t, subject)

	t.Run("reports a stable, valid provider slug", func(t *testing.T) {
		adapter := subject.NewAdapter(t, "https://unused.invalid")
		if got := adapter.Provider(); got != subject.Provider {
			t.Fatalf("adapter reports %q, the subject declares %q", got, subject.Provider)
		}
		if !adapter.Provider().Valid() {
			t.Fatalf("%q is not a valid provider slug", adapter.Provider())
		}
		first, second := adapter.Provider(), adapter.Provider()
		if first != second {
			t.Fatalf("the slug is not stable across calls: %q then %q", first, second)
		}
	})

	t.Run("streams a normalized, well-framed event sequence", func(t *testing.T) {
		run := execute(t, subject, ScenarioStreaming, streamingRequest(subject), nil)
		assertWellFramedStream(t, run)
		assertTerminal(t, run, contract.EventDone)
		assertReport(t, run, contract.OutcomeCompleted)

		if run.report.UsageSource != contract.UsageProviderReported {
			t.Errorf("the upstream reported usage; the report says %q", run.report.UsageSource)
		}
		if len(run.report.Units) == 0 {
			t.Error("the upstream reported usage; the report carries no units")
		}
		if !run.sawEvent(contract.EventUsage) {
			t.Error("no usage event reached the stream, so a client cannot see what it is being metered")
		}
		if run.report.TimeToFirstTokenMs == nil {
			t.Error("output was produced; the report carries no time to first token")
		}
		assertUsagePartitionsTheRequest(t, run.report.Units, subject.StreamedUsage)
	})

	t.Run("produces the same normalized shape from a non-streamed upstream", func(t *testing.T) {
		request := streamingRequest(subject)
		request.Stream = false
		run := execute(t, subject, ScenarioNonStreaming, request, nil)
		assertWellFramedStream(t, run)
		assertTerminal(t, run, contract.EventDone)
		assertReport(t, run, contract.OutcomeCompleted)
		if !run.sawEvent(contract.EventDelta) {
			t.Error("a non-streamed response produced no output events")
		}
		// The same request, metered by the other of the adapter's two read
		// paths. They normalize the same provider numbers and are written
		// separately, so one of them drifting is not a hypothetical.
		assertUsagePartitionsTheRequest(t, run.report.Units, subject.StreamedUsage)
	})

	t.Run("classifies a failure that arrives after the response started", func(t *testing.T) {
		// The upstream answered 200 and began streaming, so no HTTP status will
		// ever carry this failure. An adapter that reads the frame it cannot
		// use and stops would report a truncated answer as a completed one.
		run := execute(t, subject, ScenarioMidStreamError, streamingRequest(subject), nil)
		assertWellFramedStream(t, run)
		failure := assertFailure(t, run)

		if failure.UpstreamCategory == nil {
			t.Fatal("a mid-stream failure carries no upstreamCategory, so neither failover nor a circuit breaker can see it")
		}
		if !provider.AttributableCategory(*failure.UpstreamCategory) {
			t.Errorf("a mid-stream overload was classified %q, which the platform reads as the request's fault", *failure.UpstreamCategory)
		}
		if run.report == nil {
			t.Fatal("a request that failed part-way produced no usage report, so what the upstream already did cannot be settled")
		}
		switch run.report.Outcome {
		case contract.OutcomePartial, contract.OutcomeFailed:
		default:
			t.Errorf("a mid-stream failure settled as %q", run.report.Outcome)
		}
	})

	// A provider that omits its usage block is the case the contract is sharpest
	// about, and this scenario asserted nothing about `units` — which is how the
	// gap below went unnoticed.
	//
	// `normalizedUsageReportSchema` refuses a `completed` report with an empty
	// unit list, and the policy for one is refuse-and-release: the report is
	// rejected and the hold is released, never estimated and charged. So an
	// adapter that streams a full answer and measures nothing produces a request
	// that ran, cost Relay money upstream, and can never be settled — and it
	// reaches Oxy looking like a success.
	//
	// Measured on this suite: with the assertion absent, both shipped adapters
	// returned zero units for this scenario, while each one's own `normalizeUsage`
	// documented `requests: 1` as "always reported" — true only on the path where
	// a usage block ARRIVES, which is the one path this scenario does not take.
	t.Run("settles a provider that reports no usage as an estimate", func(t *testing.T) {
		run := execute(t, subject, ScenarioNoUsage, streamingRequest(subject), nil)
		assertWellFramedStream(t, run)
		assertReport(t, run, contract.OutcomeCompleted)
		if run.report.UsageSource == contract.UsageProviderReported {
			t.Error("the upstream reported nothing; the report claims the provider reported it")
		}
		if len(run.report.Units) == 0 {
			t.Fatal("the report carries no units; the contract refuses a completed request that measured nothing, so this request could never be settled")
		}
		if !reportsUnit(run.report, contract.UnitRequests) {
			t.Errorf("the report's units are %v; the request itself is the one thing an adapter measures without the provider's help",
				run.report.Units)
		}
		// The counts the provider did not send must NOT be invented. Reporting a
		// token quantity here would be a number nobody measured, arriving in the
		// same shape as one a provider counted.
		for _, quantity := range run.report.Units {
			if quantity.Unit != contract.UnitRequests {
				t.Errorf("the report claims %d %s the upstream never reported", quantity.Quantity, quantity.Unit)
			}
		}
	})

	t.Run("streams tool calls that a client can reassemble", func(t *testing.T) {
		run := execute(t, subject, ScenarioToolCalls, streamingRequest(subject), nil)
		assertWellFramedStream(t, run)

		calls := make(map[string]bool)
		completed := make(map[string]bool)
		for _, event := range run.events {
			call, isCall := event.(*contract.StreamToolCallEvent)
			if !isCall {
				continue
			}
			if call.ToolCallID == "" {
				t.Error("a tool-call event carries no id, so its fragments cannot be joined")
			}
			calls[call.ToolCallID] = true
			if call.Complete {
				completed[call.ToolCallID] = true
			}
		}
		if len(calls) == 0 {
			t.Fatal("the upstream streamed a tool call; none reached the stream")
		}
		for id := range calls {
			if !completed[id] {
				t.Errorf("tool call %q was never marked complete, so a client never learns when to parse it", id)
			}
		}
	})

	t.Run("classifies a transient rate limit as retryable", func(t *testing.T) {
		run := execute(t, subject, ScenarioRateLimited, streamingRequest(subject), nil)
		failure := assertFailure(t, run)
		if failure.Code != contract.CodeRateLimited {
			t.Errorf("a 429 burst limit was classified %q, expected rate_limited", failure.Code)
		}
		if !failure.Retryable {
			t.Error("a transient rate limit was reported as non-retryable")
		}
		// The same classification is what failover and the circuit breakers
		// read. An adapter that reported a rate limit under a category the
		// platform treats as the request's fault would leave a saturated
		// deployment in rotation forever, failing every request sent to it.
		if failure.UpstreamCategory == nil {
			t.Fatal("an upstream failure carries no upstreamCategory, so nothing can tell whether the deployment or the request is at fault")
		}
		if !provider.AttributableCategory(*failure.UpstreamCategory) {
			t.Errorf("a rate limit was classified %q, which the platform reads as the request's fault: no failover and no breaker would ever see it", *failure.UpstreamCategory)
		}
	})

	t.Run("classifies an exhausted platform account as neither a throttle nor the customer's balance", func(t *testing.T) {
		run := execute(t, subject, ScenarioQuotaExhausted, streamingRequest(subject), nil)
		failure := assertFailure(t, run)
		if failure.Retryable {
			t.Errorf("an exhausted account was reported retryable under code %q; only a human raises it", failure.Code)
		}
		if failure.Code == contract.CodeRateLimited {
			t.Error("an exhausted account was classified as a rate limit; on one provider they share a status and they are opposite answers")
		}
		// The account is the PLATFORM's with this provider. Every code below
		// names the CUSTOMER's money, and each one sends them to top up, raise
		// or wait on something that is not what failed — right about
		// retryability and wrong about who can act, which reads as actionable
		// while the action does nothing.
		switch failure.Code {
		case contract.CodeQuotaExceeded, contract.CodeInsufficientBalance, contract.CodeSpendingLimitExceeded:
			t.Errorf("the platform's own account with the provider was reported as %q, which names the customer's money", failure.Code)
		}
	})

	t.Run("classifies a refused platform credential as nobody's to retry", func(t *testing.T) {
		// The upstream refused RELAY's credential. Three classifications are
		// available and two of them are wrong: `authentication_failed` sends the
		// customer to rotate their own key, and `provider_error` — retryable —
		// sends every client into a retry loop against a request that cannot
		// succeed until an operator rotates ours.
		run := execute(t, subject, ScenarioCredentialRefused, streamingRequest(subject), nil)
		failure := assertFailure(t, run)

		if failure.Code == contract.CodeAuthenticationFailed {
			t.Error("a refused PLATFORM credential was reported as the customer's authentication failing, which sends them to rotate a key that is not the problem")
		}
		if failure.Retryable {
			t.Errorf("a refused platform credential was reported retryable under code %q; no client retry reaches the operator who has to rotate the key", failure.Code)
		}
		if failure.UpstreamCategory == nil {
			t.Fatal("a refused platform credential carries no upstreamCategory, so no breaker can take this route out of rotation")
		}
		// Non-retryable for the CLIENT and attributable to the DEPLOYMENT are
		// not in tension: the key belongs to this route, and another deployment
		// of the same model holds a different one.
		if !provider.AttributableCategory(*failure.UpstreamCategory) {
			t.Errorf("a refused platform credential was classified %q, so a route with a dead key stays in rotation failing every request sent to it", *failure.UpstreamCategory)
		}
	})

	t.Run("never lets an upstream credential reach the customer", func(t *testing.T) {
		if subject.APIKey == "" {
			t.Fatal("the subject declares no API key, so this check would pass vacuously")
		}
		run := execute(t, subject, ScenarioCredentialEchoed, streamingRequest(subject), nil)
		failure := assertFailure(t, run)

		encoded, err := json.Marshal(run.events)
		if err != nil {
			t.Fatalf("encoding the emitted stream: %v", err)
		}
		if strings.Contains(string(encoded), subject.APIKey) {
			t.Fatalf("the configured upstream credential appears in the customer-visible stream:\n%s", encoded)
		}
		// Positive control on the test itself: the upstream must actually have
		// echoed the credential, or "no leak" is what a scenario that never
		// mentioned it also reports.
		if !run.upstreamEchoedCredential {
			t.Fatal("the fake upstream did not echo the credential, so this check measured nothing")
		}

		// And the customer must still be told what went wrong. There are two
		// ways to keep a credential out of an error and only one of them is
		// acceptable: removing the VALUE, which an adapter can do because it is
		// holding the bytes it sent, or letting the contract's refusal throw the
		// whole message away. The second passes the assertion above while
		// destroying the diagnostic, so it is a distinct failure and is checked
		// as one.
		if failure.ProviderError == nil || failure.ProviderError.Message == nil {
			t.Fatal("the upstream's message was dropped entirely, so the customer is told a request failed and nothing about why")
		}
		if *failure.ProviderError.Message == contract.WithheldErrorText {
			t.Error("the upstream's diagnostic was withheld wholesale: the adapter left the credential in the text and the contract's last-resort refusal removed the message with it. Redact the configured credential by exact match (provider.RedactSecret) so the message survives")
		}
	})

	for _, refusal := range subject.Refusals() {
		t.Run("refuses before spending anything: "+refusal.Name, func(t *testing.T) {
			if refusal.Code.Retryable() {
				t.Fatalf("the subject expects refusal with %q, which is retryable; a request the provider cannot express can never succeed on a retry", refusal.Code)
			}
			run := execute(t, subject, ScenarioStreaming, refusal.Request, nil)
			failure := assertFailure(t, run)
			if failure.Code != refusal.Code {
				t.Errorf("refused with %q, the subject expects %q", failure.Code, refusal.Code)
			}
			if failure.Retryable {
				t.Error("a request the provider cannot express was reported retryable")
			}
			if failure.Param == nil {
				t.Errorf("the refusal names no field, so the customer is told their request is wrong and not which part of it (expected %q)", refusal.Param)
			} else if *failure.Param != refusal.Param {
				t.Errorf("the refusal names field %q, the subject expects %q", *failure.Param, refusal.Param)
			}
			if got := run.upstream.RequestCount(); got != 0 {
				t.Errorf("the upstream received %d requests for a request that should have been refused before translation completed", got)
			}
			// A refusal to translate is Relay's, not the provider's. Dressing it
			// as an upstream failure would count it against the deployment's
			// health and eventually take a perfectly healthy route out of
			// rotation because one customer kept sending a request it cannot
			// express.
			if failure.UpstreamCategory != nil {
				t.Errorf("a refusal to translate carries upstreamCategory %q, which would count against the deployment that never saw the request", *failure.UpstreamCategory)
			}
		})
	}

	t.Run("a client disconnect cancels the upstream call", func(t *testing.T) {
		// The control runs first: it establishes what an UNINTERRUPTED request
		// looks like from the upstream's own side. Without it, "the upstream
		// saw its caller go away" is also what a normally-finished request
		// reports, and the cancellation check would prove nothing.
		control := execute(t, subject, ScenarioSlowStream, streamingRequest(subject), nil)
		assertTerminal(t, control, contract.EventDone)
		if got := control.upstream.CancelledAfterChunks(); got != -1 {
			t.Fatalf("the control request was not cancelled, yet the upstream observed a disconnect after %d chunks", got)
		}
		if written, total := control.upstream.Written(), control.upstream.TotalChunks; written != total {
			t.Fatalf("the control request wrote %d of %d chunks; the scenario is not running to completion", written, total)
		}

		cancelled := execute(t, subject, ScenarioSlowStream, streamingRequest(subject), cancelAfterFirstDelta)

		// The upstream's observation is asynchronous: Execute returns as soon
		// as the adapter's own read fails, which can be before the provider's
		// end of the connection notices. Polling to a deadline is the honest
		// way to wait for it — reading the counter once measures scheduling
		// order, not propagation.
		observed := -1
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if observed = cancelled.upstream.CancelledAfterChunks(); observed != -1 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		switch {
		case observed == -1:
			t.Fatalf("the client cancelled, and the upstream never observed a disconnect after 3s (it wrote %d of %d chunks): the cancellation did not reach the provider",
				cancelled.upstream.Written(), cancelled.upstream.TotalChunks)
		case observed >= cancelled.upstream.TotalChunks:
			t.Fatalf("the upstream observed the disconnect only after writing all %d chunks, which is what completing normally looks like", observed)
		}
		if written := cancelled.upstream.Written(); written >= cancelled.upstream.TotalChunks {
			t.Fatalf("the upstream wrote all %d chunks despite the cancellation", cancelled.upstream.TotalChunks)
		}

		if cancelled.report == nil {
			t.Fatal("a cancelled request produced no usage report, so the reservation cannot be settled or refunded exactly")
		}
		if cancelled.report.Outcome != contract.OutcomeCancelled {
			t.Errorf("a cancelled request settled as %q", cancelled.report.Outcome)
		}
		for _, event := range cancelled.events {
			if event.EventType() == contract.EventDone {
				t.Error("a done event followed a cancellation, which is indistinguishable from a completed request")
			}
		}
	})

	t.Run("reports health without a credential and without leaking one", func(t *testing.T) {
		adapter := subject.NewUnconfigured(t)
		health := adapter.Health(context.Background())
		if health.Status != provider.HealthUnconfigured {
			t.Errorf("an adapter with no credential reports %q, expected %q", health.Status, provider.HealthUnconfigured)
		}
		if health.Provider != subject.Provider {
			t.Errorf("health names provider %q, expected %q", health.Provider, subject.Provider)
		}
		if _, err := health.CheckedAt.Time(); err != nil {
			t.Errorf("health carries an unparseable timestamp %q: %v", health.CheckedAt, err)
		}
	})

	t.Run("reports health against a reachable upstream", func(t *testing.T) {
		upstream := subject.StartUpstream(t, ScenarioStreaming)
		adapter := subject.NewAdapter(t, upstream.URL)
		health := adapter.Health(context.Background())
		if health.Status != provider.HealthOK {
			t.Errorf("a reachable upstream reports %q, expected %q (%s)", health.Status, provider.HealthOK, health.Detail)
		}
		encoded, err := json.Marshal(health)
		if err != nil {
			t.Fatalf("encoding health: %v", err)
		}
		if subject.APIKey != "" && strings.Contains(string(encoded), subject.APIKey) {
			t.Fatalf("the health projection carries a credential: %s", encoded)
		}
	})
}

func requireSubject(t *testing.T, subject Subject) {
	t.Helper()
	switch {
	case subject.Name == "":
		t.Fatal("conformance: the subject has no name")
	case !subject.Provider.Valid():
		t.Fatalf("conformance: %q is not a provider slug", subject.Provider)
	case !subject.ModelReference.Pinned():
		t.Fatalf("conformance: %q must be revision-pinned", subject.ModelReference)
	case subject.UpstreamModelID == "":
		t.Fatal("conformance: the subject declares no upstream model id")
	case subject.NewAdapter == nil, subject.NewUnconfigured == nil, subject.StartUpstream == nil, subject.Refusals == nil:
		t.Fatal("conformance: the subject is incomplete")
	}
	if len(subject.Refusals()) == 0 {
		t.Fatal("conformance: the subject declares no refusal, so the check that a request the provider cannot express costs nothing would pass vacuously")
	}
	for index, refusal := range subject.Refusals() {
		if refusal.Name == "" || refusal.Request == nil || refusal.Code == "" || refusal.Param == "" {
			t.Fatalf("conformance: refusal %d is incomplete", index)
		}
	}
	requireMeasurableUsage(t, subject.StreamedUsage)
}

// requireMeasurableUsage is the vacuity floor under the partition check.
//
// Every one of these bounds exists because the check below it goes quiet
// without it: with no cached tokens, an adapter that never reports
// cached_input_tokens passes; with cached equal to the whole prompt, an adapter
// that subtracts when it should not produces zero and so does a correct one.
// The failure this measures is worth real money, so the fixture is required to
// be one where both readings differ and both are visible.
func requireMeasurableUsage(t *testing.T, usage StreamedUsage) {
	t.Helper()
	switch {
	case usage.CachedPromptTokens <= 0:
		t.Fatal("conformance: the scenario reports no cached input tokens, so nothing distinguishes a nested report from a disjoint one")
	case usage.ReasoningTokens <= 0:
		t.Fatal("conformance: the scenario reports no reasoning tokens, so nothing distinguishes a nested report from a disjoint one")
	case usage.PromptTokens <= usage.CachedPromptTokens:
		t.Fatal("conformance: every prompt token is a cached one, so an adapter reporting zero uncached input tokens cannot be told from a correct one")
	case usage.OutputTokens <= usage.ReasoningTokens:
		t.Fatal("conformance: every output token is a reasoning one, so an adapter reporting zero visible output tokens cannot be told from a correct one")
	}
}

// assertUsagePartitionsTheRequest is the money check.
//
// The contract's units partition a request: each token is counted once, under
// exactly one unit, so a price applied to every unit sums to what was served.
// Providers report their own numbers with their own nesting, and the adapter is
// what turns one into the other — silently, since a nested report and a disjoint
// one are the same four non-negative integers and both look plausible on a
// receipt.
func assertUsagePartitionsTheRequest(t *testing.T, units []contract.UsageQuantity, expected StreamedUsage) {
	t.Helper()
	reported := make(map[contract.UsageUnit]int, len(units))
	for _, quantity := range units {
		reported[quantity.Unit] = quantity.Quantity
	}

	if got := reported[contract.UnitInputTokens] + reported[contract.UnitCachedInputTokens]; got != expected.PromptTokens {
		t.Errorf("input_tokens (%d) + cached_input_tokens (%d) = %d, and the request consumed %d prompt tokens: the units do not partition the request, so a price applied to each of them does not sum to it",
			reported[contract.UnitInputTokens], reported[contract.UnitCachedInputTokens], got, expected.PromptTokens)
	}
	if got := reported[contract.UnitOutputTokens] + reported[contract.UnitReasoningTokens]; got != expected.OutputTokens {
		t.Errorf("output_tokens (%d) + reasoning_tokens (%d) = %d, and the request produced %d output tokens: the units do not partition the request",
			reported[contract.UnitOutputTokens], reported[contract.UnitReasoningTokens], got, expected.OutputTokens)
	}
	if got := reported[contract.UnitCachedInputTokens]; got != expected.CachedPromptTokens {
		t.Errorf("cached_input_tokens is %d, and %d prompt tokens were served from the cache: a cached token charged at the uncached price is a real overcharge",
			got, expected.CachedPromptTokens)
	}
	if got := reported[contract.UnitReasoningTokens]; got != expected.ReasoningTokens {
		t.Errorf("reasoning_tokens is %d, and %d output tokens were reasoning", got, expected.ReasoningTokens)
	}
	if got := reported[contract.UnitRequests]; got != 1 {
		t.Errorf("the report meters %d requests; a per-request price has nothing to multiply otherwise", got)
	}
}

/* -------------------------------------------------------------------------- */
/*  Driving one request                                                       */
/* -------------------------------------------------------------------------- */

type run struct {
	events                   []contract.StreamEvent
	report                   *contract.UsageReport
	failure                  *contract.Error
	upstream                 *Upstream
	upstreamEchoedCredential bool
}

func (r *run) sawEvent(kind contract.StreamEventType) bool {
	for _, event := range r.events {
		if event.EventType() == kind {
			return true
		}
	}
	return false
}

// interceptor lets a test act on the stream as it arrives — cancelling, for
// instance, at the moment the first output reaches the client.
type interceptor func(event contract.StreamEvent, cancel context.CancelFunc)

func cancelAfterFirstDelta(event contract.StreamEvent, cancel context.CancelFunc) {
	if event.EventType() == contract.EventDelta {
		cancel()
	}
}

func execute(t *testing.T, subject Subject, scenario Scenario, request *contract.Request, intercept interceptor) *run {
	t.Helper()

	upstream := subject.StartUpstream(t, scenario)
	adapter := subject.NewAdapter(t, upstream.URL)

	// One deployment, deliberately. Failover is the executor's behaviour and is
	// covered where two deployments exist; here its absence is an assertion —
	// see assertWellFramedStream, which requires no route switch to appear.
	inventoryJSON := fmt.Sprintf(`{
		"snapshotId":"snap_conformance",
		"issuedAt":%q,
		"deployments":[{
			"deploymentId":"dep_conformance",
			"provider":%q,
			"modelReference":%q,
			"upstreamModelId":%q,
			"region":"test-region",
			"current":true
		}]
	}`, contract.NewTimestamp(time.Now()), subject.Provider, subject.ModelReference, subject.UpstreamModelID)

	path := filepath.Join(t.TempDir(), "inventory.json")
	if err := os.WriteFile(path, []byte(inventoryJSON), 0o600); err != nil {
		t.Fatalf("writing the conformance inventory: %v", err)
	}
	store, err := inventory.NewStore(inventory.Config{Path: path})
	if err != nil {
		t.Fatalf("building the conformance inventory: %v", err)
	}
	registry, err := provider.NewRegistry(adapter)
	if err != nil {
		t.Fatalf("registering the adapter: %v", err)
	}
	executor, err := relay.NewExecutor(relay.Config{
		Inventory: store,
		Providers: registry,
		Rotation:  rotation.NewRegistry(rotation.Policy{}, nil),
	})
	if err != nil {
		t.Fatalf("building the executor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := &run{upstream: upstream}
	var mutex sync.Mutex
	sink := func(event contract.StreamEvent) error {
		mutex.Lock()
		result.events = append(result.events, event)
		mutex.Unlock()
		if intercept != nil {
			intercept(event, cancel)
		}
		return nil
	}

	executed := executor.Execute(ctx, request, sink)
	result.report = executed.Report
	result.failure = executed.Failure
	result.upstreamEchoedCredential = scenario == ScenarioCredentialEchoed && upstream.RequestCount() > 0
	return result
}

/* -------------------------------------------------------------------------- */
/*  Invariants                                                                */
/* -------------------------------------------------------------------------- */

// assertWellFramedStream checks every framing rule the contract states in prose:
// one start, first; a monotonic sequence with no gaps; the request id on every
// event; the declared schema version on every event; and nothing after a
// terminal event.
func assertWellFramedStream(t *testing.T, r *run) {
	t.Helper()
	if len(r.events) == 0 {
		t.Fatal("the stream carried no events")
	}
	if first := r.events[0].EventType(); first != contract.EventStart {
		t.Fatalf("the stream opens with a %q event, not start", first)
	}

	starts, terminals := 0, 0
	for index, event := range r.events {
		if event.Sequence() != index {
			t.Errorf("event %d carries sequence %d; sequences must be monotonic from zero so a redelivery is detectable", index, event.Sequence())
		}
		switch event.EventType() {
		case contract.EventStart:
			starts++
		case contract.EventRouteSwitch:
			// The conformance inventory declares ONE deployment, so there is
			// nowhere to switch to. A switch appearing here would mean the
			// executor re-routed a request to a deployment that does not exist
			// in the set it resolved.
			t.Errorf("event %d is a route switch, and the conformance inventory has a single deployment", index)
		case contract.EventDone, contract.EventError:
			terminals++
			if index != len(r.events)-1 {
				t.Errorf("a %q event at position %d is followed by %d more events", event.EventType(), index, len(r.events)-index-1)
			}
		}

		encoded, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("event %d does not encode: %v", index, err)
		}
		var generic struct {
			SchemaVersion int    `json:"schemaVersion"`
			RequestID     string `json:"requestId"`
			Type          string `json:"type"`
		}
		if err := json.Unmarshal(encoded, &generic); err != nil {
			t.Fatalf("event %d does not decode: %v", index, err)
		}
		if generic.SchemaVersion != contract.SchemaVersion {
			t.Errorf("event %d carries schemaVersion %d, expected %d", index, generic.SchemaVersion, contract.SchemaVersion)
		}
		if generic.RequestID == "" {
			t.Errorf("event %d carries no requestId, so a proxy or a reconnecting client could not attribute it", index)
		}
		if generic.Type != string(event.EventType()) {
			t.Errorf("event %d encodes type %q while reporting %q", index, generic.Type, event.EventType())
		}
	}
	if starts != 1 {
		t.Errorf("the stream carries %d start events, expected exactly 1", starts)
	}
	if terminals != 1 {
		t.Errorf("the stream carries %d terminal events, expected exactly 1", terminals)
	}

	start, isStart := r.events[0].(*contract.StreamStartEvent)
	if !isStart {
		t.Fatalf("the first event reports start but is a %T", r.events[0])
	}
	if !start.ResolvedModelReference.Pinned() {
		t.Errorf("the start event reports %q, which is not revision-pinned: a customer is not told which weights answered", start.ResolvedModelReference)
	}
	if !start.ServingProvider.Valid() {
		t.Errorf("the start event names serving provider %q", start.ServingProvider)
	}
}

func assertTerminal(t *testing.T, r *run, expected contract.StreamEventType) {
	t.Helper()
	if len(r.events) == 0 {
		t.Fatal("the stream carried no events")
	}
	if got := r.events[len(r.events)-1].EventType(); got != expected {
		t.Fatalf("the stream ends with %q, expected %q", got, expected)
	}
}

func assertFailure(t *testing.T, r *run) *contract.Error {
	t.Helper()
	if r.failure == nil {
		t.Fatal("the request was expected to fail and did not")
	}
	if r.failure.RequestID == "" {
		t.Error("the error carries no requestId, so a customer cannot correlate it with anything")
	}
	if r.failure.Retryable && !r.failure.Code.Retryable() {
		t.Errorf("%q is retryable in the error body and non-retryable in the contract", r.failure.Code)
	}
	assertTerminal(t, r, contract.EventError)
	return r.failure
}

// assertReport checks the technical usage record settlement runs against.
func assertReport(t *testing.T, r *run, expected contract.RequestOutcome) {
	t.Helper()
	if r.report == nil {
		t.Fatal("no usage report was produced, so the request cannot be settled")
	}
	if err := r.report.Validate(); err != nil {
		t.Fatalf("the usage report would be rejected by the contract: %v", err)
	}
	if r.report.Outcome != expected {
		t.Errorf("the report says %q, expected %q", r.report.Outcome, expected)
	}
	if r.report.RouteSwitches != 0 {
		t.Errorf("the report counts %d route switches, and the conformance inventory declares one deployment", r.report.RouteSwitches)
	}
	if r.report.DeploymentID == nil || *r.report.DeploymentID == "" {
		t.Error("the report names no deployment, so a charge cannot be attributed to a route")
	}
	started, err := r.report.StartedAt.Time()
	if err != nil {
		t.Fatalf("the report's startedAt is unparseable: %v", err)
	}
	if time.Since(started) > time.Minute {
		t.Errorf("the report's startedAt is %s old; it is not the instant this request began", time.Since(started))
	}
}

/* -------------------------------------------------------------------------- */
/*  Fixtures                                                                  */
/* -------------------------------------------------------------------------- */

// maxOutputTokens is populated on every fixture below.
//
// Not as a courtesy to any one provider, though one of them requires it: a
// minimal fixture exercises the translation of nothing optional, and this is a
// field whose absence an adapter could paper over by inventing a default. It is
// also the one optional request field a provider is known to make mandatory,
// which is a contract finding rather than a fixture detail — see README, "What
// Oxy still has to decide".
const maxOutputTokens = 256

func streamingRequest(subject Subject) *contract.Request {
	reference := subject.ModelReference
	limit := maxOutputTokens
	return &contract.Request{
		MaxOutputTokens: &limit,
		SchemaVersion:   contract.RequestEnvelopeVersion,
		Attribution: contract.Attribution{
			Principal: contract.AuthenticatedPrincipal{
				Billing:         contract.BillingPrincipal{AccountID: "acc_conformance"},
				ApplicationID:   "app_conformance",
				CredentialID:    "cred_conformance",
				Environment:     contract.EnvironmentDevelopment,
				InferenceScopes: []contract.Scope{contract.ScopeInvoke},
			},
			RequestID: "req_conformance",
		},
		Target:   contract.RoutingTarget{Kind: contract.TargetModel, ModelReference: &reference},
		Modality: contract.ModalityText,
		Input: contract.Input{
			Format: contract.InputMessages,
			Messages: []contract.Message{{
				Role:    contract.RoleUser,
				Content: []contract.ContentPart{{Type: contract.ContentPartText, Text: textOf("hello")}},
			}},
		},
		Stream: true,
		Client: contract.ClientRequestMetadata{
			APIFormat:  contract.APIFormatResponses,
			Endpoint:   "/v1/responses",
			ReceivedAt: contract.NewTimestamp(time.Now()),
		},
		RoutingPolicy: contract.RoutingPolicyReference{RoutingPolicyID: "rp_conformance", PolicyVersion: 1},
	}
}

func textOf(value string) *string { return &value }

// reportsUnit is whether a report carries a quantity for one unit.
func reportsUnit(report *contract.UsageReport, unit contract.UsageUnit) bool {
	for _, quantity := range report.Units {
		if quantity.Unit == unit {
			return true
		}
	}
	return false
}
