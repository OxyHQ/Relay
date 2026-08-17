package relay_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OxyHQ/Relay/internal/contract"
	"github.com/OxyHQ/Relay/internal/inventory"
	"github.com/OxyHQ/Relay/internal/provider"
	"github.com/OxyHQ/Relay/internal/providercost"
	"github.com/OxyHQ/Relay/internal/relay"
	"github.com/OxyHQ/Relay/internal/rotation"
)

// oneDeployment is the single-route inventory most of these tests run against.
// Failover needs two, and builds its own; see failover_test.go.
const oneDeployment = `{
  "deploymentId":"dep_test","provider":"stub",
  "modelReference":"stub/model@2026-05-01","upstreamModelId":"model",
  "region":"test-region","current":true}`

/* -------------------------------------------------------------------------- */
/*  Adapters the tests script                                                 */
/* -------------------------------------------------------------------------- */

type scriptedAdapter struct {
	slug   contract.ProviderSlug
	stream func(ctx context.Context, call *provider.Call, out provider.Emitter) (provider.Outcome, error)
	// translate, when set, replaces the pass-through translation.
	translate func(request *contract.Request, route provider.Route) (*provider.Call, error)

	mutex sync.Mutex
	calls int
}

func (s *scriptedAdapter) Provider() contract.ProviderSlug {
	if s.slug == "" {
		return "stub"
	}
	return s.slug
}

func (s *scriptedAdapter) Translate(request *contract.Request, route provider.Route) (*provider.Call, error) {
	if s.translate != nil {
		return s.translate(request, route)
	}
	return &provider.Call{Route: route, Stream: request.Stream}, nil
}

func (s *scriptedAdapter) Stream(ctx context.Context, call *provider.Call, out provider.Emitter) (provider.Outcome, error) {
	s.mutex.Lock()
	s.calls++
	s.mutex.Unlock()
	return s.stream(ctx, call, out)
}

func (s *scriptedAdapter) Health(context.Context) provider.Health {
	return provider.Health{Provider: s.Provider(), Status: provider.HealthOK, CheckedAt: contract.NewTimestamp(time.Now())}
}

// attempts is how many times this adapter's Stream was entered. Failover tests
// turn on it: "the request was retried elsewhere" and "the request was retried
// on the same deployment twice" produce the same stream and different counts.
func (s *scriptedAdapter) attempts() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.calls
}

/* -------------------------------------------------------------------------- */
/*  Harness                                                                   */
/* -------------------------------------------------------------------------- */

// harness builds an executor over a written inventory snapshot, the way the
// binary does. The snapshot is a real file because the store's whole purpose is
// re-reading one, and a test that bypassed it would exercise a wiring nothing
// runs.
type harness struct {
	deployments string
	adapters    []provider.Adapter
	rotation    *rotation.Registry
	costs       *providercost.Cards
	issuedAt    time.Time
	now         func() time.Time
	// failoverAuthorized stands in for the routing-policy value the envelope
	// does not carry. Every test that exercises failover sets it, because with
	// it false Relay deliberately never chooses among deployments at all — see
	// relay.Config.AssumeFailoverAuthorized, and the test that pins that
	// default.
	failoverAuthorized bool
}

func (h harness) build(t *testing.T) *relay.Executor {
	t.Helper()

	issuedAt := h.issuedAt
	if issuedAt.IsZero() {
		issuedAt = time.Now()
	}
	document := fmt.Sprintf(`{"snapshotId":"snap_relay_test","issuedAt":%q,"deployments":[%s]}`,
		contract.NewTimestamp(issuedAt), h.deployments)

	path := filepath.Join(t.TempDir(), "inventory.json")
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatalf("writing the inventory: %v", err)
	}
	store, err := inventory.NewStore(inventory.Config{
		Path:   path,
		Now:    h.now,
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("building the inventory store: %v", err)
	}

	registry, err := provider.NewRegistry(h.adapters...)
	if err != nil {
		t.Fatalf("registering: %v", err)
	}
	rotationRegistry := h.rotation
	if rotationRegistry == nil {
		rotationRegistry = rotation.NewRegistry(rotation.Policy{}, h.now)
	}
	executor, err := relay.NewExecutor(relay.Config{
		Inventory:                store,
		Providers:                registry,
		Rotation:                 rotationRegistry,
		Costs:                    h.costs,
		AssumeFailoverAuthorized: h.failoverAuthorized,
		Now:                      h.now,
	})
	if err != nil {
		t.Fatalf("building the executor: %v", err)
	}
	return executor
}

func (h harness) run(t *testing.T, request *contract.Request) ([]contract.StreamEvent, relay.Result) {
	t.Helper()
	var events []contract.StreamEvent
	result := h.build(t).Execute(context.Background(), request, func(event contract.StreamEvent) error {
		events = append(events, event)
		return nil
	})
	return events, result
}

func execute(t *testing.T, adapter provider.Adapter, request *contract.Request) ([]contract.StreamEvent, relay.Result) {
	t.Helper()
	return harness{deployments: oneDeployment, adapters: []provider.Adapter{adapter}}.run(t, request)
}

func baseRequest() *contract.Request {
	reference := contract.ModelReference("stub/model@2026-05-01")
	text := "hi"
	return &contract.Request{
		SchemaVersion: contract.RequestEnvelopeVersion,
		Attribution: contract.Attribution{
			Principal: contract.AuthenticatedPrincipal{
				Billing:         contract.BillingPrincipal{AccountID: "acc_test"},
				ApplicationID:   "app_test",
				CredentialID:    "cred_test",
				Environment:     contract.EnvironmentDevelopment,
				InferenceScopes: []contract.Scope{contract.ScopeInvoke},
			},
			RequestID: "req_test",
		},
		Target:   contract.RoutingTarget{Kind: contract.TargetModel, ModelReference: &reference},
		Modality: contract.ModalityText,
		Input: contract.Input{
			Format: contract.InputMessages,
			Messages: []contract.Message{{
				Role:    contract.RoleUser,
				Content: []contract.ContentPart{{Type: contract.ContentPartText, Text: &text}},
			}},
		},
		Stream: true,
		Client: contract.ClientRequestMetadata{
			APIFormat:  contract.APIFormatResponses,
			Endpoint:   "/v1/responses",
			ReceivedAt: contract.NewTimestamp(time.Now()),
		},
		RoutingPolicy: contract.RoutingPolicyReference{RoutingPolicyID: "rp", PolicyVersion: 1},
	}
}

func happyAdapter() *scriptedAdapter {
	return &scriptedAdapter{stream: func(_ context.Context, call *provider.Call, out provider.Emitter) (provider.Outcome, error) {
		if err := out.Start(call.Route.ModelReference, time.Now()); err != nil {
			return provider.Outcome{}, err
		}
		if err := out.Delta(0, contract.ChannelOutputText, "hello"); err != nil {
			return provider.Outcome{}, err
		}
		units := []contract.UsageQuantity{{Unit: contract.UnitRequests, Quantity: 1}}
		if err := out.Usage(units, contract.UsageProviderReported); err != nil {
			return provider.Outcome{}, err
		}
		return provider.Outcome{Units: units, UsageSource: contract.UsageProviderReported, FinishReason: contract.FinishStop}, nil
	}}
}

/* -------------------------------------------------------------------------- */
/*  Refusals                                                                  */
/* -------------------------------------------------------------------------- */

// TestARoutingProfileTargetIsRefusedWithTheFieldNamed pins the one place this
// build knowingly serves less than the contract describes. Resolving a profile
// needs its candidate list, and the envelope carries a routing policy REFERENCE
// rather than a snapshot — so choosing a model here would be exactly the silent
// substitution the platform forbids.
func TestARoutingProfileTargetIsRefusedWithTheFieldNamed(t *testing.T) {
	request := baseRequest()
	profile := contract.RoutingProfileSlug("auto")
	request.Target = contract.RoutingTarget{Kind: contract.TargetRoutingProfile, RoutingProfile: &profile}

	events, result := execute(t, happyAdapter(), request)

	if result.Failure == nil {
		t.Fatal("a routing-profile target was served")
	}
	if result.Failure.Code != contract.CodeInvalidRequest {
		t.Errorf("refused with %q", result.Failure.Code)
	}
	if result.Failure.Retryable {
		t.Error("the refusal is retryable, but no retry can add a policy snapshot to the envelope")
	}
	if result.Failure.Param == nil || *result.Failure.Param != "target.routingProfile" {
		t.Errorf("the refusal names %v as the field at fault", result.Failure.Param)
	}
	if len(events) != 1 || events[0].EventType() != contract.EventError {
		t.Errorf("the refusal produced %d events", len(events))
	}
}

func TestAnEnvelopeWithoutTheInvokeScopeIsRefused(t *testing.T) {
	request := baseRequest()
	request.Attribution.Principal.InferenceScopes = []contract.Scope{contract.ScopeModelsRead}

	_, result := execute(t, happyAdapter(), request)

	if result.Failure == nil {
		t.Fatal("an envelope without inference:invoke was served")
	}
	if result.Failure.Code != contract.CodeInsufficientScope {
		t.Errorf("refused with %q", result.Failure.Code)
	}
}

func TestAnUnroutableModelIsRefusedAsNotFound(t *testing.T) {
	request := baseRequest()
	reference := contract.ModelReference("stub/other@2026-05-01")
	request.Target.ModelReference = &reference

	_, result := execute(t, happyAdapter(), request)

	if result.Failure == nil {
		t.Fatal("an unroutable model was served")
	}
	if result.Failure.Code != contract.CodeModelNotFound {
		t.Errorf("refused with %q", result.Failure.Code)
	}
	if result.Failure.Retryable {
		t.Error("model_not_found was reported retryable; no identical retry makes a route appear")
	}
}

/* -------------------------------------------------------------------------- */
/*  Framing the emitter enforces                                              */
/* -------------------------------------------------------------------------- */

func TestTheEmitterRefusesAStreamTheContractCannotDescribe(t *testing.T) {
	cases := []struct {
		name   string
		stream func(ctx context.Context, call *provider.Call, out provider.Emitter) (provider.Outcome, error)
		expect string
	}{
		{
			name: "output before the stream started",
			stream: func(_ context.Context, _ *provider.Call, out provider.Emitter) (provider.Outcome, error) {
				return provider.Outcome{}, out.Delta(0, contract.ChannelOutputText, "hello")
			},
			expect: "precedes the stream's start event",
		},
		{
			name: "two start events",
			stream: func(_ context.Context, call *provider.Call, out provider.Emitter) (provider.Outcome, error) {
				if err := out.Start(call.Route.ModelReference, time.Now()); err != nil {
					return provider.Outcome{}, err
				}
				return provider.Outcome{}, out.Start(call.Route.ModelReference, time.Now())
			},
			expect: "second start event",
		},
		{
			name: "a start event naming an unpinned model",
			stream: func(_ context.Context, _ *provider.Call, out provider.Emitter) (provider.Outcome, error) {
				return provider.Outcome{}, out.Start("stub/model", time.Now())
			},
			expect: "not revision-pinned",
		},
		{
			name: "a usage event carrying no units",
			stream: func(_ context.Context, call *provider.Call, out provider.Emitter) (provider.Outcome, error) {
				if err := out.Start(call.Route.ModelReference, time.Now()); err != nil {
					return provider.Outcome{}, err
				}
				return provider.Outcome{}, out.Usage(nil, contract.UsageProviderReported)
			},
			expect: "at least one unit",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			adapter := &scriptedAdapter{stream: func(ctx context.Context, call *provider.Call, out provider.Emitter) (provider.Outcome, error) {
				err := func() error {
					_, err := testCase.stream(ctx, call, out)
					return err
				}()
				if err == nil {
					t.Fatal("the emitter accepted an event it should have refused")
				}
				if !strings.Contains(err.Error(), testCase.expect) {
					t.Errorf("the emitter refused with %q, expected it to mention %q", err, testCase.expect)
				}
				// Returned so the executor treats this as a failed request,
				// which is what it is.
				return provider.Outcome{}, err
			}}
			_, result := execute(t, adapter, baseRequest())
			if result.Failure == nil {
				t.Error("a stream the contract cannot describe was reported as a success")
			}
		})
	}
}

// TestAnAdapterThatCompletesWithoutStartingIsAFailure covers the shape that
// would otherwise hand settlement a receipt for output nobody saw.
func TestAnAdapterThatCompletesWithoutStartingIsAFailure(t *testing.T) {
	adapter := &scriptedAdapter{stream: func(context.Context, *provider.Call, provider.Emitter) (provider.Outcome, error) {
		return provider.Outcome{
			Units:       []contract.UsageQuantity{{Unit: contract.UnitOutputTokens, Quantity: 500}},
			UsageSource: contract.UsageProviderReported,
		}, nil
	}}

	_, result := execute(t, adapter, baseRequest())

	if result.Failure == nil {
		t.Fatal("an adapter that never started a stream was reported as a success")
	}
	if result.Report == nil || result.Report.Outcome != contract.OutcomeFailed {
		t.Errorf("the report says %v", result.Report)
	}
}

/* -------------------------------------------------------------------------- */
/*  Settlement                                                                */
/* -------------------------------------------------------------------------- */

func TestASuccessfulRequestProducesASettleableReport(t *testing.T) {
	events, result := execute(t, happyAdapter(), baseRequest())

	if result.Failure != nil {
		t.Fatalf("the request failed: %v", result.Failure)
	}
	if result.Report == nil {
		t.Fatal("no usage report was produced")
	}
	if err := result.Report.Validate(); err != nil {
		t.Fatalf("the report would be rejected by the contract: %v", err)
	}
	if result.Report.Outcome != contract.OutcomeCompleted {
		t.Errorf("the report says %q", result.Report.Outcome)
	}
	if result.Report.GenerationID == nil || *result.Report.GenerationID == "" {
		t.Error("no generation id was allocated, so the request has no receipt handle")
	}
	if last := events[len(events)-1]; last.EventType() != contract.EventDone {
		t.Errorf("the stream ends with %q", last.EventType())
	}
	// The data plane measures; the control plane prices. A receipt id on a
	// done event would be Relay quoting a settlement it did not compute.
	done := events[len(events)-1].(*contract.StreamDoneEvent)
	if done.ReceiptID != nil {
		t.Error("the done event carries a receipt id, which only settlement can produce")
	}
}

// TestAnAdapterThatMeasuresNothingStillSaysHowItKnows: an estimate that is
// indistinguishable from a provider's own count is one nobody can reconcile.
//
// The adapter reports the single unit a real one reports when its provider sent
// no usage block — `provider.CountRequest` — and nothing else. That is the case
// this test is about: the counts are absent, so the SOURCE has to say so.
func TestAnAdapterThatMeasuresNothingStillSaysHowItKnows(t *testing.T) {
	adapter := &scriptedAdapter{stream: func(_ context.Context, call *provider.Call, out provider.Emitter) (provider.Outcome, error) {
		if err := out.Start(call.Route.ModelReference, time.Now()); err != nil {
			return provider.Outcome{}, err
		}
		return provider.Outcome{
			Units:        provider.CountRequest(nil),
			FinishReason: contract.FinishStop,
		}, nil
	}}

	_, result := execute(t, adapter, baseRequest())

	if result.Report == nil {
		t.Fatal("no report was produced")
	}
	if result.Report.UsageSource != contract.UsageEstimated {
		t.Errorf("a report with no measurement claims source %q", result.Report.UsageSource)
	}
	if len(result.Report.Units) != 1 || result.Report.Units[0].Unit != contract.UnitRequests {
		t.Errorf("the report's units are %v; the request itself is the one thing measured here", result.Report.Units)
	}
}

// TestACompletedReportWithNoUnitsIsRefused is the executor's half of the
// contract's `completed`-carries-a-unit rule.
//
// The published schema refuses that shape and the policy for one is
// refuse-and-release: the report is rejected and the hold is released, never
// estimated and charged. Every shipped adapter now attaches
// `provider.CountRequest` at the clean end of a stream, so this is the gate for
// the NEXT adapter — one that succeeds while measuring nothing produces a
// request that ran, cost money upstream and can never be settled, and Relay must
// say so rather than hand back a report that looks fine.
func TestACompletedReportWithNoUnitsIsRefused(t *testing.T) {
	measuring := &scriptedAdapter{stream: func(_ context.Context, call *provider.Call, out provider.Emitter) (provider.Outcome, error) {
		if err := out.Start(call.Route.ModelReference, time.Now()); err != nil {
			return provider.Outcome{}, err
		}
		return provider.Outcome{FinishReason: contract.FinishStop}, nil
	}}

	_, result := execute(t, measuring, baseRequest())

	if result.Report != nil {
		t.Fatalf("a completed report with no units was returned as settleable: %v", result.Report.Units)
	}
	if result.Failure == nil || result.Failure.Code != contract.CodeInternalError {
		t.Fatalf("the caller was told %v", result.Failure)
	}

	// The control: the identical adapter with ONE unit is settled, so the
	// refusal is the empty list and not the fixture.
	counted := &scriptedAdapter{stream: func(_ context.Context, call *provider.Call, out provider.Emitter) (provider.Outcome, error) {
		if err := out.Start(call.Route.ModelReference, time.Now()); err != nil {
			return provider.Outcome{}, err
		}
		return provider.Outcome{Units: provider.CountRequest(nil), FinishReason: contract.FinishStop}, nil
	}}
	if _, control := execute(t, counted, baseRequest()); control.Report == nil {
		t.Fatalf("the one-unit control produced no report either: %v", control.Failure)
	}
}

// TestAReportTheContractWouldRejectIsNotReturnedAsIfItWereFine: an invalid
// usage report is not a lost log line — it is the record settlement runs
// against, so a request that executed and cannot be settled must say so.
func TestAReportTheContractWouldRejectIsNotReturnedAsIfItWereFine(t *testing.T) {
	adapter := &scriptedAdapter{stream: func(_ context.Context, call *provider.Call, out provider.Emitter) (provider.Outcome, error) {
		if err := out.Start(call.Route.ModelReference, time.Now()); err != nil {
			return provider.Outcome{}, err
		}
		return provider.Outcome{
			// One unit reported twice: the contract's usage report refuses it,
			// because a unit is settled once, as a total.
			Units: []contract.UsageQuantity{
				{Unit: contract.UnitOutputTokens, Quantity: 10},
				{Unit: contract.UnitOutputTokens, Quantity: 20},
			},
			UsageSource:  contract.UsageProviderReported,
			FinishReason: contract.FinishStop,
		}, nil
	}}

	_, result := execute(t, adapter, baseRequest())

	if result.Report != nil {
		t.Fatal("a report the contract would reject was returned as if it were settleable")
	}
	if result.Failure == nil || result.Failure.Code != contract.CodeInternalError {
		t.Fatalf("the unsettleable request reported %v", result.Failure)
	}
}
