package contract

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// The descriptor comparison proves Relay's STRUCTURE matches the contract. It
// cannot prove the VALUES do: a timestamp in the wrong spelling, a model
// reference that fails the published grammar, an error marked retryable under a
// code that forbids it, a unit reported twice — all of those are structurally
// perfect and rejected at Oxy's parse.
//
// So this file writes one fixture per wire shape, marshalled by the same Go
// types the server uses, and `tools/contract/validate.mjs` parses each with the
// published Zod schema itself. Nothing about the round trip is re-implemented:
// the acceptance decision is made by the contract's own code.
//
// The invalid fixtures are the validator's vacuity floor. A validate.mjs that
// silently accepted everything — a bad schema lookup, an empty directory, a
// swallowed exception — would report the same success on the valid ones, so it
// is required to REJECT each of these and fails if it does not.

const fixtureDir = "testdata/wire"

type fixture struct {
	Schema string `json:"schema"`
	Case   string `json:"case"`
	Value  any    `json:"value"`
}

func TestWriteWireFixtures(t *testing.T) {
	valid := append(validFixtures(t), credentialTextFixtures(false)...)
	invalid := append(invalidFixtures(), credentialTextFixtures(true)...)

	// Floors, so "the validator found nothing wrong" cannot be what an empty
	// directory looks like. They are exact rather than minimums for the same
	// reason the not-applicable list is exact.
	// 12 wire shapes plus the 6 credential-text strings the published schema
	// must ACCEPT; 12 controls plus the 6 it must REJECT.
	if len(valid) != 18 {
		t.Fatalf("expected 18 valid fixtures, built %d; update the floor deliberately", len(valid))
	}
	if len(invalid) != 18 {
		t.Fatalf("expected 18 invalid control fixtures, built %d; update the floor deliberately", len(invalid))
	}

	writeFixtures(t, filepath.Join(fixtureDir, "valid"), valid)
	writeFixtures(t, filepath.Join(fixtureDir, "invalid"), invalid)
}

func writeFixtures(t *testing.T, dir string, fixtures []fixture) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("clearing %s: %v", dir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
	for index, item := range fixtures {
		encoded, err := json.MarshalIndent(item, "", "  ")
		if err != nil {
			t.Fatalf("%s/%s: %v", item.Schema, item.Case, err)
		}
		name := fmt.Sprintf("%02d-%s-%s.json", index, item.Schema, item.Case)
		if err := os.WriteFile(filepath.Join(dir, name), append(encoded, '\n'), 0o644); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
	}
}

func pointerTo[T any](value T) *T { return &value }

// sampleAttribution is the attribution block every produced shape carries.
func sampleAttribution() Attribution {
	return Attribution{
		Principal: AuthenticatedPrincipal{
			Billing:         BillingPrincipal{AccountID: "acc_01JQZ"},
			ApplicationID:   "app_01JQZ",
			CredentialID:    "cred_01JQZ",
			Environment:     EnvironmentProduction,
			InferenceScopes: []Scope{ScopeInvoke, ScopeModelsRead},
		},
		UserID:       pointerTo(UserID("usr_01JQZ")),
		RequestID:    "req_01JQZABCDEF",
		GenerationID: pointerTo(GenerationID("gen_01JQZABCDEF")),
	}
}

// validFixtures deliberately populates every OPTIONAL field as well as every
// required one. An optional field that drifted is invisible in a minimal
// fixture, and minimal fixtures are exactly how a contract test stays green
// through a rename.
func validFixtures(t *testing.T) []fixture {
	t.Helper()
	attribution := sampleAttribution()
	started := Timestamp("2026-08-16T09:41:00.000Z")
	completed := Timestamp("2026-08-16T09:41:02.500Z")

	request := Request{
		SchemaVersion: RequestEnvelopeVersion,
		Attribution:   attribution,
		Target: RoutingTarget{
			Kind:           TargetModel,
			ModelReference: pointerTo(ModelReference("openai/gpt-5@2026-05-01")),
		},
		Modality: ModalityText,
		Input: Input{
			Format: InputMessages,
			Messages: []Message{
				{
					Role:    RoleSystem,
					Content: []ContentPart{{Type: ContentPartText, Text: pointerTo("be concise")}},
				},
				{
					Role: RoleUser,
					Name: pointerTo("ada"),
					Content: []ContentPart{
						{Type: ContentPartText, Text: pointerTo("describe this")},
						{
							Type:   ContentPartImage,
							Detail: pointerTo(ImageDetailHigh),
							Source: &ContentSource{Kind: ContentSourceURL, URL: pointerTo("https://example.test/a.png")},
						},
						{
							Type: ContentPartFile,
							Source: &ContentSource{
								Kind:      ContentSourceInline,
								MediaType: pointerTo("application/pdf"),
								Data:      pointerTo("JVBERi0xLjQK"),
							},
							Filename: pointerTo("report.pdf"),
						},
						{
							Type:   ContentPartAudio,
							Source: &ContentSource{Kind: ContentSourceURL, URL: pointerTo("https://example.test/a.wav")},
						},
					},
				},
				{
					Role: RoleAssistant,
					Content: []ContentPart{
						{Type: ContentPartText, Text: pointerTo("")},
						// The variant 1.2.0 added. It is CONTENT: the request
						// succeeded and the provider billed for it, so a fixture
						// that omitted it would leave the only new content
						// variant unexercised on both sides of the wire.
						{Type: ContentPartRefusal, Text: pointerTo("I can't help with that.")},
					},
					ToolCalls: []ToolCall{
						{ID: "call_1", Name: "lookup", Arguments: `{"q":"x"}`},
					},
				},
				{
					Role:       RoleTool,
					ToolCallID: pointerTo("call_1"),
					Content:    []ContentPart{{Type: ContentPartText, Text: pointerTo("42")}},
				},
			},
		},
		Stream:          true,
		MaxOutputTokens: pointerTo(1024),
		Sampling: SamplingParameters{
			Temperature:      pointerTo(0.7),
			TopP:             pointerTo(0.95),
			TopK:             pointerTo(40),
			FrequencyPenalty: pointerTo(0.1),
			PresencePenalty:  pointerTo(-0.1),
			Seed:             pointerTo(7),
			StopSequences:    []string{"\n\n"},
		},
		Tools: []ToolDefinition{{
			Type:        "function",
			Name:        "lookup",
			Description: pointerTo("look something up"),
			Parameters:  map[string]any{"type": "object", "properties": map[string]any{}},
			Strict:      pointerTo(true),
		}},
		ToolChoice: &ToolChoice{Function: &ToolChoiceFunction{Type: "function", Name: "lookup"}},
		ResponseFormat: &ResponseFormat{
			Type:   ResponseFormatJSONSchema,
			Name:   pointerTo("answer"),
			Schema: map[string]any{"type": "object"},
			Strict: pointerTo(true),
		},
		Client: ClientRequestMetadata{
			APIFormat:       APIFormatResponses,
			Endpoint:        "/v1/responses",
			ClientRequestID: pointerTo("client-42"),
			ReceivedAt:      started,
			Labels:          map[string]string{"team": "search"},
		},
		IdempotencyKey: pointerTo(IdempotencyKey("idem_01JQZ")),
		RoutingPolicy:  RoutingPolicyReference{RoutingPolicyID: "rp_01JQZ", PolicyVersion: 3},
		// The list 1.2.0 added, in the order Oxy authorized it. This request PINS
		// a revision, and the published refinement forbids a cross-model
		// substitute for one — so the cross-model variant is exercised by
		// `profileTarget` below, where it is legal, rather than here where it
		// would make the whole fixture invalid.
		AuthorizedRoutes: []AuthorizedRoute{
			{
				Substitution:   SubstitutionSameModel,
				DeploymentID:   "dep_primary",
				ModelReference: "openai/gpt-5@2026-05-01",
				Provider:       "openai",
				Regions:        []Region{"us-west-2", "us-east-1"},
			},
			{
				Substitution:   SubstitutionSameModel,
				DeploymentID:   "dep_secondary",
				ModelReference: "openai/gpt-5@2026-05-01",
				Provider:       "azure-openai",
				Regions:        []Region{"eu-west-1"},
			},
		},
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("the request fixture does not satisfy Relay's own validation: %v", err)
	}

	textTarget := request
	textTarget.Target = RoutingTarget{Kind: TargetRoutingProfile, RoutingProfile: pointerTo(RoutingProfileSlug("auto"))}
	textTarget.Input = Input{Format: InputText, Text: pointerTo("embed me")}
	textTarget.Modality = ModalityEmbedding
	textTarget.ToolChoice = &ToolChoice{Mode: pointerTo(ToolChoiceAuto)}
	// A routing-profile target is where a cross-model substitute is legal: the
	// customer named a strategy rather than weights, so the pinned-request
	// refinement does not apply. This is the ONLY fixture that carries the
	// `cross_model` variant, and it is here because a variant no fixture
	// produces is a variant the published schema never checks Relay against.
	textTarget.AuthorizedRoutes = []AuthorizedRoute{
		{
			Substitution:   SubstitutionSameModel,
			DeploymentID:   "dep_primary",
			ModelReference: "openai/gpt-5@2026-05-01",
			Provider:       "openai",
			Regions:        []Region{"us-west-2"},
		},
		{
			Substitution:       SubstitutionCrossModel,
			DeploymentID:       "dep_other_model",
			ModelReference:     "anthropic/claude-4-5@2026-04-01",
			Provider:           "anthropic",
			Regions:            []Region{"us-west-2"},
			AuthorizedByPolicy: pointerTo(true),
		},
	}
	if err := textTarget.Validate(); err != nil {
		t.Fatalf("the routing-profile fixture does not satisfy Relay's own validation: %v", err)
	}

	batch := textTarget
	batch.Input = Input{Format: InputTextBatch, Texts: []string{"a", "b"}}

	usageReport := UsageReport{
		SchemaVersion:          SchemaVersion,
		RequestID:              attribution.RequestID,
		GenerationID:           attribution.GenerationID,
		Attribution:            attribution,
		Outcome:                OutcomeCompleted,
		Units:                  []UsageQuantity{{Unit: UnitInputTokens, Quantity: 314}, {Unit: UnitOutputTokens, Quantity: 204}},
		UsageSource:            UsageProviderReported,
		ResolvedModelReference: "openai/gpt-5@2026-05-01",
		ServingProvider:        "openai",
		DeploymentID:           pointerTo(DeploymentID("dep_openai_gpt5_use1")),
		RouteSwitches:          0,
		StartedAt:              started,
		CompletedAt:            completed,
		TimeToFirstTokenMs:     pointerTo(180),
	}
	if err := usageReport.Validate(); err != nil {
		t.Fatalf("the usage report fixture does not satisfy Relay's own validation: %v", err)
	}

	failure := NewError(attribution.RequestID, CodeProviderOverloaded, "upstream is overloaded").
		WithRetryAfter(2000).
		WithUpstream(UpstreamOverloaded, &ProviderErrorPassthrough{
			Provider: "openai",
			Status:   pointerTo(503),
			Code:     pointerTo("overloaded"),
			Message:  pointerTo("slow down"),
		})

	return []fixture{
		{Schema: "inferenceRequestSchema", Case: "messages-with-every-optional-field", Value: request},
		{Schema: "inferenceRequestSchema", Case: "text-input-routing-profile", Value: textTarget},
		{Schema: "inferenceRequestSchema", Case: "text-batch-input", Value: batch},
		{Schema: "normalizedUsageReportSchema", Case: "completed", Value: usageReport},
		{Schema: "inferenceErrorSchema", Case: "retryable-with-upstream", Value: failure},
		{Schema: "inferenceStreamEventSchema", Case: "start", Value: &StreamStartEvent{
			SchemaVersion: SchemaVersion, Type: EventStart, RequestID: attribution.RequestID, Seq: 0,
			GenerationID: attribution.GenerationID, ResolvedModelReference: "openai/gpt-5@2026-05-01",
			ServingProvider: "openai", StartedAt: started,
		}},
		{Schema: "inferenceStreamEventSchema", Case: "delta", Value: &StreamDeltaEvent{
			SchemaVersion: SchemaVersion, Type: EventDelta, RequestID: attribution.RequestID, Seq: 1,
			OutputIndex: 0, Channel: ChannelOutputText, Text: "hello",
		}},
		{Schema: "inferenceStreamEventSchema", Case: "tool-call", Value: &StreamToolCallEvent{
			SchemaVersion: SchemaVersion, Type: EventToolCall, RequestID: attribution.RequestID, Seq: 2,
			ToolCallID: "call_1", Name: pointerTo("lookup"), ArgumentsDelta: pointerTo(`{"q":`), Complete: false,
		}},
		{Schema: "inferenceStreamEventSchema", Case: "usage", Value: &StreamUsageEvent{
			SchemaVersion: SchemaVersion, Type: EventUsage, RequestID: attribution.RequestID, Seq: 3,
			Units: []UsageQuantity{{Unit: UnitOutputTokens, Quantity: 204}}, UsageSource: UsageProviderReported,
		}},
		{Schema: "inferenceStreamEventSchema", Case: "route-switch-deployment", Value: &StreamRouteSwitchEvent{
			SchemaVersion: SchemaVersion, Type: EventRouteSwitch, RequestID: attribution.RequestID, Seq: 4,
			Reason: SwitchDeploymentUnavailable,
			Detail: RouteSwitchDetail{
				Scope: SwitchScopeDeployment, ToProvider: "openai",
				ModelReference: pointerTo(ModelReference("openai/gpt-5@2026-05-01")),
				ToDeploymentID: pointerTo(DeploymentID("dep_openai_gpt5_usw2")),
			},
			OccurredAt: started,
		}},
		{Schema: "inferenceStreamEventSchema", Case: "error", Value: &StreamErrorEvent{
			SchemaVersion: SchemaVersion, Type: EventError, RequestID: attribution.RequestID, Seq: 5,
			Error: *failure,
		}},
		{Schema: "inferenceStreamEventSchema", Case: "done", Value: &StreamDoneEvent{
			SchemaVersion: SchemaVersion, Type: EventDone, RequestID: attribution.RequestID, Seq: 6,
			GenerationID: attribution.GenerationID, FinishReason: FinishStop, CompletedAt: completed,
		}},
	}
}

// credentialTextCases is the table that verifies Relay's Go reading of the
// published credential refusal against the published predicate ITSELF.
//
// The contract expresses that refusal as four regexes inside a `.refine()`, so
// it cannot be read off the descriptor, and two of them use a negative
// lookahead that Go's RE2 engine cannot express — this package restates them
// with a capture and a second check. A restatement is a re-implementation, and
// a test that re-implements the code under test measures the re-implementation.
//
// So every string below travels BOTH ways: `TestCredentialShapedAgreesWithGo`
// asserts Relay's own CredentialShaped() classifies it as stated, and each one
// is also emitted as a fixture — refused ones as invalid controls the published
// schema must REJECT, accepted ones as valid fixtures it must PARSE. The two
// halves disagreeing is what a drifted restatement looks like.
//
// The accepted half is not decoration. A refusal that fires on everything
// closes the hole and destroys every diagnostic with it, and the second entry
// below is exactly the string Relay emits after provider.RedactSecret has done
// its work: if the contract refused that, correct redaction would be
// indistinguishable from no redaction at all.
var credentialTextCases = []struct {
	name    string
	text    string
	refused bool
}{
	// The header spelling a literal `authorization|api_key` pattern missed, and
	// the reason the prefix group exists.
	{"header-named-credential", "request rejected: headers were {x-api-key: relay0fake0credential0value}", true},
	// The residue a SPAN redaction leaves: the marker is gone, the secret is
	// not. Refusing this is what stops a producer from redacting the wrong half.
	{"span-redacted-marker", "request rejected: headers were {x-[redacted] relay0fake0credential0value}", true},
	{"bare-bearer-token", "upstream said: Bearer abcdefghijklmnop", true},
	// No marker at all: the layer that survives a producer stripping one.
	{"issued-token-grammar", "the key sk-abcdefghijklmnop was not accepted", true},
	{"json-web-token", "value eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpM", true},
	{"cloud-provider-key", "the value AIzaSyD0abcdefghijklmnopqrstuvwxyz01 was rejected", true},

	{"ordinary-diagnostic", "model claude-opus is overloaded; retry in 2s (request 01JABCDEF)", false},
	// What Relay emits once the adapter has removed the value it sent.
	{"correctly-redacted-value", "request rejected: headers were {x-api-key: [redacted]}", false},
	{"marker-with-a-short-value", "authorization: none", false},
	{"marker-with-a-masked-value", "api_key=***", false},
	// An opaque run that is a request id and not a credential. Refusing this is
	// what an entropy heuristic would do, and the reason the contract does not
	// use one.
	{"opaque-request-id", "request 01JABCDEFGHJKMNPQRSTVWXYZ failed upstream", false},
	// The contract's STATED blind spot, pinned so it stays a known limit rather
	// than an assumption: a credential with no marker, no issued-token prefix
	// and no placeholder beside it is bytes that look like a request id. This
	// string carries a secret and the published schema accepts it — which is
	// why the control that matters is provider.RedactSecret, applied by the
	// adapter that still holds the bytes it sent.
	{"unmarked-credential-the-pattern-cannot-see", "the key relay0fake0credential0value was not accepted", false},
}

// TestCredentialShapedAgreesWithGo is half the check; validate.mjs is the other
// half, and neither is sufficient alone.
func TestCredentialShapedAgreesWithGo(t *testing.T) {
	for _, testCase := range credentialTextCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := CredentialShaped(testCase.text); got != testCase.refused {
				t.Errorf("CredentialShaped(%q) = %t, the contract's own predicate says %t", testCase.text, got, testCase.refused)
			}
			// SafeErrorText withholds the WHOLE string or none of it. A span
			// redaction here is the defect this table exists for.
			safe := SafeErrorText(testCase.text)
			if testCase.refused && safe == testCase.text {
				t.Error("a credential-shaped message was emitted unchanged")
			}
			if !testCase.refused && safe != testCase.text {
				t.Errorf("an acceptable diagnostic was altered:\n want %q\n got  %q", testCase.text, safe)
			}
		})
	}
}

// credentialTextFixtures renders the table above as wire fixtures.
func credentialTextFixtures(refused bool) []fixture {
	var fixtures []fixture
	for _, testCase := range credentialTextCases {
		if testCase.refused != refused {
			continue
		}
		fixtures = append(fixtures, fixture{
			Schema: "inferenceErrorSchema",
			Case:   "message-" + testCase.name,
			Value: map[string]any{
				"schemaVersion": 1, "code": "provider_error", "message": testCase.text,
				"retryable": true, "requestId": "req_01JQZABCDEF",
			},
		})
	}
	return fixtures
}

// invalidFixtures are values the published schemas MUST reject. Each is exactly
// one mutation away from a valid fixture, so a validator that accepts one is
// not merely lenient — it is not reading the schema it claims to.
func invalidFixtures() []fixture {
	attribution := sampleAttribution()
	started := Timestamp("2026-08-16T09:41:00.000Z")

	return []fixture{
		{
			Schema: "inferenceErrorSchema",
			Case:   "non-retryable-code-marked-retryable",
			Value: map[string]any{
				"schemaVersion": 1, "code": "invalid_request", "message": "nope",
				"retryable": true, "requestId": "req_01JQZABCDEF",
			},
		},
		{
			Schema: "inferenceErrorSchema",
			Case:   "credential-shaped-message",
			Value: map[string]any{
				"schemaVersion": 1, "code": "provider_error",
				"message":   "upstream rejected Bearer sk-abcdefghijklmnop",
				"retryable": true, "requestId": "req_01JQZABCDEF",
			},
		},
		{
			Schema: "normalizedUsageReportSchema",
			Case:   "one-unit-reported-twice",
			Value: map[string]any{
				"schemaVersion": 1, "requestId": "req_01JQZABCDEF", "attribution": attribution,
				"outcome": "completed",
				"units": []map[string]any{
					{"unit": "input_tokens", "quantity": 1},
					{"unit": "input_tokens", "quantity": 2},
				},
				"usageSource": "provider_reported", "resolvedModelReference": "openai/gpt-5@2026-05-01",
				"servingProvider": "openai", "routeSwitches": 0,
				"startedAt": started, "completedAt": started,
			},
		},
		{
			Schema: "normalizedUsageReportSchema",
			Case:   "completed-before-started",
			Value: map[string]any{
				"schemaVersion": 1, "requestId": "req_01JQZABCDEF", "attribution": attribution,
				"outcome": "completed", "units": []map[string]any{{"unit": "input_tokens", "quantity": 1}},
				"usageSource": "provider_reported", "resolvedModelReference": "openai/gpt-5@2026-05-01",
				"servingProvider": "openai", "routeSwitches": 0,
				"startedAt": "2026-08-16T09:41:02.500Z", "completedAt": "2026-08-16T09:41:00.000Z",
			},
		},
		{
			Schema: "inferenceStreamEventSchema",
			Case:   "unknown-event-type",
			Value: map[string]any{
				"schemaVersion": 1, "type": "heartbeat", "requestId": "req_01JQZABCDEF", "sequence": 1,
			},
		},
		{
			Schema: "inferenceRequestSchema",
			Case:   "client-metadata-carrying-an-ip",
			Value: map[string]any{
				"schemaVersion": 1, "attribution": attribution,
				"target":   map[string]any{"kind": "model", "modelReference": "openai/gpt-5"},
				"modality": "text",
				"input": map[string]any{
					"format":   "messages",
					"messages": []map[string]any{{"role": "user", "content": []map[string]any{{"type": "text", "text": "hi"}}}},
				},
				"stream": false, "sampling": map[string]any{}, "tools": []any{},
				"client": map[string]any{
					"apiFormat": "responses", "endpoint": "/v1/responses",
					"receivedAt": started, "ip": "203.0.113.7",
				},
				"routingPolicy": map[string]any{"routingPolicyId": "rp_01JQZ", "policyVersion": 1},
			},
		},
		// The union's own rule, in both directions. These are the two ways an
		// authorized route can claim to be something it is not, and both must be
		// refused by the PUBLISHED schema rather than only by Relay's own
		// `validate` — otherwise Relay is the only thing holding a rule the wire
		// is supposed to hold, and an Oxy-side bug would reach it as a legal
		// envelope.
		{
			Schema: "inferenceRequestSchema",
			Case:   "same-model-route-claiming-policy-authorization",
			Value: requestWithAuthorizedRoutes(attribution, started, []map[string]any{{
				"substitution": "same_model", "deploymentId": "dep_primary",
				"modelReference": "openai/gpt-5@2026-05-01", "provider": "openai",
				"regions": []string{"us-west-2"}, "authorizedByPolicy": true,
			}}),
		},
		{
			Schema: "inferenceRequestSchema",
			Case:   "cross-model-route-without-policy-authorization",
			Value: requestWithAuthorizedRoutes(attribution, started, []map[string]any{{
				"substitution": "cross_model", "deploymentId": "dep_other_model",
				"modelReference": "anthropic/claude-4-5@2026-04-01", "provider": "anthropic",
				"regions": []string{"us-west-2"},
			}}),
		},
		{
			Schema: "inferenceRequestSchema",
			Case:   "authorized-routes-sent-empty",
			Value:  requestWithAuthorizedRoutes(attribution, started, []map[string]any{}),
		},
		// The three cross-field branches. `Candidates` reads `substitution`, so
		// each of these is a way an envelope could talk Relay into serving
		// weights nobody authorized, and each must be refused by the wire rather
		// than only by Relay.
		{
			Schema: "inferenceRequestSchema",
			Case:   "primary-route-claiming-to-be-a-substitution",
			Value: requestWithAuthorizedRoutes(attribution, started, []map[string]any{{
				"substitution": "cross_model", "deploymentId": "dep_primary",
				"modelReference": "anthropic/claude-4-5@2026-04-01", "provider": "anthropic",
				"regions": []string{"us-west-2"}, "authorizedByPolicy": true,
			}}),
		},
		{
			Schema: "inferenceRequestSchema",
			Case:   "same-model-label-on-a-different-model-line",
			Value: requestWithAuthorizedRoutes(attribution, started, []map[string]any{
				{
					"substitution": "same_model", "deploymentId": "dep_primary",
					"modelReference": "openai/gpt-5@2026-05-01", "provider": "openai",
					"regions": []string{"us-west-2"},
				},
				{
					"substitution": "same_model", "deploymentId": "dep_other_model",
					"modelReference": "anthropic/claude-4-5@2026-04-01", "provider": "anthropic",
					"regions": []string{"us-west-2"},
				},
			}),
		},
		{
			Schema: "inferenceRequestSchema",
			Case:   "cross-model-substitute-for-a-pinned-request",
			Value: pinnedRequestWithAuthorizedRoutes(attribution, started, []map[string]any{
				{
					"substitution": "same_model", "deploymentId": "dep_primary",
					"modelReference": "openai/gpt-5@2026-05-01", "provider": "openai",
					"regions": []string{"us-west-2"},
				},
				{
					"substitution": "cross_model", "deploymentId": "dep_other_model",
					"modelReference": "anthropic/claude-4-5@2026-04-01", "provider": "anthropic",
					"regions": []string{"us-west-2"}, "authorizedByPolicy": true,
				},
			}),
		},
	}
}

// requestWithAuthorizedRoutes is the smallest legal envelope with an
// `authorizedRoutes` list substituted in, so each control above is exactly one
// mutation away from valid and the rejection can only be about the list.
func requestWithAuthorizedRoutes(attribution any, started Timestamp, routes []map[string]any) map[string]any {
	return map[string]any{
		"schemaVersion": 1, "attribution": attribution,
		"target":   map[string]any{"kind": "model", "modelReference": "openai/gpt-5"},
		"modality": "text",
		"input": map[string]any{
			"format":   "messages",
			"messages": []map[string]any{{"role": "user", "content": []map[string]any{{"type": "text", "text": "hi"}}}},
		},
		"stream": false, "sampling": map[string]any{}, "tools": []any{},
		"client": map[string]any{
			"apiFormat": "responses", "endpoint": "/v1/responses", "receivedAt": started,
		},
		"routingPolicy":    map[string]any{"routingPolicyId": "rp_01JQZ", "policyVersion": 1},
		"authorizedRoutes": routes,
	}
}

// pinnedRequestWithAuthorizedRoutes is the same envelope with a REVISION-PINNED
// target, which is what makes the cross-model refinement apply at all.
func pinnedRequestWithAuthorizedRoutes(attribution any, started Timestamp, routes []map[string]any) map[string]any {
	request := requestWithAuthorizedRoutes(attribution, started, routes)
	request["target"] = map[string]any{"kind": "model", "modelReference": "openai/gpt-5@2026-05-01"}
	return request
}
