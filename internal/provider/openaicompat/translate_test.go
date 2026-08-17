package openaicompat

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/OxyHQ/Relay/internal/contract"
	"github.com/OxyHQ/Relay/internal/provider"
)

// Translation is the half of an adapter with no schema to check it: the
// contract validates what arrives, and the provider validates what is sent, but
// nothing validates the mapping between them except these tests.

func testRoute() provider.Route {
	return provider.Route{
		DeploymentID:    "dep_test",
		Provider:        "openai",
		ModelReference:  "openai/gpt-5@2026-05-01",
		UpstreamModelID: "gpt-5-2026-05-01",
		Region:          "us-east-1",
	}
}

func testAdapter(t *testing.T) *Adapter {
	t.Helper()
	adapter, err := New(Config{Provider: "openai", BaseURL: "https://upstream.invalid/v1", APIKey: fakeAPIKey})
	if err != nil {
		t.Fatalf("building the adapter: %v", err)
	}
	return adapter
}

func requestWith(messages []contract.Message) *contract.Request {
	reference := contract.ModelReference("openai/gpt-5@2026-05-01")
	return &contract.Request{
		SchemaVersion: contract.RequestEnvelopeVersion,
		Attribution: contract.Attribution{
			Principal: contract.AuthenticatedPrincipal{
				Billing:         contract.BillingPrincipal{AccountID: "acc"},
				ApplicationID:   "app",
				CredentialID:    "cred",
				Environment:     contract.EnvironmentDevelopment,
				InferenceScopes: []contract.Scope{contract.ScopeInvoke},
			},
			RequestID: "req",
		},
		Target:   contract.RoutingTarget{Kind: contract.TargetModel, ModelReference: &reference},
		Modality: contract.ModalityText,
		Input:    contract.Input{Format: contract.InputMessages, Messages: messages},
		Stream:   true,
		Client: contract.ClientRequestMetadata{
			APIFormat:  contract.APIFormatResponses,
			Endpoint:   "/v1/responses",
			ReceivedAt: contract.NewTimestamp(time.Now()),
		},
		RoutingPolicy: contract.RoutingPolicyReference{RoutingPolicyID: "rp", PolicyVersion: 1},
	}
}

func translateToMap(t *testing.T, request *contract.Request) map[string]any {
	t.Helper()
	call, err := testAdapter(t).Translate(request, testRoute())
	if err != nil {
		t.Fatalf("translating: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(call.Body, &body); err != nil {
		t.Fatalf("the translated body does not decode: %v", err)
	}
	return body
}

func textPartOf(value string) contract.ContentPart {
	return contract.ContentPart{Type: contract.ContentPartText, Text: &value}
}

func TestTranslateSendsTheUpstreamModelIdAndAsksForUsage(t *testing.T) {
	body := translateToMap(t, requestWith([]contract.Message{{
		Role: contract.RoleUser, Content: []contract.ContentPart{textPartOf("hi")},
	}}))

	// The canonical model reference is Oxy's; what the provider is called is
	// the inventory's. Sending the former upstream is a 404 that reads as an
	// outage.
	if body["model"] != "gpt-5-2026-05-01" {
		t.Errorf("the upstream request names model %v", body["model"])
	}
	// Without this, a streamed request reports no usage at all — which is what
	// the code this was ported from did.
	options, present := body["stream_options"].(map[string]any)
	if !present || options["include_usage"] != true {
		t.Errorf("a streamed request did not ask for usage: %v", body["stream_options"])
	}
}

// TestTranslateSendsNoSamplingDefaults: the contract says an absent sampling
// parameter means the route's own default. Alia's adapters substituted
// temperature 0.7 and max_tokens 8192, which silently changed every request
// nobody configured.
func TestTranslateSendsNoSamplingDefaults(t *testing.T) {
	body := translateToMap(t, requestWith([]contract.Message{{
		Role: contract.RoleUser, Content: []contract.ContentPart{textPartOf("hi")},
	}}))

	for _, field := range []string{"temperature", "max_tokens", "top_p", "frequency_penalty", "presence_penalty", "seed", "stop"} {
		if _, present := body[field]; present {
			t.Errorf("the upstream request carries %q, which the customer did not set", field)
		}
	}
}

// TestASingleTextPartBecomesAPlainString: several providers claiming OpenAI
// compatibility accept only the string form for system and tool messages, and
// the array form is the one that fails on them.
func TestASingleTextPartBecomesAPlainString(t *testing.T) {
	body := translateToMap(t, requestWith([]contract.Message{{
		Role: contract.RoleSystem, Content: []contract.ContentPart{textPartOf("be brief")},
	}}))

	messages := body["messages"].([]any)
	content := messages[0].(map[string]any)["content"]
	if content != "be brief" {
		t.Errorf("a single text part translated to %#v, expected a plain string", content)
	}
}

func TestMultipartContentTranslatesToTheProvidersOwnShapes(t *testing.T) {
	mediaType, data := "image/png", "aGVsbG8="
	body := translateToMap(t, requestWith([]contract.Message{{
		Role: contract.RoleUser,
		Content: []contract.ContentPart{
			textPartOf("what is this"),
			{
				Type:   contract.ContentPartImage,
				Detail: pointer(contract.ImageDetailHigh),
				Source: &contract.ContentSource{Kind: contract.ContentSourceURL, URL: pointer("https://example.test/a.png")},
			},
			{
				Type: contract.ContentPartImage,
				Source: &contract.ContentSource{
					Kind: contract.ContentSourceInline, MediaType: &mediaType, Data: &data,
				},
			},
		},
	}}))

	parts := body["messages"].([]any)[0].(map[string]any)["content"].([]any)
	if len(parts) != 3 {
		t.Fatalf("translated to %d parts", len(parts))
	}
	if parts[0].(map[string]any)["type"] != "text" {
		t.Errorf("part 0 is %v", parts[0])
	}

	remote := parts[1].(map[string]any)
	if remote["type"] != "image_url" {
		t.Errorf("part 1 is %v", remote["type"])
	}
	if url := remote["image_url"].(map[string]any); url["url"] != "https://example.test/a.png" || url["detail"] != "high" {
		t.Errorf("part 1 carries %v", url)
	}

	inline := parts[2].(map[string]any)["image_url"].(map[string]any)
	if inline["url"] != "data:image/png;base64,aGVsbG8=" {
		t.Errorf("an inline image became %v, expected a data URI", inline["url"])
	}
}

func TestAssistantToolCallsAndToolAnswersSurviveTranslation(t *testing.T) {
	body := translateToMap(t, requestWith([]contract.Message{
		{
			Role:    contract.RoleAssistant,
			Content: []contract.ContentPart{textPartOf("")},
			ToolCalls: []contract.ToolCall{{
				ID: "call_1", Name: "lookup", Arguments: `{"q":"relay"}`,
			}},
		},
		{
			Role:       contract.RoleTool,
			ToolCallID: pointer("call_1"),
			Content:    []contract.ContentPart{textPartOf("42")},
		},
	}))

	messages := body["messages"].([]any)
	assistant := messages[0].(map[string]any)
	calls := assistant["tool_calls"].([]any)
	if len(calls) != 1 {
		t.Fatalf("the assistant message carries %d tool calls", len(calls))
	}
	call := calls[0].(map[string]any)
	if call["id"] != "call_1" || call["type"] != "function" {
		t.Errorf("the tool call translated to %v", call)
	}
	function := call["function"].(map[string]any)
	// Carried as JSON TEXT, not a parsed object: models emit invalid JSON often
	// enough that parsing it would turn a recoverable mistake into a rejection.
	if function["arguments"] != `{"q":"relay"}` {
		t.Errorf("the tool call's arguments translated to %#v", function["arguments"])
	}

	toolAnswer := messages[1].(map[string]any)
	if toolAnswer["tool_call_id"] != "call_1" {
		t.Errorf("the tool answer names call %v", toolAnswer["tool_call_id"])
	}
}

// TestTranslateRefusesWhatItCannotExpress covers the refusals that exist so a
// request the protocol cannot carry costs nothing. Each of these would
// otherwise be a request that succeeds while doing something other than what
// was asked.
func TestTranslateRefusesWhatItCannotExpress(t *testing.T) {
	cases := []struct {
		name    string
		build   func() *contract.Request
		code    contract.ErrorCode
		mention string
	}{
		{
			name: "a sampling parameter the protocol has no field for",
			build: func() *contract.Request {
				request := requestWith([]contract.Message{{Role: contract.RoleUser, Content: []contract.ContentPart{textPartOf("hi")}}})
				request.Sampling.TopK = pointer(40)
				return request
			},
			code:    contract.CodeInvalidRequest,
			mention: "sampling.topK",
		},
		{
			name: "audio the protocol only accepts inline",
			build: func() *contract.Request {
				return requestWith([]contract.Message{{
					Role: contract.RoleUser,
					Content: []contract.ContentPart{{
						Type:   contract.ContentPartAudio,
						Source: &contract.ContentSource{Kind: contract.ContentSourceURL, URL: pointer("https://example.test/a.wav")},
					}},
				}})
			},
			code:    contract.CodeUnsupportedModality,
			mention: "input.messages[0].content[0]",
		},
		{
			name: "an audio media type with no protocol format",
			build: func() *contract.Request {
				mediaType, data := "audio/ogg", "AAAA"
				return requestWith([]contract.Message{{
					Role: contract.RoleUser,
					Content: []contract.ContentPart{{
						Type:   contract.ContentPartAudio,
						Source: &contract.ContentSource{Kind: contract.ContentSourceInline, MediaType: &mediaType, Data: &data},
					}},
				}})
			},
			code:    contract.CodeUnsupportedModality,
			mention: "wav or mp3",
		},
		{
			name: "a modality this endpoint does not produce",
			build: func() *contract.Request {
				request := requestWith([]contract.Message{{Role: contract.RoleUser, Content: []contract.ContentPart{textPartOf("hi")}}})
				request.Modality = contract.ModalityImage
				return request
			},
			code:    contract.CodeUnsupportedModality,
			mention: "modality",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := testAdapter(t).Translate(testCase.build(), testRoute())
			if err == nil {
				t.Fatal("translated")
			}
			var unsupported provider.ErrUnsupported
			if !errors.As(err, &unsupported) {
				t.Fatalf("refused with %T: %v", err, err)
			}
			if unsupported.Code != testCase.code {
				t.Errorf("refused with %q, expected %q", unsupported.Code, testCase.code)
			}
			if unsupported.Code.Retryable() {
				t.Errorf("%q is retryable; a request the provider cannot express can never succeed on a retry", unsupported.Code)
			}
			if !strings.Contains(unsupported.Param+" "+unsupported.Detail, testCase.mention) {
				t.Errorf("the refusal (%s / %s) does not mention %q", unsupported.Param, unsupported.Detail, testCase.mention)
			}
		})
	}
}

func TestTranslatedCallsCarryNoCredential(t *testing.T) {
	call, err := testAdapter(t).Translate(requestWith([]contract.Message{{
		Role: contract.RoleUser, Content: []contract.ContentPart{textPartOf("hi")},
	}}), testRoute())
	if err != nil {
		t.Fatalf("translating: %v", err)
	}
	// A translated call that carried a credential would be one struct away from
	// a debug log. Authentication is applied at send time and nowhere else.
	encoded, err := json.Marshal(call)
	if err != nil {
		t.Fatalf("encoding the call: %v", err)
	}
	if strings.Contains(string(encoded), fakeAPIKey) {
		t.Fatalf("the translated call carries the credential: %s", encoded)
	}
}

/* -------------------------------------------------------------------------- */
/*  Usage normalization                                                       */
/* -------------------------------------------------------------------------- */

// TestUsageUnitsAreDisjoint pins the decision the protocol's nesting forces.
//
// `prompt_tokens` INCLUDES cached tokens and `completion_tokens` INCLUDES
// reasoning tokens, while the contract's units are a flat list. Reported as
// nested totals, a price applied to every unit would charge the cached and
// reasoning tokens twice. The contract does not say which reading it intends;
// this is the one Relay implements, and the difference is real money on a
// reasoning model.
func TestUsageUnitsAreDisjoint(t *testing.T) {
	units := normalizeUsage(&chatUsage{
		PromptTokens:        pointer(100),
		CompletionTokens:    pointer(60),
		PromptTokensDetails: &promptTokensDetails{CachedTokens: pointer(40)},
		CompletionDetails:   &completionTokensInfo{ReasoningTokens: pointer(25)},
	})

	got := make(map[contract.UsageUnit]int, len(units))
	for _, quantity := range units {
		if _, duplicate := got[quantity.Unit]; duplicate {
			t.Fatalf("unit %q is reported twice", quantity.Unit)
		}
		got[quantity.Unit] = quantity.Quantity
	}

	want := map[contract.UsageUnit]int{
		contract.UnitRequests:          1,
		contract.UnitInputTokens:       60, // 100 prompt − 40 cached
		contract.UnitCachedInputTokens: 40,
		contract.UnitOutputTokens:      35, // 60 completion − 25 reasoning
		contract.UnitReasoningTokens:   25,
	}
	for unit, quantity := range want {
		if got[unit] != quantity {
			t.Errorf("%s is %d, expected %d", unit, got[unit], quantity)
		}
	}
	if got[contract.UnitInputTokens]+got[contract.UnitCachedInputTokens] != 100 {
		t.Error("the input units do not sum to the prompt tokens the provider reported")
	}
	if got[contract.UnitOutputTokens]+got[contract.UnitReasoningTokens] != 60 {
		t.Error("the output units do not sum to the completion tokens the provider reported")
	}
}

func TestUsageAlwaysCountsTheRequestItself(t *testing.T) {
	// A provider that returns no token counts would otherwise produce a receipt
	// with no units at all, which the contract rejects — and a per-request
	// price would have nothing to multiply.
	units := normalizeUsage(&chatUsage{})
	if len(units) != 1 || units[0].Unit != contract.UnitRequests || units[0].Quantity != 1 {
		t.Errorf("a usage report with no token counts produced %v", units)
	}
}

func TestUsageNeverGoesNegative(t *testing.T) {
	// A provider whose details exceed its totals is reporting something
	// inconsistent; a negative quantity would be rejected by the contract and
	// lose the whole record.
	units := normalizeUsage(&chatUsage{
		PromptTokens:        pointer(10),
		PromptTokensDetails: &promptTokensDetails{CachedTokens: pointer(50)},
	})
	for _, quantity := range units {
		if quantity.Quantity < 0 {
			t.Errorf("%s is %d", quantity.Unit, quantity.Quantity)
		}
	}
}

func TestFinishReasonsMapToTheContractsClosedSet(t *testing.T) {
	for upstream, expected := range map[string]contract.FinishReason{
		"stop":           contract.FinishStop,
		"length":         contract.FinishLength,
		"max_tokens":     contract.FinishLength,
		"tool_calls":     contract.FinishToolCalls,
		"function_call":  contract.FinishToolCalls,
		"content_filter": contract.FinishContentFilter,
		"something_new":  contract.FinishStop,
	} {
		if got := mapFinishReason(upstream); got != expected {
			t.Errorf("%q mapped to %q, expected %q", upstream, got, expected)
		}
	}
}

func pointer[T any](value T) *T { return &value }

// TestAReplayedRefusalTravelsOnTheMessageAndNotAsAContentPart covers the
// `refusal` content part contract 1.2.0 added.
//
// This protocol keeps an assistant's refusal on the MESSAGE — the same field the
// adapter reads a refusal from on the way back — while the contract models it as
// a content part. The two shapes have to be reconciled here: a customer replaying
// a conversation that contains a refusal cannot be asked to do it.
//
// Dropping it would be the silent failure. An assistant turn that DECLINED would
// reach the model as an assistant turn that said nothing, and the model would
// answer a different conversation while the request reported success.
func TestAReplayedRefusalTravelsOnTheMessageAndNotAsAContentPart(t *testing.T) {
	refusal := "I can't help with that."
	body := translateToMap(t, requestWith([]contract.Message{
		{Role: contract.RoleUser, Content: []contract.ContentPart{textPartOf("do the thing")}},
		{Role: contract.RoleAssistant, Content: []contract.ContentPart{
			{Type: contract.ContentPartRefusal, Text: &refusal},
		}},
		{Role: contract.RoleUser, Content: []contract.ContentPart{textPartOf("why not?")}},
	}))

	messages, ok := body["messages"].([]any)
	if !ok || len(messages) != 3 {
		t.Fatalf("the translated body carries %v messages", body["messages"])
	}
	assistant, ok := messages[1].(map[string]any)
	if !ok {
		t.Fatalf("the assistant message is %T", messages[1])
	}
	if assistant["refusal"] != refusal {
		t.Errorf("the assistant message carries refusal %v, want %q", assistant["refusal"], refusal)
	}
	// It is NOT also a content part: sending both would show the model the same
	// text twice, in a field the protocol does not define for it.
	if content, present := assistant["content"]; present {
		if rendered, _ := json.Marshal(content); strings.Contains(string(rendered), refusal) {
			t.Errorf("the refusal is also in content: %s", rendered)
		}
	}

	// THE CONTROL: an ordinary text part in the same position still travels as
	// content and never as a refusal, so the assertion above is about the part
	// TYPE and not about assistant messages generally.
	control := translateToMap(t, requestWith([]contract.Message{
		{Role: contract.RoleUser, Content: []contract.ContentPart{textPartOf("do the thing")}},
		{Role: contract.RoleAssistant, Content: []contract.ContentPart{textPartOf("sure")}},
	}))
	controlMessages := control["messages"].([]any)
	controlAssistant := controlMessages[1].(map[string]any)
	if _, present := controlAssistant["refusal"]; present {
		t.Errorf("an ordinary assistant message carries a refusal field: %v", controlAssistant["refusal"])
	}
	if controlAssistant["content"] != "sure" {
		t.Errorf("the control assistant message carries content %v", controlAssistant["content"])
	}
}

// TestTwoRefusalsInOneMessageAreRefusedRatherThanJoined: the field holds one
// string, and concatenating two would invent a refusal the model never produced.
func TestTwoRefusalsInOneMessageAreRefusedRatherThanJoined(t *testing.T) {
	first, second := "no", "still no"
	_, err := testAdapter(t).Translate(requestWith([]contract.Message{{
		Role: contract.RoleAssistant,
		Content: []contract.ContentPart{
			{Type: contract.ContentPartRefusal, Text: &first},
			{Type: contract.ContentPartRefusal, Text: &second},
		},
	}}), testRoute())

	var unsupported provider.ErrUnsupported
	if !errors.As(err, &unsupported) {
		t.Fatalf("two refusals in one message translated to %v", err)
	}
	if unsupported.Code != contract.CodeUnsupportedModality {
		t.Errorf("the refusal carries code %q", unsupported.Code)
	}

	// The control: ONE refusal in the same position translates cleanly, so the
	// error is the duplication and not the part type.
	if _, err := testAdapter(t).Translate(requestWith([]contract.Message{{
		Role:    contract.RoleAssistant,
		Content: []contract.ContentPart{{Type: contract.ContentPartRefusal, Text: &first}},
	}}), testRoute()); err != nil {
		t.Fatalf("one refusal was refused too: %v", err)
	}
}
