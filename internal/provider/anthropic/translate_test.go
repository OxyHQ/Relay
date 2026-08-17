package anthropic

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/OxyHQ/Relay/internal/contract"
	"github.com/OxyHQ/Relay/internal/provider"
)

func testRoute() provider.Route {
	return provider.Route{
		DeploymentID:    "dep_test",
		Provider:        Slug,
		ModelReference:  fakeModelReference,
		UpstreamModelID: "claude-fake-2026-05-01",
		Region:          "test-region",
	}
}

func newTestAdapter(t *testing.T) *Adapter {
	t.Helper()
	adapter, err := New(Config{BaseURL: "https://upstream.invalid/v1", APIKey: fakeAPIKey})
	if err != nil {
		t.Fatalf("building the adapter: %v", err)
	}
	return adapter
}

// translate runs a request through the adapter and decodes the body it would
// have sent, which is the only way to see what a customer's request became.
func translate(t *testing.T, request *contract.Request) (messagesRequest, *provider.Call) {
	t.Helper()
	call, err := newTestAdapter(t).Translate(request, testRoute())
	if err != nil {
		t.Fatalf("translating: %v", err)
	}
	var body messagesRequest
	if err := json.Unmarshal(call.Body, &body); err != nil {
		t.Fatalf("decoding the translated body: %v", err)
	}
	return body, call
}

func textPart(text string) contract.ContentPart {
	return contract.ContentPart{Type: contract.ContentPartText, Text: &text}
}

// TestTheSystemPromptLeavesTheMessageList covers the structural difference a
// chat-completions body does not have: this protocol carries instructions in a
// field of their own, and a system message left in the list is a 400.
func TestTheSystemPromptLeavesTheMessageList(t *testing.T) {
	request := baseRequest("req_system")
	request.Input.Messages = []contract.Message{
		{Role: contract.RoleSystem, Content: []contract.ContentPart{textPart("be terse")}},
		{Role: contract.RoleUser, Content: []contract.ContentPart{textPart("hello")}},
		{Role: contract.RoleDeveloper, Content: []contract.ContentPart{textPart("and precise")}},
	}

	body, _ := translate(t, request)

	if len(body.Messages) != 1 || body.Messages[0].Role != "user" {
		t.Fatalf("the message list is %#v; only the user message belongs in it", body.Messages)
	}
	if len(body.System) != 2 {
		t.Fatalf("the system prompt carries %d blocks, expected both instruction messages", len(body.System))
	}
	if body.System[0].Text != "be terse" || body.System[1].Text != "and precise" {
		t.Errorf("the instructions arrived out of order or altered: %#v", body.System)
	}
}

// TestAToolResultBecomesAUserMessage covers the other structural difference:
// this protocol has no tool role.
func TestAToolResultBecomesAUserMessage(t *testing.T) {
	callID := "toolu_fake"
	request := baseRequest("req_tool_result")
	request.Input.Messages = []contract.Message{
		{Role: contract.RoleUser, Content: []contract.ContentPart{textPart("what is the weather")}},
		{
			Role:      contract.RoleAssistant,
			Content:   []contract.ContentPart{},
			ToolCalls: []contract.ToolCall{{ID: callID, Name: "lookup", Arguments: `{"city":"Madrid"}`}},
		},
		{
			Role:       contract.RoleTool,
			ToolCallID: &callID,
			Content:    []contract.ContentPart{textPart("22C")},
		},
	}

	body, _ := translate(t, request)

	if len(body.Messages) != 3 {
		t.Fatalf("expected three messages, got %d: %#v", len(body.Messages), body.Messages)
	}
	if body.Messages[2].Role != "user" {
		t.Errorf("a tool result arrived as role %q; this protocol carries one as a user message", body.Messages[2].Role)
	}
	encoded, err := json.Marshal(body.Messages[2].Content)
	if err != nil {
		t.Fatalf("encoding the tool result: %v", err)
	}
	if !strings.Contains(string(encoded), `"tool_use_id":"`+callID+`"`) {
		t.Errorf("the tool result does not name the call it answers: %s", encoded)
	}

	// The assistant's own call travels as a tool_use block with a PARSED input,
	// which is where the contract's JSON text has to become an object.
	encoded, err = json.Marshal(body.Messages[1].Content)
	if err != nil {
		t.Fatalf("encoding the assistant message: %v", err)
	}
	if !strings.Contains(string(encoded), `"type":"tool_use"`) || !strings.Contains(string(encoded), `"city":"Madrid"`) {
		t.Errorf("the assistant's tool call did not survive translation: %s", encoded)
	}
}

// TestToolCallArgumentsThatAreNotAnObjectAreRefusedWithTheFieldNamed covers the
// one place a model's own output can fail this translation.
//
// The contract carries arguments as JSON TEXT precisely because models emit
// invalid JSON; this protocol takes an object. Sending it anyway produces an
// upstream 400 that the customer reads as their own fault.
func TestToolCallArgumentsThatAreNotAnObjectAreRefusedWithTheFieldNamed(t *testing.T) {
	request := baseRequest("req_bad_arguments")
	request.Input.Messages = []contract.Message{
		{Role: contract.RoleUser, Content: []contract.ContentPart{textPart("go on")}},
		{
			Role:      contract.RoleAssistant,
			Content:   []contract.ContentPart{},
			ToolCalls: []contract.ToolCall{{ID: "toolu_fake", Name: "lookup", Arguments: `{"city":`}},
		},
	}

	_, err := newTestAdapter(t).Translate(request, testRoute())
	var unsupported provider.ErrUnsupported
	if !asUnsupported(err, &unsupported) {
		t.Fatalf("expected a refusal, got %v", err)
	}
	if unsupported.Code != contract.CodeInvalidRequest {
		t.Errorf("refused with %q", unsupported.Code)
	}
	if unsupported.Param != "input.messages[1].toolCalls[0].arguments" {
		t.Errorf("the refusal names %q; the customer needs the path to the fragment at fault", unsupported.Param)
	}
}

// TestSamplingParametersThisProtocolCannotExpressAreRefused covers the rule
// that a dropped parameter changes what the model does while reporting success.
//
// `topK` is in the table as the control: the OpenAI Chat Completions protocol
// has no top_k and its adapter refuses one, this protocol does have it, and a
// suite that only ever saw the first adapter would read "top_k is unsupported"
// as a property of the platform.
func TestSamplingParametersThisProtocolCannotExpressAreRefused(t *testing.T) {
	value := 0.5
	seed := 7
	topK := 40

	for name, testCase := range map[string]struct {
		apply   func(*contract.Request)
		refused bool
		param   string
	}{
		"frequency penalty": {apply: func(r *contract.Request) { r.Sampling.FrequencyPenalty = &value }, refused: true, param: "sampling.frequencyPenalty"},
		"presence penalty":  {apply: func(r *contract.Request) { r.Sampling.PresencePenalty = &value }, refused: true, param: "sampling.presencePenalty"},
		"seed":              {apply: func(r *contract.Request) { r.Sampling.Seed = &seed }, refused: true, param: "sampling.seed"},
		"top k":             {apply: func(r *contract.Request) { r.Sampling.TopK = &topK }, refused: false},
	} {
		t.Run(name, func(t *testing.T) {
			request := baseRequest("req_sampling")
			testCase.apply(request)

			call, err := newTestAdapter(t).Translate(request, testRoute())
			if !testCase.refused {
				if err != nil {
					t.Fatalf("this protocol expresses %s, and the adapter refused it: %v", name, err)
				}
				var body messagesRequest
				if err := json.Unmarshal(call.Body, &body); err != nil {
					t.Fatalf("decoding the translated body: %v", err)
				}
				if body.TopK == nil || *body.TopK != topK {
					t.Errorf("top_k did not reach the upstream body: %#v", body.TopK)
				}
				return
			}

			var unsupported provider.ErrUnsupported
			if !asUnsupported(err, &unsupported) {
				t.Fatalf("expected a refusal for %s, got %v", name, err)
			}
			if unsupported.Param != testCase.param {
				t.Errorf("the refusal names %q, expected %q", unsupported.Param, testCase.param)
			}
			if unsupported.Code.Retryable() {
				t.Errorf("%s was refused with the retryable code %q", name, unsupported.Code)
			}
		})
	}
}

// TestToolChoiceModesMapOntoThisProtocolsOwn covers the vocabulary difference:
// the contract's `required` is this protocol's `any`.
func TestToolChoiceModesMapOntoThisProtocolsOwn(t *testing.T) {
	for _, testCase := range []struct {
		mode contract.ToolChoiceMode
		want string
	}{
		{contract.ToolChoiceAuto, "auto"},
		{contract.ToolChoiceNone, "none"},
		{contract.ToolChoiceRequired, "any"},
	} {
		t.Run(string(testCase.mode), func(t *testing.T) {
			mode := testCase.mode
			request := baseRequest("req_tool_choice")
			request.Tools = []contract.ToolDefinition{{Type: "function", Name: "lookup", Parameters: map[string]any{"type": "object"}}}
			request.ToolChoice = &contract.ToolChoice{Mode: &mode}

			body, _ := translate(t, request)
			if body.ToolChoice == nil || body.ToolChoice.Type != testCase.want {
				t.Fatalf("tool choice %q became %#v, expected type %q", testCase.mode, body.ToolChoice, testCase.want)
			}
			if len(body.Tools) != 1 || body.Tools[0].InputSchema == nil {
				t.Errorf("the tool definition lost its input schema, which this protocol requires: %#v", body.Tools)
			}
		})
	}

	named := baseRequest("req_tool_choice_named")
	named.Tools = []contract.ToolDefinition{{Type: "function", Name: "lookup", Parameters: map[string]any{"type": "object"}}}
	named.ToolChoice = &contract.ToolChoice{Function: &contract.ToolChoiceFunction{Type: "function", Name: "lookup"}}
	body, _ := translate(t, named)
	if body.ToolChoice == nil || body.ToolChoice.Type != "tool" || body.ToolChoice.Name == nil || *body.ToolChoice.Name != "lookup" {
		t.Errorf("a named tool choice became %#v", body.ToolChoice)
	}
}

// TestTheTranslatedCallCarriesNoCredentialAndTheRequiredVersion pins both
// halves of this protocol's authentication.
func TestTheTranslatedCallCarriesNoCredentialAndTheRequiredVersion(t *testing.T) {
	body, call := translate(t, baseRequest("req_headers"))

	if got := call.Header.Get("anthropic-version"); got != apiVersion {
		t.Errorf("the call carries anthropic-version %q, and this protocol requires it on every request", got)
	}
	encoded, err := json.Marshal(call)
	if err != nil {
		t.Fatalf("encoding the call: %v", err)
	}
	if strings.Contains(string(encoded), fakeAPIKey) {
		t.Fatalf("the translated call carries the credential: %s", encoded)
	}
	if body.MaxTokens != 256 {
		t.Errorf("max_tokens is %d; the caller asked for 256", body.MaxTokens)
	}
	if body.Model != testRoute().UpstreamModelID {
		t.Errorf("the body names model %q, and the route resolved %q", body.Model, testRoute().UpstreamModelID)
	}
}

// asUnsupported unwraps a refusal. It is errors.As with a name, kept here so
// each assertion above reads as the thing it is checking.
func asUnsupported(err error, target *provider.ErrUnsupported) bool {
	return errors.As(err, target)
}

// TestAReplayedRefusalIsRefusedBecauseThisProtocolHasNowhereToPutIt is the
// dialect that CANNOT carry the `refusal` content part contract 1.2.0 added.
//
// The messages api has no refusal block: a refusal from the model arrives as
// ordinary `text` with `stop_reason: "refusal"`. So the only way to replay one
// would be to send it as text — which would tell the model that a refusal was the
// assistant's own prose, and it would answer a different conversation while the
// request reported success. `openaicompat` carries it on the message instead.
//
// The refusal is REFUSED with the part named, per this repo's rule that what a
// provider cannot express is refused rather than dropped.
func TestAReplayedRefusalIsRefusedBecauseThisProtocolHasNowhereToPutIt(t *testing.T) {
	refusal := "I can't help with that."
	request := baseRequest("req_replayed_refusal")
	request.Input.Messages = []contract.Message{
		{Role: contract.RoleUser, Content: []contract.ContentPart{textPart("do the thing")}},
		{Role: contract.RoleAssistant, Content: []contract.ContentPart{
			{Type: contract.ContentPartRefusal, Text: &refusal},
		}},
	}

	_, err := newTestAdapter(t).Translate(request, testRoute())
	var unsupported provider.ErrUnsupported
	if !asUnsupported(err, &unsupported) {
		t.Fatalf("a replayed refusal translated to %v; this protocol has no block for one", err)
	}
	if unsupported.Code != contract.CodeUnsupportedModality {
		t.Errorf("refused with %q", unsupported.Code)
	}
	// The reason has to be the real one. "No representation for a %q part" would
	// be true of a dialect that keeps it somewhere else, and only one of those is
	// a request the customer can fix.
	if !strings.Contains(unsupported.Detail, "stop_reason") {
		t.Errorf("the refusal does not say why this protocol cannot carry it: %q", unsupported.Detail)
	}

	// THE CONTROL: the same conversation with an ordinary assistant text turn
	// translates cleanly, so the refusal is about the part type and not about
	// assistant messages or about this fixture.
	control := baseRequest("req_replayed_text")
	control.Input.Messages = []contract.Message{
		{Role: contract.RoleUser, Content: []contract.ContentPart{textPart("do the thing")}},
		{Role: contract.RoleAssistant, Content: []contract.ContentPart{textPart("sure")}},
	}
	if _, err := newTestAdapter(t).Translate(control, testRoute()); err != nil {
		t.Fatalf("an ordinary assistant turn was refused too: %v", err)
	}
}
