// Package anthropic adapts the Anthropic Messages API.
//
// It is the second provider adapter in this repository, and it exists to answer
// a question one adapter cannot: whether provider.Adapter describes a PROVIDER
// or merely describes the first one written against it. Anthropic was chosen
// because it disagrees with the OpenAI Chat Completions protocol on every axis
// the interface names:
//
//   - Named SSE events (`message_start`, `content_block_delta`, `message_stop`)
//     rather than one repeated frame shape closed by a `[DONE]` sentinel.
//   - Output arrives as indexed content BLOCKS with their own start and stop
//     events, and reasoning is a block type rather than a field on a delta.
//   - Usage is split across two events and the output count is CUMULATIVE, so
//     an adapter that adds the numbers it sees over-reports the request.
//   - Its usage fields nest the other way round: `input_tokens` EXCLUDES cached
//     tokens where an OpenAI-compatible `prompt_tokens` includes them, while
//     `output_tokens` includes reasoning exactly as `completion_tokens` does.
//   - A failure can arrive INSIDE the stream, after a 200, as an `error` event.
//   - Authentication is `x-api-key` with a mandatory `anthropic-version`, not a
//     bearer token.
//   - The system prompt is hoisted out of the message list, a tool result is a
//     user message rather than a role, and `max_tokens` is REQUIRED.
//
// It is a port of Alia's `anthropic` provider
// (`packages/api/src/internal/providers/lib/providers/anthropic.ts`), which
// converted OpenAI-shaped messages into this protocol and then converted the
// response stream back into OpenAI-shaped chunks. What the port deliberately
// does NOT preserve:
//
//   - Alia's conversion read only `content_block_delta` with a text delta and
//     `message_stop`. Tool calls, reasoning, stop reasons and the whole of
//     `usage` were dropped on the floor, so a request that called a tool
//     produced no tool call downstream and every request reported no usage at
//     all. On a platform that bills from the usage record that is a billing
//     hole, not a missing nicety.
//   - Alia defaulted `max_tokens: 8192` and `temperature: 0.7` when the caller
//     set neither. This port sends neither: the contract says an absent
//     sampling parameter means the route's own default, and this protocol makes
//     `max_tokens` mandatory — so a request that omits `maxOutputTokens` is
//     REFUSED with the field named rather than silently capped at a number
//     nobody asked for.
//   - Alia forced `stream: true` and returned the raw upstream stream to its
//     caller, with no normalization, no cancellation and no error
//     classification.
//
// No live provider call has been made from this code. There is no Anthropic
// credential in this repository, in its tests, or in CI; the adapter is
// exercised against a fake upstream that speaks the real wire format.
package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/OxyHQ/Relay/internal/contract"
	"github.com/OxyHQ/Relay/internal/provider"
)

// apiVersion is the Messages API version this adapter is written against. The
// provider requires the header on every request and uses it to keep response
// shapes stable, so it is pinned here beside the code that parses them rather
// than being configurable: a version somebody changed in an environment
// variable would change the JSON this package decodes.
const apiVersion = "2023-06-01"

// Config describes the provider.
type Config struct {
	// BaseURL is the API root, e.g. https://api.anthropic.com/v1.
	BaseURL string
	// APIKey is Relay's own credential. It is read from the process
	// environment, never from a request, never from a file in this repository,
	// and never written to a log, an error or a usage record. An empty key is a
	// supported state: the adapter reports itself unconfigured rather than
	// failing at the first request.
	APIKey string
	// HTTPClient is optional; a nil client uses a default with no global
	// timeout, because the deadline belongs to the request context and a
	// client-level timeout would cut a legitimately long generation.
	HTTPClient *http.Client
}

// Adapter implements provider.Adapter for the Anthropic Messages API.
type Adapter struct {
	config Config
	client *http.Client
}

// Slug is the catalogue slug this adapter serves. It is a constant rather than
// configuration: unlike the OpenAI Chat Completions protocol, which seven
// providers speak, this wire format belongs to one provider.
const Slug contract.ProviderSlug = "anthropic"

// New builds an adapter, refusing a configuration that could not serve.
func New(config Config) (*Adapter, error) {
	if config.BaseURL == "" {
		return nil, fmt.Errorf("anthropic: no base URL")
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	config.BaseURL = strings.TrimSuffix(config.BaseURL, "/")
	return &Adapter{config: config, client: client}, nil
}

// Provider implements provider.Adapter.
func (a *Adapter) Provider() contract.ProviderSlug { return Slug }

// Translate implements provider.Adapter.
//
// Every refusal below happens before a single byte is sent upstream, which is
// the whole reason translation is a separate, pure method: a request this
// protocol cannot express must cost nothing.
func (a *Adapter) Translate(request *contract.Request, route provider.Route) (*provider.Call, error) {
	if request.Modality != contract.ModalityText {
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeUnsupportedModality,
			Param:  "modality",
			Detail: fmt.Sprintf("the messages api produces text; %q needs a different upstream endpoint", request.Modality),
		}
	}
	if request.Input.Format != contract.InputMessages {
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeUnsupportedModality,
			Param:  "input.format",
			Detail: fmt.Sprintf("the messages api takes a conversation; %q is an embedding input", request.Input.Format),
		}
	}
	if request.MaxOutputTokens == nil {
		// The contract makes maxOutputTokens optional and this protocol makes
		// max_tokens mandatory, so there is no request to send. The alternatives
		// are both worse than refusing: a number chosen here silently truncates
		// an answer the customer asked to be unbounded, and one chosen per
		// deployment moves the same invention into a configuration file where
		// nobody reads it. See README, "What Oxy still has to decide".
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeInvalidRequest,
			Param:  "maxOutputTokens",
			Detail: "the messages api requires an output token limit on every request, and choosing one here would cap an answer the caller did not ask to cap",
		}
	}

	system, messages, err := translateConversation(request.Input.Messages)
	if err != nil {
		return nil, err
	}

	body := messagesRequest{
		Model:         route.UpstreamModelID,
		Messages:      messages,
		MaxTokens:     *request.MaxOutputTokens,
		Stream:        request.Stream,
		System:        system,
		Temperature:   request.Sampling.Temperature,
		TopP:          request.Sampling.TopP,
		TopK:          request.Sampling.TopK,
		StopSequences: request.Sampling.StopSequences,
	}
	if request.Sampling.FrequencyPenalty != nil {
		// Dropping it silently would change what the model does while reporting
		// success, which is the one thing translation must never do.
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeInvalidRequest,
			Param:  "sampling.frequencyPenalty",
			Detail: "the messages api has no frequency penalty",
		}
	}
	if request.Sampling.PresencePenalty != nil {
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeInvalidRequest,
			Param:  "sampling.presencePenalty",
			Detail: "the messages api has no presence penalty",
		}
	}
	if request.Sampling.Seed != nil {
		// A seed the provider ignores is worse than no seed: the caller asked
		// for reproducibility and would be told they had it.
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeInvalidRequest,
			Param:  "sampling.seed",
			Detail: "the messages api has no seed parameter, so a reproducible sample cannot be requested",
		}
	}
	if request.ResponseFormat != nil && request.ResponseFormat.Type != contract.ResponseFormatText {
		// The protocol has structured outputs, but under a shape this adapter
		// has no way to verify against a live provider. Refusing names the
		// field; translating it half-way would produce free text where the
		// caller's parser expects JSON.
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeInvalidRequest,
			Param:  "responseFormat.type",
			Detail: fmt.Sprintf("this build does not translate a %q response format for the messages api", request.ResponseFormat.Type),
		}
	}

	for _, tool := range request.Tools {
		schema := tool.Parameters
		if schema == nil {
			// The provider requires an input schema on every tool. An empty
			// object is not an invention: it is the same statement the contract
			// makes by carrying no parameters, spelled the way this protocol
			// spells it.
			schema = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		body.Tools = append(body.Tools, toolParam{
			Name:        tool.Name,
			Description: tool.Description,
			InputSchema: schema,
		})
	}
	if choice := request.ToolChoice; choice != nil {
		translated, err := translateToolChoice(*choice)
		if err != nil {
			return nil, err
		}
		body.ToolChoice = translated
	}

	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("anthropic: encoding the upstream request: %w", err)
	}

	header := http.Header{}
	header.Set("Content-Type", "application/json")
	header.Set("Accept", map[bool]string{true: "text/event-stream", false: "application/json"}[request.Stream])
	header.Set("anthropic-version", apiVersion)

	return &provider.Call{
		Route:  route,
		Method: http.MethodPost,
		URL:    a.config.BaseURL + "/messages",
		Body:   encoded,
		Header: header,
		Stream: request.Stream,
	}, nil
}

// Health implements provider.Adapter.
//
// The probe is the provider's own model listing: the cheapest call that proves
// both reachability and that the configured credential is accepted, which are
// the two things a route decision turns on.
func (a *Adapter) Health(ctx context.Context) provider.Health {
	health := provider.Health{
		Provider:  Slug,
		CheckedAt: contract.NewTimestamp(time.Now()),
	}
	if a.config.APIKey == "" {
		// Distinct from unavailable on purpose: an operator reading
		// "unavailable" goes looking at the provider, and the answer is here.
		health.Status = provider.HealthUnconfigured
		health.Detail = "no credential is configured for this provider"
		return health
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, a.config.BaseURL+"/models", nil)
	if err != nil {
		health.Status = provider.HealthUnavailable
		health.Detail = a.safeText(err.Error())
		return health
	}
	a.authorize(request)

	startedAt := time.Now()
	response, err := a.client.Do(request)
	if err != nil {
		health.Status = provider.HealthUnavailable
		health.Detail = a.safeText(err.Error())
		return health
	}
	defer func() { _ = response.Body.Close() }()

	latency := int(time.Since(startedAt).Milliseconds())
	health.LatencyMs = &latency
	switch {
	case response.StatusCode >= 200 && response.StatusCode < 300:
		health.Status = provider.HealthOK
	case response.StatusCode == http.StatusTooManyRequests, response.StatusCode >= 500:
		health.Status = provider.HealthDegraded
		health.Detail = fmt.Sprintf("the provider answered %d", response.StatusCode)
	default:
		health.Status = provider.HealthUnavailable
		health.Detail = fmt.Sprintf("the provider answered %d", response.StatusCode)
	}
	return health
}

// authorize applies the credential. It is the only method that touches it, and
// it is deliberately not part of Call: a translated call that carried a
// credential would be one struct away from a debug log.
//
// The header is `x-api-key` rather than `Authorization`, which is not a detail
// — see safeText.
func (a *Adapter) authorize(request *http.Request) {
	if a.config.APIKey != "" {
		request.Header.Set("x-api-key", a.config.APIKey)
	}
	request.Header.Set("anthropic-version", apiVersion)
}

// safeText renders upstream text for a customer.
//
// It removes THIS adapter's own credential by exact match before applying the
// contract's shape-based redaction, and that order matters. The contract's
// pattern refuses `authorization:` and `bearer <token>` and `sk-…`; against a
// provider that authenticates with `x-api-key`, an echoed header matches the
// marker and the VALUE beside it matches nothing, so redacting alone would
// leave the credential in a message the contract then accepts. An exact match
// against the key the adapter is holding cannot miss for that reason.
func (a *Adapter) safeText(text string) string {
	return contract.SafeErrorText(provider.RedactSecret(text, a.config.APIKey))
}

func (a *Adapter) send(ctx context.Context, call *provider.Call) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, call.Method, call.URL, bytes.NewReader(call.Body))
	if err != nil {
		return nil, fmt.Errorf("anthropic: building the upstream request: %w", err)
	}
	request.Header = call.Header.Clone()
	a.authorize(request)
	return a.client.Do(request)
}

/* -------------------------------------------------------------------------- */
/*  Conversation translation                                                  */
/* -------------------------------------------------------------------------- */

// translateConversation splits the normalized message list the way this
// protocol wants it: a system prompt of its own, and messages that alternate
// user and assistant with tool results carried as user content.
//
// Two structural differences from a chat-completions body are handled here
// rather than being refused, because both are expressible:
//
//   - `system` and `developer` messages leave the list entirely. They are
//     concatenated in order, so a conversation that set instructions in two
//     places keeps both.
//   - A `tool` message becomes a USER message carrying a `tool_result` block.
//     This protocol has no tool role, and a tool result sent as anything else
//     is a 400 the customer would see as their own fault.
func translateConversation(messages []contract.Message) ([]systemBlock, []messageParam, error) {
	var (
		system    []systemBlock
		converted []messageParam
	)
	for index, message := range messages {
		switch message.Role {
		case contract.RoleSystem, contract.RoleDeveloper:
			text := plainText(message.Content)
			if text == "" {
				continue
			}
			system = append(system, systemBlock{Type: "text", Text: text})

		case contract.RoleTool:
			content, err := translateParts(message.Content)
			if err != nil {
				return nil, nil, annotate(err, fmt.Sprintf("input.messages[%d]", index))
			}
			converted = append(converted, messageParam{
				Role: "user",
				Content: []any{toolResultBlockParam{
					Type:      "tool_result",
					ToolUseID: derefString(message.ToolCallID),
					Content:   content,
				}},
			})

		case contract.RoleUser, contract.RoleAssistant:
			content, err := translateParts(message.Content)
			if err != nil {
				return nil, nil, annotate(err, fmt.Sprintf("input.messages[%d]", index))
			}
			for callIndex, call := range message.ToolCalls {
				translated, err := translateToolCall(call)
				if err != nil {
					return nil, nil, annotate(err, fmt.Sprintf("input.messages[%d].toolCalls[%d]", index, callIndex))
				}
				content = append(content, translated)
			}
			if len(content) == 0 {
				// This protocol rejects an empty content list. A message that
				// carried nothing translatable is dropped rather than sent as
				// an empty one, which would fail the whole request over a part
				// that said nothing.
				continue
			}
			converted = append(converted, messageParam{Role: string(message.Role), Content: content})

		default:
			return nil, nil, provider.ErrUnsupported{
				Code:   contract.CodeInvalidRequest,
				Param:  fmt.Sprintf("input.messages[%d].role", index),
				Detail: fmt.Sprintf("%q is not a role this protocol expresses", message.Role),
			}
		}
	}
	if len(converted) == 0 {
		return nil, nil, provider.ErrUnsupported{
			Code:   contract.CodeInvalidRequest,
			Param:  "input.messages",
			Detail: "the messages api needs at least one user or assistant message; this request carried only instructions",
		}
	}
	return system, converted, nil
}

func translateParts(parts []contract.ContentPart) ([]any, error) {
	converted := make([]any, 0, len(parts))
	for index, part := range parts {
		translated, err := translateContentPart(part)
		if err != nil {
			return nil, annotate(err, fmt.Sprintf("content[%d]", index))
		}
		converted = append(converted, translated)
	}
	return converted, nil
}

func translateContentPart(part contract.ContentPart) (any, error) {
	switch part.Type {
	case contract.ContentPartText:
		return textBlockParam{Type: "text", Text: derefString(part.Text)}, nil

	case contract.ContentPartImage:
		source, err := imageSourceFor(part.Source)
		if err != nil {
			return nil, err
		}
		return imageBlockParam{Type: "image", Source: source}, nil

	case contract.ContentPartFile:
		source, err := documentSourceFor(part.Source)
		if err != nil {
			return nil, err
		}
		return documentBlockParam{Type: "document", Source: source, Title: part.Filename}, nil

	case contract.ContentPartAudio:
		// The messages api has no audio content block. Transcribing it here
		// would make Relay decide what the customer's audio says.
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeUnsupportedModality,
			Detail: "the messages api has no audio content block",
		}

	case contract.ContentPartRefusal:
		// REFUSED, and this is the dialect that cannot carry it. The messages api
		// has no refusal block: a refusal from the model arrives as ordinary
		// `text` with `stop_reason: "refusal"`, so the only way to replay one
		// would be to send it as text — which would tell the model that Oxy's
		// refusal was the assistant's own prose, and it would answer differently.
		//
		// Named rather than left to the default arm, because "this protocol has
		// no place for it" and "this protocol keeps it somewhere else" are
		// different answers and only one of them is a request the customer can
		// fix. `openaicompat` is the dialect that carries it, on the message.
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeUnsupportedModality,
			Detail: "the messages api has no refusal content block; a refusal arrives as text with stop_reason refusal, so replaying one as text would change what the model is told",
		}

	default:
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeUnsupportedModality,
			Detail: fmt.Sprintf("the messages api has no representation for a %q part", part.Type),
		}
	}
}

func imageSourceFor(source *contract.ContentSource) (imageSource, error) {
	switch source.Kind {
	case contract.ContentSourceURL:
		return imageSource{Type: "url", URL: derefString(source.URL)}, nil
	case contract.ContentSourceInline:
		return imageSource{
			Type:      "base64",
			MediaType: derefString(source.MediaType),
			Data:      derefString(source.Data),
		}, nil
	default:
		return imageSource{}, provider.ErrUnsupported{
			Code:   contract.CodeInvalidRequest,
			Detail: fmt.Sprintf("%q is not a content source kind", source.Kind),
		}
	}
}

func documentSourceFor(source *contract.ContentSource) (documentSource, error) {
	switch source.Kind {
	case contract.ContentSourceURL:
		return documentSource{Type: "url", URL: derefString(source.URL)}, nil
	case contract.ContentSourceInline:
		return documentSource{
			Type:      "base64",
			MediaType: derefString(source.MediaType),
			Data:      derefString(source.Data),
		}, nil
	default:
		return documentSource{}, provider.ErrUnsupported{
			Code:   contract.CodeInvalidRequest,
			Detail: fmt.Sprintf("%q is not a content source kind", source.Kind),
		}
	}
}

// translateToolCall converts an assistant's earlier tool call back into the
// provider's shape.
//
// The contract carries a tool call's arguments as the JSON TEXT the model
// emitted, deliberately: models emit invalid JSON often enough that parsing it
// at the boundary would turn a recoverable model mistake into a rejected
// message. This protocol takes a parsed object, so the parse has to happen
// somewhere — and it happens here, where it can be refused with the field named
// rather than producing an upstream 400 the customer reads as their own fault.
func translateToolCall(call contract.ToolCall) (toolUseBlockParam, error) {
	input := map[string]any{}
	if trimmed := strings.TrimSpace(call.Arguments); trimmed != "" {
		if err := json.Unmarshal([]byte(trimmed), &input); err != nil {
			return toolUseBlockParam{}, provider.ErrUnsupported{
				Code:   contract.CodeInvalidRequest,
				Param:  "arguments",
				Detail: "the messages api takes a tool call's arguments as an object, and these are not a JSON object",
			}
		}
	}
	return toolUseBlockParam{Type: "tool_use", ID: call.ID, Name: call.Name, Input: input}, nil
}

func translateToolChoice(choice contract.ToolChoice) (*toolChoiceParam, error) {
	switch {
	case choice.Function != nil:
		name := choice.Function.Name
		return &toolChoiceParam{Type: "tool", Name: &name}, nil
	case choice.Mode == nil:
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeInvalidRequest,
			Param:  "toolChoice",
			Detail: "a tool choice names neither a mode nor a function",
		}
	}
	switch *choice.Mode {
	case contract.ToolChoiceAuto:
		return &toolChoiceParam{Type: "auto"}, nil
	case contract.ToolChoiceNone:
		return &toolChoiceParam{Type: "none"}, nil
	case contract.ToolChoiceRequired:
		// This protocol spells "you must call one of them" as `any`.
		return &toolChoiceParam{Type: "any"}, nil
	default:
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeInvalidRequest,
			Param:  "toolChoice",
			Detail: fmt.Sprintf("%q is not a tool-choice mode", *choice.Mode),
		}
	}
}

// plainText joins a message's text parts. Used only for the system prompt,
// which this protocol carries as text blocks and nothing else.
func plainText(parts []contract.ContentPart) string {
	var builder strings.Builder
	for _, part := range parts {
		if part.Type != contract.ContentPartText {
			continue
		}
		if builder.Len() > 0 {
			builder.WriteString("\n")
		}
		builder.WriteString(derefString(part.Text))
	}
	return builder.String()
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// annotate adds the request path to an unsupported-request refusal, so the
// customer is told which message or part could not be expressed rather than
// that "the request" could not be.
func annotate(err error, path string) error {
	var unsupported provider.ErrUnsupported
	// errors.As rather than a type assertion: a refusal that has been wrapped
	// on its way up would otherwise lose its path annotation and be reported to
	// the customer as "the request" rather than as the part at fault.
	if !errors.As(err, &unsupported) {
		return err
	}
	if unsupported.Param == "" {
		unsupported.Param = path
	} else {
		unsupported.Param = path + "." + unsupported.Param
	}
	return unsupported
}
