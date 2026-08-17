// Package openaicompat adapts the OpenAI Chat Completions wire protocol.
//
// It is a port of Alia's provider adapters
// (`packages/api/src/internal/providers/lib/providers/`), and specifically of
// the `openai` one. That choice is not arbitrary: seven of Alia's adapters —
// openai, together, xai, cerebras, hyperbolic, digitalocean and openrouter —
// are byte-identical apart from a base URL and the word in their error string,
// because they all speak this one protocol. Porting the protocol rather than a
// provider is what makes the next six a configuration line and a conformance
// registration instead of six more files.
//
// What the port deliberately does NOT preserve from Alia:
//
//   - Alia's `proxy()` returned the upstream's raw `ReadableStream` to its
//     caller. There was no normalization, no usage, no cancellation and no
//     error classification — the customer received whatever OpenAI happened to
//     say. All four are the data plane's job under this contract.
//   - Alia never sent `stream_options.include_usage`, so a streamed request
//     reported no usage at all. On a platform that bills from the usage record,
//     a faithful port of that would be a billing hole.
//   - Alia defaulted `temperature: 0.7` and `max_tokens: 8192` when the caller
//     specified none. This port sends neither: the contract says an absent
//     sampling parameter means the route's own default, and inventing one at
//     the adapter silently changes every request nobody configured.
//
// No live provider call has been made from this code. It is exercised against a
// fake upstream that speaks the real wire format.
package openaicompat

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

// Config describes one provider that speaks this protocol.
type Config struct {
	// Provider is the catalogue slug this instance serves.
	Provider contract.ProviderSlug
	// BaseURL is the provider's API root, e.g. https://api.openai.com/v1.
	BaseURL string
	// APIKey is Relay's own credential for this provider. It is read from the
	// process environment, never from a request, never from a file in this
	// repository, and never written to a log, an error or a usage record. An
	// empty key is a supported state: the adapter reports itself unconfigured
	// rather than failing at the first request.
	APIKey string
	// Headers are extra non-secret headers the provider expects. OpenRouter's
	// attribution headers are the reason this exists.
	Headers map[string]string
	// HTTPClient is optional; a nil client uses a default with no global
	// timeout, because the deadline belongs to the request context and a
	// client-level timeout would cut a legitimately long generation.
	HTTPClient *http.Client
}

// Adapter implements provider.Adapter for one OpenAI-compatible provider.
type Adapter struct {
	config Config
	client *http.Client
}

// New builds an adapter, refusing a configuration that could not serve.
func New(config Config) (*Adapter, error) {
	if !config.Provider.Valid() {
		return nil, fmt.Errorf("openaicompat: %q is not a provider slug", config.Provider)
	}
	if config.BaseURL == "" {
		return nil, fmt.Errorf("openaicompat: %s has no base URL", config.Provider)
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	config.BaseURL = strings.TrimSuffix(config.BaseURL, "/")
	return &Adapter{config: config, client: client}, nil
}

// Provider implements provider.Adapter.
func (a *Adapter) Provider() contract.ProviderSlug { return a.config.Provider }

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
			Detail: fmt.Sprintf("chat completions produces text; %q needs a different upstream endpoint", request.Modality),
		}
	}
	if request.Input.Format != contract.InputMessages {
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeUnsupportedModality,
			Param:  "input.format",
			Detail: fmt.Sprintf("chat completions takes a conversation; %q is an embedding input", request.Input.Format),
		}
	}

	messages := make([]chatMessage, 0, len(request.Input.Messages))
	for index, message := range request.Input.Messages {
		translated, err := translateMessage(message)
		if err != nil {
			return nil, annotate(err, fmt.Sprintf("input.messages[%d]", index))
		}
		messages = append(messages, translated)
	}

	body := chatRequest{
		Model:            route.UpstreamModelID,
		Messages:         messages,
		Stream:           request.Stream,
		MaxTokens:        request.MaxOutputTokens,
		Temperature:      request.Sampling.Temperature,
		TopP:             request.Sampling.TopP,
		FrequencyPenalty: request.Sampling.FrequencyPenalty,
		PresencePenalty:  request.Sampling.PresencePenalty,
		Seed:             request.Sampling.Seed,
		Stop:             request.Sampling.StopSequences,
	}
	if request.Stream {
		body.StreamOptions = &streamOptions{IncludeUsage: true}
	}
	if request.Sampling.TopK != nil {
		// top_k has no representation in this protocol. Dropping it silently
		// would change what the model does while reporting success, so the
		// request is refused with the field named.
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeInvalidRequest,
			Param:  "sampling.topK",
			Detail: "chat completions has no top_k parameter",
		}
	}
	for _, tool := range request.Tools {
		body.Tools = append(body.Tools, chatTool{
			Type: "function",
			Function: chatFunction{
				Name:        tool.Name,
				Description: tool.Description,
				Parameters:  tool.Parameters,
				Strict:      tool.Strict,
			},
		})
	}
	if choice := request.ToolChoice; choice != nil {
		switch {
		case choice.Mode != nil:
			body.ToolChoice = string(*choice.Mode)
		case choice.Function != nil:
			body.ToolChoice = map[string]any{
				"type":     "function",
				"function": map[string]any{"name": choice.Function.Name},
			}
		}
	}
	if format := request.ResponseFormat; format != nil {
		translated, err := translateResponseFormat(*format)
		if err != nil {
			return nil, err
		}
		body.ResponseFormat = translated
	}

	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("openaicompat: encoding the upstream request: %w", err)
	}

	header := http.Header{}
	header.Set("Content-Type", "application/json")
	header.Set("Accept", map[bool]string{true: "text/event-stream", false: "application/json"}[request.Stream])
	for name, value := range a.config.Headers {
		header.Set(name, value)
	}

	return &provider.Call{
		Route:  route,
		Method: http.MethodPost,
		URL:    a.config.BaseURL + "/chat/completions",
		Body:   encoded,
		Header: header,
		Stream: request.Stream,
	}, nil
}

// Health implements provider.Adapter.
//
// The probe is the provider's own model listing: it is the cheapest call that
// proves both reachability and that the configured credential is accepted,
// which are the two things a route decision turns on.
func (a *Adapter) Health(ctx context.Context) provider.Health {
	health := provider.Health{
		Provider:  a.config.Provider,
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
		health.Detail = contract.SafeErrorText(err.Error())
		return health
	}
	a.authorize(request)

	startedAt := time.Now()
	response, err := a.client.Do(request)
	if err != nil {
		health.Status = provider.HealthUnavailable
		health.Detail = contract.SafeErrorText(err.Error())
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
func (a *Adapter) authorize(request *http.Request) {
	if a.config.APIKey != "" {
		request.Header.Set("Authorization", "Bearer "+a.config.APIKey)
	}
}

func (a *Adapter) send(ctx context.Context, call *provider.Call) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, call.Method, call.URL, bytes.NewReader(call.Body))
	if err != nil {
		return nil, fmt.Errorf("openaicompat: building the upstream request: %w", err)
	}
	request.Header = call.Header.Clone()
	a.authorize(request)
	return a.client.Do(request)
}

/* -------------------------------------------------------------------------- */
/*  Message translation                                                       */
/* -------------------------------------------------------------------------- */

func translateMessage(message contract.Message) (chatMessage, error) {
	translated := chatMessage{
		Role:       string(message.Role),
		Name:       message.Name,
		ToolCallID: message.ToolCallID,
	}
	for _, call := range message.ToolCalls {
		translated.ToolCalls = append(translated.ToolCalls, chatToolCall{
			ID:       call.ID,
			Type:     "function",
			Function: chatCallFunction{Name: call.Name, Arguments: call.Arguments},
		})
	}

	// A refusal is lifted out of the content list and onto the message, because
	// that is where this protocol keeps one — the same field Relay reads a
	// refusal FROM on the way back. The contract models it as a content part
	// (`inferenceContentPartSchema`'s `refusal` variant, contract 1.2.0), so
	// replaying a conversation that contains one is only possible if the two
	// shapes are reconciled here rather than at the customer.
	//
	// Dropping it instead would change the conversation the model sees while
	// reporting success: an assistant turn that declined would come back as an
	// assistant turn that said nothing.
	remaining := make([]contract.ContentPart, 0, len(message.Content))
	for _, part := range message.Content {
		if part.Type != contract.ContentPartRefusal {
			remaining = append(remaining, part)
			continue
		}
		if translated.Refusal != nil {
			// Two refusals in one message have no representation: the field holds
			// one string, and joining them would invent a refusal the model never
			// produced.
			return chatMessage{}, provider.ErrUnsupported{
				Code:   contract.CodeUnsupportedModality,
				Detail: "chat completions carries one refusal per message; this message has two",
			}
		}
		refusal := derefString(part.Text)
		translated.Refusal = &refusal
	}

	// A single text part becomes a plain string rather than a one-element
	// array. Several providers claiming OpenAI compatibility accept only the
	// string form for system and tool messages, and the array form is the one
	// that fails on them.
	if len(remaining) == 1 && remaining[0].Type == contract.ContentPartText {
		translated.Content = derefString(remaining[0].Text)
		return translated, nil
	}

	parts := make([]any, 0, len(remaining))
	for index, part := range remaining {
		converted, err := translateContentPart(part)
		if err != nil {
			return chatMessage{}, annotate(err, fmt.Sprintf("content[%d]", index))
		}
		parts = append(parts, converted)
	}
	if len(parts) > 0 {
		translated.Content = parts
	}
	return translated, nil
}

func translateContentPart(part contract.ContentPart) (any, error) {
	switch part.Type {
	case contract.ContentPartText:
		return textPart{Type: "text", Text: derefString(part.Text)}, nil

	case contract.ContentPartImage:
		url, err := contentURL(part.Source)
		if err != nil {
			return nil, err
		}
		spec := imageURLSpec{URL: url}
		if part.Detail != nil {
			detail := string(*part.Detail)
			spec.Detail = &detail
		}
		return imagePart{Type: "image_url", ImageURL: spec}, nil

	case contract.ContentPartAudio:
		if part.Source.Kind != contract.ContentSourceInline {
			// The protocol takes audio as inline base64 only. Fetching the URL
			// here would make Relay the one that downloads customer content,
			// which is a data-handling decision nobody has made.
			return nil, provider.ErrUnsupported{
				Code:   contract.CodeUnsupportedModality,
				Detail: "chat completions takes audio inline; a url source would require Relay to fetch customer content",
			}
		}
		format, err := audioFormat(derefString(part.Source.MediaType))
		if err != nil {
			return nil, err
		}
		return audioPart{Type: "input_audio", InputAudio: inputAudioRef{
			Data:   derefString(part.Source.Data),
			Format: format,
		}}, nil

	case contract.ContentPartFile:
		if part.Source.Kind != contract.ContentSourceInline {
			return nil, provider.ErrUnsupported{
				Code:   contract.CodeUnsupportedModality,
				Detail: "chat completions takes files inline; a url source would require Relay to fetch customer content",
			}
		}
		return filePart{Type: "file", File: fileRefSpec{
			FileData: dataURI(derefString(part.Source.MediaType), derefString(part.Source.Data)),
			Filename: part.Filename,
		}}, nil

	case contract.ContentPartRefusal:
		// Unreachable through `translateMessage`, which lifts a refusal onto the
		// message before this runs. Named rather than left to the default arm so
		// the reason is the real one — this protocol HAS a place for a refusal,
		// just not among the content parts.
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeUnsupportedModality,
			Detail: "chat completions carries a refusal on the message, not as a content part",
		}

	default:
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeUnsupportedModality,
			Detail: fmt.Sprintf("chat completions has no representation for a %q part", part.Type),
		}
	}
}

func contentURL(source *contract.ContentSource) (string, error) {
	switch source.Kind {
	case contract.ContentSourceURL:
		return derefString(source.URL), nil
	case contract.ContentSourceInline:
		return dataURI(derefString(source.MediaType), derefString(source.Data)), nil
	default:
		return "", provider.ErrUnsupported{
			Code:   contract.CodeInvalidRequest,
			Detail: fmt.Sprintf("%q is not a content source kind", source.Kind),
		}
	}
}

func dataURI(mediaType, data string) string {
	return "data:" + mediaType + ";base64," + data
}

// audioFormat maps a media type to the protocol's short format name. The
// protocol takes a format rather than a media type, and an unrecognised one is
// refused rather than guessed: a wrong format is decoded as noise and billed as
// audio.
func audioFormat(mediaType string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(strings.SplitN(mediaType, ";", 2)[0])) {
	case "audio/wav", "audio/wave", "audio/x-wav":
		return "wav", nil
	case "audio/mpeg", "audio/mp3":
		return "mp3", nil
	default:
		return "", provider.ErrUnsupported{
			Code:   contract.CodeUnsupportedModality,
			Detail: fmt.Sprintf("chat completions accepts wav or mp3 audio; %q is neither", mediaType),
		}
	}
}

func translateResponseFormat(format contract.ResponseFormat) (*responseFormat, error) {
	switch format.Type {
	case contract.ResponseFormatText:
		return nil, nil
	case contract.ResponseFormatJSONObject:
		return &responseFormat{Type: "json_object"}, nil
	case contract.ResponseFormatJSONSchema:
		if format.Name == nil || format.Schema == nil {
			return nil, provider.ErrUnsupported{
				Code:   contract.CodeInvalidRequest,
				Param:  "responseFormat",
				Detail: "a json_schema response format needs a name and a schema",
			}
		}
		return &responseFormat{
			Type: "json_schema",
			JSONSchema: &jsonSchemaSpec{
				Name:   *format.Name,
				Schema: format.Schema,
				Strict: format.Strict,
			},
		}, nil
	default:
		return nil, provider.ErrUnsupported{
			Code:   contract.CodeInvalidRequest,
			Param:  "responseFormat.type",
			Detail: fmt.Sprintf("%q is not a response format", format.Type),
		}
	}
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
