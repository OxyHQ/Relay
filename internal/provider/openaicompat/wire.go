package openaicompat

// The upstream wire shapes, exactly as the OpenAI Chat Completions API defines
// them. They are separate from the contract types on purpose: this file is the
// only place in Relay that knows what a provider's JSON looks like, and the
// moment a provider's field name leaks past the adapter the boundary is gone.
//
// Every field is a pointer or a slice where the upstream may omit it, because
// the difference between "zero tokens" and "no usage reported" is the
// difference between a settled receipt and an estimated one.

type chatRequest struct {
	Model         string         `json:"model"`
	Messages      []chatMessage  `json:"messages"`
	Stream        bool           `json:"stream"`
	StreamOptions *streamOptions `json:"stream_options,omitempty"`

	MaxTokens        *int     `json:"max_tokens,omitempty"`
	Temperature      *float64 `json:"temperature,omitempty"`
	TopP             *float64 `json:"top_p,omitempty"`
	FrequencyPenalty *float64 `json:"frequency_penalty,omitempty"`
	PresencePenalty  *float64 `json:"presence_penalty,omitempty"`
	Seed             *int     `json:"seed,omitempty"`
	Stop             []string `json:"stop,omitempty"`

	Tools          []chatTool      `json:"tools,omitempty"`
	ToolChoice     any             `json:"tool_choice,omitempty"`
	ResponseFormat *responseFormat `json:"response_format,omitempty"`
}

// streamOptions is what makes a streamed request report usage at all.
//
// Alia's adapters never sent it, which is why the code this is ported from
// produced no usage for any streamed request. On a platform where the stream IS
// the product and the usage record is what a customer is charged from, that is
// not a missing nicety.
type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content,omitempty"`
	// Refusal is where this protocol puts an assistant's refusal, on the MESSAGE
	// rather than among its content parts — which is also where Relay READS one
	// from on the way back (`completionMessage.Refusal`). The contract carries it
	// as a content part, so the translation is structural; see
	// `translateMessage`.
	Refusal    *string        `json:"refusal,omitempty"`
	Name       *string        `json:"name,omitempty"`
	ToolCallID *string        `json:"tool_call_id,omitempty"`
	ToolCalls  []chatToolCall `json:"tool_calls,omitempty"`
}

type chatToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function chatCallFunction `json:"function"`
}

type chatCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type chatTool struct {
	Type     string       `json:"type"`
	Function chatFunction `json:"function"`
}

type chatFunction struct {
	Name        string         `json:"name"`
	Description *string        `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
	Strict      *bool          `json:"strict,omitempty"`
}

type responseFormat struct {
	Type       string          `json:"type"`
	JSONSchema *jsonSchemaSpec `json:"json_schema,omitempty"`
}

type jsonSchemaSpec struct {
	Name   string         `json:"name"`
	Schema map[string]any `json:"schema"`
	Strict *bool          `json:"strict,omitempty"`
}

type textPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type imagePart struct {
	Type     string       `json:"type"`
	ImageURL imageURLSpec `json:"image_url"`
}

type imageURLSpec struct {
	URL    string  `json:"url"`
	Detail *string `json:"detail,omitempty"`
}

type audioPart struct {
	Type       string        `json:"type"`
	InputAudio inputAudioRef `json:"input_audio"`
}

type inputAudioRef struct {
	Data   string `json:"data"`
	Format string `json:"format"`
}

type filePart struct {
	Type string      `json:"type"`
	File fileRefSpec `json:"file"`
}

type fileRefSpec struct {
	FileData string  `json:"file_data"`
	Filename *string `json:"filename,omitempty"`
}

/* -------------------------------------------------------------------------- */
/*  Responses                                                                 */
/* -------------------------------------------------------------------------- */

// chatChunk is one streamed frame.
//
// Error is on the frame because this protocol can fail AFTER a 200: the
// upstream opens the stream, writes some output, and then writes an error
// object where a chunk belongs. There is no status left to carry that failure,
// so a decoder that ignored the field would report a truncated answer as a
// completed one.
type chatChunk struct {
	Choices []streamChoice `json:"choices"`
	Usage   *chatUsage     `json:"usage"`
	Error   *upstreamError `json:"error"`
}

type streamChoice struct {
	Index        int         `json:"index"`
	Delta        streamDelta `json:"delta"`
	FinishReason *string     `json:"finish_reason"`
}

// streamDelta covers the fields the OpenAI-compatible providers actually emit.
//
// `reasoning_content` and `reasoning` are the two spellings in use for
// reasoning output. Both are read, because a provider whose reasoning arrives
// under the unread spelling would have it rendered as answer text — a product
// bug, not a display preference.
type streamDelta struct {
	Content          *string             `json:"content"`
	Refusal          *string             `json:"refusal"`
	ReasoningContent *string             `json:"reasoning_content"`
	Reasoning        *string             `json:"reasoning"`
	ToolCalls        []streamToolCallRef `json:"tool_calls"`
}

type streamToolCallRef struct {
	Index    int                 `json:"index"`
	ID       *string             `json:"id"`
	Type     *string             `json:"type"`
	Function *streamCallFunction `json:"function"`
}

type streamCallFunction struct {
	Name      *string `json:"name"`
	Arguments *string `json:"arguments"`
}

// chatCompletion is a non-streamed response.
type chatCompletion struct {
	Choices []completionChoice `json:"choices"`
	Usage   *chatUsage         `json:"usage"`
}

type completionChoice struct {
	Index        int               `json:"index"`
	Message      completionMessage `json:"message"`
	FinishReason *string           `json:"finish_reason"`
}

type completionMessage struct {
	Content          *string        `json:"content"`
	Refusal          *string        `json:"refusal"`
	ReasoningContent *string        `json:"reasoning_content"`
	ToolCalls        []chatToolCall `json:"tool_calls"`
}

type chatUsage struct {
	PromptTokens        *int                  `json:"prompt_tokens"`
	CompletionTokens    *int                  `json:"completion_tokens"`
	PromptTokensDetails *promptTokensDetails  `json:"prompt_tokens_details"`
	CompletionDetails   *completionTokensInfo `json:"completion_tokens_details"`
}

type promptTokensDetails struct {
	CachedTokens *int `json:"cached_tokens"`
}

type completionTokensInfo struct {
	ReasoningTokens *int `json:"reasoning_tokens"`
}

// upstreamErrorBody is the error envelope every OpenAI-compatible provider
// returns, over HTTP and inside the stream alike.
type upstreamErrorBody struct {
	Error upstreamError `json:"error"`
}

type upstreamError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    any    `json:"code"`
}
