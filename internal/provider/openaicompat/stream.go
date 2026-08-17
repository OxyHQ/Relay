package openaicompat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/OxyHQ/Relay/internal/contract"
	"github.com/OxyHQ/Relay/internal/provider"
	"github.com/OxyHQ/Relay/internal/sse"
)

// doneSentinel is how this protocol marks the end of a stream. It is not JSON,
// so a decoder that parsed every frame would fail on the last one.
const doneSentinel = "[DONE]"

// Stream implements provider.Adapter.
//
// Cancellation is not a courtesy here. The context is handed to the HTTP
// request, so cancelling it tears down the upstream connection — an adapter
// physically cannot ignore it and still make the call. What this method is
// responsible for is the other half: returning the units it measured up to the
// moment it was cut off, because a partial stream is a settlement case and a
// refund can only be exact if the measurement is.
func (a *Adapter) Stream(ctx context.Context, call *provider.Call, out provider.Emitter) (provider.Outcome, error) {
	outcome := provider.Outcome{UsageSource: contract.UsageEstimated}

	response, err := a.send(ctx, call)
	if err != nil {
		return outcome, a.transportFailure(ctx, err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return outcome, a.upstreamFailure(response)
	}
	if call.Stream {
		return a.readStream(ctx, response.Body, call, out)
	}
	return a.readComplete(response.Body, call, out)
}

// readStream consumes the provider's SSE stream.
func (a *Adapter) readStream(ctx context.Context, body io.Reader, call *provider.Call, out provider.Emitter) (provider.Outcome, error) {
	outcome := provider.Outcome{UsageSource: contract.UsageEstimated}

	if err := out.Start(call.Route.ModelReference, time.Now()); err != nil {
		return outcome, err
	}

	calls := newToolCallAccumulator()
	decoder := sse.NewDecoder(body)

	for {
		frame, more := decoder.Next()
		if !more {
			break
		}
		if frame.Data == "" || frame.Data == doneSentinel {
			continue
		}

		var chunk chatChunk
		if err := json.Unmarshal([]byte(frame.Data), &chunk); err != nil {
			// One unreadable frame is not a reason to discard a stream that is
			// otherwise producing output and usage. It IS a reason to stop
			// claiming the provider reported anything precisely, so the outcome
			// keeps whatever source it has and the frame is skipped.
			continue
		}

		if chunk.Error != nil {
			// A failure that arrived after a 200, in the place a chunk belongs.
			// The units measured so far travel with it: the upstream did that
			// work and will invoice for it whether or not the answer arrived.
			return outcome, a.streamFailure(*chunk.Error)
		}

		if chunk.Usage != nil {
			outcome.Units = normalizeUsage(chunk.Usage)
			outcome.UsageSource = contract.UsageProviderReported
		}

		for _, choice := range chunk.Choices {
			if err := emitDelta(out, choice); err != nil {
				return outcome, err
			}
			if err := calls.absorb(out, choice.Delta.ToolCalls); err != nil {
				return outcome, err
			}
			if choice.FinishReason != nil {
				outcome.FinishReason = mapFinishReason(*choice.FinishReason)
				if err := calls.complete(out); err != nil {
					return outcome, err
				}
			}
		}
	}

	if err := decoder.Err(); err != nil {
		return outcome, a.transportFailure(ctx, err)
	}
	if ctx.Err() != nil {
		// The scanner can end cleanly on a cancelled request, which would
		// otherwise read as a stream that finished normally — and settle as a
		// completed one.
		return outcome, ctx.Err()
	}

	if outcome.UsageSource == contract.UsageProviderReported && len(outcome.Units) > 0 {
		if err := out.Usage(outcome.Units, outcome.UsageSource); err != nil {
			return outcome, err
		}
	}
	// The one unit this adapter measures itself, for the provider that reported
	// none. See provider.CountRequest: a `completed` report with no units is
	// refused by the contract and released rather than settled.
	outcome.Units = provider.CountRequest(outcome.Units)
	if outcome.FinishReason == "" {
		outcome.FinishReason = contract.FinishStop
	}
	return outcome, nil
}

// readComplete consumes a non-streamed response.
//
// The same normalized events are produced either way. Relay's own surface is
// always an event stream; `stream` on the envelope controls the UPSTREAM call,
// and the Oxy edge renders whichever dialect the customer asked for.
func (a *Adapter) readComplete(body io.Reader, call *provider.Call, out provider.Emitter) (provider.Outcome, error) {
	outcome := provider.Outcome{UsageSource: contract.UsageEstimated}

	raw, err := io.ReadAll(body)
	if err != nil {
		return outcome, fmt.Errorf("openaicompat: reading the upstream response: %w", err)
	}
	var completion chatCompletion
	if err := json.Unmarshal(raw, &completion); err != nil {
		return outcome, provider.ErrUpstream{
			Code:     contract.CodeProviderError,
			Category: contract.UpstreamUnknown,
			Detail:   "the provider returned a response this protocol cannot read",
			Passthrough: &contract.ProviderErrorPassthrough{
				Provider: a.config.Provider,
			},
		}
	}

	if err := out.Start(call.Route.ModelReference, time.Now()); err != nil {
		return outcome, err
	}
	if completion.Usage != nil {
		outcome.Units = normalizeUsage(completion.Usage)
		outcome.UsageSource = contract.UsageProviderReported
	}

	for _, choice := range completion.Choices {
		if err := emitCompletionMessage(out, choice); err != nil {
			return outcome, err
		}
		for _, call := range choice.Message.ToolCalls {
			if err := out.ToolCall(provider.ToolCallDelta{
				ID:             call.ID,
				Name:           call.Function.Name,
				ArgumentsDelta: call.Function.Arguments,
				Complete:       true,
			}); err != nil {
				return outcome, err
			}
		}
		if choice.FinishReason != nil {
			outcome.FinishReason = mapFinishReason(*choice.FinishReason)
		}
	}

	if outcome.UsageSource == contract.UsageProviderReported && len(outcome.Units) > 0 {
		if err := out.Usage(outcome.Units, outcome.UsageSource); err != nil {
			return outcome, err
		}
	}
	// The one unit this adapter measures itself, for the provider that reported
	// none. See provider.CountRequest: a `completed` report with no units is
	// refused by the contract and released rather than settled.
	outcome.Units = provider.CountRequest(outcome.Units)
	if outcome.FinishReason == "" {
		outcome.FinishReason = contract.FinishStop
	}
	return outcome, nil
}

func emitDelta(out provider.Emitter, choice streamChoice) error {
	if text := choice.Delta.Content; text != nil && *text != "" {
		if err := out.Delta(choice.Index, contract.ChannelOutputText, *text); err != nil {
			return err
		}
	}
	if text := choice.Delta.Refusal; text != nil && *text != "" {
		if err := out.Delta(choice.Index, contract.ChannelRefusal, *text); err != nil {
			return err
		}
	}
	for _, text := range []*string{choice.Delta.ReasoningContent, choice.Delta.Reasoning} {
		if text != nil && *text != "" {
			if err := out.Delta(choice.Index, contract.ChannelReasoning, *text); err != nil {
				return err
			}
		}
	}
	return nil
}

func emitCompletionMessage(out provider.Emitter, choice completionChoice) error {
	if text := choice.Message.ReasoningContent; text != nil && *text != "" {
		if err := out.Delta(choice.Index, contract.ChannelReasoning, *text); err != nil {
			return err
		}
	}
	if text := choice.Message.Refusal; text != nil && *text != "" {
		if err := out.Delta(choice.Index, contract.ChannelRefusal, *text); err != nil {
			return err
		}
	}
	if text := choice.Message.Content; text != nil && *text != "" {
		if err := out.Delta(choice.Index, contract.ChannelOutputText, *text); err != nil {
			return err
		}
	}
	return nil
}

/* -------------------------------------------------------------------------- */
/*  Tool calls                                                                */
/* -------------------------------------------------------------------------- */

// toolCallAccumulator tracks streamed tool calls by their index.
//
// The protocol identifies a streamed call by its position, sends the id and
// name once on the first frame, and then sends argument fragments with neither.
// Without the accumulator every fragment after the first would be emitted as an
// anonymous call, which a client cannot join back together.
type toolCallAccumulator struct {
	ids map[int]string
}

func newToolCallAccumulator() *toolCallAccumulator {
	return &toolCallAccumulator{ids: make(map[int]string)}
}

func (t *toolCallAccumulator) absorb(out provider.Emitter, refs []streamToolCallRef) error {
	for _, ref := range refs {
		if ref.ID != nil && *ref.ID != "" {
			t.ids[ref.Index] = *ref.ID
		}
		id, known := t.ids[ref.Index]
		if !known {
			// A fragment for a call whose id never arrived cannot be attributed
			// to anything. Emitting it under an invented id would produce a
			// call the client can neither join nor answer.
			continue
		}
		delta := provider.ToolCallDelta{ID: id}
		if ref.Function != nil {
			if ref.Function.Name != nil {
				delta.Name = *ref.Function.Name
			}
			if ref.Function.Arguments != nil {
				delta.ArgumentsDelta = *ref.Function.Arguments
			}
		}
		if delta.Name == "" && delta.ArgumentsDelta == "" {
			continue
		}
		if err := out.ToolCall(delta); err != nil {
			return err
		}
	}
	return nil
}

// complete marks every accumulated call finished, so a client knows when the
// argument text it has been concatenating is worth parsing.
func (t *toolCallAccumulator) complete(out provider.Emitter) error {
	indexes := make([]int, 0, len(t.ids))
	for index := range t.ids {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	for _, index := range indexes {
		if err := out.ToolCall(provider.ToolCallDelta{ID: t.ids[index], Complete: true}); err != nil {
			return err
		}
	}
	t.ids = make(map[int]string)
	return nil
}

/* -------------------------------------------------------------------------- */
/*  Usage                                                                     */
/* -------------------------------------------------------------------------- */

// normalizeUsage maps the protocol's token counts onto the contract's units.
//
// Two of the mappings are decisions rather than transcriptions, because the
// protocol's counts NEST and the contract's units are a flat list:
//
//   - `prompt_tokens` includes `prompt_tokens_details.cached_tokens`, and
//   - `completion_tokens` includes `completion_tokens_details.reasoning_tokens`.
//
// They are reported here as DISJOINT quantities — input excludes cached,
// output excludes reasoning — so that a price applied to every unit sums to the
// request rather than charging the cached and reasoning tokens twice. The
// contract does not say which reading it intends, and the two differ by real
// money on a reasoning model. See README, "What Oxy still has to decide".
//
// `requests: 1` is always reported: a per-request price has nothing to multiply
// otherwise, and a provider that returns no token counts at all would produce a
// receipt with no units, which the contract rejects.
func normalizeUsage(usage *chatUsage) []contract.UsageQuantity {
	units := []contract.UsageQuantity{{Unit: contract.UnitRequests, Quantity: 1}}

	cached := 0
	if usage.PromptTokensDetails != nil && usage.PromptTokensDetails.CachedTokens != nil {
		cached = nonNegative(*usage.PromptTokensDetails.CachedTokens)
	}
	reasoning := 0
	if usage.CompletionDetails != nil && usage.CompletionDetails.ReasoningTokens != nil {
		reasoning = nonNegative(*usage.CompletionDetails.ReasoningTokens)
	}

	if usage.PromptTokens != nil {
		units = append(units, contract.UsageQuantity{
			Unit:     contract.UnitInputTokens,
			Quantity: nonNegative(*usage.PromptTokens - cached),
		})
	}
	if cached > 0 {
		units = append(units, contract.UsageQuantity{Unit: contract.UnitCachedInputTokens, Quantity: cached})
	}
	if usage.CompletionTokens != nil {
		units = append(units, contract.UsageQuantity{
			Unit:     contract.UnitOutputTokens,
			Quantity: nonNegative(*usage.CompletionTokens - reasoning),
		})
	}
	if reasoning > 0 {
		units = append(units, contract.UsageQuantity{Unit: contract.UnitReasoningTokens, Quantity: reasoning})
	}
	return units
}

func nonNegative(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

func mapFinishReason(reason string) contract.FinishReason {
	switch reason {
	case "stop":
		return contract.FinishStop
	case "length", "max_tokens":
		return contract.FinishLength
	case "tool_calls", "function_call":
		return contract.FinishToolCalls
	case "content_filter":
		return contract.FinishContentFilter
	default:
		// An unrecognised reason is reported as a normal stop rather than
		// guessed at. The alternative — inventing a category — would put a
		// wrong reason on a receipt, and this one is at least honest about
		// having no information.
		return contract.FinishStop
	}
}

/* -------------------------------------------------------------------------- */
/*  Failures                                                                  */
/* -------------------------------------------------------------------------- */

// transportFailure classifies a failure that happened before or during the read.
func (a *Adapter) transportFailure(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(ctx.Err(), context.Canceled):
		// The caller withdrew. Returned unclassified so the executor can settle
		// it as a cancellation rather than as a provider failure — they are
		// different reasons on a reversal.
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded), errors.Is(ctx.Err(), context.DeadlineExceeded):
		return provider.ErrUpstream{
			Code:        contract.CodeProviderTimeout,
			Category:    contract.UpstreamTimeout,
			Detail:      fmt.Sprintf("%s did not respond within the request deadline", a.config.Provider),
			Passthrough: &contract.ProviderErrorPassthrough{Provider: a.config.Provider},
		}
	default:
		return provider.ErrUpstream{
			Code:        contract.CodeProviderError,
			Category:    contract.UpstreamServerError,
			Detail:      fmt.Sprintf("the connection to %s failed", a.config.Provider),
			Passthrough: &contract.ProviderErrorPassthrough{Provider: a.config.Provider},
		}
	}
}

// upstreamFailure classifies a non-2xx response.
//
// Retryability comes from the mapping below, never from the status alone. The
// contract is explicit that a 429 from an exhausted daily quota and a 429 from
// a momentary burst limit are the same status and different answers, and this
// is the function that has the information to tell them apart.
func (a *Adapter) upstreamFailure(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))

	var parsed upstreamErrorBody
	_ = json.Unmarshal(body, &parsed)

	passthrough := &contract.ProviderErrorPassthrough{Provider: a.config.Provider}
	status := response.StatusCode
	passthrough.Status = &status
	if parsed.Error.Type != "" {
		code := parsed.Error.Type
		passthrough.Code = &code
	}
	if parsed.Error.Message != "" {
		// Provider errors routinely echo the request that caused them, which is
		// where an upstream credential escapes. The adapter's own key is removed
		// by exact match here, before the contract's shape-based redaction runs
		// over whatever else the message contains.
		message := a.safeText(parsed.Error.Message)
		passthrough.Message = &message
	}

	failure := provider.ErrUpstream{Passthrough: passthrough}
	switch {
	case status == http.StatusTooManyRequests && parsed.Error.Type == "insufficient_quota":
		// A quota is an account-level ceiling that only a human raises; a rate
		// limit clears on its own. Same status, opposite retryability.
		//
		// The account at fault is the PLATFORM's with this provider, never the
		// customer's, so this is `provider_billing_refused` rather than
		// `quota_exceeded`: the latter names the customer's own ceiling and
		// would send them to top up an account that is not the one at fault.
		failure.Code, failure.Category = contract.CodeProviderBillingRefused, contract.UpstreamQuota
		failure.Detail = fmt.Sprintf("the platform's own %s account cannot be billed for this request", a.config.Provider)
	case status == http.StatusTooManyRequests:
		failure.Code, failure.Category = contract.CodeRateLimited, contract.UpstreamRateLimit
		failure.Detail = fmt.Sprintf("%s rate-limited this request", a.config.Provider)
		failure.RetryAfterMs = provider.RetryAfterMs(response.Header)
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		// Relay's own credential was refused, NOT the customer's.
		// `authentication_failed` would tell a customer their key is bad when
		// the key at fault is ours; `provider_error` would tell them to retry a
		// request that cannot succeed until an operator rotates a key.
		// `provider_credential_invalid` is the platform group's one
		// non-retryable code and exists for exactly this. The category stays
		// `authentication`, which is attributable — so the breaker still takes
		// this deployment out of rotation, and a failover to a deployment
		// holding a different credential is still permitted.
		failure.Code, failure.Category = contract.CodeProviderCredentialInvalid, contract.UpstreamAuthentication
		failure.Detail = fmt.Sprintf("%s refused the platform's credential for this route", a.config.Provider)
	case status == http.StatusNotFound:
		// The upstream does not have the model this route names. From the
		// customer's side there is no working route for what they asked for,
		// and no identical retry can create one.
		failure.Code, failure.Category = contract.CodeModelNotFound, contract.UpstreamInvalidReq
		failure.Detail = fmt.Sprintf("%s does not serve the model this route names", a.config.Provider)
	case status == http.StatusRequestTimeout, status == http.StatusGatewayTimeout:
		failure.Code, failure.Category = contract.CodeProviderTimeout, contract.UpstreamTimeout
		failure.Detail = fmt.Sprintf("%s timed out", a.config.Provider)
	case status == http.StatusServiceUnavailable:
		failure.Code, failure.Category = contract.CodeProviderOverloaded, contract.UpstreamOverloaded
		failure.Detail = fmt.Sprintf("%s is overloaded", a.config.Provider)
		failure.RetryAfterMs = provider.RetryAfterMs(response.Header)
	case status >= 500:
		failure.Code, failure.Category = contract.CodeProviderError, contract.UpstreamServerError
		failure.Detail = fmt.Sprintf("%s returned an internal error", a.config.Provider)
	case parsed.Error.Type == "content_filter" || parsed.Error.Type == "content_policy_violation":
		failure.Code, failure.Category = contract.CodeUpstreamContentFiltered, contract.UpstreamContentFilter
		failure.Detail = "the provider's content filter rejected this request"
	case status == http.StatusRequestEntityTooLarge:
		failure.Code, failure.Category = contract.CodeRequestTooLarge, contract.UpstreamInvalidReq
		failure.Detail = "the request is larger than the provider accepts"
	default:
		failure.Code, failure.Category = contract.CodeInvalidRequest, contract.UpstreamInvalidReq
		failure.Detail = fmt.Sprintf("%s rejected the request", a.config.Provider)
	}
	return failure
}

// streamFailure classifies an error object that arrived INSIDE the stream,
// after the upstream had already answered 200.
//
// There is no status to classify from, so the provider's own error type is all
// there is. An unrecognised one is reported as an unclassified provider failure
// rather than guessed at: a failure nobody can attribute must not take a
// deployment out of rotation for everybody.
func (a *Adapter) streamFailure(reported upstreamError) error {
	failure := provider.ErrUpstream{
		Passthrough: &contract.ProviderErrorPassthrough{Provider: a.config.Provider},
	}
	if reported.Type != "" {
		kind := reported.Type
		failure.Passthrough.Code = &kind
	}
	if reported.Message != "" {
		message := a.safeText(reported.Message)
		failure.Passthrough.Message = &message
	}

	switch reported.Type {
	case "insufficient_quota":
		failure.Code, failure.Category = contract.CodeProviderBillingRefused, contract.UpstreamQuota
		failure.Detail = fmt.Sprintf("the platform's own %s account cannot be billed for this request", a.config.Provider)
	case "rate_limit_exceeded":
		failure.Code, failure.Category = contract.CodeRateLimited, contract.UpstreamRateLimit
		failure.Detail = fmt.Sprintf("%s rate-limited this request part-way through it", a.config.Provider)
	case "server_error", "internal_error", "overloaded_error":
		failure.Code, failure.Category = contract.CodeProviderError, contract.UpstreamServerError
		failure.Detail = fmt.Sprintf("%s failed part-way through the response", a.config.Provider)
	case "content_filter", "content_policy_violation":
		failure.Code, failure.Category = contract.CodeUpstreamContentFiltered, contract.UpstreamContentFilter
		failure.Detail = "the provider's content filter stopped this response"
	default:
		failure.Code, failure.Category = contract.CodeProviderError, contract.UpstreamUnknown
		failure.Detail = fmt.Sprintf("%s failed part-way through the response and named no reason this build knows", a.config.Provider)
	}
	return failure
}

// safeText renders upstream text for a customer.
//
// It removes THIS adapter's own credential by exact match before applying the
// contract's shape-based redaction. The contract's pattern is a heuristic keyed
// to the shapes providers echo — `bearer <token>`, `sk-…` — and a provider that
// echoes a key some other way would defeat it; the adapter holds the exact bytes
// it sent, so it does not have to guess. See provider.RedactSecret.
func (a *Adapter) safeText(text string) string {
	return contract.SafeErrorText(provider.RedactSecret(text, a.config.APIKey))
}
