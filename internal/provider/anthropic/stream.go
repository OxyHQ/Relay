package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/OxyHQ/Relay/internal/contract"
	"github.com/OxyHQ/Relay/internal/provider"
	"github.com/OxyHQ/Relay/internal/sse"
)

// Stream implements provider.Adapter.
//
// Cancellation is not a courtesy here. The context is handed to the HTTP
// request, so cancelling it tears down the upstream connection — an adapter
// physically cannot ignore it and still make the call. What this method is
// responsible for is the other half: returning the units it measured up to the
// moment it was cut off, because a partial stream is a settlement case and a
// refund can only be exact if the measurement is.
//
// This protocol makes that half easier and harder at once. Easier, because the
// input token count arrives in the FIRST event, so even a stream cut off after
// one chunk has an exact input measurement. Harder, because the output count
// arrives last and is cumulative, so a cancelled request has only the
// provisional count from the opening event — which is why a stream that never
// reached its final `message_delta` is reported as an estimate rather than as
// the provider's own number.
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

// readStream consumes the provider's event stream.
//
// The shape of this loop is the protocol's, and it is not the shape a
// chat-completions stream has. Output arrives as indexed content BLOCKS, each
// opened by its own event that declares what KIND of block it is; the deltas
// that follow carry no kind of their own, so the block map below is what tells
// a reasoning delta from an answer delta and a tool call's arguments from
// either. There is no terminal sentinel: `message_stop` ends the stream, and a
// stream that simply stops was cut off.
func (a *Adapter) readStream(ctx context.Context, body io.Reader, call *provider.Call, out provider.Emitter) (provider.Outcome, error) {
	outcome := provider.Outcome{UsageSource: contract.UsageEstimated}

	if err := out.Start(call.Route.ModelReference, time.Now()); err != nil {
		return outcome, err
	}

	blocks := newBlockTracker()
	meter := &usageMeter{}
	decoder := sse.NewDecoder(body)

	for {
		frame, more := decoder.Next()
		if !more {
			break
		}
		if frame.Data == "" {
			continue
		}

		var event streamEvent
		if err := json.Unmarshal([]byte(frame.Data), &event); err != nil {
			// One unreadable frame is not a reason to discard a stream that is
			// otherwise producing output and usage. It IS a reason to stop
			// claiming the provider reported anything precisely, so the outcome
			// keeps whatever source it has and the frame is skipped.
			continue
		}

		switch event.Type {
		case eventMessageStart:
			if event.Message != nil {
				meter.absorb(event.Message.Usage, false)
			}

		case eventContentBlockStart:
			toolCall, started := blocks.start(event)
			if started {
				if err := out.ToolCall(toolCall); err != nil {
					return measured(outcome, meter), err
				}
			}

		case eventContentBlockDelta:
			if err := a.emitDelta(out, blocks, event); err != nil {
				return measured(outcome, meter), err
			}

		case eventContentBlockStop:
			toolCall, finished := blocks.stop(event)
			if finished {
				if err := out.ToolCall(toolCall); err != nil {
					return measured(outcome, meter), err
				}
			}

		case eventMessageDelta:
			// The only event that carries the FINAL output count, and it
			// carries it cumulatively rather than as an increment.
			meter.absorb(event.Usage, true)
			if event.Delta != nil && event.Delta.StopReason != nil {
				outcome.FinishReason = mapStopReason(*event.Delta.StopReason)
			}

		case eventError:
			// A failure that arrives after a 200. Nothing about the HTTP
			// exchange says this request failed, so an adapter that ignored
			// this frame would report a truncated answer as a complete one.
			detail := errorDetail{}
			if event.Error != nil {
				detail = *event.Error
			}
			return measured(outcome, meter), a.midStreamFailure(detail)

		case eventMessageStop, eventPing:
			// message_stop ends the stream; the read loop ends with the body.
			// A ping is a keep-alive and carries nothing.

		default:
			// The provider's versioning policy says new event types may be
			// added and that a client must tolerate them. Ignoring one is safe
			// precisely because everything Relay meters or emits arrives in the
			// events above.
		}
	}

	if err := decoder.Err(); err != nil {
		return measured(outcome, meter), a.transportFailure(ctx, err)
	}
	if ctx.Err() != nil {
		// The scanner can end cleanly on a cancelled request, which would
		// otherwise read as a stream that finished normally — and settle as a
		// completed one.
		return measured(outcome, meter), ctx.Err()
	}

	outcome = measured(outcome, meter)
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

// measured attaches whatever the meter has to an outcome, including on the
// failure paths above: a stream that failed part-way still consumed the input
// tokens the provider reported in its first event, and a settlement that
// dropped them would refund a customer for work that really was done.
func measured(outcome provider.Outcome, meter *usageMeter) provider.Outcome {
	outcome.Units, outcome.UsageSource = meter.units()
	return outcome
}

func (a *Adapter) emitDelta(out provider.Emitter, blocks *blockTracker, event streamEvent) error {
	if event.Delta == nil || event.Index == nil {
		return nil
	}
	switch event.Delta.Type {
	case deltaText:
		if text := derefString(event.Delta.Text); text != "" {
			return out.Delta(*event.Index, contract.ChannelOutputText, text)
		}

	case deltaThinking:
		if text := derefString(event.Delta.Thinking); text != "" {
			return out.Delta(*event.Index, contract.ChannelReasoning, text)
		}

	case deltaInputJSON:
		id, known := blocks.toolCallID(*event.Index)
		if !known {
			// A fragment for a call whose opening event never arrived cannot be
			// attributed to anything, and emitting it under an invented id would
			// produce a call the client can neither join nor answer.
			return nil
		}
		if fragment := derefString(event.Delta.PartialJSON); fragment != "" {
			return out.ToolCall(provider.ToolCallDelta{ID: id, ArgumentsDelta: fragment})
		}

	case deltaSignature:
		// The encrypted signature of a thinking block. It is read so that it is
		// not mistaken for output, and dropped because no contract stream event
		// has a field for provider-opaque block metadata — see README, "What Oxy
		// still has to decide". Emitting it as reasoning text would render an
		// encrypted blob to a customer as though the model had said it.
	}
	return nil
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
		return outcome, fmt.Errorf("anthropic: reading the upstream response: %w", err)
	}
	var complete message
	if err := json.Unmarshal(raw, &complete); err != nil {
		return outcome, provider.ErrUpstream{
			Code:        contract.CodeProviderError,
			Category:    contract.UpstreamUnknown,
			Detail:      "the provider returned a response this protocol cannot read",
			Passthrough: &contract.ProviderErrorPassthrough{Provider: Slug},
		}
	}

	if err := out.Start(call.Route.ModelReference, time.Now()); err != nil {
		return outcome, err
	}

	meter := &usageMeter{}
	meter.absorb(complete.Usage, true)

	for index, block := range complete.Content {
		switch block.Type {
		case blockThinking:
			if text := derefString(block.Thinking); text != "" {
				if err := out.Delta(index, contract.ChannelReasoning, text); err != nil {
					return measured(outcome, meter), err
				}
			}
		case blockRedacted:
			// Safety-redacted reasoning: an opaque blob with no readable text.
			// There is nothing to render and nothing to meter separately.
		case blockText:
			if text := derefString(block.Text); text != "" {
				if err := out.Delta(index, contract.ChannelOutputText, text); err != nil {
					return measured(outcome, meter), err
				}
			}
		case blockToolUse:
			arguments := "{}"
			if len(block.Input) > 0 {
				arguments = string(block.Input)
			}
			if err := out.ToolCall(provider.ToolCallDelta{
				ID:             derefString(block.ID),
				Name:           derefString(block.Name),
				ArgumentsDelta: arguments,
				Complete:       true,
			}); err != nil {
				return measured(outcome, meter), err
			}
		}
	}

	if complete.StopReason != nil {
		outcome.FinishReason = mapStopReason(*complete.StopReason)
	}
	outcome = measured(outcome, meter)
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

/* -------------------------------------------------------------------------- */
/*  Content blocks                                                            */
/* -------------------------------------------------------------------------- */

// blockTracker remembers what kind of thing each open content block is.
//
// It exists because this protocol declares a block's kind ONCE, in the event
// that opens it, and the deltas that follow carry only an index. Without it
// every argument fragment of a tool call would be an anonymous delta, and a
// client would have no way to tell reasoning from an answer.
type blockTracker struct {
	toolCalls map[int]string
}

func newBlockTracker() *blockTracker {
	return &blockTracker{toolCalls: make(map[int]string)}
}

// start records an opening block and reports the tool call it announces, if it
// announces one. The id and the name both arrive here, which is why this
// protocol needs no accumulator to join a name to its fragments.
func (b *blockTracker) start(event streamEvent) (provider.ToolCallDelta, bool) {
	if event.Index == nil || event.ContentBlock == nil || event.ContentBlock.Type != blockToolUse {
		return provider.ToolCallDelta{}, false
	}
	id := derefString(event.ContentBlock.ID)
	if id == "" {
		return provider.ToolCallDelta{}, false
	}
	b.toolCalls[*event.Index] = id
	return provider.ToolCallDelta{ID: id, Name: derefString(event.ContentBlock.Name)}, true
}

// stop reports the tool call a closing block completes, so a client knows when
// the argument text it has been concatenating is worth parsing.
func (b *blockTracker) stop(event streamEvent) (provider.ToolCallDelta, bool) {
	if event.Index == nil {
		return provider.ToolCallDelta{}, false
	}
	id, isToolCall := b.toolCalls[*event.Index]
	if !isToolCall {
		return provider.ToolCallDelta{}, false
	}
	delete(b.toolCalls, *event.Index)
	return provider.ToolCallDelta{ID: id, Complete: true}, true
}

func (b *blockTracker) toolCallID(index int) (string, bool) {
	id, known := b.toolCalls[index]
	return id, known
}

/* -------------------------------------------------------------------------- */
/*  Usage                                                                     */
/* -------------------------------------------------------------------------- */

// usageMeter collects this protocol's token counts, which arrive in two events
// and are CUMULATIVE rather than incremental.
//
// Cumulative is the whole reason this is a struct and not a sum: `message_start`
// reports the output tokens generated so far (usually one or two) and the final
// `message_delta` reports the total. Adding them would over-report every request
// by the opening count — a small, plausible, permanent overcharge.
type usageMeter struct {
	inputTokens         int
	cacheReadTokens     int
	cacheCreationTokens int
	outputTokens        int
	thinkingTokens      int
	// seenAny records that the provider reported something, which is what
	// separates "no usage" from "zero usage".
	seenAny bool
	// final records that the event carrying the authoritative output count has
	// arrived. Before it, the output number is provisional, and a provisional
	// number reported as the provider's own is one nobody can reconcile.
	final bool
}

func (m *usageMeter) absorb(reported *usage, final bool) {
	if reported == nil {
		return
	}
	m.seenAny = true
	m.final = m.final || final
	// Latest wins, per field: every count this protocol sends is a total for
	// the request so far, so a later event supersedes an earlier one and a
	// field an event omits is one it had nothing new to say about.
	if reported.InputTokens != nil {
		m.inputTokens = nonNegative(*reported.InputTokens)
	}
	if reported.CacheReadInputTokens != nil {
		m.cacheReadTokens = nonNegative(*reported.CacheReadInputTokens)
	}
	if reported.CacheCreationInputTokens != nil {
		m.cacheCreationTokens = nonNegative(*reported.CacheCreationInputTokens)
	}
	if reported.OutputTokens != nil {
		m.outputTokens = nonNegative(*reported.OutputTokens)
	}
	if reported.OutputTokensDetails != nil && reported.OutputTokensDetails.ThinkingTokens != nil {
		m.thinkingTokens = nonNegative(*reported.OutputTokensDetails.ThinkingTokens)
	}
}

// units maps what this provider reported onto the contract's units, and reports
// how much the resulting numbers can be trusted.
//
// The contract's units PARTITION a request: `cached_input_tokens` is disjoint
// from `input_tokens` and `reasoning_tokens` from `output_tokens`, so a price
// applied to every reported unit sums to exactly what was served
// (OxyHQ/oxy#1019). This provider's own numbers are half nested and half
// disjoint, and the two halves need OPPOSITE treatment:
//
//   - `input_tokens` is documented as "input tokens which were not read from or
//     used to create a cache", with the total being
//     `cache_read + cache_creation + input_tokens`. It is ALREADY disjoint from
//     the cache counts, so subtracting the cache read from it — the arithmetic
//     an OpenAI-compatible provider needs — would report fewer input tokens
//     than the request consumed, and the units would no longer sum to it.
//   - `output_tokens` is documented as "the inclusive, authoritative total used
//     for billing", with `output_tokens_details.thinking_tokens` broken out of
//     it. It IS nested, so the reasoning tokens are subtracted, exactly as an
//     OpenAI-compatible `completion_tokens` needs.
//
// Cache CREATION tokens are folded into `input_tokens`. They are input tokens
// the model processed, and the contract has no unit for a cache write — see
// README, "What Oxy still has to decide". Reporting them as
// `cached_input_tokens` would be worse than approximate: that unit means a
// cache READ, which this provider prices at a tenth of the input rate while
// charging a premium for the write.
func (m *usageMeter) units() ([]contract.UsageQuantity, contract.UsageSource) {
	if !m.seenAny {
		// Nothing was reported. An empty unit list with an estimated source is
		// the honest answer; `requests: 1` alone would look like a metered
		// request that happened to consume no tokens.
		return nil, contract.UsageEstimated
	}

	units := []contract.UsageQuantity{{Unit: contract.UnitRequests, Quantity: 1}}
	units = append(units, contract.UsageQuantity{
		Unit:     contract.UnitInputTokens,
		Quantity: m.inputTokens + m.cacheCreationTokens,
	})
	if m.cacheReadTokens > 0 {
		units = append(units, contract.UsageQuantity{Unit: contract.UnitCachedInputTokens, Quantity: m.cacheReadTokens})
	}
	units = append(units, contract.UsageQuantity{
		Unit:     contract.UnitOutputTokens,
		Quantity: nonNegative(m.outputTokens - m.thinkingTokens),
	})
	if m.thinkingTokens > 0 {
		units = append(units, contract.UsageQuantity{Unit: contract.UnitReasoningTokens, Quantity: m.thinkingTokens})
	}

	source := contract.UsageEstimated
	if m.final {
		source = contract.UsageProviderReported
	}
	return units, source
}

func nonNegative(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

// mapStopReason maps this protocol's stop reasons onto the contract's.
func mapStopReason(reason string) contract.FinishReason {
	switch reason {
	case "end_turn", "stop_sequence", "pause_turn":
		return contract.FinishStop
	case "max_tokens", "model_context_window_exceeded":
		return contract.FinishLength
	case "tool_use":
		return contract.FinishToolCalls
	case "refusal":
		// The MODEL declined, which is a property of the answer — as against a
		// content filter, which is an upstream system removing one. The contract
		// separates the two, so collapsing them here would report the terminal
		// event less specifically than the stream that produced it.
		return contract.FinishRefusal
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
			Detail:      fmt.Sprintf("%s did not respond within the request deadline", Slug),
			Passthrough: &contract.ProviderErrorPassthrough{Provider: Slug},
		}
	default:
		return provider.ErrUpstream{
			Code:        contract.CodeProviderError,
			Category:    contract.UpstreamServerError,
			Detail:      fmt.Sprintf("the connection to %s failed", Slug),
			Passthrough: &contract.ProviderErrorPassthrough{Provider: Slug},
		}
	}
}

// upstreamFailure classifies a non-2xx response.
func (a *Adapter) upstreamFailure(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))

	var parsed errorBody
	_ = json.Unmarshal(body, &parsed)

	status := response.StatusCode
	failure := a.classify(parsed.Error, status)
	failure.Passthrough.Status = &status
	if failure.Code.Retryable() {
		if wait := provider.RetryAfterMs(response.Header); wait > 0 {
			failure.RetryAfterMs = wait
		}
	}
	return failure
}

// midStreamFailure classifies an `error` event, which arrives after a 200 and
// therefore has no status to be classified from.
//
// That is the case that makes classifying by the provider's own error TYPE the
// primary rule rather than a refinement of the status: here there is no status
// at all, and a failure nobody classified is retried nowhere and trips no
// breaker — so an overloaded provider would keep receiving traffic.
func (a *Adapter) midStreamFailure(detail errorDetail) error {
	return a.classify(detail, 0)
}

// classify maps the provider's own error vocabulary onto the contract's.
//
// Retryability comes from this mapping, never from a status alone. The two
// cases that make the point are this provider's own: a `rate_limit_error`
// clears by itself and a `billing_error` does not, and neither shares a status
// with the other — where an OpenAI-compatible provider sends both as a 429.
// Reading the type is what makes one adapter's classification mean the same
// thing as the other's.
func (a *Adapter) classify(detail errorDetail, status int) provider.ErrUpstream {
	failure := provider.ErrUpstream{
		Passthrough: &contract.ProviderErrorPassthrough{Provider: Slug},
	}
	if detail.Type != "" {
		code := detail.Type
		failure.Passthrough.Code = &code
	}
	if detail.Message != "" {
		// The credential is removed by exact match HERE, before the message
		// travels any further: this is the field an upstream echoes a request
		// into, and the contract's shape-based redaction does not cover a
		// header this provider names differently. See Adapter.safeText.
		message := a.safeText(detail.Message)
		failure.Passthrough.Message = &message
	}

	switch detail.Type {
	case errorRateLimit:
		failure.Code, failure.Category = contract.CodeRateLimited, contract.UpstreamRateLimit
		failure.Detail = fmt.Sprintf("%s rate-limited this request", Slug)

	case errorBilling:
		// The PLATFORM's account with this provider cannot be billed, which no
		// retry and no customer can fix. `quota_exceeded` — the closest code
		// before the contract had this one — is the customer's own ceiling, so
		// reporting it here is retryability-correct and diagnostically wrong:
		// it reads as actionable and the action does nothing.
		failure.Code, failure.Category = contract.CodeProviderBillingRefused, contract.UpstreamQuota
		failure.Detail = fmt.Sprintf("the platform's own %s account cannot be billed for this request", Slug)

	case errorAuthentication, errorPermission:
		// Relay's own credential was refused, NOT the customer's.
		// `authentication_failed` would tell a customer their key is bad when
		// the key at fault is ours and send them to rotate the wrong secret;
		// `provider_error` would tell them to retry a request that cannot
		// succeed until an operator rotates a key. `provider_credential_invalid`
		// is the platform group's one non-retryable code and exists for exactly
		// this. The category stays `authentication`, which is attributable — so
		// the breaker still takes this deployment out of rotation, and a
		// same-model failover to a deployment holding a different credential is
		// still permitted. A permission error belongs here too: it is the same
		// credential, lacking access somebody has to grant it.
		failure.Code, failure.Category = contract.CodeProviderCredentialInvalid, contract.UpstreamAuthentication
		failure.Detail = fmt.Sprintf("%s refused the platform's credential for this route", Slug)

	case errorNotFound:
		// The upstream does not have the model this route names. From the
		// customer's side there is no working route for what they asked for,
		// and no identical retry can create one.
		failure.Code, failure.Category = contract.CodeModelNotFound, contract.UpstreamInvalidReq
		failure.Detail = fmt.Sprintf("%s does not serve the model this route names", Slug)

	case errorRequestTooLarge:
		failure.Code, failure.Category = contract.CodeRequestTooLarge, contract.UpstreamInvalidReq
		failure.Detail = "the request is larger than the provider accepts"

	case errorTimeout:
		failure.Code, failure.Category = contract.CodeProviderTimeout, contract.UpstreamTimeout
		failure.Detail = fmt.Sprintf("%s timed out while processing this request", Slug)

	case errorOverloaded:
		failure.Code, failure.Category = contract.CodeProviderOverloaded, contract.UpstreamOverloaded
		failure.Detail = fmt.Sprintf("%s is overloaded", Slug)

	case errorAPI:
		failure.Code, failure.Category = contract.CodeProviderError, contract.UpstreamServerError
		failure.Detail = fmt.Sprintf("%s returned an internal error", Slug)

	case errorInvalidRequest:
		failure.Code, failure.Category = contract.CodeInvalidRequest, contract.UpstreamInvalidReq
		failure.Detail = fmt.Sprintf("%s rejected the request", Slug)

	default:
		a.classifyByStatus(&failure, status)
	}
	return failure
}

// classifyByStatus is the fallback for an error type this build does not know.
//
// The provider's versioning policy says the type values may grow, so an
// unrecognised one is expected rather than exceptional. What it must not do is
// guess a retryable code: an unclassifiable failure is not safe to retry, and a
// mid-stream failure with no status at all is not safe to blame on the
// deployment either.
func (a *Adapter) classifyByStatus(failure *provider.ErrUpstream, status int) {
	switch {
	case status == http.StatusTooManyRequests:
		failure.Code, failure.Category = contract.CodeRateLimited, contract.UpstreamRateLimit
		failure.Detail = fmt.Sprintf("%s rate-limited this request", Slug)
	case status == http.StatusRequestTimeout, status == http.StatusGatewayTimeout:
		failure.Code, failure.Category = contract.CodeProviderTimeout, contract.UpstreamTimeout
		failure.Detail = fmt.Sprintf("%s timed out", Slug)
	case status >= 500:
		failure.Code, failure.Category = contract.CodeProviderError, contract.UpstreamServerError
		failure.Detail = fmt.Sprintf("%s returned an internal error", Slug)
	case status >= 400:
		failure.Code, failure.Category = contract.CodeInvalidRequest, contract.UpstreamInvalidReq
		failure.Detail = fmt.Sprintf("%s rejected the request", Slug)
	default:
		failure.Code, failure.Category = contract.CodeProviderError, contract.UpstreamUnknown
		failure.Detail = fmt.Sprintf("%s failed part-way through the response and named no reason this build knows", Slug)
	}
}
