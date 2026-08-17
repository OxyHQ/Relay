// Package relay executes one normalized inference request: it resolves the
// routes that serve it, hands the request to an adapter, fails over to another
// deployment of the SAME model revision when one fails, frames what the adapter
// reports as a normalized event stream, and produces the technical usage record
// settlement runs against.
//
// Everything a customer could be told "no" about was already decided at the Oxy
// edge — scopes, account access, credential status, spend. Relay does not
// re-derive any of it; the envelope it receives is an already-authorized
// instruction (ADR 0006).
//
// # Failover is same-model, structurally
//
// A request resolves to an inventory.RouteSet: one model reference and the
// endpoints that serve it. Every candidate this executor can attempt comes from
// that one set, and the set holds the reference once for all of them — so the
// second attempt is the same weights at a different address, and a candidate
// naming a different model is not a case that is excluded here but a value that
// cannot be built. Cross-model fallback would need a routing policy authorising
// the substitution, and the envelope carries a policy REFERENCE rather than a
// snapshot, so this build has nothing to authorise it with. See README.
package relay

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/OxyHQ/Relay/internal/contract"
	"github.com/OxyHQ/Relay/internal/inventory"
	"github.com/OxyHQ/Relay/internal/provider"
	"github.com/OxyHQ/Relay/internal/providercost"
	"github.com/OxyHQ/Relay/internal/rotation"
)

// Executor turns an envelope into a stream and a usage report.
type Executor struct {
	inventory                *inventory.Store
	registry                 *provider.Registry
	rotation                 *rotation.Registry
	costs                    *providercost.Cards
	assumeFailoverAuthorized bool
	now                      func() time.Time
}

// Config wires an Executor.
type Config struct {
	// Inventory is the configuration snapshot store. It is a store rather than
	// an inventory because the snapshot is reloaded under a running process and
	// a request must read the one that is current when it arrives.
	Inventory *inventory.Store
	// Providers is the adapter registry.
	Providers *provider.Registry
	// Rotation holds the circuit breakers and health scores. Required: an
	// executor with no breakers would retry a failing provider into the ground,
	// which is the failure mode failover introduces if nothing bounds it.
	Rotation *rotation.Registry
	// Costs prices upstream attempts. Optional — absent means cost is not
	// measured, and every measurement says so rather than reporting zero.
	Costs *providercost.Cards
	// AssumeFailoverAuthorized permits Relay to choose among the deployments of
	// one model reference: to order them by health, and to retry a failed one
	// on another.
	//
	// It is false by default, and that default is a contract finding rather
	// than caution. The published `routingFallbackPolicySchema` gives the
	// CUSTOMER two booleans that govern exactly this behaviour — `disabled` and
	// `sameModelDeployment` — alongside `allowedRegions` and `deniedRegions`
	// that govern where a request may be served. The envelope carries a routing
	// policy REFERENCE and none of those values, so Relay cannot tell a
	// customer who asked for failover from one who switched it off. Failing
	// over anyway would silently override a control the platform advertises,
	// which is the failure this whole boundary exists to prevent; so with this
	// false a reference resolves to its declared primary deployment and
	// nowhere else, exactly as it did before failover existed.
	//
	// Setting it true is a statement by the operator that every caller of this
	// process has a routing policy permitting same-model failover across every
	// deployment in its inventory — true of a first-party canary, and of
	// nothing else. `cmd/relay` requires a dated, reasoned acknowledgement to
	// set it, so it cannot be switched on by an empty environment variable and
	// then forgotten. See README, "What Oxy still has to decide".
	AssumeFailoverAuthorized bool
	// Now is the clock, injectable for tests.
	Now func() time.Time
}

// NewExecutor wires the executor, refusing a configuration that cannot serve.
func NewExecutor(config Config) (*Executor, error) {
	switch {
	case config.Inventory == nil:
		return nil, fmt.Errorf("relay: no inventory store")
	case config.Providers == nil:
		return nil, fmt.Errorf("relay: no adapter registry")
	case config.Rotation == nil:
		return nil, fmt.Errorf("relay: no rotation registry")
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &Executor{
		inventory:                config.Inventory,
		registry:                 config.Providers,
		rotation:                 config.Rotation,
		costs:                    config.Costs,
		assumeFailoverAuthorized: config.AssumeFailoverAuthorized,
		now:                      now,
	}, nil
}

// Result is what one execution produced.
//
// Report is present whenever the request reached an adapter, including when it
// was cancelled or failed part-way: a partial stream is a settlement case, so
// the absence of a report is what makes a refund impossible rather than
// approximate.
type Result struct {
	Report *contract.UsageReport
	// Failure is the terminal error, if the request ended in one. It has been
	// emitted on the stream, except when the client cancelled: writing to a
	// client that withdrew is pointless, and a cancelled request whose events
	// kept flowing would be indistinguishable from one that completed.
	Failure *contract.Error
	// UpstreamCost is what this request cost Relay at its providers, including
	// attempts that failed and produced nothing for the customer. It is an
	// operator number: nothing in it crosses back to Oxy, and no contract shape
	// has a field it could occupy.
	UpstreamCost providercost.Record
}

// ErrClientGone reports that the sink stopped accepting events. The executor
// treats it as a cancellation rather than a failure, because the customer
// withdrawing is not the provider failing.
var ErrClientGone = errors.New("relay: the event sink is gone")

// attempt is one candidate's turn.
type attempt struct {
	route   provider.Route
	outcome provider.Outcome
	err     error
	// cancelled records that this attempt ended because the customer withdrew.
	// It is decided in the loop, where the request context is in scope, rather
	// than re-derived at settlement from an error that no longer carries it.
	cancelled bool
}

// Execute runs the request, emitting normalized events to sink.
//
// Cancelling ctx cancels the upstream call. That is not a convention the
// adapters are asked to honour politely: the context is the only handle they
// are given for the HTTP request, so an adapter that ignores it cannot make an
// upstream call at all.
func (e *Executor) Execute(ctx context.Context, request *contract.Request, sink Sink) Result {
	requestID := request.Attribution.RequestID
	generationID := e.generationID(request)
	startedAt := e.now()

	candidates, failure := e.resolve(request, startedAt)
	if failure != nil {
		emit := newEmitter(sink, requestID, generationID, startedAt)
		_ = emit.finishWithError(failure)
		return Result{Failure: failure}
	}

	emit := newEmitter(sink, requestID, generationID, startedAt)
	var (
		usage    []providercost.AttemptUsage
		switches int
		last     *attempt
		skipped  []contract.DeploymentID
		// abandoned is the attempt this request is moving away from, set when a
		// deployment fails in a way another one could survive. The switch is
		// announced from HERE, at the top of the attempt that actually replaces
		// it — announcing it at the point of failure would tell the customer
		// about a re-route to a deployment whose own breaker then refuses it,
		// and count a switch that never happened.
		abandoned *attempt
	)

	for _, route := range candidates {
		permit, admitted := e.rotation.Admit(route.DeploymentID)
		if !admitted {
			// The breaker is doing its job. This is route SELECTION, not a
			// route switch: nothing was attempted here.
			skipped = append(skipped, route.DeploymentID)
			continue
		}

		adapter, found := e.registry.Lookup(route.Provider)
		if !found {
			// The inventory routes somewhere this process cannot reach. The
			// server refuses to start in this state, so reaching it means the
			// snapshot changed under a running process — a configuration fault,
			// which says nothing about whether the provider is healthy.
			permit.NotAttributable()
			skipped = append(skipped, route.DeploymentID)
			continue
		}

		if abandoned != nil {
			err := emit.routeSwitch(switchReason(abandoned.err), abandoned.route, route, e.now())
			switch {
			case errors.Is(err, errSwitchTooLate):
				// Output has already reached the customer, so this request
				// cannot move. The abandoned attempt is the one that settles.
				permit.NotAttributable()
				last = abandoned
				abandoned = nil
			case err != nil:
				permit.NotAttributable()
				return e.settle(request, generationID, abandoned, usage, switches, startedAt, emit, err)
			}
			if abandoned == nil {
				break
			}
			switches++
			abandoned = nil
		}

		call, err := adapter.Translate(request, route)
		if err != nil {
			// A refusal to translate is about the REQUEST, and it is terminal.
			// Trying another deployment would make what a request means depend
			// on which route happened to be healthy, so an identical envelope
			// would be refused now and served in five minutes.
			permit.NotAttributable()
			failure := translationFailure(requestID, err)
			_ = emit.finishWithError(failure)
			return Result{Failure: failure}
		}

		emit.serving(route.Provider)
		outcome, streamErr := adapter.Stream(ctx, call, emit)
		usage = append(usage, providercost.AttemptUsage{
			DeploymentID: route.DeploymentID,
			Provider:     route.Provider,
			Served:       streamErr == nil,
			Units:        outcome.Units,
		})
		last = &attempt{
			route:     route,
			outcome:   outcome,
			err:       streamErr,
			cancelled: streamErr != nil && isCancellation(ctx, streamErr),
		}

		switch {
		case streamErr == nil:
			permit.Succeeded()
		case last.cancelled:
			// The customer withdrew. Nothing about this deployment failed, and
			// re-running the request elsewhere would serve output nobody is
			// listening for.
			permit.NotAttributable()
		case !provider.DeploymentAttributable(streamErr):
			// A request the provider cannot express, a content filter, an
			// unclassifiable failure: the same answer everywhere, so failing
			// over would spend a second time to be refused a second time.
			permit.NotAttributable()
		default:
			// A failure another deployment could survive. Nothing is announced
			// yet: whether this request can actually move depends on finding a
			// candidate whose breaker admits it, and on nothing having reached
			// the customer — both of which are decided at the top of the next
			// iteration.
			permit.Failed()
			abandoned = last
			last = nil
			continue
		}
		break
	}

	switch {
	case last != nil:
		return e.settle(request, generationID, last, usage, switches, startedAt, emit, nil)
	case abandoned != nil:
		// The last attempt failed and there was nowhere left to go. The customer
		// is told about that failure, not about a route switch that never
		// happened.
		return e.settle(request, generationID, abandoned, usage, switches, startedAt, emit, nil)
	}

	// Every candidate was refused before it was attempted. Nothing ran, so
	// there is nothing to settle — but the customer is owed a retry hint that
	// is a fact rather than a guess.
	refusal := e.everyRouteOutOfRotation(requestID, candidates, skipped, startedAt)
	_ = emit.finishWithError(refusal)
	return Result{Failure: refusal}
}

// settle builds and validates the usage report, emits the terminal event, and
// prices what the attempts cost upstream.
func (e *Executor) settle(
	request *contract.Request,
	generationID *contract.GenerationID,
	last *attempt,
	usage []providercost.AttemptUsage,
	switches int,
	startedAt time.Time,
	emit *emitter,
	sinkErr error,
) Result {
	requestID := request.Attribution.RequestID
	completedAt := e.now()
	cost := e.costs.MeasureRequest(requestID, usage)

	report := &contract.UsageReport{
		SchemaVersion: contract.SchemaVersion,
		RequestID:     requestID,
		GenerationID:  generationID,
		Attribution:   withGeneration(request.Attribution, generationID),
		// The units of an attempt that failed are deliberately absent: the
		// customer never received that output, so charging for it would be
		// wrong. The provider will invoice for it all the same, which is what
		// UpstreamCost records.
		Units:                  last.outcome.Units,
		UsageSource:            last.outcome.UsageSource,
		ResolvedModelReference: last.route.ModelReference,
		ServingProvider:        last.route.Provider,
		DeploymentID:           &last.route.DeploymentID,
		RouteSwitches:          switches,
		StartedAt:              contract.NewTimestamp(startedAt),
		CompletedAt:            contract.NewTimestamp(completedAt),
	}
	if ttft := emit.timeToFirstToken(); ttft > 0 {
		milliseconds := int(ttft.Milliseconds())
		report.TimeToFirstTokenMs = &milliseconds
	}
	if report.UsageSource == "" {
		// An adapter that measured nothing still has to say how it knows. The
		// honest answer is that the number is a reconstruction, and marking it
		// so is what lets settlement apply its estimation policy knowingly
		// instead of treating a guess as a provider's count.
		report.UsageSource = contract.UsageEstimated
	}

	switch {
	case sinkErr != nil:
		// The stream could not be delivered. Whatever the upstream did still
		// happened and is still owed, so the report is built and only the
		// terminal event is lost.
		report.Outcome = outcomeFor(last)
	case last.err == nil && !emit.started:
		// An adapter that returns success without ever emitting a start event
		// has produced a stream the contract cannot describe. Reporting it as
		// completed would hand settlement a receipt for output nobody saw.
		report.Outcome = contract.OutcomeFailed
		failure := contract.NewError(requestID, contract.CodeInternalError,
			fmt.Sprintf("the %s adapter completed without starting a stream", last.route.Provider))
		_ = emit.finishWithError(failure)
		return e.finalize(report, failure, nil, cost)
	case last.err == nil:
		report.Outcome = contract.OutcomeCompleted
		if err := emit.finishWithDone(finishReasonOr(last.outcome.FinishReason, contract.FinishStop), completedAt); err != nil {
			return e.finalize(report, nil, err, cost)
		}
	case last.cancelled:
		report.Outcome = contract.OutcomeCancelled
		// The stream is not written to again: the client that cancelled is not
		// listening, and a cancelled request whose events kept flowing would be
		// indistinguishable from one that completed.
		return e.finalize(report, contract.NewError(requestID, contract.CodeCancelled, "the client cancelled the request"), nil, cost)
	default:
		report.Outcome = outcomeFor(last)
		failure := upstreamFailure(requestID, last.route.Provider, last.err)
		emitErr := emit.finishWithError(failure)
		return e.finalize(report, failure, emitErr, cost)
	}
	return e.finalize(report, nil, sinkErr, cost)
}

func outcomeFor(last *attempt) contract.RequestOutcome {
	if last.err == nil {
		return contract.OutcomeCompleted
	}
	if len(last.outcome.Units) > 0 {
		return contract.OutcomePartial
	}
	return contract.OutcomeFailed
}

// finalize validates the usage report before returning it.
//
// A report Oxy's parse rejects is not a lost log line: it is the record
// settlement runs against, so emitting an invalid one produces a request that
// executed, cost money upstream, and can never be settled or refunded. When the
// report is unusable the executor says so rather than handing back something
// that looks fine. The upstream cost survives either way — Relay was still
// invoiced for the work.
func (e *Executor) finalize(report *contract.UsageReport, failure *contract.Error, sinkErr error, cost providercost.Record) Result {
	if err := report.Validate(); err != nil {
		return Result{
			Failure: contract.NewError(report.RequestID, contract.CodeInternalError,
				fmt.Sprintf("the usage report for this request is not settleable: %v", err)),
			UpstreamCost: cost,
		}
	}
	if sinkErr != nil && failure == nil {
		// The events could not be delivered. The work still happened and is
		// still owed, so the report stands; only the stream is lost.
		failure = contract.NewError(report.RequestID, contract.CodeCancelled, "the event stream could not be delivered")
	}
	return Result{Report: report, Failure: failure, UpstreamCost: cost}
}

/* -------------------------------------------------------------------------- */
/*  Route resolution                                                          */
/* -------------------------------------------------------------------------- */

// resolve turns the envelope's target into the ordered candidates that serve it.
func (e *Executor) resolve(request *contract.Request, at time.Time) ([]provider.Route, *contract.Error) {
	requestID := request.Attribution.RequestID

	if err := request.Validate(); err != nil {
		return nil, contract.NewError(requestID, contract.CodeInvalidRequest, err.Error())
	}
	if !request.Attribution.HasScope(contract.ScopeInvoke) {
		// Not a customer authorization decision — the edge already made that.
		// An envelope arriving without the invoke scope is a malformed
		// instruction from the edge, and serving it would mean Relay deciding
		// something it has no basis to decide.
		return nil, contract.NewError(requestID, contract.CodeInsufficientScope,
			"the envelope does not carry inference:invoke")
	}

	if request.Target.Kind == contract.TargetRoutingProfile {
		// Resolving a profile needs its candidate list, which lives in the Oxy
		// catalogue and is not in the envelope: it carries a routing policy
		// REFERENCE, not a snapshot. Choosing a model here would be exactly the
		// silent substitution the platform forbids, so the request is refused
		// with the field named. See README, "What Oxy still has to decide".
		return nil, contract.NewError(requestID, contract.CodeInvalidRequest,
			"this build serves concrete model targets only: the envelope carries a routing policy reference, not the snapshot a profile would have to be resolved against",
		).WithParam("target.routingProfile")
	}

	set, err := e.inventory.Current().Resolve(*request.Target.ModelReference, at)
	if err != nil {
		var noRoute inventory.ErrNoRoute
		if errors.As(err, &noRoute) {
			return nil, contract.NewError(requestID, contract.CodeModelNotFound,
				fmt.Sprintf("no deployment serves %q", noRoute.Reference)).WithParam("target.modelReference")
		}
		var stale inventory.ErrSnapshotTooStale
		if errors.As(err, &stale) {
			// The configuration snapshot has stopped being re-issued, so which
			// revision is current is no longer a question Relay can answer.
			// Pinned references are unaffected, and saying so is the difference
			// between an outage and a degradation the caller can work around.
			return nil, contract.NewError(requestID, contract.CodeServiceUnavailable,
				fmt.Sprintf("the deployment configuration has not been re-issued for %s, so the current revision of %q cannot be resolved; a revision-pinned reference is still served",
					stale.Age.Round(time.Second), stale.Reference))
		}
		return nil, contract.NewError(requestID, contract.CodeInternalError, err.Error())
	}

	candidates := set.Candidates()

	if len(request.AuthorizedRoutes) > 0 {
		// The envelope carries the result of applying the customer's routing
		// policy, so the authorization question is answered for this request and
		// answered per request rather than per process.
		authorized, failure := e.authorizedCandidates(request, set, candidates)
		if failure != nil {
			return nil, failure
		}
		e.order(authorized)
		return authorized, nil
	}

	if !e.assumeFailoverAuthorized && len(candidates) > 1 {
		// No list, and no process-wide acknowledgement. Choosing among the
		// deployments of one reference — which to try first, and whether to try a
		// second — is governed by routing-policy values this envelope does not
		// carry. Without them Relay makes the weakest choice available to it: the
		// deployment the inventory declared first, and no other. See
		// Config.AssumeFailoverAuthorized.
		candidates = candidates[:1]
	}
	e.order(candidates)
	return candidates, nil
}

// authorizedCandidates narrows the inventory's routes to the ones the Oxy edge
// authorized for THIS request, in the order it authorized them.
//
// ## What this does and does not decide
//
// It decides nothing about the customer. `provider` and `regions` on an entry
// describe what Oxy already permitted and are not re-checked here — re-deriving
// admissibility from them would rebuild, in Go, the enforcement engine the list
// exists to remove. What the list gives Relay is a set and an order: a
// deployment absent from it is unreachable because it never becomes a candidate,
// which is a stronger statement than a check that could be forgotten.
//
// ## Cross-model entries are dropped, not refused
//
// A `cross_model` entry is contract-legal and Relay carries it, but it never
// becomes a candidate. That keeps the guarantee `internal/inventory` and the
// emitter are built on — every candidate for one request serves one model
// reference — so the `route_switch` emitter still has no argument from which it
// could describe a substitution. Making cross-model reachable is a separate
// decision that has to revisit those two guarantees; see README.
//
// ## The reference check is defence in depth, and it is deliberate
//
// `Request.Validate` already refuses a `same_model` entry on a different model
// line, and `Candidates` stamps one reference onto every route. This re-checks
// the entry against the resolved set anyway, because serving weights the
// customer did not ask for is the one failure this whole design exists to make
// impossible, and it should not rest on a single check in one language.
func (e *Executor) authorizedCandidates(
	request *contract.Request,
	set inventory.RouteSet,
	candidates []provider.Route,
) ([]provider.Route, *contract.Error) {
	requestID := request.Attribution.RequestID

	byDeployment := make(map[contract.DeploymentID]provider.Route, len(candidates))
	for _, route := range candidates {
		byDeployment[route.DeploymentID] = route
	}

	authorized := make([]provider.Route, 0, len(request.AuthorizedRoutes))
	unknown := make([]contract.DeploymentID, 0, len(request.AuthorizedRoutes))
	for _, entry := range request.AuthorizedRoutes {
		if entry.Substitution != contract.SubstitutionSameModel {
			continue
		}
		if entry.ModelReference != set.Reference() {
			return nil, contract.NewError(requestID, contract.CodeInvalidRequest,
				fmt.Sprintf("authorized route %q serves %q, but this request resolved to %q",
					entry.DeploymentID, entry.ModelReference, set.Reference()),
			).WithParam("authorizedRoutes")
		}
		route, servable := byDeployment[entry.DeploymentID]
		if !servable {
			// Oxy authorized a deployment this build cannot reach: its snapshot
			// and Relay's inventory disagree. Skipped rather than fatal — the
			// remaining entries are still authorized — but named in the refusal
			// below if nothing is left, because "no route" and "no route I was
			// allowed to use" send an operator to different places.
			unknown = append(unknown, entry.DeploymentID)
			continue
		}
		authorized = append(authorized, route)
	}

	if len(authorized) == 0 {
		message := fmt.Sprintf("none of the %d authorized routes for this request is in this build's inventory", len(request.AuthorizedRoutes))
		if len(unknown) > 0 {
			message = fmt.Sprintf("%s (unknown deployments: %s)", message, joinDeployments(unknown))
		}
		return nil, contract.NewError(requestID, contract.CodeServiceUnavailable, message).
			WithParam("authorizedRoutes")
	}
	return authorized, nil
}

func joinDeployments(ids []contract.DeploymentID) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, string(id))
	}
	return strings.Join(parts, ", ")
}

// order sorts candidates by rotation preference: deployments whose breakers
// would admit a request first, healthier ones before flakier ones, and the
// inventory's own order as the tie-break so a route is not chosen at random.
func (e *Executor) order(candidates []provider.Route) {
	ranks := make(map[contract.DeploymentID]rotation.Rank, len(candidates))
	for _, route := range candidates {
		ranks[route.DeploymentID] = e.rotation.Rank(route.DeploymentID)
	}
	sort.SliceStable(candidates, func(a, b int) bool {
		return ranks[candidates[a].DeploymentID].Before(ranks[candidates[b].DeploymentID])
	})
}

// everyRouteOutOfRotation builds the refusal for a request whose every
// deployment is open.
func (e *Executor) everyRouteOutOfRotation(
	requestID contract.RequestID,
	candidates []provider.Route,
	skipped []contract.DeploymentID,
	at time.Time,
) *contract.Error {
	failure := contract.NewError(requestID, contract.CodeDeploymentUnavailable,
		fmt.Sprintf("all %d deployments serving this model are out of rotation", len(candidates)))
	if wait, known := e.rotation.SoonestProbe(skipped, at); known {
		// A retry hint the breaker can actually keep: this is when the earliest
		// of them will admit its next trial, not a number chosen to look
		// reasonable.
		failure = failure.WithRetryAfter(int(wait.Milliseconds()))
	}
	return failure
}

/* -------------------------------------------------------------------------- */
/*  Classification                                                            */
/* -------------------------------------------------------------------------- */

// switchReason maps a failure to the contract's reason for the route switch the
// customer is shown.
func switchReason(err error) contract.RouteSwitchReason {
	var upstream provider.ErrUpstream
	if !errors.As(err, &upstream) {
		return contract.SwitchProviderError
	}
	switch upstream.Category {
	case contract.UpstreamRateLimit:
		return contract.SwitchRateLimited
	case contract.UpstreamQuota:
		return contract.SwitchCapacity
	case contract.UpstreamTimeout:
		return contract.SwitchProviderTimeout
	case contract.UpstreamOverloaded:
		return contract.SwitchProviderOverloaded
	default:
		return contract.SwitchProviderError
	}
}

/* -------------------------------------------------------------------------- */
/*  Identifiers and small helpers                                             */
/* -------------------------------------------------------------------------- */

// generationID allocates the id for this generation.
//
// Relay allocates it; Oxy allocates requestId. The contract's identifiers
// module says the data plane generates both, but requestId is REQUIRED on the
// inbound envelope, so it cannot be. See README, "What Oxy still has to decide".
func (e *Executor) generationID(request *contract.Request) *contract.GenerationID {
	if existing := request.Attribution.GenerationID; existing != nil && *existing != "" {
		return existing
	}
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		// A generation id is a correlation handle, not a security boundary. If
		// the system's entropy source is failing, the request is in far more
		// trouble than an unnamed generation, and refusing here would turn a
		// degraded host into a total outage.
		return nil
	}
	id := contract.GenerationID("gen_" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(entropy[:])))
	return &id
}

func withGeneration(attribution contract.Attribution, generationID *contract.GenerationID) contract.Attribution {
	attribution.GenerationID = generationID
	return attribution
}

func finishReasonOr(reason, fallback contract.FinishReason) contract.FinishReason {
	if reason == "" {
		return fallback
	}
	return reason
}

// isCancellation distinguishes the customer withdrawing from the provider
// failing. They settle differently — one is a refund reason, the other is an
// upstream failure — so conflating them would put the wrong reason on a
// reversal.
func isCancellation(ctx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, ErrClientGone) ||
		errors.Is(ctx.Err(), context.Canceled)
}

// translationFailure maps an adapter's refusal to translate.
func translationFailure(requestID contract.RequestID, err error) *contract.Error {
	var unsupported provider.ErrUnsupported
	if errors.As(err, &unsupported) {
		failure := contract.NewError(requestID, unsupported.Code, unsupported.Detail)
		if unsupported.Param != "" {
			failure = failure.WithParam(unsupported.Param)
		}
		return failure
	}
	return contract.NewError(requestID, contract.CodeInvalidRequest, err.Error())
}

// upstreamFailure maps an adapter's execution failure. An adapter that
// classified the failure itself is believed; anything else is an unclassified
// provider error, which is non-retryable by the contract's rule that a failure
// nobody can classify is not safe to retry.
func upstreamFailure(requestID contract.RequestID, slug contract.ProviderSlug, err error) *contract.Error {
	var upstream provider.ErrUpstream
	if errors.As(err, &upstream) {
		failure := contract.NewError(requestID, upstream.Code, upstream.Detail)
		if upstream.RetryAfterMs > 0 {
			failure = failure.WithRetryAfter(upstream.RetryAfterMs)
		}
		return failure.WithUpstream(upstream.Category, upstream.Passthrough)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return contract.NewError(requestID, contract.CodeProviderTimeout,
			fmt.Sprintf("%s did not respond in time", slug)).
			WithUpstream(contract.UpstreamTimeout, nil)
	}
	return contract.NewError(requestID, contract.CodeProviderError, err.Error()).
		WithUpstream(contract.UpstreamUnknown, &contract.ProviderErrorPassthrough{Provider: slug})
}
