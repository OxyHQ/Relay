package contract

import "fmt"

// UsageReport is the data plane's technical account of one request — the only
// usage shape Relay produces.
//
// No money and no price appear on it. Relay measures units and names the route
// it used; the control plane decides what that costs. Keeping the two apart is
// what allows a price to be corrected after the fact without re-running or
// re-measuring anything, and it is the reason there is no Money type in this
// package at all.
type UsageReport struct {
	SchemaVersion          int             `json:"schemaVersion"`
	RequestID              RequestID       `json:"requestId"`
	GenerationID           *GenerationID   `json:"generationId,omitempty"`
	Attribution            Attribution     `json:"attribution"`
	Outcome                RequestOutcome  `json:"outcome"`
	Units                  []UsageQuantity `json:"units"`
	UsageSource            UsageSource     `json:"usageSource"`
	ResolvedModelReference ModelReference  `json:"resolvedModelReference"`
	ServingProvider        ProviderSlug    `json:"servingProvider"`
	DeploymentID           *DeploymentID   `json:"deploymentId,omitempty"`
	RouteSwitches          int             `json:"routeSwitches"`
	StartedAt              Timestamp       `json:"startedAt"`
	CompletedAt            Timestamp       `json:"completedAt"`
	TimeToFirstTokenMs     *int            `json:"timeToFirstTokenMs,omitempty"`
}

// Validate applies the refinements the published schema carries, so a report
// Relay would emit and Oxy would reject is caught on this side of the wire.
//
// A rejected usage report is not a lost log line: it is the record settlement
// runs against, so the failure mode of emitting an invalid one is a request
// that executed, cost money upstream, and can never be settled or refunded.
func (r *UsageReport) Validate() error {
	if r.SchemaVersion != SchemaVersion {
		return fmt.Errorf("contract: usage report schemaVersion must be %d", SchemaVersion)
	}
	if r.RequestID == "" {
		return fmt.Errorf("contract: usage report requestId is required")
	}
	if !isMember(r.Outcome, requestOutcomeValues) {
		return fmt.Errorf("contract: %q is not a request outcome", r.Outcome)
	}
	if !isMember(r.UsageSource, usageSourceValues) {
		return fmt.Errorf("contract: %q is not a usage source", r.UsageSource)
	}
	if !r.ResolvedModelReference.Valid() {
		return fmt.Errorf("contract: %q is not a model reference", r.ResolvedModelReference)
	}
	if !r.ServingProvider.Valid() {
		return fmt.Errorf("contract: %q is not a provider slug", r.ServingProvider)
	}
	if r.RouteSwitches < 0 || r.RouteSwitches > 100 {
		return fmt.Errorf("contract: routeSwitches %d is outside the contract's range", r.RouteSwitches)
	}
	seen := make(map[UsageUnit]struct{}, len(r.Units))
	for _, quantity := range r.Units {
		if !isMember(quantity.Unit, usageUnitValues) {
			return fmt.Errorf("contract: %q is not a usage unit", quantity.Unit)
		}
		if quantity.Quantity < 0 {
			return fmt.Errorf("contract: %s quantity is negative", quantity.Unit)
		}
		if _, duplicate := seen[quantity.Unit]; duplicate {
			return fmt.Errorf("contract: each unit is reported once, as a total (%s repeats)", quantity.Unit)
		}
		seen[quantity.Unit] = struct{}{}
	}
	// A `completed` report asserts the customer received the whole answer, so
	// "delivered in full, consumed nothing measurable" is a contradiction — and
	// it is one that BILLS NOTHING, because settlement prices the reported units
	// and sums. An empty list on a completed request is a free request produced
	// by a provider that omitted its usage block.
	//
	// The rule is CONDITIONAL, exactly as published: `partial`, `failed` and
	// `cancelled` legitimately carry no units, and in this build `failed` is
	// DERIVED from having none (`outcomeFor`). An unconditional minimum would
	// refuse those reports, and a refused report is a request that ran, cost
	// money upstream, and can never be settled or refunded.
	if r.Outcome == OutcomeCompleted && len(r.Units) == 0 {
		return fmt.Errorf("contract: a completed request consumed something; report at least one unit")
	}
	started, err := r.StartedAt.Time()
	if err != nil {
		return fmt.Errorf("contract: startedAt: %w", err)
	}
	completed, err := r.CompletedAt.Time()
	if err != nil {
		return fmt.Errorf("contract: completedAt: %w", err)
	}
	if completed.Before(started) {
		return fmt.Errorf("contract: a request cannot complete before it started")
	}
	return nil
}
