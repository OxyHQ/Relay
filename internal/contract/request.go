package contract

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// The discriminated unions below are flattened: one Go struct per union,
// carrying the discriminator plus every variant's fields, with a field left
// required only when every variant requires it.
//
// The alternative — an interface per union — buys type-level exhaustiveness at
// the cost of a custom marshaller for each, and the exhaustiveness it buys is
// already provided by contract_test.go comparing the flattened field set
// against the published variants. Validate() carries the per-variant rules the
// flattening cannot express, and is where a malformed union is refused.

// ContentSourceKind discriminates where binary or remote content comes from.
type ContentSourceKind string

const (
	ContentSourceURL    ContentSourceKind = "url"
	ContentSourceInline ContentSourceKind = "inline"
)

var contentSourceKindValues = []ContentSourceKind{ContentSourceURL, ContentSourceInline}

// ContentSource is transient by contract: neither form is persisted by default,
// and neither appears in a receipt, a log line or a telemetry event.
type ContentSource struct {
	Kind      ContentSourceKind `json:"kind"`
	URL       *string           `json:"url,omitempty"`
	MediaType *string           `json:"mediaType,omitempty"`
	Data      *string           `json:"data,omitempty"`
}

// ContentPartType discriminates one part of a message's content.
type ContentPartType string

const (
	ContentPartText  ContentPartType = "text"
	ContentPartImage ContentPartType = "image"
	ContentPartAudio ContentPartType = "audio"
	ContentPartFile  ContentPartType = "file"
	// ContentPartRefusal is a refusal the MODEL produced, carried back as part
	// of an assistant message rather than as an error.
	//
	// It is content, not a failure: the request succeeded, the provider billed
	// for it and the customer is owed the text. Relay therefore normalizes it
	// like any other part and never converts it into an `inferenceErrorSchema`
	// — an adapter that mapped it to an error would refund a request the
	// provider will invoice for, and would lose the only explanation the
	// customer gets.
	ContentPartRefusal ContentPartType = "refusal"
)

var contentPartTypeValues = []ContentPartType{
	ContentPartText,
	ContentPartImage,
	ContentPartAudio,
	ContentPartFile,
	ContentPartRefusal,
}

// ContentPart is one part of a message. A message is always a list of parts.
type ContentPart struct {
	Type     ContentPartType `json:"type"`
	Text     *string         `json:"text,omitempty"`
	Source   *ContentSource  `json:"source,omitempty"`
	Detail   *ImageDetail    `json:"detail,omitempty"`
	Filename *string         `json:"filename,omitempty"`
}

// ToolCall is a tool call an assistant made.
//
// Arguments is the JSON TEXT the model emitted, not a parsed object: models
// emit invalid JSON often enough that parsing here would turn a recoverable
// model mistake into a rejected message.
type ToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// Message is one normalized message.
type Message struct {
	Role       MessageRole   `json:"role"`
	Content    []ContentPart `json:"content"`
	Name       *string       `json:"name,omitempty"`
	ToolCallID *string       `json:"toolCallId,omitempty"`
	ToolCalls  []ToolCall    `json:"toolCalls,omitempty"`
}

// InputFormat discriminates the request's input.
type InputFormat string

const (
	InputMessages  InputFormat = "messages"
	InputText      InputFormat = "text"
	InputTextBatch InputFormat = "text_batch"
)

var inputFormatValues = []InputFormat{InputMessages, InputText, InputTextBatch}

// Input is the request's input. Three formats, because the modalities genuinely
// differ and pretending a batch is a one-message conversation loses the batch
// boundary that metering and provider translation depend on.
type Input struct {
	Format   InputFormat `json:"format"`
	Messages []Message   `json:"messages,omitempty"`
	Text     *string     `json:"text,omitempty"`
	Texts    []string    `json:"texts,omitempty"`
}

// SamplingParameters are all optional: absent means the route's own default.
type SamplingParameters struct {
	Temperature      *float64 `json:"temperature,omitempty"`
	TopP             *float64 `json:"topP,omitempty"`
	TopK             *int     `json:"topK,omitempty"`
	FrequencyPenalty *float64 `json:"frequencyPenalty,omitempty"`
	PresencePenalty  *float64 `json:"presencePenalty,omitempty"`
	Seed             *int     `json:"seed,omitempty"`
	StopSequences    []string `json:"stopSequences,omitempty"`
}

// ToolDefinition is a tool the model may call.
//
// Parameters is a JSON Schema document carried as an opaque object: validating
// the customer's JSON Schema against a meta schema would reject documents
// providers accept.
type ToolDefinition struct {
	Type        string         `json:"type"`
	Name        string         `json:"name"`
	Description *string        `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters"`
	Strict      *bool          `json:"strict,omitempty"`
}

// ToolChoiceFunction names the one tool the model must call.
type ToolChoiceFunction struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

// ToolChoice is the contract's one union that is not discriminated by a field:
// either a mode string or an object naming a function. It therefore needs a
// hand-written codec, and contract_test.go pins that codec by round-tripping
// both arms.
type ToolChoice struct {
	Mode     *ToolChoiceMode
	Function *ToolChoiceFunction
}

// MarshalJSON renders whichever arm is set.
func (t ToolChoice) MarshalJSON() ([]byte, error) {
	switch {
	case t.Mode != nil && t.Function != nil:
		return nil, fmt.Errorf("contract: tool choice carries both a mode and a function")
	case t.Mode != nil:
		return json.Marshal(*t.Mode)
	case t.Function != nil:
		return json.Marshal(*t.Function)
	default:
		return nil, fmt.Errorf("contract: tool choice carries neither a mode nor a function")
	}
}

// UnmarshalJSON accepts either arm and refuses anything else, rather than
// leaving an empty ToolChoice that a translator would read as "auto".
func (t *ToolChoice) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '"' {
		var mode ToolChoiceMode
		if err := json.Unmarshal(trimmed, &mode); err != nil {
			return err
		}
		for _, allowed := range toolChoiceModeValues {
			if mode == allowed {
				t.Mode, t.Function = &mode, nil
				return nil
			}
		}
		return fmt.Errorf("contract: %q is not a tool-choice mode", string(mode))
	}
	var function ToolChoiceFunction
	if err := json.Unmarshal(trimmed, &function); err != nil {
		return err
	}
	if function.Type != "function" || function.Name == "" {
		return fmt.Errorf("contract: a tool-choice object must name a function")
	}
	t.Mode, t.Function = nil, &function
	return nil
}

// ResponseFormatType discriminates a structured-output request.
type ResponseFormatType string

const (
	ResponseFormatText       ResponseFormatType = "text"
	ResponseFormatJSONObject ResponseFormatType = "json_object"
	ResponseFormatJSONSchema ResponseFormatType = "json_schema"
)

var responseFormatTypeValues = []ResponseFormatType{ResponseFormatText, ResponseFormatJSONObject, ResponseFormatJSONSchema}

// ResponseFormat is free text, any JSON object, or a named schema.
type ResponseFormat struct {
	Type   ResponseFormatType `json:"type"`
	Name   *string            `json:"name,omitempty"`
	Schema map[string]any     `json:"schema,omitempty"`
	Strict *bool              `json:"strict,omitempty"`
}

// RoutingTargetKind discriminates the two questions a caller can ask: "serve
// THIS model" versus "choose one for me".
type RoutingTargetKind string

const (
	TargetModel          RoutingTargetKind = "model"
	TargetRoutingProfile RoutingTargetKind = "routing_profile"
)

var routingTargetKindValues = []RoutingTargetKind{TargetModel, TargetRoutingProfile}

// RoutingTarget is what the request asks to be served.
type RoutingTarget struct {
	Kind           RoutingTargetKind   `json:"kind"`
	ModelReference *ModelReference     `json:"modelReference,omitempty"`
	RoutingProfile *RoutingProfileSlug `json:"routingProfile,omitempty"`
}

// RoutingPolicyReference records which policy, at which of the customer's own
// revisions, a request was admitted under.
//
// It is a reference and not a snapshot. That is the contract as published, and
// it is the reason Relay cannot enforce provider allowlists, residency, retention
// or price ceilings from the envelope alone — see README, "What Oxy still has to
// decide".
type RoutingPolicyReference struct {
	RoutingPolicyID string `json:"routingPolicyId"`
	PolicyVersion   int    `json:"policyVersion"`
}

// RouteSubstitution says what KIND of substitution an authorized route entry
// represents, and it is the contract's discriminator rather than a Relay idea.
type RouteSubstitution string

const (
	// SubstitutionSameModel is the same model revision at a different
	// deployment — the only substitution this build performs.
	SubstitutionSameModel RouteSubstitution = "same_model"
	// SubstitutionCrossModel is DIFFERENT WEIGHTS, and carrying it requires the
	// contract's `authorizedByPolicy: true` literal. Relay represents it because
	// the published union has two variants; it never selects one. See
	// `AuthorizedRoute`.
	SubstitutionCrossModel RouteSubstitution = "cross_model"
)

var routeSubstitutionValues = []RouteSubstitution{SubstitutionSameModel, SubstitutionCrossModel}

// AuthorizedRoute is one route the Oxy edge has ALREADY authorized for this
// request, in the order Oxy wants it tried.
//
// ## Why this exists, and what it replaces
//
// `RoutingPolicyReference` is a reference, so failover previously had nothing to
// check a replacement against: no provider allowlist, no residency, no retention
// requirement, no price ceiling. The answer is not to ship a policy engine here
// — it is for Oxy to do the filtering it already does and send the RESULT. With
// a list, failover is "take the next entry", and a route outside the customer's
// policy is unreachable because it is absent, not because Relay declined it.
// Nothing in this file interprets a policy, which is the property that matters.
//
// ## The fields are pre-authorization, NOT instructions to re-check
//
// `Provider` and `Regions` describe what Oxy authorized; Relay does not filter
// on them. Re-deriving admissibility from them would rebuild the enforcement
// engine this shape exists to avoid, in the language with no schema to check it.
//
// ## `AuthorizedByPolicy` and cross-model substitution
//
// The published union has two variants and Relay must be able to carry both, so
// this struct is the flattened union: `AuthorizedByPolicy` is absent on a
// `same_model` entry and the literal `true` on a `cross_model` one.
//
// **Relay never selects a `cross_model` entry.** `Candidates` filters to
// `same_model`, so a cross-model route is not a case that is excluded downstream
// but a value that never becomes a candidate — the same shape as the guarantee
// in `internal/inventory`, where an `Endpoint` cannot carry its own model
// reference. Making cross-model reachable is not a matter of relaxing a check
// here: it would require the `route_switch` emitter to accept the substitution
// arguments it is documented as not having, and it is deliberately a separate
// decision (README, "Explicitly out of scope").
type AuthorizedRoute struct {
	Substitution   RouteSubstitution `json:"substitution"`
	DeploymentID   DeploymentID      `json:"deploymentId"`
	ModelReference ModelReference    `json:"modelReference"`
	Provider       ProviderSlug      `json:"provider"`
	Regions        []Region          `json:"regions"`
	// AuthorizedByPolicy is the contract's `true` literal, present only on a
	// `cross_model` entry. A pointer because its ABSENCE is what distinguishes
	// the two variants on the wire.
	AuthorizedByPolicy *bool `json:"authorizedByPolicy,omitempty"`
}

func (r AuthorizedRoute) validate() error {
	if !isMember(r.Substitution, routeSubstitutionValues) {
		return fmt.Errorf("%q is not a route substitution", r.Substitution)
	}
	if r.DeploymentID == "" {
		return fmt.Errorf("deploymentId is required")
	}
	if !r.ModelReference.Valid() {
		return fmt.Errorf("%q is not a model reference", r.ModelReference)
	}
	if !r.Provider.Valid() {
		return fmt.Errorf("%q is not a provider slug", r.Provider)
	}
	if len(r.Regions) == 0 {
		return fmt.Errorf("an authorized route must name at least one region")
	}
	for _, region := range r.Regions {
		if !region.Valid() {
			return fmt.Errorf("%q is not a region", region)
		}
	}
	// The union's own rule, checked in BOTH directions. A `cross_model` entry
	// without the literal is not a cross-model route Oxy authorized, and a
	// `same_model` entry carrying it is an entry whose two halves disagree about
	// what it is — either way the envelope means something nobody wrote.
	switch r.Substitution {
	case SubstitutionCrossModel:
		if r.AuthorizedByPolicy == nil || !*r.AuthorizedByPolicy {
			return fmt.Errorf("a cross_model route must carry authorizedByPolicy: true")
		}
	case SubstitutionSameModel:
		if r.AuthorizedByPolicy != nil {
			return fmt.Errorf("a same_model route must not carry authorizedByPolicy")
		}
	}
	return nil
}

// ClientRequestMetadata is what the edge records about the CALL, as opposed to
// its content.
//
// The contract makes this object strict as a privacy control: Oxy never
// persists a user IP, raw, hashed or geo-derived, and this is the natural place
// somebody would add one. Relay therefore never adds a field to it either, and
// never logs its Labels beside request content.
type ClientRequestMetadata struct {
	APIFormat       APIFormat         `json:"apiFormat"`
	Endpoint        string            `json:"endpoint"`
	ClientRequestID *string           `json:"clientRequestId,omitempty"`
	ReceivedAt      Timestamp         `json:"receivedAt"`
	Labels          map[string]string `json:"labels,omitempty"`
}

// Request is the canonical internal envelope Oxy's public edge forwards.
type Request struct {
	SchemaVersion   int                    `json:"schemaVersion"`
	Attribution     Attribution            `json:"attribution"`
	Target          RoutingTarget          `json:"target"`
	Modality        Modality               `json:"modality"`
	Input           Input                  `json:"input"`
	Stream          bool                   `json:"stream"`
	MaxOutputTokens *int                   `json:"maxOutputTokens,omitempty"`
	Sampling        SamplingParameters     `json:"sampling"`
	Tools           []ToolDefinition       `json:"tools,omitempty"`
	ToolChoice      *ToolChoice            `json:"toolChoice,omitempty"`
	ResponseFormat  *ResponseFormat        `json:"responseFormat,omitempty"`
	Client          ClientRequestMetadata  `json:"client"`
	IdempotencyKey  *IdempotencyKey        `json:"idempotencyKey,omitempty"`
	RoutingPolicy   RoutingPolicyReference `json:"routingPolicy"`
	// AuthorizedRoutes is the ordered list of routes Oxy has already authorized
	// for this request, primary first. OPTIONAL in the contract, so an absent
	// list is not a malformed envelope — it is an edge that has not been taught
	// to send one, and Relay behaves exactly as it did before the field existed.
	// See `AuthorizedRoute`.
	AuthorizedRoutes []AuthorizedRoute `json:"authorizedRoutes,omitempty"`
}

// Validate carries the per-variant and cross-field rules the Go types cannot
// express, and it is the whole of what Relay checks about an envelope's
// structure. It deliberately does NOT re-check anything the Oxy edge decided:
// scopes, balances, account access and model permissions are the control
// plane's, already resolved, and re-deriving them here is the replica-lag
// hazard ADR 0006 rejects.
func (r *Request) Validate() error {
	if r.SchemaVersion != RequestEnvelopeVersion {
		return fmt.Errorf("contract: envelope schemaVersion %d is not implemented by this build (expected %d)", r.SchemaVersion, RequestEnvelopeVersion)
	}
	if r.Attribution.RequestID == "" {
		return fmt.Errorf("contract: attribution.requestId is required")
	}
	if r.Attribution.Principal.Billing.AccountID == "" {
		return fmt.Errorf("contract: attribution.principal.billing.accountId is required")
	}
	if err := r.Target.validate(); err != nil {
		return err
	}
	if err := r.Input.validate(); err != nil {
		return err
	}
	if !isMember(r.Modality, modalityValues) {
		return fmt.Errorf("contract: %q is not a modality", r.Modality)
	}
	if !isMember(r.Client.APIFormat, apiFormatValues) {
		return fmt.Errorf("contract: %q is not an api format", r.Client.APIFormat)
	}
	if r.MaxOutputTokens != nil && *r.MaxOutputTokens <= 0 {
		return fmt.Errorf("contract: maxOutputTokens must be positive")
	}
	if r.ToolChoice != nil && len(r.Tools) == 0 {
		return fmt.Errorf("contract: a tool choice requires at least one tool definition")
	}
	seenTools := make(map[string]struct{}, len(r.Tools))
	for _, tool := range r.Tools {
		if _, duplicate := seenTools[tool.Name]; duplicate {
			return fmt.Errorf("contract: tool names must be unique within one request (%q repeats)", tool.Name)
		}
		seenTools[tool.Name] = struct{}{}
	}
	return r.validateAuthorizedRoutes()
}

// validateAuthorizedRoutes restates the published refinement on
// `authorizedRoutes`, branch for branch.
//
// These are CROSS-FIELD rules the Go types cannot express, and they are mirrored
// here rather than left to Oxy for one reason: `Candidates` reads
// `Substitution`, so a mislabelled entry — `same_model` on different weights —
// is the one input that could make Relay serve a model the customer did not ask
// for. The contract refuses it, and this is Relay refusing to be the only thing
// that did not check. `internal/contract/testdata` feeds each branch to the
// published schema as a rejection control, so the two enforcements are held
// against each other rather than assumed to agree.
func (r *Request) validateAuthorizedRoutes() error {
	// `.min(1)` upstream. An EMPTY list is refused rather than read as "no
	// list": the two mean opposite things — nothing authorized versus nothing
	// sent — and treating an explicit emptiness as an absence is how a request
	// gets served through a route nobody authorized.
	if r.AuthorizedRoutes != nil && len(r.AuthorizedRoutes) == 0 {
		return fmt.Errorf("contract: authorizedRoutes was sent empty; omit the field or name at least one route")
	}
	if len(r.AuthorizedRoutes) == 0 {
		return nil
	}

	seenDeployments := make(map[DeploymentID]struct{}, len(r.AuthorizedRoutes))
	for index, route := range r.AuthorizedRoutes {
		if err := route.validate(); err != nil {
			return fmt.Errorf("contract: authorizedRoutes[%d]: %w", index, err)
		}
		// Failing over to the deployment that just failed is not failover, and
		// the duplicate would make `routeSwitches` count a switch that changed
		// nothing.
		if _, duplicate := seenDeployments[route.DeploymentID]; duplicate {
			return fmt.Errorf("contract: authorizedRoutes names deployment %q more than once", route.DeploymentID)
		}
		seenDeployments[route.DeploymentID] = struct{}{}
	}

	// The primary is not a substitution for itself, and every other entry's
	// `substitution` is read RELATIVE to it.
	primary := r.AuthorizedRoutes[0]
	if primary.Substitution != SubstitutionSameModel {
		return fmt.Errorf("contract: the first authorized route is the primary and cannot be a substitution")
	}
	primaryLine := primary.ModelReference.ModelID()

	if r.Target.Kind == TargetModel && r.Target.ModelReference != nil {
		target := *r.Target.ModelReference
		if target.Pinned() {
			// A pinned request is served on exactly those weights, so the
			// primary must BE them and no substitute may cross the model line.
			if primary.ModelReference != target {
				return fmt.Errorf("contract: a pinned request is served on exactly the revision it pinned (%q), not %q", target, primary.ModelReference)
			}
			for index, route := range r.AuthorizedRoutes {
				if route.Substitution == SubstitutionCrossModel {
					return fmt.Errorf("contract: authorizedRoutes[%d]: a request that pinned a revision authorizes no cross-model substitute", index)
				}
			}
		} else if primaryLine != ModelID(target) {
			return fmt.Errorf("contract: the primary authorized route must serve the model the request named (%q), not %q", target, primaryLine)
		}
	}

	// A mislabelled entry is the whole failure mode. `same_model` on a different
	// line is a substitution wearing the label that needs no authorization;
	// `cross_model` on the same line claims an authorization the customer never
	// had to give.
	for index, route := range r.AuthorizedRoutes {
		line := route.ModelReference.ModelID()
		if route.Substitution == SubstitutionSameModel && line != primaryLine {
			return fmt.Errorf("contract: authorizedRoutes[%d] serves %q, not %q, so it is a cross-model substitute", index, line, primaryLine)
		}
		if route.Substitution == SubstitutionCrossModel && line == primaryLine {
			return fmt.Errorf("contract: authorizedRoutes[%d] serves %q, so it is same-model failover", index, primaryLine)
		}
	}
	return nil
}

func (t RoutingTarget) validate() error {
	switch t.Kind {
	case TargetModel:
		if t.ModelReference == nil {
			return fmt.Errorf("contract: a model target must name a model reference")
		}
		if !t.ModelReference.Valid() {
			return fmt.Errorf("contract: %q is not a model reference", *t.ModelReference)
		}
		if t.RoutingProfile != nil {
			return fmt.Errorf("contract: a model target cannot also name a routing profile")
		}
	case TargetRoutingProfile:
		if t.RoutingProfile == nil {
			return fmt.Errorf("contract: a routing-profile target must name a profile")
		}
		if !t.RoutingProfile.Valid() {
			return fmt.Errorf("contract: %q is not a routing-profile slug", *t.RoutingProfile)
		}
		if t.ModelReference != nil {
			return fmt.Errorf("contract: a routing-profile target cannot also name a model")
		}
	default:
		return fmt.Errorf("contract: %q is not a routing-target kind", t.Kind)
	}
	return nil
}

func (i Input) validate() error {
	switch i.Format {
	case InputMessages:
		if len(i.Messages) == 0 {
			return fmt.Errorf("contract: a messages input must carry at least one message")
		}
		for index, message := range i.Messages {
			if err := message.validate(); err != nil {
				return fmt.Errorf("contract: input.messages[%d]: %w", index, err)
			}
		}
	case InputText:
		if i.Text == nil {
			return fmt.Errorf("contract: a text input must carry text")
		}
	case InputTextBatch:
		if len(i.Texts) == 0 {
			return fmt.Errorf("contract: a text_batch input must carry at least one text")
		}
	default:
		return fmt.Errorf("contract: %q is not an input format", i.Format)
	}
	return nil
}

func (m Message) validate() error {
	if !isMember(m.Role, messageRoleValues) {
		return fmt.Errorf("%q is not a message role", m.Role)
	}
	if m.Role == RoleTool && m.ToolCallID == nil {
		return fmt.Errorf("a tool message must name the tool call it answers")
	}
	if m.Role != RoleTool && m.ToolCallID != nil {
		return fmt.Errorf("only a tool message answers a tool call")
	}
	if m.Role != RoleAssistant && m.ToolCalls != nil {
		return fmt.Errorf("only an assistant message makes tool calls")
	}
	for index, part := range m.Content {
		if err := part.validate(); err != nil {
			return fmt.Errorf("content[%d]: %w", index, err)
		}
	}
	return nil
}

func (p ContentPart) validate() error {
	switch p.Type {
	case ContentPartText, ContentPartRefusal:
		if p.Text == nil {
			return fmt.Errorf("a %s part must carry text", p.Type)
		}
	case ContentPartImage, ContentPartAudio, ContentPartFile:
		if p.Source == nil {
			return fmt.Errorf("a %s part must carry a source", p.Type)
		}
		return p.Source.validate()
	default:
		return fmt.Errorf("%q is not a content-part type", p.Type)
	}
	return nil
}

func (s ContentSource) validate() error {
	switch s.Kind {
	case ContentSourceURL:
		if s.URL == nil || *s.URL == "" {
			return fmt.Errorf("a url source must carry a url")
		}
	case ContentSourceInline:
		if s.MediaType == nil || s.Data == nil {
			return fmt.Errorf("an inline source must carry a media type and data")
		}
	default:
		return fmt.Errorf("%q is not a content-source kind", s.Kind)
	}
	return nil
}

func isMember[T comparable](value T, allowed []T) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}
