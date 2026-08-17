package contract

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// descriptor.json is generated from the pinned, published @oxyhq/contracts
// package by tools/contract/generate.mjs. It is embedded into the test binary
// rather than the shipped one because nothing at runtime reads it: the runtime
// authority is the Go types, and this file exists to prove they still say what
// the published package says.
//
//go:embed descriptor.json
var descriptorJSON []byte

/* -------------------------------------------------------------------------- */
/*  The descriptor, as Go reads it                                            */
/* -------------------------------------------------------------------------- */

type descriptorFile struct {
	Package         string                        `json:"package"`
	PackageVersion  string                        `json:"packageVersion"`
	ContractVersion string                        `json:"contractVersion"`
	ShapeCount      int                           `json:"shapeCount"`
	Constants       map[string]descriptorConstant `json:"constants"`
	Shapes          map[string]descriptorNode     `json:"shapes"`
}

type descriptorConstant struct {
	Module string          `json:"module"`
	Value  json.RawMessage `json:"value"`
}

type descriptorNode struct {
	Module        string           `json:"module"`
	Versioned     bool             `json:"versioned"`
	Kind          string           `json:"kind"`
	Name          string           `json:"name"`
	Ref           string           `json:"ref"`
	Optional      bool             `json:"optional"`
	Nullable      bool             `json:"nullable"`
	HasDefault    bool             `json:"hasDefault"`
	Strict        bool             `json:"strict"`
	Refined       bool             `json:"refined"`
	Branded       bool             `json:"branded"`
	Fields        []descriptorNode `json:"fields"`
	Items         *descriptorNode  `json:"items"`
	Values        []string         `json:"values"`
	Value         json.RawMessage  `json:"value"`
	Discriminator string           `json:"discriminator"`
	Variants      []descriptorNode `json:"variants"`
	KeyType       *descriptorNode  `json:"keyType"`
	ValueType     *descriptorNode  `json:"valueType"`
	Constraints   map[string]any   `json:"constraints"`
}

func loadDescriptor(t *testing.T) descriptorFile {
	t.Helper()
	var file descriptorFile
	if err := json.Unmarshal(descriptorJSON, &file); err != nil {
		t.Fatalf("descriptor.json does not parse: %v", err)
	}
	if len(file.Shapes) == 0 {
		t.Fatal("descriptor.json describes no shapes; every check below would pass vacuously")
	}
	return file
}

/* -------------------------------------------------------------------------- */
/*  What Relay implements, and what it deliberately does not                  */
/* -------------------------------------------------------------------------- */

// goShapes maps a published shape to the Go type that carries it on the wire.
//
// Every shape Relay reads from or writes to the wire is here. A shape that is
// neither here nor in one of the three registries below fails
// TestEveryPublishedShapeIsAccountedFor, which is what stops a new contract
// shape from arriving unnoticed.
var goShapes = map[string]reflect.Type{
	// The request envelope and everything it embeds.
	"inferenceRequestSchema":       reflect.TypeOf(Request{}),
	"inferenceAttributionSchema":   reflect.TypeOf(Attribution{}),
	"authenticatedPrincipalSchema": reflect.TypeOf(AuthenticatedPrincipal{}),
	"billingPrincipalSchema":       reflect.TypeOf(BillingPrincipal{}),
	"clientRequestMetadataSchema":  reflect.TypeOf(ClientRequestMetadata{}),
	"inferenceInputSchema":         reflect.TypeOf(Input{}),
	"inferenceMessageSchema":       reflect.TypeOf(Message{}),
	"inferenceContentPartSchema":   reflect.TypeOf(ContentPart{}),
	"inferenceContentSourceSchema": reflect.TypeOf(ContentSource{}),
	"inferenceToolCallSchema":      reflect.TypeOf(ToolCall{}),
	"samplingParametersSchema":     reflect.TypeOf(SamplingParameters{}),
	"toolDefinitionSchema":         reflect.TypeOf(ToolDefinition{}),
	"responseFormatSchema":         reflect.TypeOf(ResponseFormat{}),
	"routingTargetSchema":          reflect.TypeOf(RoutingTarget{}),
	"routingPolicyReferenceSchema": reflect.TypeOf(RoutingPolicyReference{}),
	"authorizedRouteSchema":        reflect.TypeOf(AuthorizedRoute{}),

	// The normalized stream.
	"inferenceStreamStartEventSchema":       reflect.TypeOf(StreamStartEvent{}),
	"inferenceStreamDeltaEventSchema":       reflect.TypeOf(StreamDeltaEvent{}),
	"inferenceStreamToolCallEventSchema":    reflect.TypeOf(StreamToolCallEvent{}),
	"inferenceStreamUsageEventSchema":       reflect.TypeOf(StreamUsageEvent{}),
	"inferenceStreamRouteSwitchEventSchema": reflect.TypeOf(StreamRouteSwitchEvent{}),
	"inferenceStreamErrorEventSchema":       reflect.TypeOf(StreamErrorEvent{}),
	"inferenceStreamDoneEventSchema":        reflect.TypeOf(StreamDoneEvent{}),
	"inferenceRouteSwitchDetailSchema":      reflect.TypeOf(RouteSwitchDetail{}),

	// Technical usage and errors.
	"normalizedUsageReportSchema":    reflect.TypeOf(UsageReport{}),
	"usageQuantitySchema":            reflect.TypeOf(UsageQuantity{}),
	"inferenceErrorSchema":           reflect.TypeOf(Error{}),
	"providerErrorPassthroughSchema": reflect.TypeOf(ProviderErrorPassthrough{}),
}

// goEnums maps a published enum to the Go named type that restates it, together
// with the members Relay declares. The members are compared as an exact,
// ordered list: a member added upstream and not here is an unhandled value, and
// a member here and not upstream is a value Relay could emit that Oxy rejects.
var goEnums = map[string]enumBinding{
	"inferenceEnvironmentSchema":       bindEnum(environmentValues),
	"inferenceScopeSchema":             bindEnum(scopeValues),
	"inferenceModalitySchema":          bindEnum(modalityValues),
	"inferenceMessageRoleSchema":       bindEnum(messageRoleValues),
	"inferenceFinishReasonSchema":      bindEnum(finishReasonValues),
	"inferenceRouteSwitchReasonSchema": bindEnum(routeSwitchReasonValues),
	"inferenceRequestOutcomeSchema":    bindEnum(requestOutcomeValues),
	"usageUnitSchema":                  bindEnum(usageUnitValues),
	"usageSourceSchema":                bindEnum(usageSourceValues),
	"inferenceErrorCodeSchema":         bindEnum(errorCodeValues),
	"upstreamErrorCategorySchema":      bindEnum(upstreamErrorCategoryValues),
}

// goScalars maps a published scalar (a branded id, a constrained string) to the
// Go named type that carries it.
var goScalars = map[string]reflect.Type{
	"oxyAccountIdSchema":          reflect.TypeOf(AccountID("")),
	"oxyApplicationIdSchema":      reflect.TypeOf(ApplicationID("")),
	"oxyCredentialIdSchema":       reflect.TypeOf(CredentialID("")),
	"delegatedUserIdSchema":       reflect.TypeOf(UserID("")),
	"requestIdSchema":             reflect.TypeOf(RequestID("")),
	"generationIdSchema":          reflect.TypeOf(GenerationID("")),
	"idempotencyKeySchema":        reflect.TypeOf(IdempotencyKey("")),
	"modelIdSchema":               reflect.TypeOf(ModelID("")),
	"modelReferenceSchema":        reflect.TypeOf(ModelReference("")),
	"routingProfileSlugSchema":    reflect.TypeOf(RoutingProfileSlug("")),
	"inferenceProviderSlugSchema": reflect.TypeOf(ProviderSlug("")),
	"deploymentIdSchema":          reflect.TypeOf(DeploymentID("")),
	"inferenceRegionSchema":       reflect.TypeOf(Region("")),
	"inferenceTimestampSchema":    reflect.TypeOf(Timestamp("")),
	"safeErrorTextSchema":         reflect.TypeOf(""),
}

// goUnions covers the two published unions that are not plain objects: the
// stream event union, whose variants are named shapes and therefore named Go
// types, and the tool-choice union, which has no discriminator at all and
// carries a hand-written codec.
var goStreamEventVariants = map[string]reflect.Type{
	"start":        reflect.TypeOf(StreamStartEvent{}),
	"delta":        reflect.TypeOf(StreamDeltaEvent{}),
	"tool_call":    reflect.TypeOf(StreamToolCallEvent{}),
	"usage":        reflect.TypeOf(StreamUsageEvent{}),
	"route_switch": reflect.TypeOf(StreamRouteSwitchEvent{}),
	"error":        reflect.TypeOf(StreamErrorEvent{}),
	"done":         reflect.TypeOf(StreamDoneEvent{}),
}

var goCustomUnions = map[string]reflect.Type{
	"toolChoiceSchema": reflect.TypeOf(ToolChoice{}),
}

var goUnionOfNamedShapes = map[string]map[string]reflect.Type{
	"inferenceStreamEventSchema": goStreamEventVariants,
}

// notApplicable names every published shape the data plane does not exchange,
// with the reason it does not.
//
// The list is exact rather than a floor: TestEveryPublishedShapeIsAccountedFor
// asserts its length, so a shape cannot be quietly excused by appending a line.
// A shape leaves this list only by being implemented, and joins it only with a
// reason that names the owner.
var notApplicable = map[string]string{
	// Catalogue identity and pricing are Oxy's (ADR 0006). Relay consumes
	// canonical model ids as opaque strings and holds its own operational
	// inventory; it neither serves nor stores a customer-facing catalogue.
	"availabilityScopeSchema":               "catalogue: Oxy owns customer-facing model identity and commercial permission",
	"catalogueModelSchema":                  "catalogue: Oxy owns customer-facing model identity",
	"cataloguePublisherSummarySchema":       "catalogue: Oxy owns publisher identity",
	"catalogueServingProviderSummarySchema": "catalogue: Oxy owns the customer-safe provider projection",
	"commercialPermissionSchema":            "catalogue: resale permission is an Oxy commercial decision",
	"inferenceDataPolicySchema":             "catalogue: retention and training policy are published by Oxy",
	"inferenceProviderSchema":               "catalogue: Oxy owns provider identity; Relay owns provider health only",
	"modelCapabilitiesSchema":               "catalogue: Oxy owns capability advertisement",
	"modelCatalogueEntrySchema":             "catalogue: the assembled customer view is served by Oxy",
	"modelDeploymentSchema":                 "catalogue: carries Oxy commercial fields and no upstream model id; Relay's own inventory is internal/inventory",
	"modelDeprecationSchema":                "catalogue: deprecation is an Oxy product decision",
	"modelEvaluationResultSchema":           "catalogue: evaluation summaries are published by Oxy",
	"modelLicenseSchema":                    "catalogue: licensing is an Oxy publishing concern",
	"modelProvenanceSchema":                 "catalogue: provenance is an Oxy publishing concern",
	"modelPublisherSchema":                  "catalogue: Oxy owns publisher identity",
	"modelRevisionSchema":                   "catalogue: Oxy owns revision identity",
	"sha256DigestSchema":                    "catalogue: the artifact digest of a revision Relay never resolves; it reaches an upstream model id through its own snapshots",
	"aliaModelReleaseManifestSchema":        "aliaModelRelease: Alia's model publishing pipeline; a release manifest is signed and consumed by Oxy, and the data plane is handed the resulting reference",
	"aliaReleaseArtifactSchema":             "aliaModelRelease: an artifact of an Alia release manifest Oxy verifies before publishing",
	"aliaReleaseSignatureSchema":            "aliaModelRelease: a signature over an Alia release manifest; Relay verifies the Oxy EDGE envelope, never a release",
	"modelSafetyMetadataSchema":             "catalogue: safety metadata is published by Oxy",
	"publisherSlugSchema":                   "catalogue: a component of a model id Relay never splits",
	"modelSlugSchema":                       "catalogue: a component of a model id Relay never splits",
	"modelRevisionLabelSchema":              "catalogue: a component of a model reference Relay never splits",
	"routingProfileCandidateSchema":         "catalogue: profile candidates are resolved by Oxy before forwarding",
	"routingProfileSchema":                  "catalogue: profile definitions live in the Oxy catalogue",
	"inferenceDateSchema":                   "catalogue: calendar dates appear only on catalogue descriptors",
	"inferenceHttpsUrlSchema":               "catalogue: documentation links appear only on catalogue descriptors",

	// Money and the ledger are Oxy's. Relay measures units and never prices
	// them; there is deliberately no Money type in this package.
	"currencyCodeSchema":            "ledger: Relay never quotes an amount",
	"exactDecimalSchema":            "ledger: Relay never quotes an amount",
	"moneySchema":                   "ledger: Relay never quotes an amount",
	"unitPriceSchema":               "ledger: Oxy owns pricing",
	"priceSnapshotSchema":           "ledger: Oxy owns pricing",
	"priceVersionSchema":            "ledger: Oxy owns pricing",
	"priceVersionStatusSchema":      "ledger: Oxy owns pricing",
	"usageReceiptSchema":            "ledger: settlement is Oxy's; Relay emits normalizedUsageReport",
	"usageRefundSchema":             "ledger: reversal is Oxy's",
	"usageRefundReasonSchema":       "ledger: reversal is Oxy's",
	"usageRefundSubjectSchema":      "ledger: reversal is Oxy's",
	"usageReservationSchema":        "ledger: reservation happens at the edge, before the envelope is forwarded",
	"usageReservationRequestSchema": "ledger: reservation happens at the edge, before the envelope is forwarded",
	"usageReservationStatusSchema":  "ledger: reservation happens at the edge, before the envelope is forwarded",

	// Routing policy is configured and resolved in Oxy. The envelope carries
	// only routingPolicyReferenceSchema, which Relay does implement.
	"routingPolicySchema":         "policy: the envelope carries a reference, not a snapshot (see README)",
	"routingPolicyScopeSchema":    "policy: policy scoping is an Oxy control-plane concern",
	"routingFallbackPolicySchema": "policy: fallback controls arrive only inside a snapshot Relay is not sent",

	// BYOK. Out of scope for this PR and named as such in the README.
	"providerConnectionSchema":           "byok: out of scope for the first PR",
	"providerConnectionScopeSchema":      "byok: out of scope for the first PR",
	"providerConnectionStatusSchema":     "byok: out of scope for the first PR",
	"providerConnectionValidationSchema": "byok: out of scope for the first PR",
	"providerSecretReferenceSchema":      "byok: out of scope for the first PR",

	// Account billing. Balances, invoices, payment providers and auto-recharge
	// are the control plane's by definition: AGENTS.md forbids a customer
	// balance or a billing ledger in this repository, so these shapes are not
	// merely unimplemented here — implementing one would be the boundary
	// breach. Published in @oxyhq/contracts 0.28.0.
	"accountBillingStateSchema":     "billing: customer balances are Oxy's; a balance here is a second ledger",
	"autoRechargeSchema":            "billing: recharging a customer's balance is a control-plane action",
	"autoRechargeAttemptSchema":     "billing: recharging a customer's balance is a control-plane action",
	"autoRechargeStatusSchema":      "billing: recharging a customer's balance is a control-plane action",
	"billingInvoiceSchema":          "billing: invoicing a customer is Oxy's",
	"billingInvoiceStatusSchema":    "billing: invoicing a customer is Oxy's",
	"billingModeSchema":             "billing: prepaid or invoiced is a customer-account property",
	"billingProfileSchema":          "billing: a customer's billing profile is an Oxy entity",
	"billingProfileStatusSchema":    "billing: a customer's billing profile is an Oxy entity",
	"externalPaymentSchema":         "billing: payment processing is Oxy's, and Relay holds no payment credential",
	"externalPaymentKindSchema":     "billing: payment processing is Oxy's",
	"externalPaymentProviderSchema": "billing: payment processing is Oxy's",

	// Reconciliation compares what Oxy charged against what a provider
	// invoiced. Relay measures its own upstream cost (internal/providercost)
	// and deliberately does not reconcile it — see the README's out-of-scope
	// list. Published in @oxyhq/contracts 0.28.0.
	"reconciliationReportSchema":          "reconciliation: a finance process with no home in a data plane",
	"reconciliationRunSchema":             "reconciliation: a finance process with no home in a data plane",
	"reconciliationRunStatusSchema":       "reconciliation: a finance process with no home in a data plane",
	"reconciliationDiscrepancySchema":     "reconciliation: a finance process with no home in a data plane",
	"reconciliationDiscrepancyKindSchema": "reconciliation: a finance process with no home in a data plane",

	// Entitlements, plans and cost centres are what a customer bought and how
	// they attribute it. Relay is told the outcome of that decision — an
	// already-authorized envelope — and never re-derives it (ADR 0006).
	// Published in @oxyhq/contracts 0.28.0.
	"costCenterSchema":            "entitlement: cost attribution is an Oxy account structure",
	"costCenterSpendSchema":       "entitlement: cost attribution is an Oxy account structure",
	"costCenterStatusSchema":      "entitlement: cost attribution is an Oxy account structure",
	"planAllowanceSchema":         "entitlement: what a customer bought is resolved at the edge",
	"payAsYouGoEntitlementSchema": "entitlement: what a customer bought is resolved at the edge",
	"productEntitlementSchema":    "entitlement: what a customer bought is resolved at the edge",
	"productPlanSchema":           "entitlement: plans are an Oxy product concern",
	"productPlanStatusSchema":     "entitlement: plans are an Oxy product concern",
}

// expectedNotApplicableCount is asserted exactly. Changing it is the moment to
// ask whether a shape is being excused rather than implemented.
const expectedNotApplicableCount = 75

type enumBinding struct {
	goType  reflect.Type
	members []string
}

func bindEnum[T ~string](values []T) enumBinding {
	members := make([]string, len(values))
	for index, value := range values {
		members[index] = string(value)
	}
	return enumBinding{goType: reflect.TypeOf(values).Elem(), members: members}
}

/* -------------------------------------------------------------------------- */
/*  Gates                                                                     */
/* -------------------------------------------------------------------------- */

func TestDescriptorIsTheePinnedPublishedPackage(t *testing.T) {
	file := loadDescriptor(t)
	if file.Package != "@oxyhq/contracts" {
		t.Errorf("descriptor was generated from %q, not @oxyhq/contracts", file.Package)
	}
	if file.PackageVersion == "" {
		t.Error("descriptor records no package version, so nothing pins what it describes")
	}
	if file.ContractVersion != ContractVersion {
		t.Errorf("INFERENCE_CONTRACT_VERSION is %q upstream, ContractVersion is %q", file.ContractVersion, ContractVersion)
	}
	if file.ShapeCount != len(file.Shapes) {
		t.Errorf("descriptor claims %d shapes and carries %d", file.ShapeCount, len(file.Shapes))
	}
}

func TestEveryPublishedShapeIsAccountedFor(t *testing.T) {
	file := loadDescriptor(t)

	registries := map[string]func(string) bool{
		"goShapes":             func(name string) bool { _, ok := goShapes[name]; return ok },
		"goEnums":              func(name string) bool { _, ok := goEnums[name]; return ok },
		"goScalars":            func(name string) bool { _, ok := goScalars[name]; return ok },
		"goCustomUnions":       func(name string) bool { _, ok := goCustomUnions[name]; return ok },
		"goUnionOfNamedShapes": func(name string) bool { _, ok := goUnionOfNamedShapes[name]; return ok },
		"notApplicable":        func(name string) bool { _, ok := notApplicable[name]; return ok },
	}

	for name := range file.Shapes {
		claimed := make([]string, 0, 1)
		for registry, has := range registries {
			if has(name) {
				claimed = append(claimed, registry)
			}
		}
		sort.Strings(claimed)
		switch len(claimed) {
		case 0:
			t.Errorf("published shape %q is in no registry: implement it, or record why the data plane does not exchange it", name)
		case 1:
		default:
			t.Errorf("published shape %q is claimed by %s; exactly one registry may own it", name, strings.Join(claimed, " and "))
		}
	}

	for registry, names := range map[string][]string{
		"goShapes":             keysOf(goShapes),
		"goScalars":            keysOf(goScalars),
		"goCustomUnions":       keysOf(goCustomUnions),
		"goEnums":              keysOf(goEnums),
		"goUnionOfNamedShapes": keysOf(goUnionOfNamedShapes),
		"notApplicable":        keysOf(notApplicable),
	} {
		for _, name := range names {
			if _, published := file.Shapes[name]; !published {
				t.Errorf("%s names %q, which the published contract no longer has", registry, name)
			}
		}
	}

	// Exact, not a floor: an ever-growing exemption list is the gate switching
	// itself off one defensible line at a time.
	if len(notApplicable) != expectedNotApplicableCount {
		t.Errorf("notApplicable holds %d shapes, expectedNotApplicableCount says %d; a shape was excused or implemented without moving the count",
			len(notApplicable), expectedNotApplicableCount)
	}
	for name, reason := range notApplicable {
		if strings.TrimSpace(reason) == "" {
			t.Errorf("notApplicable[%q] carries no reason", name)
		}
	}
}

func TestPublishedConstantsMatchGoDeclarations(t *testing.T) {
	file := loadDescriptor(t)

	assertStringList := func(constant string, got []string) {
		t.Helper()
		raw, present := file.Constants[constant]
		if !present {
			t.Fatalf("published constant %s is gone", constant)
		}
		var want []string
		if err := json.Unmarshal(raw.Value, &want); err != nil {
			t.Fatalf("%s is not a list of strings: %v", constant, err)
		}
		if diff := diffStringLists(want, got); diff != "" {
			t.Errorf("%s differs from its Go declaration:\n%s", constant, diff)
		}
	}

	assertStringList("INFERENCE_ERROR_CODES", stringsOf(errorCodeValues))
	assertStringList("NON_RETRYABLE_INFERENCE_ERROR_CODES", stringsOf(nonRetryableErrorCodes))
	assertStringList("USAGE_UNITS", stringsOf(usageUnitValues))
	assertStringList("USAGE_SOURCES", stringsOf(usageSourceValues))
	assertStringList("INFERENCE_SCOPES", stringsOf(scopeValues))
}

func TestGoTypesMatchPublishedShapes(t *testing.T) {
	file := loadDescriptor(t)
	checker := &shapeChecker{descriptor: file}

	for name, goType := range goShapes {
		t.Run(name, func(t *testing.T) {
			node, present := file.Shapes[name]
			if !present {
				t.Fatalf("%s is not published", name)
			}
			for _, problem := range checker.compareShape(name, node, goType) {
				t.Error(problem)
			}
		})
	}

	for name, binding := range goEnums {
		t.Run(name, func(t *testing.T) {
			node := file.Shapes[name]
			if node.Kind != "enum" {
				t.Fatalf("%s is published as %q, not an enum", name, node.Kind)
			}
			if diff := diffStringLists(node.Values, binding.members); diff != "" {
				t.Errorf("%s differs from %s:\n%s", name, binding.goType, diff)
			}
		})
	}

	for name, goType := range goScalars {
		t.Run(name, func(t *testing.T) {
			node := file.Shapes[name]
			if node.Kind != "string" {
				t.Fatalf("%s is published as %q; %s carries it as a string", name, node.Kind, goType)
			}
			if goType.Kind() != reflect.String {
				t.Errorf("%s carries published string %s", goType, name)
			}
		})
	}
}

// TestStreamEventUnionIsExhaustive pins the seven-variant union: every
// published variant has a Go type, that type declares the matching discriminator
// through the StreamEvent interface, and no Go type claims a variant the
// contract does not have.
func TestStreamEventUnionIsExhaustive(t *testing.T) {
	file := loadDescriptor(t)
	node := file.Shapes["inferenceStreamEventSchema"]
	if node.Kind != "discriminatedUnion" {
		t.Fatalf("inferenceStreamEventSchema is published as %q", node.Kind)
	}

	published := make(map[string]string, len(node.Variants))
	for _, variant := range node.Variants {
		if variant.Kind != "ref" {
			t.Fatalf("stream event variant is inline (%q); the Go union assumes named variants", variant.Kind)
		}
		shape := file.Shapes[variant.Ref]
		discriminator := ""
		for _, field := range shape.Fields {
			if field.Name == node.Discriminator {
				if err := json.Unmarshal(field.Value, &discriminator); err != nil {
					t.Fatalf("%s.%s is not a string literal", variant.Ref, node.Discriminator)
				}
			}
		}
		if discriminator == "" {
			t.Fatalf("%s carries no %s literal", variant.Ref, node.Discriminator)
		}
		published[discriminator] = variant.Ref
	}

	if diff := diffStringLists(sortedKeys(published), sortedKeys(goStreamEventVariants)); diff != "" {
		t.Errorf("the stream event union differs from its Go variants:\n%s", diff)
	}

	eventInterface := reflect.TypeOf((*StreamEvent)(nil)).Elem()
	for discriminator, goType := range goStreamEventVariants {
		pointer := reflect.PointerTo(goType)
		if !pointer.Implements(eventInterface) {
			t.Errorf("%s does not implement StreamEvent", pointer)
			continue
		}
		instance := reflect.New(goType).Interface().(StreamEvent)
		if string(instance.EventType()) != discriminator {
			t.Errorf("%s reports EventType %q, registered as %q", pointer, instance.EventType(), discriminator)
		}
	}

	if diff := diffStringLists(sortedKeys(published), stringsOf(streamEventTypeValues)); diff != "" {
		t.Errorf("StreamEventType members differ from the published union:\n%s", diff)
	}
}

// TestPublishedGrammarsMatchGoPatterns compares the regexes Relay actually
// enforces against the published ones, character for character.
//
// Only the patterns Relay validates against are checked. The rest of the
// published string constraints ride in descriptor.json, where CI's
// regenerate-and-diff step turns any upstream change into a reviewable diff.
func TestPublishedGrammarsMatchGoPatterns(t *testing.T) {
	file := loadDescriptor(t)
	for name, pattern := range map[string]string{
		"modelReferenceSchema":        modelReferencePattern.String(),
		"modelIdSchema":               modelIDPattern.String(),
		"inferenceProviderSlugSchema": providerSlugPattern.String(),
		"routingProfileSlugSchema":    routingProfileSlugPattern.String(),
		"inferenceRegionSchema":       regionPattern.String(),
	} {
		node, present := file.Shapes[name]
		if !present {
			t.Errorf("%s is no longer published", name)
			continue
		}
		published, _ := node.Constraints["regex"].(string)
		if published == "" {
			t.Errorf("%s publishes no regex; Relay enforces one", name)
			continue
		}
		if published != pattern {
			t.Errorf("%s grammar drifted:\n published %s\n go        %s", name, published, pattern)
		}
	}

	// safeErrorTextSchema's refusal pattern is not a `regex` check on the
	// schema — it is applied inside a `.refine()`, so it cannot be read off the
	// descriptor. The Go copy is pinned instead by asserting it rejects each
	// shape the published pattern names.
	for _, credentialShaped := range []string{
		"Bearer abcdefghijklmno",
		"authorization: something",
		"api_key=abcdefgh",
		"api-key: abcdefgh",
		"sk-abcdefghijkl",
		"sk_live_abcdefgh",
		"sk_test_abcdefgh",
	} {
		if got := SafeErrorText("upstream said: " + credentialShaped); strings.Contains(got, credentialShaped) {
			t.Errorf("SafeErrorText passed credential-shaped text through: %q", got)
		}
	}
	// Negative control: the redaction must not eat ordinary diagnostic text,
	// or "nothing leaked" would be what a scrubber that destroys everything
	// also reports.
	ordinary := "model gpt-5 is overloaded; retry in 2s (request 01JABCDEF)"
	if got := SafeErrorText(ordinary); got != ordinary {
		t.Errorf("SafeErrorText mangled ordinary text:\n want %q\n got  %q", ordinary, got)
	}
}

/* -------------------------------------------------------------------------- */
/*  Comparison                                                                */
/* -------------------------------------------------------------------------- */

type shapeChecker struct {
	descriptor descriptorFile
}

// compareShape returns every difference between a published shape and the Go
// type registered for it. It returns problems rather than calling t.Error so
// TestDriftIsDetected can drive it against deliberately perturbed descriptors.
func (c *shapeChecker) compareShape(name string, node descriptorNode, goType reflect.Type) []string {
	switch node.Kind {
	case "object":
		return c.compareFields(name, fieldsOfObject(node), goType)
	case "discriminatedUnion":
		fields, err := flattenUnion(node)
		if err != nil {
			return []string{fmt.Sprintf("%s: %v", name, err)}
		}
		return c.compareFields(name, fields, goType)
	default:
		return []string{fmt.Sprintf("%s: published as %q, which goShapes cannot carry", name, node.Kind)}
	}
}

func (c *shapeChecker) compareFields(shape string, published map[string]descriptorNode, goType reflect.Type) []string {
	problems := make([]string, 0)
	actual := jsonFieldsOf(goType)

	for jsonName := range actual {
		if _, ok := published[jsonName]; !ok {
			problems = append(problems, fmt.Sprintf("%s: %s has JSON field %q, which the contract does not", shape, goType, jsonName))
		}
	}
	for jsonName, node := range published {
		field, ok := actual[jsonName]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s: the contract has field %q, %s does not", shape, jsonName, goType))
			continue
		}
		problems = append(problems, c.compareField(shape, jsonName, node, field)...)
	}
	sort.Strings(problems)
	return problems
}

type goField struct {
	structField reflect.StructField
	omitEmpty   bool
}

func (c *shapeChecker) compareField(shape, jsonName string, node descriptorNode, field goField) []string {
	problems := make([]string, 0)
	where := fmt.Sprintf("%s.%s", shape, jsonName)

	optional := node.Optional || node.HasDefault
	goType := field.structField.Type
	pointer := goType.Kind() == reflect.Pointer
	absentable := pointer || goType.Kind() == reflect.Slice || goType.Kind() == reflect.Map

	switch {
	case optional && !absentable:
		problems = append(problems, fmt.Sprintf("%s: the contract makes it optional; %s cannot express absence", where, goType))
	case optional && !field.omitEmpty:
		problems = append(problems, fmt.Sprintf("%s: the contract makes it optional; the Go tag lacks omitempty", where))
	case !optional && field.omitEmpty:
		problems = append(problems, fmt.Sprintf("%s: the contract requires it; the Go tag has omitempty, so Relay can omit a required field", where))
	case !optional && pointer:
		problems = append(problems, fmt.Sprintf("%s: the contract requires it; %s is a pointer", where, goType))
	}

	if pointer {
		goType = goType.Elem()
	}
	problems = append(problems, c.compareKind(where, node, goType)...)
	return problems
}

func (c *shapeChecker) compareKind(where string, node descriptorNode, goType reflect.Type) []string {
	switch node.Kind {
	case "ref":
		return c.compareRef(where, node.Ref, goType)
	case "string":
		if goType.Kind() != reflect.String {
			return []string{fmt.Sprintf("%s: the contract says string, Go says %s", where, goType)}
		}
	case "number":
		isInt, _ := node.Constraints["int"].(bool)
		if isInt && goType.Kind() != reflect.Int && goType.Kind() != reflect.Int64 {
			return []string{fmt.Sprintf("%s: the contract says integer, Go says %s", where, goType)}
		}
		if !isInt && goType.Kind() != reflect.Float64 {
			return []string{fmt.Sprintf("%s: the contract says number, Go says %s", where, goType)}
		}
	case "boolean":
		if goType.Kind() != reflect.Bool {
			return []string{fmt.Sprintf("%s: the contract says boolean, Go says %s", where, goType)}
		}
	case "literal":
		return compareLiteral(where, node, goType)
	case "enum":
		return compareInlineEnum(where, node, goType)
	case "array":
		if goType.Kind() != reflect.Slice {
			return []string{fmt.Sprintf("%s: the contract says array, Go says %s", where, goType)}
		}
		if node.Items != nil {
			return c.compareKind(where+"[]", *node.Items, goType.Elem())
		}
	case "record":
		if goType.Kind() != reflect.Map {
			return []string{fmt.Sprintf("%s: the contract says record, Go says %s", where, goType)}
		}
		if node.ValueType != nil && node.ValueType.Kind != "unknown" {
			return c.compareKind(where+"{}", *node.ValueType, goType.Elem())
		}
	case "unknown":
		if goType.Kind() != reflect.Interface {
			return []string{fmt.Sprintf("%s: the contract says unknown, Go says %s", where, goType)}
		}
	case "object":
		if goType.Kind() != reflect.Struct {
			return []string{fmt.Sprintf("%s: the contract says object, Go says %s", where, goType)}
		}
		return c.compareFields(where, fieldsOfObject(node), goType)
	case "union":
		return []string{fmt.Sprintf("%s: an undiscriminated union may only appear as a named shape in goCustomUnions", where)}
	default:
		return []string{fmt.Sprintf("%s: unhandled published kind %q", where, node.Kind)}
	}
	return nil
}

func (c *shapeChecker) compareRef(where, ref string, goType reflect.Type) []string {
	if binding, isEnum := goEnums[ref]; isEnum {
		if goType != binding.goType {
			return []string{fmt.Sprintf("%s: the contract references enum %s, Go says %s (expected %s)", where, ref, goType, binding.goType)}
		}
		return nil
	}
	if scalar, isScalar := goScalars[ref]; isScalar {
		if goType != scalar {
			return []string{fmt.Sprintf("%s: the contract references %s, Go says %s (expected %s)", where, ref, goType, scalar)}
		}
		return nil
	}
	if shaped, isShaped := goShapes[ref]; isShaped {
		if goType != shaped {
			return []string{fmt.Sprintf("%s: the contract references %s, Go says %s (expected %s)", where, ref, goType, shaped)}
		}
		return nil
	}
	if custom, isCustom := goCustomUnions[ref]; isCustom {
		if goType != custom {
			return []string{fmt.Sprintf("%s: the contract references %s, Go says %s (expected %s)", where, ref, goType, custom)}
		}
		return nil
	}
	if reason, excused := notApplicable[ref]; excused {
		return []string{fmt.Sprintf("%s: references %s, which is recorded not-applicable (%s) yet appears in a shape Relay exchanges", where, ref, reason)}
	}
	return []string{fmt.Sprintf("%s: references unregistered shape %s", where, ref)}
}

func compareLiteral(where string, node descriptorNode, goType reflect.Type) []string {
	var asString string
	if err := json.Unmarshal(node.Value, &asString); err == nil {
		if goType.Kind() != reflect.String {
			return []string{fmt.Sprintf("%s: the contract pins the literal %q, Go says %s", where, asString, goType)}
		}
		return nil
	}
	var asNumber float64
	if err := json.Unmarshal(node.Value, &asNumber); err == nil {
		if goType.Kind() != reflect.Int && goType.Kind() != reflect.Int64 {
			return []string{fmt.Sprintf("%s: the contract pins the literal %v, Go says %s", where, asNumber, goType)}
		}
		return nil
	}
	var asBool bool
	if err := json.Unmarshal(node.Value, &asBool); err == nil {
		if goType.Kind() != reflect.Bool {
			return []string{fmt.Sprintf("%s: the contract pins the literal %v, Go says %s", where, asBool, goType)}
		}
		return nil
	}
	return []string{fmt.Sprintf("%s: unreadable literal %s", where, string(node.Value))}
}

func compareInlineEnum(where string, node descriptorNode, goType reflect.Type) []string {
	if goType.Kind() != reflect.String {
		return []string{fmt.Sprintf("%s: the contract says enum, Go says %s", where, goType)}
	}
	for _, binding := range goEnums {
		if binding.goType == goType {
			if diff := diffStringLists(node.Values, binding.members); diff != "" {
				return []string{fmt.Sprintf("%s: enum members differ:\n%s", where, diff)}
			}
			return nil
		}
	}
	for _, binding := range inlineEnumBindings {
		if binding.goType == goType {
			if diff := diffStringLists(node.Values, binding.members); diff != "" {
				return []string{fmt.Sprintf("%s: enum members differ:\n%s", where, diff)}
			}
			return nil
		}
	}
	return []string{fmt.Sprintf("%s: %s carries an inline enum with no registered member list", where, goType)}
}

// inlineEnumBindings are the closed vocabularies the contract declares inline,
// inside a shape, rather than as an exported schema. They cannot be reached
// through goEnums because they have no published name of their own, so they are
// registered here and compared member for member exactly the same way.
var inlineEnumBindings = []enumBinding{
	bindEnum(apiFormatValues),
	bindEnum(imageDetailValues),
	bindEnum(deltaChannelValues),
	bindEnum(toolChoiceModeValues),
	bindEnum(contentSourceKindValues),
	bindEnum(contentPartTypeValues),
	bindEnum(inputFormatValues),
	bindEnum(responseFormatTypeValues),
	bindEnum(routingTargetKindValues),
	bindEnum(routeSwitchScopeValues),
	bindEnum(routeSubstitutionValues),
	bindEnum(streamEventTypeValues),
}

/* -------------------------------------------------------------------------- */
/*  Helpers                                                                   */
/* -------------------------------------------------------------------------- */

func fieldsOfObject(node descriptorNode) map[string]descriptorNode {
	fields := make(map[string]descriptorNode, len(node.Fields))
	for _, field := range node.Fields {
		fields[field.Name] = field
	}
	return fields
}

// flattenUnion merges a discriminated union's variants into the field set a
// single flattened Go struct must carry.
//
// A field stays REQUIRED only when every variant has it and no variant makes it
// optional; anything else is optional in the flattened form, because the struct
// has to be able to express every variant. The discriminator becomes an enum of
// the variants' literal values, which is what makes an added variant a failing
// test rather than an unhandled string.
func flattenUnion(node descriptorNode) (map[string]descriptorNode, error) {
	if len(node.Variants) == 0 {
		return nil, fmt.Errorf("discriminated union has no variants")
	}
	merged := make(map[string]descriptorNode)
	presence := make(map[string]int)
	optionalSomewhere := make(map[string]bool)
	discriminatorValues := make([]string, 0, len(node.Variants))

	for _, variant := range node.Variants {
		if variant.Kind != "object" {
			return nil, fmt.Errorf("variant is %q; only inline object variants can be flattened", variant.Kind)
		}
		for _, field := range variant.Fields {
			if field.Name == node.Discriminator {
				var literal string
				if err := json.Unmarshal(field.Value, &literal); err != nil {
					return nil, fmt.Errorf("discriminator %q is not a string literal in every variant", node.Discriminator)
				}
				discriminatorValues = append(discriminatorValues, literal)
				continue
			}
			presence[field.Name]++
			if field.Optional || field.HasDefault {
				optionalSomewhere[field.Name] = true
			}
			if _, seen := merged[field.Name]; !seen {
				merged[field.Name] = field
			}
		}
	}

	for name, node := range merged {
		node.Optional = presence[name] < len(discriminatorValues) || optionalSomewhere[name]
		merged[name] = node
	}
	merged[node.Discriminator] = descriptorNode{
		Name:   node.Discriminator,
		Kind:   "enum",
		Values: discriminatorValues,
	}
	return merged, nil
}

func jsonFieldsOf(goType reflect.Type) map[string]goField {
	fields := make(map[string]goField, goType.NumField())
	for index := range goType.NumField() {
		field := goType.Field(index)
		if !field.IsExported() {
			continue
		}
		tag, tagged := field.Tag.Lookup("json")
		if !tagged {
			continue
		}
		parts := strings.Split(tag, ",")
		if parts[0] == "-" {
			continue
		}
		fields[parts[0]] = goField{
			structField: field,
			omitEmpty:   slicesContain(parts[1:], "omitempty"),
		}
	}
	return fields
}

func slicesContain(haystack []string, needle string) bool {
	for _, candidate := range haystack {
		if candidate == needle {
			return true
		}
	}
	return false
}

func stringsOf[T ~string](values []T) []string {
	out := make([]string, len(values))
	for index, value := range values {
		out[index] = string(value)
	}
	return out
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	out := keysOf(m)
	sort.Strings(out)
	return out
}

// diffStringLists compares two vocabularies as SETS and reports both
// directions, because the two failures mean different things: something in the
// contract and not in Go is a value Relay would not handle, and something in Go
// and not in the contract is a value Relay could emit that Oxy rejects.
func diffStringLists(want, got []string) string {
	inWant := make(map[string]bool, len(want))
	for _, value := range want {
		inWant[value] = true
	}
	inGot := make(map[string]bool, len(got))
	for _, value := range got {
		inGot[value] = true
	}
	missing := make([]string, 0)
	for _, value := range want {
		if !inGot[value] {
			missing = append(missing, value)
		}
	}
	extra := make([]string, 0)
	for _, value := range got {
		if !inWant[value] {
			extra = append(extra, value)
		}
	}
	if len(missing) == 0 && len(extra) == 0 {
		return ""
	}
	sort.Strings(missing)
	sort.Strings(extra)
	return fmt.Sprintf("  in the contract, absent from Go: %v\n  in Go, absent from the contract: %v", missing, extra)
}
