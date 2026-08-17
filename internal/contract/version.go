// Package contract holds Relay's Go representation of the Oxy↔data-plane
// inference contract published as `@oxyhq/contracts`.
//
// The types here are not a convenience mirror. They are the wire, and the
// authority for what the wire says is the published package, never this
// package: `descriptor.json` is generated from it by `tools/contract` and
// `contract_test.go` compares every type below against that descriptor field by
// field. A field renamed, added, removed or made optional on either side is a
// failing test, which is the only reason it is safe to write these structs by
// hand.
//
// Decoding rule, and it is deliberate: Relay does NOT reject unknown fields on
// an inbound envelope. The contract states that adding an optional field is an
// additive change that does not bump a shape's version, so a strict decoder
// would turn every additive Oxy change into a production outage. What Relay
// does reject is a `schemaVersion` it does not implement.
package contract

// ContractVersion is the version of the contract SET as a whole, exchanged in
// the health handshake so the two sides establish they were built against
// compatible definitions before a single request is served.
//
// Asserted against the published package's INFERENCE_CONTRACT_VERSION by
// contract_test.go, so bumping the pinned package without revisiting this
// constant fails the build.
const ContractVersion = "1.2.0"

// SchemaVersion is the per-shape version every whole-message shape in this
// contract currently carries. Each shape declares it as a literal, so a
// producer running ahead of a consumer fails at the parse instead of being
// reinterpreted.
const SchemaVersion = 1

// RequestEnvelopeVersion is the schemaVersion of the Oxy→Relay request envelope
// this build implements. An envelope carrying any other value is refused whole,
// before any field of it is read: a partially understood envelope is how a
// routing or spend constraint gets silently dropped.
const RequestEnvelopeVersion = 1
