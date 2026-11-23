# Workload contract v2

- The [contract schema](../src/pisces/schemas/workload-contract-2.0.schema.json)
  defines a valid batch, immutable fixture hashes, logical outputs, typed
  CSV columns, key uniqueness, nullability, order policy, and independent
  reference hashes. Required mock service calls are checked by fixture ID and
  count. The fixture files are frozen inputs; v2 does not yet infer or enforce
  arbitrary input-schema rules, deadlines, residency, or custom invariants.
- The [artifact binding schema](../src/pisces/schemas/artifact-binding-2.0.schema.json)
  binds that intent to artifact bytes, a target, fixtures, output locations,
  and expected tasks. Route is optional metadata. Artifact and fixture paths
  are relative to their owning JSON files. Bound file and output destinations
  are relative to the run workspace. Candidate-specific environment overrides
  and literal rewrites are not accepted in v2.
- The benchmark manifest records the seed, execution scope, order, time
  budget, repetitions, and hashes. It describes the standalone benchmark;
  it is not yet a general native experiment scheduler.

Use `verify_output` to check one actual output workspace, then
`compare_outputs` to check two workspaces against the same contract and
directly against each other. These APIs take loaded contracts and actual
outputs, not user-supplied pass flags. CSV header order is ignored; the
contract controls row order. A blank nullable cell represents null.
Unit labels describe the values expected by the reference; no implicit
conversion is performed. Decimal tolerances are absolute and inclusive.
Two values can both match the reference within tolerance yet fail the
direct pairwise check.

The oracle reads at most 32 MiB per CSV. `ignore` and `exact` retain the
100,000-row in-memory limit. For larger exact-value outputs, use
`row_order: key_sorted`: output and reference must both be strictly
ascending by the typed primary key, with no duplicates. The streaming
path compares every row, authenticates the reference, and allows up to
1,000,000 rows per CSV. Nonzero decimal tolerances are not supported in
this profile; two passing streams for the same contract have identical
typed rows.
Budget exhaustion or an unavailable reference is `not_verified`.
Verdicts contain hashes and reason codes, not cell values or host paths.
Verification assumes outputs and mock-service traces are collected by the
trusted runner, not asserted by the candidate. The workspace and Python
guards are not an isolation boundary.

The native runner accepts one measured v2 execution with no warm-ups.
Its output verdict is distinct from task coverage and input observations.
The runtime report remains ineligible for resource comparison and leaves
pairwise equivalence unevaluated. v1 contracts remain readable and their
shape checks are historical screening only, not v2 semantic verification.
