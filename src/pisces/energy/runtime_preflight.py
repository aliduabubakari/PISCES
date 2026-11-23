"""Shared execution policy for the controller and runtime worker."""

from __future__ import annotations


RUNTIME_SUPPORTED = {"airflow", "prefect", "dagster"}
ADAPTERS = {"auto", "native", "surrogate_benchmark", "representative"}
RUNTIME_MODES = {"sample", "zero_fill", "fuzz_fill", "operator_profile", "scale_probe", "synthetic"}


def runtime_policy_error(
    *, mode: str, adapter: str, target: str, experiment: bool,
    has_contract: bool, has_binding: bool, scale_factor: float,
    qualified_oci_target: str | None = None,
) -> str | None:
    if adapter not in ADAPTERS:
        return "unknown_execution_adapter"
    if experiment:
        if adapter != "native":
            return "experiment_requires_explicit_native_adapter"
        if target != "airflow" and not (qualified_oci_target == target and target in {"prefect", "dagster"}):
            return "experiment_target_not_supported"
        if mode not in {"sample", "zero_fill", "fuzz_fill"}:
            return "experiment_requires_explicit_local_input_mode"
        if not (has_contract and has_binding):
            return "experiment_requires_workload_contract_and_binding"
        if type(scale_factor) not in {int, float} or scale_factor != 1:
            return "experiment_requires_unscaled_carbon"
    if adapter == "native":
        if mode not in RUNTIME_MODES:
            return "native_requires_runtime_mode"
        if target not in RUNTIME_SUPPORTED:
            return "native_target_not_supported"
        if mode == "synthetic":
            return "native_hosted_synthetic_not_supported"
        if mode in {"operator_profile", "scale_probe"}:
            return "native_diagnostic_mode_not_supported"
        if has_contract != has_binding:
            return "workload_contract_and_artifact_binding_are_both_required"
        if mode == "sample" and not has_contract:
            return "native_sample_requires_workload_contract_and_binding"
    return None
