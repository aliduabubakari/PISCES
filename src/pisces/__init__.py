"""
PISCES — Pipeline Intelligent Sustainability & Carbon Evaluation Suite.

A standalone toolkit for evaluating the energy/carbon impact and
eco-design quality of LLM-generated workflow code for orchestrators
like Airflow, Prefect, and Dagster.
"""

__version__ = "0.1.0"

__all__ = [
    "EnergyEvaluationRequest",
    "EnergyEvaluationResult",
    "evaluate_energy",
    "UnifiedEnergyConfig",
    "run_unified_energy_evaluation",
]


def __getattr__(name: str):
    if name in {"EnergyEvaluationRequest", "EnergyEvaluationResult", "evaluate_energy"}:
        from .energy.runtime_evaluation import EnergyEvaluationRequest, EnergyEvaluationResult, evaluate_energy

        values = {
            "EnergyEvaluationRequest": EnergyEvaluationRequest,
            "EnergyEvaluationResult": EnergyEvaluationResult,
            "evaluate_energy": evaluate_energy,
        }
        return values[name]

    if name in {"UnifiedEnergyConfig", "run_unified_energy_evaluation"}:
        from .unified_evaluator import UnifiedEnergyConfig, run_unified_energy_evaluation

        values = {
            "UnifiedEnergyConfig": UnifiedEnergyConfig,
            "run_unified_energy_evaluation": run_unified_energy_evaluation,
        }
        return values[name]

    raise AttributeError(f"module 'pisces' has no attribute {name!r}")
