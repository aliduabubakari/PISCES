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
    "UnifiedEnergyEvaluator",
]
