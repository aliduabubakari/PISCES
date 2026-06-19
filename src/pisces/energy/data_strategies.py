from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol

from .zero_fill import generate_zero_fill_dataset


@dataclass
class DataStrategyResult:
    name: str
    rows: List[Dict[str, Any]]
    row_count: int
    schema: List[Dict[str, Any]]
    provenance: List[Dict[str, Any]]
    errors: List[Dict[str, Any]]
    meta: Dict[str, Any]

    @classmethod
    def from_dataset(cls, name: str, dataset: Dict[str, Any]) -> "DataStrategyResult":
        rows = dataset.get("rows") if isinstance(dataset.get("rows"), list) else []
        schema = dataset.get("schema") if isinstance(dataset.get("schema"), list) else []
        provenance = dataset.get("provenance") if isinstance(dataset.get("provenance"), list) else []
        errors = dataset.get("errors") if isinstance(dataset.get("errors"), list) else []
        meta = dataset.get("meta") if isinstance(dataset.get("meta"), dict) else {}
        return cls(
            name=name,
            rows=[row for row in rows if isinstance(row, dict)],
            row_count=int(dataset.get("row_count") or len(rows)),
            schema=[item for item in schema if isinstance(item, dict)],
            provenance=[item for item in provenance if isinstance(item, dict)],
            errors=[item for item in errors if isinstance(item, dict)],
            meta=meta,
        )

    def to_summary(self) -> Dict[str, Any]:
        return {
            "strategy": self.name,
            "row_count": self.row_count,
            "column_count": len(self.schema),
            "provenance": self.provenance,
            "errors": self.errors,
            "meta": self.meta,
        }


class DataStrategy(Protocol):
    name: str

    def generate(
        self,
        *,
        code_text: str,
        max_rows: int,
        seed: int,
        file_path: Optional[Path] = None,
        orchestrator: Optional[str] = None,
    ) -> DataStrategyResult:
        ...


class ZeroFillStrategy:
    name = "zero_fill"

    def generate(
        self,
        *,
        code_text: str,
        max_rows: int,
        seed: int,
        file_path: Optional[Path] = None,
        orchestrator: Optional[str] = None,
    ) -> DataStrategyResult:
        dataset = generate_zero_fill_dataset(
            code_text=code_text,
            max_rows=max_rows,
            seed=seed,
            file_path=file_path,
            orchestrator=orchestrator,
        )
        return DataStrategyResult.from_dataset(self.name, dataset)


def _fuzz_value(name: str, typ: str, *, row_index: int, rng: random.Random) -> Any:
    clean_name = name or "value"
    if typ == "int":
        return (row_index + 1) * rng.randint(1, 17)
    if typ == "float":
        return round((row_index + 1) * rng.uniform(0.25, 19.75), 6)
    if typ == "bool":
        return bool((row_index + rng.randint(0, 1)) % 2)
    if typ == "date":
        return f"2024-{((row_index % 12) + 1):02d}-{((row_index % 28) + 1):02d}"
    return f"{clean_name}_{row_index}_{rng.randint(1000, 9999)}"


def _schema_columns(schema: List[Dict[str, Any]]) -> Dict[str, str]:
    columns: Dict[str, str] = {}
    for idx, item in enumerate(schema):
        name = str(item.get("name") or f"col_{idx}")
        typ = str(item.get("type") or "str").lower()
        if typ not in {"int", "float", "str", "bool", "date"}:
            typ = "str"
        columns[name] = typ
    if not columns:
        columns = {"id": "int", "value": "float", "category": "str"}
    return columns


class FuzzFillStrategy:
    name = "fuzz_fill"

    def generate(
        self,
        *,
        code_text: str,
        max_rows: int,
        seed: int,
        file_path: Optional[Path] = None,
        orchestrator: Optional[str] = None,
    ) -> DataStrategyResult:
        base = ZeroFillStrategy().generate(
            code_text=code_text,
            max_rows=max_rows,
            seed=seed,
            file_path=file_path,
            orchestrator=orchestrator,
        )
        columns = _schema_columns(base.schema)
        rng = random.Random(int(seed))
        rows: List[Dict[str, Any]] = []
        for row_index in range(max(1, int(max_rows))):
            rows.append({
                name: _fuzz_value(name, typ, row_index=row_index, rng=rng)
                for name, typ in columns.items()
            })

        provenance = list(base.provenance)
        provenance.append(
            {
                "source_name": "fuzz_fill_generator",
                "format": "synthetic",
                "generator_version": "1.0.0",
                "base_strategy": base.name,
                "inference_method": "schema_guided_deterministic_fuzz",
            }
        )
        meta = dict(base.meta)
        meta.update(
            {
                "mode": self.name,
                "seed": seed,
                "max_rows": max_rows,
                "columns_detected": len(columns),
                "data_values": "deterministic_fuzz",
                "base_strategy": base.name,
            }
        )
        return DataStrategyResult(
            name=self.name,
            rows=rows,
            row_count=len(rows),
            schema=[{"name": name, "type": typ, "nullable": True} for name, typ in columns.items()],
            provenance=provenance,
            errors=base.errors,
            meta=meta,
        )


class ScaleProbeStrategy(FuzzFillStrategy):
    name = "scale_probe"

    @staticmethod
    def row_counts(max_rows: int) -> List[int]:
        upper = max(1, int(max_rows))
        candidates = [10, 100, upper]
        out: List[int] = []
        for value in candidates:
            bounded = max(1, min(upper, int(value)))
            if bounded not in out:
                out.append(bounded)
        return out

    def generate_probes(
        self,
        *,
        code_text: str,
        max_rows: int,
        seed: int,
        file_path: Optional[Path] = None,
        orchestrator: Optional[str] = None,
    ) -> List[DataStrategyResult]:
        probes: List[DataStrategyResult] = []
        for row_count in self.row_counts(max_rows):
            result = super().generate(
                code_text=code_text,
                max_rows=row_count,
                seed=seed,
                file_path=file_path,
                orchestrator=orchestrator,
            )
            result.name = self.name
            result.meta = dict(result.meta)
            result.meta.update(
                {
                    "mode": self.name,
                    "probe_row_count": row_count,
                    "scale_probe_row_counts": self.row_counts(max_rows),
                }
            )
            probes.append(result)
        return probes


class OperatorProfileStrategy(FuzzFillStrategy):
    name = "operator_profile"

    def generate(
        self,
        *,
        code_text: str,
        max_rows: int,
        seed: int,
        file_path: Optional[Path] = None,
        orchestrator: Optional[str] = None,
    ) -> DataStrategyResult:
        result = super().generate(
            code_text=code_text,
            max_rows=max_rows,
            seed=seed,
            file_path=file_path,
            orchestrator=orchestrator,
        )
        result.name = self.name
        result.meta = dict(result.meta)
        result.meta.update(
            {
                "mode": self.name,
                "base_strategy": "fuzz_fill",
                "data_values": "deterministic_fuzz_operator_profile",
            }
        )
        result.provenance = list(result.provenance)
        result.provenance.append(
            {
                "source_name": "operator_profile_strategy",
                "format": "synthetic",
                "generator_version": "1.0.0",
                "base_strategy": "fuzz_fill",
                "inference_method": "schema_guided_operator_profile",
            }
        )
        return result


def get_data_strategy(name: str) -> DataStrategy:
    key = (name or "zero_fill").strip().lower()
    if key == "zero_fill":
        return ZeroFillStrategy()
    if key == "fuzz_fill":
        return FuzzFillStrategy()
    if key == "scale_probe":
        return ScaleProbeStrategy()
    if key == "operator_profile":
        return OperatorProfileStrategy()
    raise ValueError(f"unknown_data_strategy:{name}")
