from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import random
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple

# Load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from ..knowledge_pack import detect_orchestrator_version, resolution_warnings, resolve_knowledge_pack
from ..subprocess_json_runner import run_cli_json
from .data_strategies import DataStrategyResult, ScaleProbeStrategy, get_data_strategy
from .design_rules import EcoDesignRuleEngine
from .oci_runtime import run_oci
from .runtime_evidence import finalize_runtime_measurement
from .runtime_preflight import RUNTIME_SUPPORTED, runtime_policy_error
from .structural_energy_analyzer import StructuralEnergyAnalyzer
from .workload_contract import WorkloadContractError, load_workload_contract


SCHEMA_VERSION = "1.4.0"
RECIPE_SCHEMA_VERSION = "1"
DEFAULT_SEED = 1729


class LLMProviderError(Exception):
    def __init__(self, code: str, message: str, *, provider: Optional[str] = None, retryable: bool = False, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.code = code
        self.provider = provider
        self.retryable = bool(retryable)
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "message": str(self),
            "provider": self.provider,
            "retryable": self.retryable,
            "details": self.details,
        }


class LLMProvider(Protocol):
    provider_name: str

    def health_check(self) -> Tuple[bool, Optional[str]]:
        ...

    def generate_synthetic_spec(self, minimized_spec: Dict[str, Any], *, seed: int, max_rows: int) -> Dict[str, Any]:
        ...


@dataclass
class EnergyEvaluationRequest:
    file_path: Path
    code_text: str
    orchestrator: str
    repo_root: Path
    runtime_python: Optional[Path] = None
    mode_selected: str = "auto"
    sample_paths: List[Path] = field(default_factory=list)
    sample_schema_map: Dict[str, str] = field(default_factory=dict)
    max_rows: int = 500
    max_tasks: int = 25
    timeout_s: int = 120
    seed: int = DEFAULT_SEED
    llm_provider: Optional[str] = None
    llm_model: Optional[str] = None
    llm_api_key_env: Optional[str] = None
    llm_base_url: Optional[str] = None
    llm_timeout_s: int = 30
    synthetic_cache_mode: str = "use"
    execution_adapter: str = "surrogate_benchmark"
    oci_image: Optional[str] = None
    measurement_minimum_duration_s: float = 0.0
    measurement_minimum_repetitions: int = 1
    measurement_maximum_repetitions: int = 1000
    measurement_warmup_repetitions: int = 0
    experiment_mode: bool = False
    workload_contract: Optional[Path] = None
    artifact_binding: Optional[Path] = None
    track_carbon: bool = False
    carbon_country_iso: Optional[str] = None
    carbon_measure_power_secs: int = 1
    carbon_scale_factor: float = 1.0
    estimate_scenario_name: Optional[str] = None
    estimate_runs_per_year: Optional[int] = None
    estimate_years: float = 1.0
    estimate_repetitions_per_run: float = 1.0
    orchestrator_version: Optional[str] = None
    knowledge_pack_mode: str = "legacy"
    knowledge_pack: Optional[Path] = None
    knowledge_pack_version: Optional[str] = None
    artifacts_dir: Optional[Path] = None


@dataclass
class EnergyEvaluationResult:
    mode_selected: str
    mode_used: str
    data_source: str
    attempted_modes: List[Dict[str, Any]]
    attempts: List[Dict[str, Any]]
    evidence: Dict[str, Any]
    measurements: Dict[str, Any]
    import_build_measurement: Optional[Dict[str, Any]]
    runtime_measurement: Optional[Dict[str, Any]]
    synthetic_info: Optional[Dict[str, Any]]
    privacy: Dict[str, Any]
    heuristic: Dict[str, Any]
    errors: List[Dict[str, Any]]
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "mode_selected": self.mode_selected,
            "mode_used": self.mode_used,
            "data_source": self.data_source,
            "attempted_modes": self.attempted_modes,
            "attempts": self.attempts,
            "evidence": self.evidence,
            "measurements": self.measurements,
            "import_build_measurement": self.import_build_measurement,
            "runtime_measurement": self.runtime_measurement,
            "synthetic_info": self.synthetic_info,
            "privacy": self.privacy,
            "heuristic": self.heuristic,
            "errors": self.errors,
            "metadata": self.metadata,
        }


class OpenAICompatibleProvider:
    """LLM provider using the openai SDK, compatible with OpenAI, OpenRouter,
    DeepInfra, DeepSeek, and any OpenAI-compatible API.

    Uses the official ``openai`` package for robust HTTP handling, retries,
    and streaming support. OpenRouter extra headers (HTTP-Referer,
    X-OpenRouter-Title) are passed when the provider is ``openrouter``.
    """

    DEFAULT_URLS = {
        "openai": "https://api.openai.com/v1",
        "openrouter": "https://openrouter.ai/api/v1",
        "deepinfra": "https://api.deepinfra.com/v1/openai",
        "deepseek": "https://api.deepseek.com/v1",
    }
    KEY_ENVS = {
        "openai": "OPENAI_API_KEY",
        "openrouter": "OPENROUTER_API_KEY",
        "deepinfra": "DEEPINFRA_API_KEY",
        "deepseek": "DEEPSEEK_API_KEY",
    }

    def __init__(
        self,
        provider_name: str,
        *,
        model: str,
        api_key_env: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout_s: int = 30,
        openrouter_referer: Optional[str] = None,
        openrouter_title: Optional[str] = None,
    ):
        self.provider_name = provider_name
        self.model = model
        self.api_key_env = api_key_env or self.KEY_ENVS.get(provider_name, "OPENAI_API_KEY")
        self.base_url = (base_url or self.DEFAULT_URLS.get(provider_name) or "").rstrip("/")
        self.timeout_s = int(timeout_s)

        # Resolve API key (env var → .env → prompt)
        self.api_key = os.environ.get(self.api_key_env)

        # OpenRouter extra headers
        self._extra_headers: Dict[str, str] = {}
        if provider_name == "openrouter":
            referer = openrouter_referer or os.environ.get("OPENROUTER_HTTP_REFERER", "")
            title = openrouter_title or os.environ.get("OPENROUTER_TITLE", "PISCES")
            if referer:
                self._extra_headers["HTTP-Referer"] = referer
            self._extra_headers["X-OpenRouter-Title"] = title

        # Lazy-initialised OpenAI client
        self._client: Any = None

    def _get_client(self) -> Any:
        """Return (and cache) the openai.OpenAI client."""
        if self._client is None:
            try:
                from openai import OpenAI
            except ImportError:
                raise LLMProviderError(
                    "missing_openai_package",
                    "The 'openai' package is required. Install with: pip install openai",
                    provider=self.provider_name,
                )

            client_kwargs: Dict[str, Any] = {
                "base_url": self.base_url,
                "api_key": self.api_key,
                "timeout": float(self.timeout_s),
            }
            if self._extra_headers:
                client_kwargs["default_headers"] = self._extra_headers

            self._client = OpenAI(**client_kwargs)
        return self._client

    def health_check(self) -> Tuple[bool, Optional[str]]:
        if not self.base_url:
            return False, "missing_base_url"
        if not self.api_key:
            return False, f"missing_api_key_env:{self.api_key_env}"
        if not self.model:
            return False, "missing_model"
        return True, None

    @staticmethod
    def _extract_json_block(text: str) -> Dict[str, Any]:
        """Extract a JSON object from a model response, handling markdown fences."""
        txt = (text or "").strip()
        if not txt:
            raise LLMProviderError("empty_response", "empty model response")
        try:
            obj = json.loads(txt)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
        # Try extracting from markdown code fences
        import re
        fence = re.search(r"```(?:json)?\s*\n?(.*?)```", txt, re.DOTALL)
        if fence:
            try:
                obj = json.loads(fence.group(1).strip())
                if isinstance(obj, dict):
                    return obj
            except Exception:
                pass
        # Last resort: find first { ... } pair
        left = txt.find("{")
        right = txt.rfind("}")
        if left >= 0 and right > left:
            block = txt[left : right + 1]
            try:
                obj = json.loads(block)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                pass
        raise LLMProviderError("invalid_response", "model response is not valid JSON object")

    def generate_synthetic_spec(
        self,
        minimized_spec: Dict[str, Any],
        *,
        seed: int,
        max_rows: int,
    ) -> Dict[str, Any]:
        """Generate a synthetic data recipe via the LLM."""
        ok, err = self.health_check()
        if not ok:
            raise LLMProviderError(
                "health_check_failed",
                err or "provider not ready",
                provider=self.provider_name,
            )

        client = self._get_client()

        prompt = {
            "task": "Create deterministic synthetic tabular data recipe for workflow energy benchmarking.",
            "constraints": {
                "no_sensitive_data": True,
                "deterministic_seed": int(seed),
                "max_rows": int(max_rows),
                "output_schema": {
                    "row_count": "int",
                    "columns": [
                        {"name": "string", "type": "int|float|str|bool|date", "example": "any"}
                    ],
                },
            },
            "workflow_spec": minimized_spec,
        }

        user_msg = (
            "Return only JSON object with fields row_count and columns. "
            "No markdown, no prose. Keep columns 3-10 and row_count <= max_rows. "
            f"Seed: {seed}."
        )

        try:
            response = client.chat.completions.create(
                model=self.model,
                temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": json.dumps(prompt, sort_keys=True)},
                    {"role": "user", "content": user_msg},
                ],
            )
            content = response.choices[0].message.content
        except Exception as e:
            # Catch openai SDK errors and normalise them
            error_msg = str(e)
            retryable = False
            if "rate" in error_msg.lower() or "timeout" in error_msg.lower() or "server" in error_msg.lower():
                retryable = True
            raise LLMProviderError(
                "llm_api_error",
                error_msg[:500],
                provider=self.provider_name,
                retryable=retryable,
                details={"model": self.model},
            )

        if not content:
            raise LLMProviderError(
                "empty_response",
                "model returned empty content",
                provider=self.provider_name,
            )

        return self._extract_json_block(content)


class AnthropicProvider:
    DEFAULT_URL = "https://api.anthropic.com/v1"

    def __init__(self, *, model: str, api_key_env: Optional[str], base_url: Optional[str], timeout_s: int):
        self.provider_name = "anthropic"
        self.model = model
        self.api_key_env = api_key_env or "ANTHROPIC_API_KEY"
        self.base_url = (base_url or self.DEFAULT_URL).rstrip("/")
        self.timeout_s = int(timeout_s)
        self.api_key = os.environ.get(self.api_key_env)

    def health_check(self) -> Tuple[bool, Optional[str]]:
        if not self.base_url:
            return False, "missing_base_url"
        if not self.api_key:
            return False, f"missing_api_key_env:{self.api_key_env}"
        if not self.model:
            return False, "missing_model"
        return True, None

    def _http_post_json(self, url: str, body: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
        req = urllib.request.Request(url, data=json.dumps(body).encode("utf-8"), headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            body_txt = e.read().decode("utf-8", errors="ignore")
            raise LLMProviderError(
                "http_error",
                f"HTTP {e.code}",
                provider=self.provider_name,
                retryable=(e.code >= 500),
                details={"status_code": e.code, "body": body_txt[-1000:]},
            )
        except urllib.error.URLError as e:
            raise LLMProviderError("network_error", str(e), provider=self.provider_name, retryable=True)
        except json.JSONDecodeError as e:
            raise LLMProviderError("invalid_json", str(e), provider=self.provider_name)

    def generate_synthetic_spec(self, minimized_spec: Dict[str, Any], *, seed: int, max_rows: int) -> Dict[str, Any]:
        ok, err = self.health_check()
        if not ok:
            raise LLMProviderError("health_check_failed", err or "provider not ready", provider=self.provider_name)

        prompt = {
            "task": "Create deterministic synthetic tabular data recipe for workflow energy benchmarking.",
            "constraints": {
                "no_sensitive_data": True,
                "deterministic_seed": int(seed),
                "max_rows": int(max_rows),
                "output_schema": {
                    "row_count": "int",
                    "columns": [{"name": "string", "type": "int|float|str|bool|date", "example": "any"}],
                },
            },
            "workflow_spec": minimized_spec,
        }

        body = {
            "model": self.model,
            "max_tokens": 1200,
            "temperature": 0,
            "messages": [
                {
                    "role": "user",
                    "content": (
                        "Return only JSON object with fields row_count and columns. "
                        "No markdown, no prose. Keep columns 3-10 and row_count <= max_rows. "
                        f"Seed: {seed}. Context: {json.dumps(prompt, sort_keys=True)}"
                    ),
                }
            ],
        }
        headers = {
            "x-api-key": self.api_key or "",
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        }
        resp = self._http_post_json(f"{self.base_url}/messages", body, headers)
        try:
            content = resp["content"][0]["text"]
        except Exception as e:
            raise LLMProviderError("invalid_response", f"missing content text: {e}", provider=self.provider_name)

        return OpenAICompatibleProvider._extract_json_block(content)


def build_provider(
    name: str,
    *,
    model: str,
    api_key_env: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout_s: int = 30,
    openrouter_referer: Optional[str] = None,
    openrouter_title: Optional[str] = None,
) -> LLMProvider:
    """Build an LLM provider instance for the given provider name."""
    key = (name or "").strip().lower()
    if key in {"openai", "openrouter", "deepinfra", "deepseek"}:
        return OpenAICompatibleProvider(
            key,
            model=model,
            api_key_env=api_key_env,
            base_url=base_url,
            timeout_s=timeout_s,
            openrouter_referer=openrouter_referer,
            openrouter_title=openrouter_title,
        )
    if key in {"anthropic", "claude"}:
        return AnthropicProvider(
            model=model,
            api_key_env=api_key_env,
            base_url=base_url,
            timeout_s=timeout_s,
        )
    raise LLMProviderError(
        "unsupported_provider",
        f"Unsupported provider: {name}",
        provider=name,
    )


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def _infer_type(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return "str"
        if text.lower() in {"true", "false"}:
            return "bool"
        try:
            int(text)
            return "int"
        except Exception:
            pass
        try:
            float(text)
            return "float"
        except Exception:
            pass
        if len(text) >= 8 and any(sep in text for sep in ("-", "/")):
            return "date"
    return "str"


def _normalize_rows(rows: List[Dict[str, Any]], max_rows: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows[: max(0, int(max_rows))]:
        if not isinstance(r, dict):
            continue
        out.append({str(k): v for k, v in r.items()})
    return out


def _load_json_rows(path: Path) -> List[Dict[str, Any]]:
    obj = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        data = obj.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
    return []


def _load_jsonl_rows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                rows.append(obj)
        except Exception:
            continue
    return rows


def _load_csv_rows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


def _discover_sample_files(paths: List[Path]) -> List[Path]:
    files: List[Path] = []
    for p in paths:
        if p.is_file():
            files.append(p)
            continue
        if p.is_dir():
            for c in sorted(p.rglob("*")):
                if c.is_file() and c.suffix.lower() in {".csv", ".json", ".jsonl"}:
                    files.append(c)
    dedup: List[Path] = []
    seen = set()
    for f in files:
        k = str(f.resolve())
        if k not in seen:
            dedup.append(f)
            seen.add(k)
    return dedup


def _schema_type_counts(schema: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in schema:
        typ = str(item.get("type") or "unknown")
        counts[typ] = counts.get(typ, 0) + 1
    return counts


def _column_hashes(columns: List[str]) -> List[str]:
    return [_sha256_text(c)[:16] for c in sorted({str(c) for c in columns})]


def _normalize_sample_schema_map(schema_map: Optional[Dict[str, str]]) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    normalized: Dict[str, str] = {}
    errors: List[Dict[str, Any]] = []
    targets: Dict[str, str] = {}
    for source, target in (schema_map or {}).items():
        src = str(source or "").strip()
        dst = str(target or "").strip()
        if not src or not dst:
            errors.append({"error_type": "InvalidSchemaMapping", "message": "mapping source and target must be non-empty"})
            continue
        if dst in targets and targets[dst] != src:
            errors.append(
                {
                    "error_type": "InvalidSchemaMapping",
                    "message": f"multiple source columns map to target:{dst}",
                    "target": dst,
                }
            )
            continue
        normalized[src] = dst
        targets[dst] = src
    return normalized, errors


def _apply_sample_schema_map(
    row: Dict[str, Any],
    schema_map: Dict[str, str],
    errors: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if not schema_map:
        return dict(row)
    mapped: Dict[str, Any] = {}
    for key, value in row.items():
        target = schema_map.get(str(key), str(key))
        if target in mapped:
            errors.append(
                {
                    "error_type": "SchemaMappingCollision",
                    "message": f"multiple values produced mapped column:{target}",
                    "target": target,
                }
            )
            continue
        mapped[target] = value
    return mapped


def load_sample_dataset(
    paths: List[Path],
    *,
    max_rows: int,
    schema_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    provenance: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    normalized_map, mapping_errors = _normalize_sample_schema_map(schema_map)
    errors.extend(mapping_errors)
    observed_source_columns: set[str] = set()

    files = _discover_sample_files(paths)
    for path in files:
        fmt = path.suffix.lower().lstrip(".")
        try:
            if path.suffix.lower() == ".csv":
                loaded = _load_csv_rows(path)
            elif path.suffix.lower() == ".json":
                loaded = _load_json_rows(path)
            elif path.suffix.lower() == ".jsonl":
                loaded = _load_jsonl_rows(path)
            else:
                loaded = []

            for r in loaded:
                observed_source_columns.update(str(k) for k in r.keys())
                rows.append(_apply_sample_schema_map(r, normalized_map, errors))
                if len(rows) >= max_rows:
                    break

            blob = path.read_bytes()
            loaded_columns = sorted({str(k) for row in loaded for k in row.keys() if isinstance(row, dict)})
            provenance.append(
                {
                    "source_name": path.name,
                    "format": fmt,
                    "file_size_bytes": len(blob),
                    "file_sha256": hashlib.sha256(blob).hexdigest(),
                    "rows_discovered": len(loaded),
                    "rows_loaded": min(len(loaded), max_rows),
                    "schema_summary": {
                        "column_count": len(loaded_columns),
                        "column_name_hashes": _column_hashes(loaded_columns),
                    },
                }
            )
            if len(rows) >= max_rows:
                break

        except Exception as e:
            errors.append({"source_name": path.name, "error_type": type(e).__name__, "message": str(e)})

    normalized_rows = _normalize_rows(rows, max_rows)
    schema: List[Dict[str, Any]] = []
    if normalized_rows:
        keys = sorted({k for r in normalized_rows for k in r.keys()})
        for k in keys:
            sample_val = None
            for r in normalized_rows:
                if k in r and r[k] not in (None, ""):
                    sample_val = r[k]
                    break
            schema.append({"name": k, "type": _infer_type(sample_val), "nullable": True})

    missing_sources = sorted([src for src in normalized_map if src not in observed_source_columns])
    if missing_sources:
        errors.append(
            {
                "error_type": "SchemaMappingMissingSource",
                "message": "schema mapping referenced columns not found in sample data",
                "source_column_hashes": _column_hashes(missing_sources),
                "missing_source_count": len(missing_sources),
            }
        )

    mapping_summary = {
        "enabled": bool(normalized_map),
        "mapped_column_count": len(normalized_map),
        "source_column_hashes": _column_hashes(list(normalized_map.keys())),
        "target_columns": sorted(normalized_map.values()),
        "missing_source_count": len(missing_sources),
    }
    schema_summary = {
        "column_count": len(schema),
        "column_name_hashes": _column_hashes([str(item.get("name") or "") for item in schema]),
        "type_counts": _schema_type_counts(schema),
        "nullable_count": sum(1 for item in schema if bool(item.get("nullable", False))),
    }

    return {
        "rows": normalized_rows,
        "row_count": len(normalized_rows),
        "schema": schema,
        "schema_summary": schema_summary,
        "provenance": provenance,
        "mapping": mapping_summary,
        "discovered_file_count": len(files),
        "errors": errors,
    }


def _sanitize_recipe(raw: Dict[str, Any], *, max_rows: int) -> Dict[str, Any]:
    row_count = raw.get("row_count", max_rows)
    try:
        row_count_i = int(row_count)
    except Exception:
        row_count_i = max_rows
    row_count_i = max(1, min(int(max_rows), row_count_i))

    columns_raw = raw.get("columns")
    cols: List[Dict[str, Any]] = []
    if isinstance(columns_raw, list):
        for c in columns_raw:
            if not isinstance(c, dict):
                continue
            name = str(c.get("name") or "col")
            typ = str(c.get("type") or "str").lower()
            if typ not in {"int", "float", "str", "bool", "date"}:
                typ = "str"
            cols.append({"name": name, "type": typ, "example": c.get("example")})

    if not cols:
        cols = [
            {"name": "id", "type": "int", "example": 1},
            {"name": "value", "type": "float", "example": 0.5},
            {"name": "category", "type": "str", "example": "A"},
        ]

    return {"row_count": row_count_i, "columns": cols, "schema_version": RECIPE_SCHEMA_VERSION}


class SyntheticRecipeStore:
    def __init__(self, path: Path):
        self.path = Path(path)

    def _load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {"schema_version": "1", "recipes": {}}
        try:
            obj = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
        return {"schema_version": "1", "recipes": {}}

    def get(self, signature_hash: str) -> Optional[Dict[str, Any]]:
        db = self._load()
        recipes = db.get("recipes")
        if not isinstance(recipes, dict):
            return None
        rec = recipes.get(signature_hash)
        return rec if isinstance(rec, dict) else None

    def put(self, signature_hash: str, recipe: Dict[str, Any]) -> None:
        db = self._load()
        recipes = db.get("recipes")
        if not isinstance(recipes, dict):
            recipes = {}
            db["recipes"] = recipes
        recipes[signature_hash] = recipe
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(db, indent=2, default=str), encoding="utf-8")


def _synthetic_cache_key(
    *,
    signature_hash: str,
    provider: str,
    model: str,
    seed: int,
    max_rows: int,
) -> str:
    payload = {
        "signature_hash": signature_hash,
        "provider": provider,
        "model": model,
        "seed": int(seed),
        "max_rows": int(max_rows),
        "recipe_schema_version": RECIPE_SCHEMA_VERSION,
    }
    return _sha256_text(json.dumps(payload, sort_keys=True))


def _recipe_to_rows(recipe: Dict[str, Any], *, signature_hash: str, seed: int, max_rows: int) -> List[Dict[str, Any]]:
    row_count = min(int(max_rows), int(recipe.get("row_count", max_rows)))
    cols = recipe.get("columns") or []
    if not isinstance(cols, list):
        cols = []

    combined_seed = int(seed) ^ int(signature_hash[:8], 16)
    rng = random.Random(combined_seed)

    rows: List[Dict[str, Any]] = []
    for i in range(max(1, row_count)):
        row: Dict[str, Any] = {}
        for c in cols:
            if not isinstance(c, dict):
                continue
            name = str(c.get("name") or f"col_{len(row)}")
            typ = str(c.get("type") or "str")
            if typ == "int":
                row[name] = rng.randint(0, 10000)
            elif typ == "float":
                row[name] = round(rng.random() * 1000.0, 6)
            elif typ == "bool":
                row[name] = bool(rng.randint(0, 1))
            elif typ == "date":
                row[name] = f"2024-01-{(i % 28) + 1:02d}"
            else:
                row[name] = f"v_{rng.randint(100, 999)}"
        rows.append(row)
    return rows


def _build_minimized_spec(file_path: Path, code_text: str, orchestrator: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    analyzer = StructuralEnergyAnalyzer()
    profile = analyzer.analyze(file_path, orchestrator=orchestrator)

    metrics = profile.get("metrics") or {}
    signals = profile.get("signals") or {}
    schedule = profile.get("schedule") or {}

    minimized = {
        "orchestrator": orchestrator,
        "file_name": file_path.name,
        "code_sha256": profile.get("code_sha256") or _sha256_text(code_text),
        "task_count": int(metrics.get("total_tasks") or 0),
        "edge_count": int(metrics.get("total_edges") or 0),
        "max_fan_out": int(metrics.get("max_fan_out") or 0),
        "parallelism_index": metrics.get("parallelism_index"),
        "operator_tiers": (signals.get("operator_tiers") or {}),
        "schedule": {
            "enabled": schedule.get("enabled"),
            "runs_per_year_estimate": schedule.get("runs_per_year_estimate"),
            "source": schedule.get("source"),
        },
    }
    return minimized, profile


def _next_mode_for(modes: List[str], idx: int) -> Optional[str]:
    if idx + 1 >= len(modes):
        return None
    return modes[idx + 1]


def _resolve_modes(selected: str) -> List[str]:
    s = (selected or "auto").strip().lower()
    if s == "auto":
        return ["sample", "fuzz_fill", "zero_fill", "heuristic"]
    if s == "zero_fill":
        return ["zero_fill", "heuristic"]
    if s == "fuzz_fill":
        return ["fuzz_fill", "zero_fill", "heuristic"]
    if s == "scale_probe":
        return ["scale_probe", "heuristic"]
    if s == "operator_profile":
        return ["operator_profile", "fuzz_fill", "zero_fill", "heuristic"]
    if s == "sample":
        return ["sample", "heuristic"]
    if s == "synthetic":
        return ["synthetic", "heuristic"]
    if s == "heuristic":
        return ["heuristic"]
    return ["heuristic"]


def _module_search_path(request: EnergyEvaluationRequest) -> str:
    paths: List[str] = []
    package_src = Path(__file__).resolve().parents[2]
    candidates = [
        package_src,
        request.repo_root / "src",
        request.file_path.parent,
    ]
    for path in candidates:
        try:
            resolved = path.resolve()
        except Exception:
            resolved = path
        if resolved.exists():
            value = str(resolved)
            if value not in paths:
                paths.append(value)

    existing = os.environ.get("PYTHONPATH")
    if existing:
        for value in existing.split(os.pathsep):
            if value and value not in paths:
                paths.append(value)
    return os.pathsep.join(paths)


def _run_import_build_measurement(request: EnergyEvaluationRequest) -> Dict[str, Any]:
    py = request.runtime_python.expanduser().absolute() if request.runtime_python else Path(os.sys.executable)
    input_payload = {
        "orchestrator": request.orchestrator,
        "workflow_file": str(request.file_path),
        "carbon_country_iso": request.carbon_country_iso,
        "carbon_measure_power_secs": int(request.carbon_measure_power_secs),
        "carbon_scale_factor": float(request.carbon_scale_factor),
    }

    with tempfile.TemporaryDirectory(prefix="pisces_import_build_") as td:
        td_path = Path(td)
        in_path = td_path / "import_input.json"
        out_path = td_path / "import_output.json"
        in_path.write_text(json.dumps(input_payload, indent=2, default=str), encoding="utf-8")

        payload = run_cli_json(
            python_exe=py,
            module_name="pisces.energy.import_runner_cli",
            args=["--in", str(in_path), "--out", str(out_path)],
            out_json=out_path,
            cwd=request.repo_root,
            env={"PYTHONPATH": _module_search_path(request)},
            timeout_s=int(request.timeout_s),
            stub_type="import_build_carbon",
            orchestrator=request.orchestrator,
            file_path=str(request.file_path),
        )
        return payload


def _run_runtime_measurement(
    *,
    request: EnergyEvaluationRequest,
    mode_name: str,
    data_source: str,
    rows: List[Dict[str, Any]],
    workload_profile: str = "standard",
    operator_tiers: Optional[Dict[str, Any]] = None,
    collect_modeled_energy: bool = True,
) -> Dict[str, Any]:
    py = request.runtime_python.expanduser().absolute() if request.runtime_python else Path(os.sys.executable)
    input_payload = {
        "orchestrator": request.orchestrator,
        "mode": mode_name,
        "data_source": data_source,
        "workflow_file": str(request.file_path),
        "execution_adapter": (request.execution_adapter or "surrogate_benchmark"),
        "collect_modeled_energy": collect_modeled_energy,
        "minimum_duration_s": float(request.measurement_minimum_duration_s),
        "minimum_repetitions": int(request.measurement_minimum_repetitions),
        "maximum_repetitions": int(request.measurement_maximum_repetitions),
        "warmup_repetitions": int(request.measurement_warmup_repetitions),
        "experiment_mode": bool(request.experiment_mode),
        "workload_contract": (
            str(request.workload_contract.expanduser().resolve()) if request.workload_contract else None
        ),
        "artifact_binding": (
            str(request.artifact_binding.expanduser().resolve()) if request.artifact_binding else None
        ),
        "max_rows": int(request.max_rows),
        "max_tasks": int(request.max_tasks),
        "seed": int(request.seed),
        "workload_profile": workload_profile,
        "operator_tiers": operator_tiers or {},
        "carbon_country_iso": request.carbon_country_iso,
        "carbon_measure_power_secs": int(request.carbon_measure_power_secs),
        "carbon_scale_factor": float(request.carbon_scale_factor),
        "rows": _normalize_rows(rows, request.max_rows),
    }

    with tempfile.TemporaryDirectory(prefix="pisces_energy_runtime_") as td:
        td_path = Path(td)
        in_path = td_path / "runtime_input.json"
        out_path = td_path / "runtime_output.json"
        in_path.write_text(json.dumps(input_payload, indent=2, default=str), encoding="utf-8")

        payload = run_cli_json(
            python_exe=py,
            module_name="pisces.energy.runtime_runner_cli",
            args=["--in", str(in_path), "--out", str(out_path)],
            out_json=out_path,
            cwd=request.repo_root,
            env={"PYTHONPATH": _module_search_path(request)},
            timeout_s=int(request.timeout_s),
            stub_type="runtime_energy",
            orchestrator=request.orchestrator,
            file_path=str(request.file_path),
        )
        return payload


def _validate_sample_dataset(dataset: Dict[str, Any], minimized_spec: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    row_count = int(dataset.get("row_count") or 0)
    schema = dataset.get("schema") or []
    task_count = int(minimized_spec.get("task_count") or 0)
    errors = dataset.get("errors") if isinstance(dataset.get("errors"), list) else []
    mapping = dataset.get("mapping") if isinstance(dataset.get("mapping"), dict) else {}
    details = {
        "row_count": row_count,
        "column_count": len(schema),
        "task_count": task_count,
        "file_count": int(dataset.get("discovered_file_count") or 0),
        "mapping": mapping,
        "error_count": len(errors),
    }
    if int(dataset.get("discovered_file_count") or 0) <= 0:
        return False, "sample_no_supported_files_found", details
    if any(str(e.get("error_type")) in {"InvalidSchemaMapping", "SchemaMappingCollision"} for e in errors if isinstance(e, dict)):
        return False, "sample_schema_mapping_invalid", details
    if any(str(e.get("error_type")) == "SchemaMappingMissingSource" for e in errors if isinstance(e, dict)):
        return False, "sample_schema_mapping_missing_sources", details
    if row_count <= 0:
        return False, "sample_dataset_empty", details
    if not schema:
        return False, "sample_schema_empty", details
    if task_count > 0 and row_count < 1:
        return False, "insufficient_rows_for_workflow", details
    return True, "ok", details


def _mode_to_tier(mode: Optional[str]) -> str:
    key = str(mode or "").strip().lower()
    if key == "import_build":
        return "import_build"
    if key == "heuristic":
        return "static"
    if key == "sample":
        return "sample_runtime"
    if key in {"zero_fill", "fuzz_fill", "scale_probe", "operator_profile", "synthetic"}:
        return "surrogate_benchmark"
    return key or "unknown"


def _normalize_attempts(attempted_modes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    attempts: List[Dict[str, Any]] = []
    for item in attempted_modes:
        if not isinstance(item, dict):
            continue
        mode = item.get("mode")
        next_mode = item.get("next_mode")
        attempts.append(
            {
                "tier": _mode_to_tier(str(mode) if mode is not None else None),
                "mode": mode,
                "status": item.get("status"),
                "reason": item.get("reason"),
                "duration_s": item.get("duration_s"),
                "next_tier": _mode_to_tier(str(next_mode) if next_mode is not None else None) if next_mode else None,
            }
        )
    return attempts


def _runtime_energy_profile(runtime_measurement: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(runtime_measurement, dict):
        return {}
    profile = runtime_measurement.get("energy_profile")
    return profile if isinstance(profile, dict) else {}


def _import_energy_profile(import_build_measurement: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(import_build_measurement, dict):
        return {}
    profile = import_build_measurement.get("energy_profile")
    return profile if isinstance(profile, dict) else {}


def _runtime_measurement_kind(mode_used: str, runtime_measurement: Dict[str, Any]) -> str:
    adapter_used = str(runtime_measurement.get("execution_adapter_used") or "").strip().lower()
    if adapter_used in {"surrogate_benchmark", "representative"}:
        return "surrogate_benchmark"
    if adapter_used in {"native", "oci"}:
        return "native_runtime"
    if adapter_used == "mixed":
        return "runtime_attempt"
    if runtime_measurement.get("status") == "failed":
        return "runtime_attempt"
    if mode_used == "sample":
        return "sample_runtime"
    return "surrogate_benchmark"


def _build_evidence(
    *,
    mode_used: str,
    data_source: str,
    execution_adapter: str,
    import_build_measurement: Optional[Dict[str, Any]],
    runtime_measurement: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    limitations: List[str] = []

    if isinstance(runtime_measurement, dict):
        measurement_kind = _runtime_measurement_kind(mode_used, runtime_measurement)
        confidence = {
            "native_runtime": "medium",
            "sample_runtime": "medium",
            "surrogate_benchmark": "low",
        }.get(measurement_kind, "low")
        if runtime_measurement.get("execution_outcome") != "completed":
            confidence = "low"
        adapter = runtime_measurement.get("execution_adapter_used")
        profile = _runtime_energy_profile(runtime_measurement)
        diagnostics = runtime_measurement.get("attempt_diagnostics") or {}
        diagnostic_profile = diagnostics.get("modeled_energy") or {}
        measurement_scope = profile.get("measurement_scope") or diagnostic_profile.get("measurement_scope")
        if measurement_kind == "surrogate_benchmark":
            limitations.append(
                "The surrogate benchmark is a PISCES diagnostic workload. It does not execute the workflow and is ineligible for route comparison."
            )
        if data_source == "zero_fill":
            limitations.append("Generated zero-fill data may miss branches and understate data-dependent work.")
        if data_source == "fuzz_fill":
            limitations.append("Generated fuzz-fill data is deterministic and schema-guided, but may not match production distributions.")
        if data_source == "scale_probe":
            limitations.append("Scale-probe uses multiple deterministic generated dataset sizes to estimate growth trends, not production volume.")
        if data_source == "operator_profile":
            limitations.append("Operator-profile mode scales a bounded synthetic workload using static operator-tier signals; it is still a surrogate.")
        if data_source == "synthetic":
            limitations.append("Synthetic data is generated from a minimized workflow spec and may not match production distributions.")
        limitations.append(
            "Operational counters describe the bounded attempt, not verified functional units."
        )
        limitations.append(
            "CodeCarbon output is secondary modeled-energy evidence unless a hardware power source is independently verified."
        )

        return {
            "measurement_kind": measurement_kind,
            "confidence": confidence,
            "production_equivalent": False,
            "data_source": data_source,
            "execution_adapter": adapter,
            "execution_adapter_requested": execution_adapter,
            "measurement_scope": measurement_scope,
            "limitations": limitations,
        }

    if isinstance(import_build_measurement, dict):
        profile = _import_energy_profile(import_build_measurement)
        measurement_scope = str(
            import_build_measurement.get("measurement_scope")
            or profile.get("measurement_scope")
            or "import_build_only"
        )
        limitations.append("Import/build measurement does not include task runtime energy.")
        if not bool(profile.get("available", False)):
            limitations.append("CodeCarbon did not produce hardware energy values for the import/build measurement.")
        if import_build_measurement.get("import_status") != "ok":
            limitations.append("Workflow import/build failed; carbon data, if present, only covers the failed attempt.")

        return {
            "measurement_kind": "import_build",
            "confidence": "low",
            "production_equivalent": False,
            "data_source": "none",
            "execution_adapter": "none",
            "measurement_scope": measurement_scope,
            "limitations": limitations,
        }

    return {
        "measurement_kind": "static",
        "confidence": "low",
        "production_equivalent": False,
        "data_source": "none",
        "execution_adapter": "none",
        "measurement_scope": "static_only",
        "limitations": [
            "Static heuristic evaluation does not execute workflow code or measure hardware energy."
        ],
    }


def _build_measurements(
    *,
    import_build_measurement: Optional[Dict[str, Any]],
    runtime_measurement: Optional[Dict[str, Any]],
    estimate: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "import_build": import_build_measurement,
        "runtime": runtime_measurement,
        "estimate": estimate,
    }


def _scenario_hash(assumptions: Dict[str, Any]) -> str:
    return _sha256_text(json.dumps(assumptions, sort_keys=True, allow_nan=False))[:16]


def _build_scenario_projection(
    *,
    request: EnergyEvaluationRequest,
    evidence: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if request.estimate_runs_per_year is None:
        return None
    refusal = {"status": "not_available", "measurement_kind": "scenario_projection"}
    try:
        runs_per_year = int(request.estimate_runs_per_year)
        years = float(request.estimate_years)
        repetitions = float(request.estimate_repetitions_per_run)
        if not math.isfinite(years) or not math.isfinite(repetitions):
            raise ValueError("Scenario assumptions must be finite")
    except (TypeError, ValueError, OverflowError):
        return {**refusal, "reason": "invalid_estimate_scenario"}

    assumptions = {
        "scenario_name": request.estimate_scenario_name or "explicit_scenario",
        "runs_per_year": runs_per_year,
        "years": years,
        "repetitions_per_run": repetitions,
    }
    measurement_kind = evidence.get("measurement_kind")
    if runs_per_year <= 0 or years <= 0 or repetitions <= 0:
        reason = "non_positive_estimate_scenario"
    elif measurement_kind == "surrogate_benchmark":
        reason = "surrogate_is_not_workflow_execution"
    else:
        # Re-enable projections only with verified units and a supported model.
        reason = "verified_workflow_projection_unavailable"
    return {
        **refusal,
        "reason": reason,
        "scenario_hash": _scenario_hash(assumptions),
        "assumptions": assumptions,
        "baseline": {
            "measurement_kind": measurement_kind,
            "confidence": evidence.get("confidence"),
            "data_source": evidence.get("data_source"),
            "execution_adapter": evidence.get("execution_adapter"),
            "measurement_scope": evidence.get("measurement_scope"),
        },
    }


def _generate_strategy_dataset(
    *,
    mode: str,
    request: EnergyEvaluationRequest,
) -> DataStrategyResult:
    strategy = get_data_strategy(mode)
    return strategy.generate(
        code_text=request.code_text,
        max_rows=request.max_rows,
        seed=request.seed,
        file_path=request.file_path,
        orchestrator=request.orchestrator,
    )


def _attempt_summary_from_strategy(result: DataStrategyResult) -> Dict[str, Any]:
    return {
        "strategy": result.name,
        "row_count": result.row_count,
        "column_count": len(result.schema),
        "columns_detected": result.meta.get("columns_detected"),
        "tasks_detected": result.meta.get("tasks_detected"),
        "inference_method": (
            result.meta.get("inference_method")
            or result.meta.get("base_strategy")
            or "schema_guided"
        ),
        "data_values": result.meta.get("data_values"),
        "meta": result.meta,
        "errors": result.errors,
    }


def _duration_slope(runs: List[Dict[str, Any]]) -> Optional[float]:
    if len(runs) < 2:
        return None
    first = runs[0]
    last = runs[-1]
    first_rows = int(first.get("row_count") or 0)
    last_rows = int(last.get("row_count") or 0)
    first_duration = first.get("duration_s")
    last_duration = last.get("duration_s")
    if not isinstance(first_duration, (int, float)) or not isinstance(last_duration, (int, float)):
        return None
    delta_rows = last_rows - first_rows
    if delta_rows <= 0:
        return None
    return round((float(last_duration) - float(first_duration)) / delta_rows, 10)


def _runtime_observation(payload: Dict[str, Any], request: EnergyEvaluationRequest) -> Dict[str, Any]:
    raw = payload.get("runtime_measurement")
    if isinstance(raw, dict):
        observation = dict(raw)
    else:
        subprocess_info = payload.get("_subprocess") or {}
        observation = {
            "status": "failed",
            "execution_outcome": "timed_out" if subprocess_info.get("timed_out") else "runner_failed",
        }
    if isinstance(payload.get("error"), dict):
        observation["error"] = payload["error"]
    finalize_runtime_measurement(
        observation,
        adapter_requested=request.execution_adapter,
        contract_requested=bool(request.workload_contract or request.artifact_binding),
    )
    return observation


def _controller_preflight(request: EnergyEvaluationRequest, *, mode: str, adapter: str) -> tuple[str | None, str | None]:
    """Return a rejection reason and, when probed, the actual target version."""
    reason = runtime_policy_error(
        mode=mode, adapter=adapter, target=request.orchestrator, experiment=request.experiment_mode,
        has_contract=bool(request.workload_contract), has_binding=bool(request.artifact_binding),
        scale_factor=request.carbon_scale_factor,
    )
    if reason or adapter != "native":
        return reason, None
    if request.experiment_mode and request.runtime_python is None:
        return "experiment_requires_runtime_python", None
    if request.runtime_python is not None:
        python = request.runtime_python.expanduser().absolute()
        if not python.is_file() or not os.access(python, os.X_OK):
            return "runtime_python_unavailable", None
    if mode == "sample" and not _discover_sample_files(request.sample_paths):
        return "no_sample_data_provided", None
    if request.workload_contract and request.artifact_binding:
        try:
            workload = load_workload_contract(
                contract_path=request.workload_contract, binding_path=request.artifact_binding,
                artifact_path=request.file_path,
            )
        except WorkloadContractError:
            return "contract_validation_failed", None
        if workload.binding["target"] != request.orchestrator:
            return "binding_target_mismatch", None
        if workload.binding.get("status") == "known_ineligible":
            return "binding_marked_known_ineligible", None
        if workload.contract["schema_version"] == "2.0" and (
            request.measurement_maximum_repetitions != 1 or request.measurement_minimum_repetitions != 1
            or request.measurement_warmup_repetitions != 0
        ):
            return "v2_requires_single_native_execution_without_warmup", None
    version = detect_orchestrator_version(request.orchestrator, python_exe=request.runtime_python)
    if not version:
        return "native_runtime_unavailable", None
    return None, version


def evaluate_energy(request: EnergyEvaluationRequest) -> EnergyEvaluationResult:
    started_all = time.time()
    errors: List[Dict[str, Any]] = []
    attempted: List[Dict[str, Any]] = []
    synthetic_info: Optional[Dict[str, Any]] = None
    import_build_measurement: Optional[Dict[str, Any]] = None
    runtime_measurement: Optional[Dict[str, Any]] = None
    mode_used = "heuristic"
    data_source = "none"

    selected_mode = (request.mode_selected or "auto").strip().lower()
    adapter = (request.execution_adapter or "surrogate_benchmark").strip().lower()
    runtime_mode = selected_mode
    if adapter == "native" and not request.experiment_mode and selected_mode == "auto":
        runtime_mode = "sample" if request.sample_paths else "fuzz_fill"
    if adapter == "oci":
        preflight_reason = None
        native_version = None
        if request.orchestrator not in {"airflow", "prefect", "dagster"}:
            preflight_reason = "oci_target_not_supported"
        elif not request.oci_image or not request.workload_contract or not request.artifact_binding:
            preflight_reason = "oci_requires_image_contract_and_binding"
        elif request.track_carbon or request.carbon_scale_factor != 1 or request.runtime_python:
            preflight_reason = "oci_requires_container_runtime_and_software_counters_only"
        elif (
            selected_mode not in {"auto", "zero_fill"} or request.sample_paths
            or request.measurement_minimum_repetitions != 1 or request.measurement_maximum_repetitions != 1
            or request.measurement_warmup_repetitions != 0 or request.measurement_minimum_duration_s != 0
        ):
            preflight_reason = "oci_requires_one_contract_bound_execution"
    else:
        preflight_reason, native_version = _controller_preflight(request, mode=runtime_mode, adapter=adapter)
    orch_version = native_version or request.orchestrator_version
    if not orch_version and not preflight_reason and adapter != "oci":
        orch_version = detect_orchestrator_version(request.orchestrator, python_exe=request.runtime_python)
    kp_resolution = resolve_knowledge_pack(
        orchestrator=request.orchestrator,
        orchestrator_version=orch_version,
        mode=request.knowledge_pack_mode,
        pack_path=request.knowledge_pack,
        pack_version=request.knowledge_pack_version,
    )

    static_analysis_failed = False
    try:
        minimized_spec, heuristic_profile = _build_minimized_spec(
            request.file_path, request.code_text, request.orchestrator,
        )
        heuristic_rules = EcoDesignRuleEngine(knowledge_pack_resolution=kp_resolution).evaluate(heuristic_profile)
    except Exception:
        if not preflight_reason:
            raise
        # An unsupported target can also prevent static analysis. Keep the rejection report.
        minimized_spec, heuristic_profile = {}, {}
        heuristic_rules = {}
        static_analysis_failed = True
    signature_hash = _sha256_text(json.dumps(minimized_spec, sort_keys=True))

    explicit_execution = request.experiment_mode or adapter in {"native", "oci"}
    modes = _resolve_modes(selected_mode)
    if explicit_execution:
        modes = [runtime_mode]
    if adapter == "oci" and not preflight_reason:
        modes = []
        try:
            workload = load_workload_contract(
                contract_path=request.workload_contract, binding_path=request.artifact_binding,
                artifact_path=request.file_path,
            )
        except (WorkloadContractError, OSError):
            preflight_reason = "oci_contract_validation_failed"
        else:
            payload = run_oci(workload, image=request.oci_image, timeout_s=request.timeout_s)
            runtime_measurement = _runtime_observation(payload, request)
            mode_used, data_source = "oci", "contract_fixtures"
            success = payload.get("ok") is True
            reason = "ok" if success else (payload.get("error") or {}).get("message") or "oci_workload_failed"
            attempted.append({"mode": "oci", "status": "success" if success else "failed", "reason": reason})
            if not success:
                errors.append({"mode": "oci", "error_type": "OCIExecutionFailed", "message": reason})
    if preflight_reason:
        modes = []
        attempted.append({"mode": selected_mode, "status": "failed", "reason": preflight_reason})
        errors.append({"mode": selected_mode, "error_type": "RuntimePreflightFailed", "message": preflight_reason})
        runtime_measurement = _runtime_observation({"runtime_measurement": {
            "status": "failed", "execution_outcome": "preflight_rejected",
            "ineligibility_reasons": [preflight_reason],
        }}, request)

    synthetic_cache_mode = (request.synthetic_cache_mode or "use").strip().lower()
    if synthetic_cache_mode not in {"use", "refresh", "off"}:
        synthetic_cache_mode = "use"

    recipe_store_path = (request.artifacts_dir or request.file_path.parent) / "synthetic_recipes.json"
    recipe_store = SyntheticRecipeStore(recipe_store_path)

    if request.track_carbon and explicit_execution and not preflight_reason:
        attempted.append({
            "mode": "import_build", "status": "skipped",
            "reason": "separate_import_probe_disabled_for_explicit_native_execution",
            "next_mode": runtime_mode,
        })
    if request.track_carbon and not preflight_reason and not explicit_execution:
        mode_start = time.time()
        import_payload = _run_import_build_measurement(request)
        import_build_measurement = (
            import_payload.get("measurement")
            if isinstance(import_payload.get("measurement"), dict)
            else import_payload
        )
        attempted.append(
            {
                "mode": "import_build",
                "status": "success" if bool(import_payload.get("ok", False)) else "failed",
                "reason": (
                    "ok"
                    if bool(import_payload.get("ok", False))
                    else "measurement_recorded_import_failed"
                    if isinstance(import_payload.get("measurement"), dict)
                    else "measurement_failed"
                ),
                "duration_s": round(time.time() - mode_start, 4),
                "next_mode": modes[0] if modes else None,
                "subprocess": import_payload.get("_subprocess"),
            }
        )

    for idx, mode in enumerate(modes):
        mode_start = time.time()
        next_mode = _next_mode_for(modes, idx)

        if mode in {"zero_fill", "fuzz_fill", "scale_probe", "operator_profile", "sample", "synthetic"} and request.orchestrator not in RUNTIME_SUPPORTED:
            attempted.append(
                {
                    "mode": mode,
                    "status": "skipped",
                    "reason": f"runtime_not_supported_for_orchestrator:{request.orchestrator}",
                    "duration_s": round(time.time() - mode_start, 4),
                    "next_mode": next_mode,
                }
            )
            continue

        if mode in {"zero_fill", "fuzz_fill", "operator_profile"}:
            strategy_result = _generate_strategy_dataset(mode=mode, request=request)

            if strategy_result.row_count <= 0:
                attempted.append(
                    {
                        "mode": mode,
                        "status": "skipped",
                        "reason": f"{mode}_generated_no_rows",
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": next_mode,
                    }
                )
                continue

            runtime = _run_runtime_measurement(
                request=request,
                mode_name=mode,
                data_source=mode,
                rows=strategy_result.rows,
                workload_profile="operator_profile" if mode == "operator_profile" else "standard",
                operator_tiers=(
                    minimized_spec.get("operator_tiers")
                    if isinstance(minimized_spec.get("operator_tiers"), dict)
                    else {}
                ),
            )
            observation = _runtime_observation(runtime, request)

            if bool(runtime.get("ok", False)):
                mode_used = mode
                data_source = mode
                runtime_measurement = observation
                attempted.append(
                    {
                        "mode": mode,
                        "status": "success",
                        "reason": "ok",
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": None,
                        f"{mode}_summary": _attempt_summary_from_strategy(strategy_result),
                    }
                )
                break

            attempted.append(
                {
                    "mode": mode,
                    "status": "failed",
                    "reason": "runtime_measurement_failed",
                    "duration_s": round(time.time() - mode_start, 4),
                    "next_mode": next_mode,
                    "subprocess": runtime.get("_subprocess"),
                    "runtime_observation": observation,
                }
            )
            errors.append(
                {
                    "mode": mode,
                    "error_type": "RuntimeMeasurementFailed",
                    "message": f"runtime measurement failed for {mode} mode",
                    "details": runtime.get("metadata") or {},
                }
            )
            if explicit_execution:
                # A failed experimental execution still belongs to the
                # explicitly requested mode.  Retaining the default
                # "heuristic" label here makes the report contradict both
                # attempted_modes and data_source.
                mode_used = mode
                runtime_measurement = observation
                data_source = mode
            continue

        if mode == "scale_probe":
            strategy = ScaleProbeStrategy()
            probes = strategy.generate_probes(
                code_text=request.code_text,
                max_rows=request.max_rows,
                seed=request.seed,
                file_path=request.file_path,
                orchestrator=request.orchestrator,
            )
            successful_runs: List[Dict[str, Any]] = []
            failed_runs: List[Dict[str, Any]] = []

            for probe in probes:
                runtime = _run_runtime_measurement(
                    request=request,
                    mode_name="scale_probe",
                    data_source="scale_probe",
                    rows=probe.rows,
                )
                observation = _runtime_observation(runtime, request)
                if bool(runtime.get("ok", False)) and isinstance(runtime.get("runtime_measurement"), dict):
                    successful_runs.append(
                        {
                            "row_count": probe.row_count,
                            "duration_s": observation.get("duration_s"),
                            "rows_processed": observation.get("rows_processed"),
                            "tasks_exercised": observation.get("tasks_exercised"),
                            "execution_adapter_used": observation.get("execution_adapter_used"),
                            "energy_profile": observation.get("energy_profile"),
                            "normalized": observation["normalized"],
                            "telemetry": observation["telemetry"],
                            "attempt_diagnostics": observation["attempt_diagnostics"],
                        }
                    )
                    runtime_measurement = observation
                else:
                    failed_runs.append(
                        {
                            "row_count": probe.row_count,
                            "reason": "runtime_measurement_failed",
                            "subprocess": runtime.get("_subprocess"),
                            "runtime_observation": observation,
                        }
                    )

            if successful_runs and isinstance(runtime_measurement, dict):
                mode_used = "scale_probe"
                data_source = "scale_probe"
                runtime_measurement = dict(runtime_measurement)
                diagnostic_slope = (
                    _duration_slope(successful_runs)
                    if all(run["execution_adapter_used"] in {"surrogate_benchmark", "representative"} for run in successful_runs)
                    else None
                )
                runtime_measurement["scale_probe"] = {
                    "status": "ok" if not failed_runs else "partial",
                    "runs": successful_runs,
                    "failed_runs": failed_runs,
                    "row_counts": [run["row_count"] for run in successful_runs],
                    "duration_slope_per_row": diagnostic_slope,
                    "duration_slope_scope": "surrogate_benchmark" if diagnostic_slope is not None else None,
                    "strategy": strategy.name,
                }
                attempted.append(
                    {
                        "mode": "scale_probe",
                        "status": "success" if not failed_runs else "partial",
                        "reason": "ok" if not failed_runs else "some_probe_runs_failed",
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": None,
                        "scale_probe_summary": {
                            "probe_count": len(probes),
                            "successful_probe_count": len(successful_runs),
                            "failed_probe_count": len(failed_runs),
                            "row_counts": [probe.row_count for probe in probes],
                            "duration_slope_per_row": diagnostic_slope,
                            "duration_slope_scope": "surrogate_benchmark" if diagnostic_slope is not None else None,
                        },
                    }
                )
                break

            attempted.append(
                {
                    "mode": "scale_probe",
                    "status": "failed",
                    "reason": "runtime_measurement_failed",
                    "duration_s": round(time.time() - mode_start, 4),
                    "next_mode": next_mode,
                    "failed_runs": failed_runs,
                }
            )
            errors.append(
                {
                    "mode": "scale_probe",
                    "error_type": "RuntimeMeasurementFailed",
                    "message": "runtime measurement failed for all scale_probe runs",
                    "details": {"failed_runs": failed_runs},
                }
            )
            if explicit_execution and failed_runs:
                mode_used = "scale_probe"
                runtime_measurement = failed_runs[-1]["runtime_observation"]
                data_source = "scale_probe"
            continue

        if mode == "sample":
            if not request.sample_paths:
                attempted.append(
                    {
                        "mode": "sample",
                        "status": "skipped",
                        "reason": "no_sample_data_provided",
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": next_mode,
                    }
                )
                continue

            dataset = load_sample_dataset(
                request.sample_paths,
                max_rows=request.max_rows,
                schema_map=request.sample_schema_map,
            )
            valid, reason, validation_details = _validate_sample_dataset(dataset, minimized_spec)
            if not valid:
                attempted.append(
                    {
                        "mode": "sample",
                        "status": "failed",
                        "reason": reason,
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": next_mode,
                        "sample_validation": validation_details,
                    }
                )
                errors.append(
                    {
                        "mode": "sample",
                        "error_type": "SampleValidationFailed",
                        "message": reason,
                        "details": validation_details,
                    }
                )
                continue

            runtime = _run_runtime_measurement(
                request=request,
                mode_name="sample",
                data_source="sample",
                rows=dataset.get("rows") or [],
            )
            observation = _runtime_observation(runtime, request)
            if bool(runtime.get("ok", False)):
                mode_used = "sample"
                data_source = "sample"
                runtime_measurement = observation
                attempted.append(
                    {
                        "mode": "sample",
                        "status": "success",
                        "reason": "ok",
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": None,
                        "sample_summary": {
                            "row_count": int(dataset.get("row_count") or 0),
                            "column_count": len(dataset.get("schema") or []),
                            "schema_summary": dataset.get("schema_summary") or {},
                            "provenance": dataset.get("provenance") or [],
                            "mapping": dataset.get("mapping") or {},
                            "validation": validation_details,
                            "errors": dataset.get("errors") or [],
                        },
                    }
                )
                break

            attempted.append(
                {
                    "mode": "sample",
                    "status": "failed",
                    "reason": "runtime_measurement_failed",
                    "duration_s": round(time.time() - mode_start, 4),
                    "next_mode": next_mode,
                    "subprocess": runtime.get("_subprocess"),
                    "runtime_observation": observation,
                }
            )
            errors.append(
                {
                    "mode": "sample",
                    "error_type": "RuntimeMeasurementFailed",
                    "message": "runtime measurement failed for sample mode",
                    "details": runtime.get("metadata") or {},
                }
            )
            if explicit_execution:
                mode_used = "sample"
                runtime_measurement = observation
                data_source = "sample"
            continue

        if mode == "synthetic":
            if not request.llm_provider or not request.llm_model:
                attempted.append(
                    {
                        "mode": "synthetic",
                        "status": "skipped",
                        "reason": "missing_llm_provider_or_model",
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": next_mode,
                    }
                )
                continue

            try:
                recipe_cache_key = _synthetic_cache_key(
                    signature_hash=signature_hash,
                    provider=str(request.llm_provider),
                    model=str(request.llm_model),
                    seed=int(request.seed),
                    max_rows=int(request.max_rows),
                )
                cached = None
                if synthetic_cache_mode == "use":
                    cached = recipe_store.get(recipe_cache_key)
                if cached and isinstance(cached.get("recipe"), dict):
                    recipe = _sanitize_recipe(cached["recipe"], max_rows=request.max_rows)
                    recipe_source = "cache"
                    provider_name = str(cached.get("provider") or request.llm_provider)
                else:
                    provider = build_provider(
                        request.llm_provider,
                        model=request.llm_model,
                        api_key_env=request.llm_api_key_env,
                        base_url=request.llm_base_url,
                        timeout_s=request.llm_timeout_s,
                    )
                    ok, err = provider.health_check()
                    if not ok:
                        raise LLMProviderError("health_check_failed", err or "provider not ready", provider=request.llm_provider)

                    raw_recipe = provider.generate_synthetic_spec(minimized_spec, seed=request.seed, max_rows=request.max_rows)
                    recipe = _sanitize_recipe(raw_recipe, max_rows=request.max_rows)
                    recipe_source = "provider"
                    provider_name = provider.provider_name
                    if synthetic_cache_mode != "off":
                        recipe_store.put(
                            recipe_cache_key,
                            {
                                "cache_key": recipe_cache_key,
                                "signature_hash": signature_hash,
                                "provider": provider.provider_name,
                                "model": request.llm_model,
                                "seed": int(request.seed),
                                "max_rows": int(request.max_rows),
                                "template_version": RECIPE_SCHEMA_VERSION,
                                "recipe_hash": _sha256_text(json.dumps(recipe, sort_keys=True)),
                                "recipe": recipe,
                                "updated_at": time.time(),
                            },
                        )

                recipe_hash = _sha256_text(json.dumps(recipe, sort_keys=True))
                rows = _recipe_to_rows(recipe, signature_hash=signature_hash, seed=request.seed, max_rows=request.max_rows)
                synthetic_info = {
                    "provider": provider_name,
                    "model": request.llm_model,
                    "seed": int(request.seed),
                    "recipe_hash": recipe_hash,
                    "signature_hash": signature_hash,
                    "recipe_cache_key": recipe_cache_key,
                    "cache_mode": synthetic_cache_mode,
                    "recipe_source": recipe_source,
                    "recipe_schema_version": recipe.get("schema_version"),
                    "row_count": int(recipe.get("row_count") or len(rows)),
                    "column_count": len(recipe.get("columns") or []),
                }

                runtime = _run_runtime_measurement(
                    request=request,
                    mode_name="synthetic",
                    data_source="synthetic",
                    rows=rows,
                )
                observation = _runtime_observation(runtime, request)
                if bool(runtime.get("ok", False)):
                    mode_used = "synthetic"
                    data_source = "synthetic"
                    runtime_measurement = observation
                    attempted.append(
                        {
                            "mode": "synthetic",
                            "status": "success",
                            "reason": "ok",
                            "duration_s": round(time.time() - mode_start, 4),
                            "next_mode": None,
                            "synthetic_info": synthetic_info,
                        }
                    )
                    break

                attempted.append(
                    {
                        "mode": "synthetic",
                        "status": "failed",
                        "reason": "runtime_measurement_failed",
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": next_mode,
                        "subprocess": runtime.get("_subprocess"),
                        "runtime_observation": observation,
                    }
                )
                errors.append(
                    {
                        "mode": "synthetic",
                        "error_type": "RuntimeMeasurementFailed",
                        "message": "runtime measurement failed for synthetic mode",
                        "details": runtime.get("metadata") or {},
                    }
                )
                if explicit_execution:
                    mode_used = "synthetic"
                    runtime_measurement = observation
                    data_source = "synthetic"
                continue

            except LLMProviderError as e:
                attempted.append(
                    {
                        "mode": "synthetic",
                        "status": "failed",
                        "reason": e.code,
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": next_mode,
                        "provider_error": e.to_dict(),
                    }
                )
                errors.append({"mode": "synthetic", "error_type": "LLMProviderError", "details": e.to_dict()})
                continue
            except Exception as e:
                attempted.append(
                    {
                        "mode": "synthetic",
                        "status": "failed",
                        "reason": "unexpected_error",
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": next_mode,
                    }
                )
                errors.append({"mode": "synthetic", "error_type": type(e).__name__, "message": str(e)})
                continue

        if mode == "heuristic":
            mode_used = "heuristic"
            data_source = "none"
            runtime_measurement = None
            attempted.append(
                {
                    "mode": "heuristic",
                    "status": "success",
                    "reason": "heuristic_fallback",
                    "duration_s": round(time.time() - mode_start, 4),
                    "next_mode": None,
                }
            )
            break

    if explicit_execution and not errors and runtime_measurement is None:
        errors.append({
            "mode": selected_mode, "error_type": "RuntimePreflightFailed",
            "message": attempted[-1]["reason"] if attempted else "runtime_not_run",
        })

    heuristic_summary = {
        "metrics": heuristic_profile.get("metrics") or {},
        "signals": {
            "operator_tiers": (heuristic_profile.get("signals") or {}).get("operator_tiers") or {},
            "xcom": (heuristic_profile.get("signals") or {}).get("xcom") or {},
        },
        "rules": {
            "rule_count": heuristic_rules.get("rule_count", 0),
            "rules": heuristic_rules.get("rules", []),
        },
    }
    if static_analysis_failed:
        heuristic_summary["rules"].update(status="not_available", reason="static_analysis_failed")

    privacy = {
        "sample_data_persisted": False,
        "raw_rows_persisted": False,
        "llm_payload_mode": "minimized_spec_only",
        "sample_provenance_mode": "file hashes, row counts, column hashes, and aggregate schema metadata only",
        "redaction_summary": [
            "raw sample rows are not persisted",
            "sample provenance stores file hashes and aggregate schema metadata only",
            "runtime payload stores only aggregate metrics",
            "llm prompt includes minimized workflow spec only",
        ],
    }

    metadata = {
        "orchestrator": request.orchestrator,
        "orchestrator_version": orch_version,
        "runtime_supported": request.orchestrator in RUNTIME_SUPPORTED,
        "signature_hash": signature_hash,
        "synthetic_cache": {
            "mode": synthetic_cache_mode,
            "path": str(recipe_store_path),
        },
        "seed": int(request.seed),
        "max_rows": int(request.max_rows),
        "max_tasks": int(request.max_tasks),
        "execution_adapter_selected": (request.execution_adapter or "surrogate_benchmark"),
        "experiment_mode": bool(request.experiment_mode),
        "measurement_window": {
            "minimum_duration_s": float(request.measurement_minimum_duration_s),
            "minimum_repetitions": int(request.measurement_minimum_repetitions),
            "maximum_repetitions": int(request.measurement_maximum_repetitions),
            "warmup_repetitions": int(
                request.measurement_warmup_repetitions
            ),
        },
        "workload_contract_requested": bool(
            request.workload_contract or request.artifact_binding
        ),
        "knowledge_pack": kp_resolution.to_dict(),
        "uncertainty": kp_resolution.uncertainty,
        "warnings": resolution_warnings(kp_resolution),
        "total_duration_s": round(time.time() - started_all, 4),
    }
    attempts = _normalize_attempts(attempted)
    evidence = _build_evidence(
        mode_used=mode_used,
        data_source=data_source,
        execution_adapter=(request.execution_adapter or "surrogate_benchmark"),
        import_build_measurement=import_build_measurement,
        runtime_measurement=runtime_measurement,
    )
    estimate = _build_scenario_projection(
        request=request,
        evidence=evidence,
    )
    measurements = _build_measurements(
        import_build_measurement=import_build_measurement,
        runtime_measurement=runtime_measurement,
        estimate=estimate,
    )

    return EnergyEvaluationResult(
        mode_selected=request.mode_selected,
        mode_used=mode_used,
        data_source=data_source,
        attempted_modes=attempted,
        attempts=attempts,
        evidence=evidence,
        measurements=measurements,
        import_build_measurement=import_build_measurement,
        runtime_measurement=runtime_measurement,
        synthetic_info=synthetic_info,
        privacy=privacy,
        heuristic=heuristic_summary,
        errors=errors,
        metadata=metadata,
    )
