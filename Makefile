# PISCES — Pipeline Intelligent Sustainability & Carbon Evaluation Suite
# =====================================================================

VENV      := .venv
PYTHON    := $(VENV)/bin/python3
PIP       := $(VENV)/bin/pip
PISCES    := $(VENV)/bin/pisces-unified

.DEFAULT_GOAL := help

# ────────────────────────────────────────────────────────────────
# Setup
# ────────────────────────────────────────────────────────────────

.PHONY: venv
venv: $(VENV)/bin/activate
$(VENV)/bin/activate:
	@echo "→ Creating virtual environment …"
	python3 -m venv $(VENV)
	@echo "✓ Virtual environment created at $(VENV)"

.PHONY: install
install: venv
	@echo "→ Installing PISCES in editable mode …"
	$(PIP) install --upgrade pip wheel > /dev/null
	$(PIP) install -r requirements.txt
	$(PIP) install -e ".[energy,cli]"
	@echo "✓ PISCES installed"

.PHONY: install-dev
install-dev: venv
	@echo "→ Installing PISCES + dev dependencies …"
	$(PIP) install --upgrade pip wheel > /dev/null
	$(PIP) install -r requirements.txt
	$(PIP) install -e ".[energy,cli,dev]"
	@echo "✓ PISCES (dev) installed"

# ────────────────────────────────────────────────────────────────
# Run
# ────────────────────────────────────────────────────────────────

.PHONY: run
run: install
	@echo "→ Running pisces-unified on a sample workflow …"
	$(PISCES) test_scripts/airflow/simple_etl.py \
		--enable-energy-eval \
		--energy-mode heuristic \
		--stdout

.PHONY: run-synthetic
run-synthetic: install
	@echo "→ Running pisces-unified with synthetic LLM data …"
	$(PISCES) test_scripts/airflow/simple_etl.py \
		--track-carbon \
		--enable-energy-eval \
		--energy-mode synthetic \
		--stdout

.PHONY: run-argo
run-argo: install
	@echo "→ Running pisces-unified on Argo YAML workflow …"
	$(PISCES) test_scripts/argo/simple_etl.yaml \
		--orchestrator argo \
		--enable-energy-eval \
		--energy-mode heuristic \
		--stdout

.PHONY: run-kestra
run-kestra: install
	@echo "→ Running pisces-unified on Kestra YAML workflow …"
	$(PISCES) test_scripts/kestra/simple_etl.yaml \
		--orchestrator kestra \
		--enable-energy-eval \
		--energy-mode heuristic \
		--stdout

.PHONY: demo
demo: install
	@echo "→ Running release demo bundle …"
	mkdir -p demo/output
	$(PYTHON) -m pisces.diagnostics test_scripts/airflow/simple_etl.py \
		--carbon-country-iso NOR \
		--out demo/output/readiness.json
	$(PISCES) test_scripts/airflow/simple_etl.py \
		--enable-energy-eval \
		--energy-mode sample \
		--energy-sample-path demo/input/weather_observations.csv \
		--energy-sample-map-file demo/input/sample_schema_map.json \
		--estimate-scenario-file demo/input/scenario_daily.json \
		--out-dir demo/output
	$(PISCES) test_scripts/airflow/simple_etl.py \
		--enable-energy-eval \
		--energy-mode fuzz_fill \
		--estimate-scenario-file demo/input/scenario_daily.json \
		--out-dir demo/output
	$(PISCES) test_scripts/argo/simple_etl.yaml \
		--orchestrator argo \
		--out-dir demo/output
	$(PYTHON) -m pisces.dag_comparator demo/output/*.json \
		--out demo/output/comparison.json
	@echo "✓ Demo reports written to demo/output"

# ────────────────────────────────────────────────────────────────
# Test
# ────────────────────────────────────────────────────────────────

.PHONY: test
test: install-dev
	@echo "→ Running all tests …"
	$(VENV)/bin/pytest tests/ -v

.PHONY: test-quick
test-quick: install-dev
	@echo "→ Running tests (quick — stop on first failure) …"
	$(VENV)/bin/pytest tests/ -x -v

# ────────────────────────────────────────────────────────────────
# Clean
# ────────────────────────────────────────────────────────────────

.PHONY: clean
clean:
	@echo "→ Cleaning build artefacts …"
	rm -rf build dist *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	@echo "✓ Cleaned"

.PHONY: clean-all
clean-all: clean
	@echo "→ Removing virtual environment …"
	rm -rf $(VENV)
	@echo "✓ All cleaned"

# ────────────────────────────────────────────────────────────────
# Help
# ────────────────────────────────────────────────────────────────

.PHONY: help
help:
	@echo "PISCES — Pipeline Intelligent Sustainability & Carbon Evaluation Suite"
	@echo ""
	@echo "SETUP"
	@echo "  make install       Create venv and install PISCES"
	@echo "  make install-dev   Create venv and install PISCES + dev tools"
	@echo ""
	@echo "RUN"
	@echo "  make run           Heuristic evaluation on sample Airflow workflow"
	@echo "  make run-synthetic LLM synthetic data evaluation (needs .env)"
	@echo "  make run-argo      Heuristic evaluation on Argo YAML workflow"
	@echo "  make run-kestra    Heuristic evaluation on Kestra YAML workflow"
	@echo "  make demo          Run release demo bundle"
	@echo ""
	@echo "TEST"
	@echo "  make test          Run all tests"
	@echo "  make test-quick    Run tests, stop on first failure"
	@echo ""
	@echo "CLEAN"
	@echo "  make clean         Remove build artefacts"
	@echo "  make clean-all     Remove venv + build artefacts"
	@echo ""
	@echo "MANUAL (after 'make install')"
	@echo "  source .venv/bin/activate"
	@echo "  pisces-unified test_scripts/airflow/weather_pipeline.py --track-carbon --enable-energy-eval --energy-mode synthetic"
