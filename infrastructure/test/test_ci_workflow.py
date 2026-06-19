from pathlib import Path


def test_ci_workflow_includes_actionlint_job():
    workflow = Path(".github/CI.yml").read_text(encoding="utf-8")
    assert "workflow-lint:" in workflow
    assert "rhysd/actionlint@v1" in workflow


def test_ci_workflow_runs_infrastructure_tests():
    workflow = Path(".github/CI.yml").read_text(encoding="utf-8")
    assert "Run infrastructure tests" in workflow
    assert "uv run pytest infrastructure/test/ -v --tb=short" in workflow
