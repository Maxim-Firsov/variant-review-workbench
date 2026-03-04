"""Minimal web application skeleton for variant-review-workbench."""

from __future__ import annotations

from pathlib import Path
from time import sleep
from uuid import uuid4

from flask import Flask, abort, jsonify, redirect, render_template, request, url_for

from .jobs import JobRunner, JobStore
from .storage import (
    UploadValidationError,
    cleanup_expired_run_directories,
    create_run_workspace,
    ensure_storage_roots,
    save_uploaded_vcf,
)

def _build_placeholder_result(
    *,
    job_id: str,
    assembly: str,
    mode: str,
    export_format: str | None,
    pharmgkb_enabled: bool,
    uploaded_vcf_path: str,
    output_dir: str,
) -> dict[str, str | bool | None]:
    """Return the placeholder job payload until real pipeline execution is wired in."""
    sleep(0.01)
    return {
        "job_id": job_id,
        "assembly": assembly,
        "mode": mode,
        "export_format": export_format,
        "pharmgkb_enabled": pharmgkb_enabled,
        "uploaded_vcf_path": uploaded_vcf_path,
        "output_dir": output_dir,
        "message": "Execution is still stubbed. Phase 4 now stores uploads in isolated run workspaces while Phase 5 wires in real reporting.",
    }


def create_app(test_config: dict | None = None) -> Flask:
    """Create the Flask application for the thin web interface."""
    project_root = Path(__file__).resolve().parents[2]
    template_dir = project_root / "templates"
    static_dir = Path(__file__).resolve().with_name("static")

    app = Flask(__name__, template_folder=str(template_dir), static_folder=str(static_dir))
    app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024
    app.config["JOB_EXECUTION_MODE"] = "threaded"
    app.config["UPLOAD_ROOT"] = str(project_root / ".web_runtime" / "uploads")
    app.config["RUN_OUTPUT_ROOT"] = str(project_root / ".web_runtime" / "runs")
    app.config["RUN_RETENTION_HOURS"] = 24
    if test_config:
        app.config.update(test_config)

    upload_root = Path(app.config["UPLOAD_ROOT"])
    run_output_root = Path(app.config["RUN_OUTPUT_ROOT"])
    ensure_storage_roots(upload_root, run_output_root)

    app.extensions["job_store"] = JobStore()
    app.extensions["job_runner"] = JobRunner(
        store=app.extensions["job_store"],
        execution_mode=app.config["JOB_EXECUTION_MODE"],
    )

    @app.get("/")
    def home() -> str:
        return render_template(
            "web/home.html.j2",
            page_title="Variant Review Workbench",
            current_page="home",
        )

    @app.get("/docs")
    def docs() -> str:
        return render_template(
            "web/docs.html.j2",
            page_title="Workbench Docs",
            current_page="docs",
        )

    @app.post("/runs")
    def create_run() -> tuple[str, int] | object:
        mode = "export_only" if request.form.get("mode") == "export_only" else "report"
        job_prefix = "export" if mode == "export_only" else "run"
        job_id = f"{job_prefix}-{uuid4().hex[:8]}"
        export_format = request.form.get("export_format", "json")
        assembly = request.form.get("assembly", "GRCh38")
        pharmgkb_enabled = request.form.get("enable_pharmgkb") == "true"
        uploaded_vcf = request.files.get("vcf_file")

        cleanup_expired_run_directories(
            upload_root=upload_root,
            run_output_root=run_output_root,
            retention_hours=int(app.config["RUN_RETENTION_HOURS"]),
        )
        try:
            workspace = create_run_workspace(job_id=job_id, upload_root=upload_root, run_output_root=run_output_root)
            workspace = save_uploaded_vcf(upload=uploaded_vcf, workspace=workspace)
        except UploadValidationError as error:
            return (
                render_template(
                    "web/home.html.j2",
                    page_title="Variant Review Workbench",
                    current_page="home",
                    form_error=str(error),
                ),
                400,
            )

        metadata = {
            "assembly": assembly,
            "pharmgkb_enabled": pharmgkb_enabled,
            "requested_export_format": export_format,
            "uploaded_vcf_path": str(workspace.uploaded_vcf_path),
            "output_dir": str(workspace.output_dir),
            "cleanup_policy": f"Run workspaces older than {app.config['RUN_RETENTION_HOURS']} hour(s) are removed opportunistically on new submissions.",
        }

        job_runner: JobRunner = app.extensions["job_runner"]
        job_runner.submit(
            job_id=job_id,
            mode=mode,
            export_format=export_format,
            metadata=metadata,
            task=lambda: _build_placeholder_result(
                job_id=job_id,
                assembly=assembly,
                mode=mode,
                export_format=export_format,
                pharmgkb_enabled=pharmgkb_enabled,
                uploaded_vcf_path=str(workspace.uploaded_vcf_path),
                output_dir=str(workspace.output_dir),
            ),
        )
        return redirect(url_for("results", run_id=job_id))

    @app.get("/runs/<run_id>")
    def results(run_id: str) -> str:
        job = app.extensions["job_store"].get_job(run_id)
        if job is None:
            abort(404)

        return render_template(
            "web/results.html.j2",
            page_title="Run Results",
            current_page="results",
            run_id=run_id,
            export_format=job.export_format,
            job=job,
        )

    @app.get("/runs/<run_id>/status")
    def run_status(run_id: str) -> tuple[object, int]:
        job = app.extensions["job_store"].get_job(run_id)
        if job is None:
            return {"error": "run not found"}, 404

        return jsonify(
            {
                "job_id": job.job_id,
                "status": job.status,
                "mode": job.mode,
                "export_format": job.export_format,
                "created_at": job.created_at,
                "updated_at": job.updated_at,
                "metadata": job.metadata,
                "result": job.result,
                "error": job.error,
            }
        ), 200

    @app.get("/healthz")
    def healthz() -> tuple[dict[str, str], int]:
        return {
            "status": "ok",
            "job_execution_mode": app.config["JOB_EXECUTION_MODE"],
            "upload_root": str(upload_root),
            "run_output_root": str(run_output_root),
        }, 200

    return app


app = create_app()
