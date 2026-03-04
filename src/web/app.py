"""Minimal web application skeleton for variant-review-workbench."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from flask import Flask, redirect, render_template, request, url_for


def create_app() -> Flask:
    """Create the Flask application for the thin web interface."""
    project_root = Path(__file__).resolve().parents[2]
    template_dir = project_root / "templates"
    static_dir = Path(__file__).resolve().with_name("static")

    app = Flask(__name__, template_folder=str(template_dir), static_folder=str(static_dir))
    app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024

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
        # Phase 2 keeps run execution as a stub so the web skeleton can be exercised safely.
        if request.form.get("mode") == "export_only":
            run_id = "export-" + uuid4().hex[:8]
            export_format = request.form.get("export_format", "json")
            return redirect(url_for("results", run_id=run_id, export_format=export_format))

        run_id = "run-" + uuid4().hex[:8]
        return redirect(url_for("results", run_id=run_id))

    @app.get("/runs/<run_id>")
    def results(run_id: str) -> str:
        return render_template(
            "web/results.html.j2",
            page_title="Run Results",
            current_page="results",
            run_id=run_id,
            export_format=request.args.get("export_format"),
        )

    @app.get("/healthz")
    def healthz() -> tuple[dict[str, str], int]:
        return {"status": "ok"}, 200

    return app


app = create_app()
