from __future__ import annotations

import unittest

from src.web import create_app


class WebAppTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = create_app({"TESTING": True, "JOB_EXECUTION_MODE": "inline"})
        self.client = self.app.test_client()

    def test_home_page_renders_form_shell(self) -> None:
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Run the workbench without touching the command line.", response.data)
        self.assertIn(b"Run Setup", response.data)

    def test_docs_page_renders_project_context(self) -> None:
        response = self.client.get("/docs")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Variant Review Workbench", response.data)
        self.assertIn(b"thin analyst-facing layer", response.data)

    def test_health_check_returns_ok(self) -> None:
        response = self.client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"status": "ok", "job_execution_mode": "inline"})

    def test_create_run_redirects_to_results_shell(self) -> None:
        response = self.client.post(
            "/runs",
            data={"assembly": "GRCh38", "export_format": "json"},
        )

        self.assertEqual(response.status_code, 302)
        self.assertIn("/runs/run-", response.headers["Location"])

    def test_export_only_submission_redirects_with_export_preference(self) -> None:
        response = self.client.post(
            "/runs",
            data={"assembly": "GRCh38", "mode": "export_only", "export_format": "md"},
        )

        self.assertEqual(response.status_code, 302)
        self.assertIn("/runs/export-", response.headers["Location"])
        redirected = self.client.get(response.headers["Location"])
        self.assertEqual(redirected.status_code, 200)
        self.assertIn(b"export_only", redirected.data)

    def test_results_page_renders_placeholder(self) -> None:
        create_response = self.client.post(
            "/runs",
            data={"assembly": "GRCh38", "export_format": "html", "enable_pharmgkb": "true"},
        )
        response = self.client.get(create_response.headers["Location"])

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Results Shell", response.data)
        self.assertIn(b"Status:", response.data)
        self.assertIn(b"html", response.data)
        self.assertIn(b"Execution is stubbed in Phase 3", response.data)

    def test_status_endpoint_returns_job_result(self) -> None:
        create_response = self.client.post(
            "/runs",
            data={"assembly": "GRCh37", "export_format": "json"},
        )
        run_path = create_response.headers["Location"]
        run_id = run_path.rstrip("/").split("/")[-1]

        response = self.client.get(f"/runs/{run_id}/status")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        assert payload is not None
        self.assertEqual(payload["job_id"], run_id)
        self.assertEqual(payload["status"], "succeeded")
        self.assertEqual(payload["metadata"]["assembly"], "GRCh37")
        self.assertEqual(payload["result"]["mode"], "report")

    def test_status_endpoint_returns_404_for_missing_run(self) -> None:
        response = self.client.get("/runs/run-missing/status")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.get_json(), {"error": "run not found"})


if __name__ == "__main__":
    unittest.main()
