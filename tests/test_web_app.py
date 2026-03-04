from __future__ import annotations

import unittest

from src.web import create_app


class WebAppTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = create_app()
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
        self.assertEqual(response.get_json(), {"status": "ok"})

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
        self.assertIn("export_format=md", response.headers["Location"])

    def test_results_page_renders_placeholder(self) -> None:
        response = self.client.get("/runs/run-demo1234?export_format=html")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Results Shell", response.data)
        self.assertIn(b"run-demo1234", response.data)
        self.assertIn(b"html", response.data)


if __name__ == "__main__":
    unittest.main()
