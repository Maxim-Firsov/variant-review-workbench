from __future__ import annotations

import threading
import time
import unittest

from src.web.jobs import JobRunner, JobStore


class JobRunnerTests(unittest.TestCase):
    def test_inline_runner_completes_job_immediately(self) -> None:
        store = JobStore()
        runner = JobRunner(store=store, execution_mode="inline")

        job = runner.submit(
            job_id="run-inline",
            mode="report",
            export_format="json",
            metadata={"assembly": "GRCh38"},
            task=lambda: {"report_html_path": "report.html"},
        )

        self.assertEqual(job.status, "succeeded")
        stored = store.get_job("run-inline")
        assert stored is not None
        self.assertEqual(stored.status, "succeeded")
        self.assertEqual(stored.result, {"report_html_path": "report.html"})
        self.assertEqual(stored.metadata["assembly"], "GRCh38")

    def test_inline_runner_records_failure_and_reraises(self) -> None:
        store = JobStore()
        runner = JobRunner(store=store, execution_mode="inline")

        with self.assertRaisesRegex(RuntimeError, "pipeline failed"):
            runner.submit(
                job_id="run-inline-fail",
                mode="report",
                export_format="json",
                metadata={},
                task=lambda: (_ for _ in ()).throw(RuntimeError("pipeline failed")),
            )

        stored = store.get_job("run-inline-fail")
        assert stored is not None
        self.assertEqual(stored.status, "failed")
        self.assertEqual(stored.error, "pipeline failed")

    def test_threaded_runner_completes_job_asynchronously(self) -> None:
        store = JobStore()
        runner = JobRunner(store=store, execution_mode="threaded", max_workers=1)
        started = threading.Event()
        release = threading.Event()

        def task() -> dict[str, str]:
            started.set()
            release.wait(timeout=2)
            return {"report_html_path": "report.html"}

        runner.submit(
            job_id="run-threaded",
            mode="report",
            export_format="html",
            metadata={},
            task=task,
        )

        self.assertTrue(started.wait(timeout=1))
        stored = store.get_job("run-threaded")
        assert stored is not None
        self.assertEqual(stored.status, "running")

        release.set()
        deadline = time.time() + 2
        while time.time() < deadline:
            stored = store.get_job("run-threaded")
            assert stored is not None
            if stored.status == "succeeded":
                break
            time.sleep(0.02)

        stored = store.get_job("run-threaded")
        assert stored is not None
        self.assertEqual(stored.status, "succeeded")
        self.assertEqual(stored.result, {"report_html_path": "report.html"})

    def test_runner_rejects_unsupported_execution_mode(self) -> None:
        with self.assertRaisesRegex(ValueError, "execution_mode must be 'threaded' or 'inline'"):
            JobRunner(store=JobStore(), execution_mode="process")


if __name__ == "__main__":
    unittest.main()
