import asyncio
import os
import subprocess
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app import debug as diagnostics


class DebugTests(unittest.TestCase):
    def test_environment_flag(self):
        for value, enabled in [("", False), ("false", False), ("0", False),
                               ("true", True), ("1", True), (" YES ", True)]:
            with self.subTest(value=value):
                result = subprocess.run(
                    [sys.executable, "-c", "from app.debug import debug; debug('probe', count=3)"],
                    env={**os.environ, "DEBUG": value}, capture_output=True, text=True, check=True,
                )
                self.assertEqual("probe" in result.stderr, enabled)
                self.assertEqual(result.stdout, "")

    def test_disabled_does_not_serialize(self):
        class MustNotSerialize:
            def __str__(self):
                raise AssertionError("disabled logs serialized a value")

        with patch.object(diagnostics, "DEBUG", False):
            diagnostics.debug("quiet", value=MustNotSerialize())

    def test_streaming_response_is_unchanged_and_secrets_are_not_logged(self):
        messages = [
            {"type": "http.response.start", "status": 206, "headers": []},
            {"type": "http.response.body", "body": b"audio", "more_body": True},
            {"type": "http.response.body", "body": b"tail", "more_body": False},
        ]
        received = []

        async def app(scope, receive, send):
            scope["route"] = SimpleNamespace(path="/clips/{filename}")
            for message in messages:
                await send(message)

        async def receive():
            raise AssertionError("middleware must not consume the request body")

        async def send(message):
            received.append(message)

        scope = {"type": "http", "method": "GET", "path": "/clips/private.wav",
                 "query_string": b"token=secret", "headers": [(b"authorization", b"secret")]}
        with patch.object(diagnostics, "DEBUG", True), patch.object(diagnostics, "debug") as log:
            asyncio.run(diagnostics.DebugRequests(app)(scope, receive, send))
            self.assertEqual(received, messages)
            self.assertEqual(log.call_args.kwargs["status"], 206)
            self.assertEqual(log.call_args.kwargs["route"], "/clips/{filename}")
            self.assertNotIn("secret", str(log.call_args_list))
            self.assertNotIn("private.wav", str(log.call_args_list))

    def test_exception_propagates(self):
        async def app(scope, receive, send):
            raise RuntimeError("private exception detail")

        with patch.object(diagnostics, "DEBUG", True), patch.object(diagnostics, "debug") as log:
            with self.assertRaises(RuntimeError):
                asyncio.run(diagnostics.DebugRequests(app)({"type": "http", "method": "GET"}, None, None))
            self.assertEqual(log.call_args.kwargs["status"], 500)
            self.assertNotIn("private exception detail", str(log.call_args_list))


if __name__ == "__main__":
    unittest.main()

