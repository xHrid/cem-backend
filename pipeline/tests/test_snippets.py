import ast
import json
import tempfile
import unittest
import wave
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from snippet_extractor import extract_clip, extract_species_snippets


class SnippetTests(unittest.TestCase):
    def test_watcher_manifest_includes_local_python_dependencies(self):
        root = Path(__file__).resolve().parents[1]
        entry = next(item for item in json.loads((root / "manifest.json").read_text()) if item["id"] == "birdnet")
        bundled = {entry["script_file"], *entry["assets"]}
        for filename in sorted(bundled):
            if not filename.endswith(".py"):
                continue
            for node in ast.walk(ast.parse((root / filename).read_text())):
                modules = ([node.module] if isinstance(node, ast.ImportFrom) else
                           [name.name for name in node.names] if isinstance(node, ast.Import) else [])
                for module in modules:
                    dependency = (module or "").split(".")[0] + ".py"
                    if (root / dependency).is_file():
                        self.assertIn(dependency, bundled, f"{filename} needs {dependency} on local watchers")

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "project.json").write_text("{}")
        self.audio = self.root / "SPOT" / "audio" / "recording.wav"
        self.audio.parent.mkdir(parents=True)
        with wave.open(str(self.audio), "wb") as output:
            output.setparams((1, 2, 8000, 0, "NONE", "not compressed"))
            output.writeframes(b"\0\0" * 8000 * 20)
        self.aggregate = self.root / "dataset" / "aggregate.csv"
        self.aggregate.parent.mkdir()
        self.rows = [
            dict(common_name="Test bird", scientific_name="Test species", spot="SPOT",
                 start_time=4, end_time=7, confidence=0.6,
                 filename="recording.wav", filepath="/old/job/recording.wav", iucn_category="LC"),
            dict(common_name="Test bird", scientific_name="Test species", spot="SPOT",
                 start_time=14, end_time=17, confidence=0.9,
                 filename="recording.wav", filepath="/old/job/recording.wav", iucn_category="LC"),
        ]
        pd.DataFrame(self.rows).to_csv(self.aggregate, index=False)

    def test_nine_second_windows_at_start_middle_and_end(self):
        for start, end in [(0, 3), (4, 7), (17, 20)]:
            with self.subTest(start=start):
                output = self.root / f"clip-{start}.wav"
                _, _, duration = extract_clip(str(self.audio), str(output), start, end)
                self.assertEqual(duration, 9)
                with wave.open(str(output)) as clip:
                    self.assertEqual(clip.getnframes(), 9 * 8000)

    def test_highest_confidence_backfill_and_reuse(self):
        result = extract_species_snippets(str(self.aggregate))
        snippet = result["species"]["SPOT_Test bird"]
        self.assertEqual(snippet["max_confidence"], 0.9)
        self.assertEqual(snippet["snippet_window"]["duration"], 9)
        output = self.root / snippet["snippet_rel_path"]
        before = output.stat().st_mtime_ns
        extract_species_snippets(str(self.aggregate))
        self.assertEqual(output.stat().st_mtime_ns, before)
        output.unlink()
        extract_species_snippets(str(self.aggregate))
        self.assertTrue(output.is_file())

    def test_rerun_with_no_new_files_backfills_existing_detections(self):
        from birdnet_predictions import run_pipeline

        result = run_pipeline([], str(self.aggregate), str(self.root / "processed.txt"))
        self.assertTrue(result.empty)
        metadata = json.loads((self.root / "snippets" / "species_snippets.json").read_text())
        self.assertEqual(metadata["total_species_snippets"], 1)
        self.assertEqual(metadata["species"]["SPOT_Test bird"]["snippet_window"]["duration"], 9)

    def test_missing_audio_is_diagnosable(self):
        self.audio.unlink()
        with patch("snippet_extractor.debug") as log:
            result = extract_species_snippets(str(self.aggregate))
        self.assertEqual(result["total_species_snippets"], 0)
        self.assertTrue(any(call.kwargs.get("reason") == "source_missing" for call in log.call_args_list))


if __name__ == "__main__":
    unittest.main()
