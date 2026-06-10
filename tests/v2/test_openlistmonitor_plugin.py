"""OpenListMonitor focused regression tests."""

from openlistmonitor import OpenListMonitor  # noqa: E402


def test_configured_video_extensions_do_not_filter_subtitles(monkeypatch):
    """`extensions` filters only primary video files, not matched subtitle extras."""
    plugin = OpenListMonitor()
    plugin._extensions = ".mp4"
    plugin._min_file_size_mb = 10
    plugin._recursive = False

    monkeypatch.setattr(plugin, "get_data", lambda key: [])
    monkeypatch.setattr(
        plugin,
        "_list_directory",
        lambda base_url, headers, path: (
            {
                "files": [
                    {
                        "name": "Show.S01E01.mp4",
                        "size": 100 * 1024 * 1024,
                        "is_dir": False,
                    },
                    {
                        "name": "Show.S01E01.ass",
                        "size": 100 * 1024,
                        "is_dir": False,
                    },
                    {
                        "name": "Show.S01E01.mkv",
                        "size": 100 * 1024 * 1024,
                        "is_dir": False,
                    },
                ]
            },
            None,
        ),
    )

    stats = {"errors": [], "dirs": 0, "files": 0}
    files = plugin._scan_directory("", {}, "/source", 0, stats)

    assert [file["name"] for file in files] == ["Show.S01E01.mp4"]
    assert files[0]["extra_file_names"] == ["Show.S01E01.ass"]
    assert plugin._is_subtitle_ext(".ass")
    assert not plugin._is_video_ext(".ass")


def test_ai_title_hints_clean_romanized_symbol_names():
    plugin = OpenListMonitor()

    hints = plugin._extract_candidate_title_hints(
        "[SubsPlease] Tamon-kun_Ima_Docchi_-_01 [1080p].mkv"
    )
    variants = plugin._build_title_punctuation_variants("Tamon-kun Ima Docchi")

    assert "Tamon-kun Ima Docchi" in hints
    assert "Tamon-kun Ima Docchi!?" in variants
    assert plugin._is_noise_candidate_title("Season 2")


def test_ai_candidates_include_parent_dirs_and_keep_top_ten(monkeypatch):
    plugin = OpenListMonitor()

    class DummyFileItem:
        path = "/downloads/Anime/Kusuriya no Hitorigoto Season 2/Episode 01/video.mkv"
        name = "video.mkv"

    monkeypatch.setattr(
        plugin,
        "_invoke_ai_recognition_candidates",
        lambda fileitem, source_meta: [
            {
                "name": f"Candidate {index}",
                "media_type": "tv",
                "confidence": 0.9 - index / 100,
            }
            for index in range(12)
        ],
    )

    heuristic = plugin._build_heuristic_recognition_candidates(DummyFileItem())
    heuristic_names = [item["name"] for item in heuristic]
    candidates = plugin._get_ai_recognition_candidates(DummyFileItem())
    names = [item["name"] for item in candidates]

    assert len(candidates) == plugin.AI_RECOGNITION_MAX_CANDIDATES
    assert "Kusuriya no Hitorigoto" in heuristic_names
    assert "Candidate 0" in names
