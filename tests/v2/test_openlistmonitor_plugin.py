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


def test_move_mode_does_not_filter_recorded_subtitle_extras(monkeypatch):
    """Move mode must retry visible subtitle extras even if old records exist."""
    plugin = OpenListMonitor()
    plugin._extensions = ".mp4"
    plugin._min_file_size_mb = 10
    plugin._recursive = False
    plugin._transfer_type = "move"

    recorded_subtitle = "/source|Show.S01E01.ass"
    monkeypatch.setattr(
        plugin,
        "get_data",
        lambda key: [recorded_subtitle]
        if key == plugin.STORE_EXTRA_FILES_KEY
        else [],
    )
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
                ]
            },
            None,
        ),
    )

    stats = {"errors": [], "dirs": 0, "files": 0}
    files = plugin._scan_directory("", {}, "/source", 0, stats)

    assert files[0]["extra_file_names"] == ["Show.S01E01.ass"]


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


def test_ai_recognition_rejects_mismatched_source_year():
    plugin = OpenListMonitor()

    class DummyMediaInfo:
        year = "2014"
        title_year = "七大罪 (2014)"

    assert not plugin._is_ai_recognition_year_compatible(
        "/downloads/[MagicStar] Kujou no Taizai 2026/Kujou.no.Taizai.EP01.mkv",
        {"name": "The Seven Deadly Sins", "year": "2026"},
        DummyMediaInfo(),
    )


def test_leftover_subtitle_target_rejects_mismatched_year():
    plugin = OpenListMonitor()

    assert not plugin._is_target_year_compatible(
        "/downloads/[MagicStar] Kujou no Taizai 2026",
        "/library/番剧/七大罪 (2014) {tmdb-62104}/Season 01",
    )
    assert plugin._is_target_year_compatible(
        "/downloads/[MagicStar] Kujou no Taizai 2026",
        "/library/番剧/Kujou no Taizai/Season 01",
    )


def test_preview_episode_count_rejects_total_episode_mismatch(monkeypatch):
    plugin = OpenListMonitor()
    monkeypatch.setattr(
        plugin,
        "_get_tmdb_season_episode_count",
        lambda tmdb_id, season: 6,
    )

    error = plugin._get_preview_episode_count_error({
        "items": [
            {
                "source": (
                    "/downloads/给你宇宙[全12集][简繁英字幕].Our.Universe.S01/"
                    "Our.Universe.S01E01.Episode.1.mkv"
                ),
                "target": (
                    "/library/我们的宇宙 (2022) {tmdb-213608}/Season 01/"
                    "Our.Universe.S01E01.mkv"
                ),
                "title": "我们的宇宙 (2022)",
                "season": 1,
            }
        ]
    })

    assert "全 12 集" in error
    assert "只有 6 集" in error


def test_preview_episode_count_rejects_source_episode_overflow(monkeypatch):
    plugin = OpenListMonitor()
    monkeypatch.setattr(
        plugin,
        "_get_tmdb_season_episode_count",
        lambda tmdb_id, season: 6,
    )

    error = plugin._get_preview_episode_count_error({
        "items": [
            {
                "source": "/downloads/Our.Universe.S01E12.Episode.12.mkv",
                "target": (
                    "/library/我们的宇宙 (2022) {tmdb-213608}/Season 01/"
                    "Our.Universe.S01E12.mkv"
                ),
                "title": "我们的宇宙 (2022)",
                "season": 1,
            }
        ]
    })

    assert "第 12 集" in error
    assert "只有 6 集" in error


def test_retry_preview_uses_ai_when_native_episode_guard_fails(monkeypatch):
    plugin = OpenListMonitor()
    plugin._ai_recognition_fallback = True

    class DummyFileItem:
        path = "/downloads/Our.Universe.S01E12.mkv"
        name = "Our.Universe.S01E12.mkv"

    class DummyMeta:
        begin_season = 1
        begin_episode = 12
        end_episode = None

    class DummyMediaType:
        value = "电视剧"

    class DummyMediaInfo:
        title_year = "给你宇宙"
        title = "给你宇宙"
        tmdb_id = 999
        type = DummyMediaType()

    monkeypatch.setattr(
        plugin,
        "_build_ai_recognition_result",
        lambda **kwargs: (DummyMeta(), DummyMediaInfo()),
    )
    monkeypatch.setattr(
        plugin,
        "_preview_remote_transfer",
        lambda **kwargs: (
            True,
            {
                "items": [
                    {
                        "source": DummyFileItem.path,
                        "target": "/library/给你宇宙 {tmdb-999}/Season 01/Our.Universe.S01E12.mkv",
                        "title": "给你宇宙",
                        "season": 1,
                        "episode": 12,
                    }
                ]
            },
        ),
    )
    monkeypatch.setattr(
        plugin,
        "_get_tmdb_season_episode_count",
        lambda tmdb_id, season: 12,
    )

    transfer_options = {}
    state, preview_data, error = plugin._retry_preview_with_ai_recognition(
        transfer_chain=None,
        fileitem=DummyFileItem(),
        target_storage="alist",
        transfer_options=transfer_options,
        transfer_type="move",
        source_meta=None,
        reason="原生识别季集不匹配",
    )

    assert state
    assert not error
    assert preview_data["items"][0]["title"] == "给你宇宙"
    assert transfer_options["recognition_source"] == "ai"
    assert "原生识别季集校验失败" in transfer_options["ai_recognition_detail"]["reason"]


def test_finish_notification_includes_ai_fallback_reason(monkeypatch):
    plugin = OpenListMonitor()
    plugin._notify = True
    messages = []

    monkeypatch.setattr(
        plugin,
        "post_message",
        lambda **kwargs: messages.append(kwargs),
    )

    plugin._send_finish_notification(
        "检查完成，发现 1 个新文件，已整理 1 个",
        {
            "target_path_rules": [{"source": "/downloads", "target": "/library"}],
            "new_files": 1,
            "transferred": 1,
            "ai_recognition_fallback": 1,
            "ai_recognition_fallback_items": [
                {
                    "name": "Our.Universe.S01E12.mkv",
                    "title": "给你宇宙",
                    "tmdb_id": 999,
                    "type": "电视剧",
                    "reason": "原生识别季集校验失败：源文件包含第 12 集",
                }
            ],
            "errors": [],
        },
    )

    assert messages
    assert "AI识别兜底：1" in messages[0]["text"]
    assert "原因：原生识别季集校验失败" in messages[0]["text"]


def test_finish_notification_includes_transfer_details(monkeypatch):
    plugin = OpenListMonitor()
    plugin._notify = True
    messages = []

    monkeypatch.setattr(
        plugin,
        "post_message",
        lambda **kwargs: messages.append(kwargs),
    )

    plugin._send_finish_notification(
        "检查完成，发现 1 个新文件，已整理 1 个",
        {
            "target_path_rules": [{"source": "/downloads", "target": "/library"}],
            "new_files": 1,
            "transferred": 1,
            "transferred_items": [
                {
                    "name": "Our.Universe.S01E12.mkv",
                    "title": "给你宇宙",
                    "type": "电视剧",
                    "season": 1,
                    "episode": 12,
                    "target": "/library/给你宇宙 {tmdb-999}/Season 01/Our.Universe.S01E12.mkv",
                    "recognition": "AI",
                    "extra_count": 2,
                }
            ],
            "errors": [],
        },
    )

    assert messages
    text = messages[0]["text"]
    assert "整理明细：" in text
    assert "Our.Universe.S01E12.mkv -> 给你宇宙" in text
    assert "S01E12" in text
    assert "目标：/library/给你宇宙 {tmdb-999}/Season 01/Our.Universe.S01E12.mkv" in text


def test_rescan_extra_files_matches_subtitles_after_video_move(monkeypatch):
    plugin = OpenListMonitor()

    monkeypatch.setattr(
        plugin,
        "_list_directory",
        lambda base_url, headers, path, refresh=None: (
            {
                "files": [
                    {
                        "name": "Kujou.no.Taizai.EP01.1080p.NF.WEB-DL.Chs.srt",
                        "is_dir": False,
                    },
                    {
                        "name": "Kujou.no.Taizai.EP01.1080p.NF.WEB-DL.Eng.srt",
                        "is_dir": False,
                    },
                    {
                        "name": "Other.Show.EP01.srt",
                        "is_dir": False,
                    },
                ]
            },
            None,
        ),
    )

    matched = plugin._find_matching_extra_names(
        "",
        {},
        "/downloads/Kujou no Taizai 2026",
        "Kujou.no.Taizai.EP01.1080p.NF.WEB-DL.mkv",
    )

    assert matched == [
        "Kujou.no.Taizai.EP01.1080p.NF.WEB-DL.Chs.srt",
        "Kujou.no.Taizai.EP01.1080p.NF.WEB-DL.Eng.srt",
    ]
