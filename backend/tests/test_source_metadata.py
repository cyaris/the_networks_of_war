from __future__ import annotations

from datetime import date

from shared import duplicate_values, participant_name_replacements

from pipeline import SOURCE_METADATA, STEP_1_SOURCE_KEYS


def test_source_tables_have_download_urls():
    missing = [metadata["key"] for metadata in SOURCE_METADATA if not metadata.get("downloads")]
    non_absolute_urls = [
        download["url"]
        for metadata in SOURCE_METADATA
        for download in metadata.get("downloads", [])
        if not download["url"].startswith(("http://", "https://"))
    ]

    assert missing == []
    assert non_absolute_urls == []


def test_json_config_items_have_unique_source_or_key_values():
    source_metadata_keys = [metadata["key"] for metadata in SOURCE_METADATA]
    participant_replacement_sources = [source for source, _ in participant_name_replacements()]

    assert duplicate_values(source_metadata_keys) == []
    assert duplicate_values(participant_replacement_sources) == []


def test_source_encoding_overrides_are_defined_in_metadata():
    encodings = {metadata["key"]: metadata["encoding"] for metadata in SOURCE_METADATA if metadata.get("encoding")}

    assert encodings == {
        "cow_trade_national": "latin-1",
        "dd_revisited": "latin-1",
        "extrastate_wars": "cp1252",
        "national_material_capabilities": "latin-1",
        "territorial_changes": "latin-1",
    }


def test_step_1_source_release_dates_are_defined_in_metadata():
    release_dates = {
        metadata["key"]: date.fromisoformat(metadata["release_date"])
        for metadata in SOURCE_METADATA
        if metadata["key"] in STEP_1_SOURCE_KEYS
    }

    assert release_dates == {
        "country_codes": date(2022, 9, 7),
        "extrastate_wars": date(2011, 12, 8),
        "interstate_mid_dyads": date(2025, 4, 6),
        "interstate_war_dyads": date(2022, 7, 12),
        "interstate_wars": date(2011, 3, 1),
        "intrastate_wars": date(2020, 4, 6),
    }
