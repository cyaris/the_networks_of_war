from __future__ import annotations

from shared import duplicate_values, participant_name_replacements

from pipeline import SOURCE_METADATA


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
