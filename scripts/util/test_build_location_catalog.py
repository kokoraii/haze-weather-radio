import hashlib
import json
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path

from build_location_catalog import build, extract_shapefile_archive, identifier_value, stable_id


class CatalogBuilderGoldenTest(unittest.TestCase):
    def test_shapefile_archive_selects_configured_member_without_path_extraction(self) -> None:
        with tempfile.TemporaryDirectory(prefix="haze-location-shape-select-") as directory:
            root = Path(directory)
            archive_path = root / "multi.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                for base in ("coarse", "land_CLCBaseZone_hybrid_unproj"):
                    archive.writestr(f"nested/{base}.shp", base.encode())
                    archive.writestr(f"nested/{base}.dbf", (base + "-dbf").encode())
                archive.writestr("../outside.shp", b"unsafe unrelated member")
            with zipfile.ZipFile(archive_path) as archive:
                selected = extract_shapefile_archive(
                    archive, root / "extract", "land_CLCBaseZone_hybrid_unproj.shp"
                )
            self.assertEqual(selected.name, "selected.shp")
            self.assertEqual(selected.read_bytes(), b"land_CLCBaseZone_hybrid_unproj")
            self.assertFalse((root / "outside.shp").exists())

    def test_stable_ids_normalize_provider_identifier_case(self) -> None:
        self.assertEqual(
            stable_id("ndbc", "marine_buoy", "18ci3"),
            stable_id("NDBC", "MARINE_BUOY", "18CI3"),
        )

    def test_identifier_parts_preserve_same_and_ugc_formats(self) -> None:
        properties = {"STATE": "ME", "FIPS": "23029"}
        self.assertEqual(
            identifier_value(properties, {"field": "FIPS", "prefix": "0"}),
            "023029",
        )
        self.assertEqual(
            identifier_value(
                properties,
                {
                    "parts": [
                        {"field": "STATE"},
                        {"literal": "C"},
                        {"field": "FIPS", "last": 3},
                    ]
                },
            ),
            "MEC029",
        )

    def test_builds_deterministic_search_and_spatial_indexes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="haze-location-golden-") as directory:
            root = Path(directory)
            source_path = root / "fixture.geojson"
            manifest_path = root / "manifest.json"
            output_path = root / "fixture.sqlite"
            source_path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "id": "00123",
                                "properties": {
                                    "code": "00123",
                                    "name": "Île Zéro",
                                    "region": "QC",
                                },
                                "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                            },
                            {
                                "type": "Feature",
                                "id": "00456",
                                "properties": {
                                    "code": "00456",
                                    "name": "Coordinate Unknown",
                                    "region": "SK",
                                },
                                "geometry": None,
                            },
                        ],
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            manifest_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "packs": [
                            {
                                "id": "fixture",
                                "sources": [
                                    {
                                        "id": "golden",
                                        "adapter": "geojson",
                                        "location": str(source_path),
                                        "kind": "weather_station",
                                        "id_fields": ["code"],
                                        "name_fields": ["name"],
                                        "region_fields": ["region"],
                                        "country": "CA",
                                        "identifiers": [
                                            {
                                                "authority": "fixture",
                                                "scheme": "same",
                                                "field": "code",
                                                "primary": True,
                                            }
                                        ],
                                        "expected_min": 2,
                                    }
                                ],
                            }
                        ],
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )

            build(manifest_path, "fixture", output_path, "2026-08-02T00:00:00Z", False)

            digest = hashlib.sha256(output_path.read_bytes()).hexdigest()
            self.assertTrue(output_path.with_suffix(".sqlite.sha256").read_text().startswith(digest))
            connection = sqlite3.connect(output_path)
            try:
                self.assertEqual(
                    connection.execute(
                        "SELECT normalized_value FROM identifiers WHERE value = '00123'"
                    ).fetchone()[0],
                    "00123",
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT latitude, longitude FROM geometries"
                    ).fetchone(),
                    (0.0, 0.0),
                )
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM geometries").fetchone()[0], 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM names_fts").fetchone()[0], 2)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM names_trigram").fetchone()[0], 2)
                self.assertEqual(
                    connection.execute(
                        "SELECT t9_digits FROM names WHERE normalized_name = 'ile zero'"
                    ).fetchone()[0],
                    "4539376",
                )
            finally:
                connection.close()

    def test_streams_archived_aliases_without_duplicate_entities_or_geometry(self) -> None:
        with tempfile.TemporaryDirectory(prefix="haze-location-delimited-") as directory:
            root = Path(directory)
            archive_path = root / "names.zip"
            manifest_path = root / "manifest.json"
            output_path = root / "fixture.sqlite"
            rows = (
                "ufi\tuni\tfull_name\tgeneric\tnt\tname_rank\tlat_dd\tlong_dd\tcc_ft\tadm1\tlang_cd\tefctv_dt\tterm_dt_f\n"
                "-42\t-100\tMontréal\tMontreal\tN\t1\t45.5019\t-73.5674\tCAN\tCA-QC\tfra\t2020-01-01\t\n"
                "-42\t-101\tMontreal\tMontreal\tV\t2\t45.5019\t-73.5674\tCAN\tCA-QC\teng\t2020-01-01\t\n"
                "-43\t-102\tUnranked\tUnranked\tV\t\t46.0\t-74.0\tCAN\tCA-QC\teng\t2020-01-01\t\n"
            )
            with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("Populated_Places.txt", rows)
            manifest_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "packs": [
                            {
                                "id": "fixture",
                                "sources": [
                                    {
                                        "id": "gns",
                                        "adapter": "delimited",
                                        "identity_authority": "nga-gns",
                                        "location": str(archive_path),
                                        "archive_contains": "Populated_Places.txt",
                                        "delimiter": "\t",
                                        "kind": "place",
                                        "id_fields": ["ufi"],
                                        "name_fields": ["full_name"],
                                        "locale_field": "lang_cd",
                                        "primary_name_when": {"field": "nt", "values": ["N"]},
                                        "include_when_present": ["name_rank"],
                                        "attribute_fields": ["name_rank", "generic"],
                                        "country_fields": ["cc_ft"],
                                        "region_fields": ["adm1"],
                                        "latitude_field": "lat_dd",
                                        "longitude_field": "long_dd",
                                        "identifiers": [
                                            {
                                                "authority": "nga",
                                                "scheme": "gns",
                                                "field": "ufi",
                                                "primary": True,
                                            },
                                            {
                                                "authority": "nga",
                                                "scheme": "gns_name",
                                                "field": "uni",
                                                "primary": False,
                                            },
                                        ],
                                        "valid_from_field": "efctv_dt",
                                        "valid_to_field": "term_dt_f",
                                        "expected_min": 2,
                                    }
                                ],
                            }
                        ],
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )

            build(manifest_path, "fixture", output_path, "2026-08-02T00:00:00Z", False)

            connection = sqlite3.connect(output_path)
            try:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM entities").fetchone()[0], 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM names").fetchone()[0], 2)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM geometries").fetchone()[0], 1)
                self.assertEqual(
                    connection.execute("SELECT name FROM names WHERE is_primary = 1").fetchone()[0],
                    "Montréal",
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM identifiers WHERE scheme = 'gns_name'"
                    ).fetchone()[0],
                    2,
                )
                self.assertEqual(
                    json.loads(
                        connection.execute("SELECT attributes_json FROM entities").fetchone()[0]
                    ),
                    {"generic": "Montreal", "name_rank": "1"},
                )
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
