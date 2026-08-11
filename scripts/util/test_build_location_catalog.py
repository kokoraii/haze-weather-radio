import hashlib
import json
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path

from build_location_catalog import build, extract_shapefile_archive, identifier_value, stable_id


class CatalogBuilderGoldenTest(unittest.TestCase):
    def test_statcan_population_enriches_existing_csd_without_new_geometry(self) -> None:
        with tempfile.TemporaryDirectory(prefix="haze-location-census-") as directory:
            root = Path(directory)
            source_path = root / "csd.geojson"
            population_path = root / "population.zip"
            manifest_path = root / "manifest.json"
            output_path = root / "fixture.sqlite"
            dguid = "2021A00054711066"
            source_path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {
                                    "CSDUID": "4711066",
                                    "CSDNAME": "Saskatoon",
                                    "DGUID": dguid,
                                    "PRUID": "47",
                                },
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": [-106.67, 52.13],
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            header = [
                "REF_DATE",
                "GEO",
                "DGUID",
                "Population 2021",
                "National rank",
                "Province rank",
            ]
            with zipfile.ZipFile(population_path, "w") as archive:
                archive.writestr(
                    "98100002.csv",
                    ",".join(header)
                    + "\n"
                    + f'2021,Saskatoon,{dguid},266141,19,1\n',
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
                                        "id": "statcan-csd",
                                        "adapter": "geojson",
                                        "identity_authority": "statcan",
                                        "location": str(source_path),
                                        "kind": "administrative_area",
                                        "id_fields": ["CSDUID"],
                                        "name_fields": ["CSDNAME"],
                                        "region_fields": ["PRUID"],
                                        "region_map": {"47": "SK"},
                                        "country": "CA",
                                        "identifiers": [
                                            {
                                                "authority": "statcan",
                                                "scheme": "sgc_dguid",
                                                "field": "DGUID",
                                                "primary": True,
                                            }
                                        ],
                                        "expected_min": 1,
                                    },
                                    {
                                        "id": "statcan-population",
                                        "adapter": "statcan_csd_population",
                                        "location": str(population_path),
                                        "archive_contains": "98100002.csv",
                                        "population_field": "Population 2021",
                                        "national_rank_field": "National rank",
                                        "province_rank_field": "Province rank",
                                        "expected_min": 1,
                                    },
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            build(manifest_path, "fixture", output_path, "2026-08-07T00:00:00Z", False)

            connection = sqlite3.connect(output_path)
            try:
                attributes = json.loads(
                    connection.execute("SELECT attributes_json FROM entities").fetchone()[0]
                )
                self.assertEqual(attributes["population"], 266141)
                self.assertEqual(attributes["census_year"], 2021)
                self.assertEqual(attributes["national_population_rank"], 19)
                self.assertEqual(attributes["province_population_rank"], 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM entities").fetchone()[0], 1)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM geometries").fetchone()[0], 1)
            finally:
                connection.close()

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

    def test_split_pack_keeps_core_representative_points_and_moves_area_wkb(self) -> None:
        with tempfile.TemporaryDirectory(prefix="haze-location-split-") as directory:
            root = Path(directory)
            source_path = root / "fixture.geojson"
            manifest_path = root / "manifest.json"
            core_path = root / "fixture.sqlite"
            geometry_path = root / "fixture-geometry.sqlite"
            member_manifest_path = root / "fixture.manifest.json"
            combined_path = root / "fixture-combined.sqlite"
            retrieved_at = "2026-08-05T00:00:00Z"
            source_path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "id": "point-1",
                                "properties": {"code": "point-1", "name": "Point Place"},
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": [-106.67, 52.13],
                                },
                            },
                            {
                                "type": "Feature",
                                "id": "polygon-1",
                                "properties": {"code": "polygon-1", "name": "Polygon Place"},
                                "geometry": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [-107.0, 52.0],
                                            [-106.0, 52.0],
                                            [-106.0, 53.0],
                                            [-107.0, 52.0],
                                        ]
                                    ],
                                },
                            },
                            {
                                "type": "Feature",
                                "id": "multipolygon-1",
                                "properties": {
                                    "code": "multipolygon-1",
                                    "name": "Multi Polygon Place",
                                },
                                "geometry": {
                                    "type": "MultiPolygon",
                                    "coordinates": [
                                        [
                                            [
                                                [-105.0, 51.0],
                                                [-104.0, 51.0],
                                                [-104.0, 52.0],
                                                [-105.0, 51.0],
                                            ]
                                        ],
                                        [
                                            [
                                                [-103.0, 50.0],
                                                [-102.0, 50.0],
                                                [-102.0, 51.0],
                                                [-103.0, 50.0],
                                            ]
                                        ],
                                    ],
                                },
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
                                        "kind": "forecast_zone",
                                        "id_fields": ["code"],
                                        "name_fields": ["name"],
                                        "identifiers": [
                                            {
                                                "authority": "fixture",
                                                "scheme": "fixture",
                                                "field": "code",
                                                "primary": True,
                                            }
                                        ],
                                        "expected_min": 3,
                                    }
                                ],
                            }
                        ],
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )

            build(
                manifest_path,
                "fixture",
                core_path,
                retrieved_at,
                False,
                geometry_path,
                member_manifest_path,
            )

            core = sqlite3.connect(core_path)
            geometry = sqlite3.connect(geometry_path)
            try:
                self.assertEqual(core.execute("SELECT COUNT(*) FROM entities").fetchone()[0], 3)
                self.assertEqual(
                    core.execute(
                        "SELECT geometry_type FROM geometries ORDER BY geometry_pk"
                    ).fetchall(),
                    [("point",), ("point",), ("point",)],
                )
                point_rows = core.execute(
                    """SELECT entities.canonical_id, geometries.latitude, geometries.longitude
                       FROM geometries JOIN entities USING(entity_pk)
                       ORDER BY entities.canonical_id"""
                ).fetchall()
                points = {row[0]: (row[1], row[2]) for row in point_rows}
                polygon_id = stable_id("golden", "forecast_zone", "polygon-1")
                multipolygon_id = stable_id("golden", "forecast_zone", "multipolygon-1")
                self.assertAlmostEqual(points[polygon_id][0], 52.3333333333)
                self.assertAlmostEqual(points[polygon_id][1], -106.3333333333)
                self.assertAlmostEqual(points[multipolygon_id][0], 50.8333333333)
                self.assertAlmostEqual(points[multipolygon_id][1], -103.3333333333)
                self.assertEqual(core.execute("SELECT rtreecheck('entity_rtree')").fetchone()[0], "ok")
                nearest_area_ids = {
                    row[0]
                    for row in core.execute(
                        """SELECT entities.canonical_id
                           FROM entity_rtree
                           JOIN geometries USING(geometry_pk)
                           JOIN entities USING(entity_pk)
                           WHERE entity_rtree.min_lon <= ? AND entity_rtree.max_lon >= ?
                             AND entity_rtree.min_lat <= ? AND entity_rtree.max_lat >= ?""",
                        (-106.3333333333, -106.3333333333, 52.3333333333, 52.3333333333),
                    ).fetchall()
                }
                self.assertIn(polygon_id, nearest_area_ids)
                geographically_biased = core.execute(
                    """SELECT entities.canonical_id
                       FROM geometries JOIN entities USING(entity_pk)
                       WHERE geometries.latitude IS NOT NULL AND geometries.longitude IS NOT NULL
                       ORDER BY ((geometries.latitude - ?) * (geometries.latitude - ?))
                              + ((geometries.longitude - ?) * (geometries.longitude - ?)),
                                entities.canonical_id
                       LIMIT 1""",
                    (52.3333333333, 52.3333333333, -106.3333333333, -106.3333333333),
                ).fetchone()[0]
                self.assertEqual(geographically_biased, polygon_id)
                self.assertEqual(
                    geometry.execute(
                        "SELECT geometry_type FROM area_geometries ORDER BY geometry_type"
                    ).fetchall(),
                    [("multipolygon",), ("polygon",)],
                )
                self.assertEqual(
                    geometry.execute(
                        "SELECT COUNT(*) FROM area_geometries WHERE source IS NOT NULL OR code IS NOT NULL"
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    geometry.execute("SELECT COUNT(*) FROM area_geometry_rtree").fetchone()[0],
                    2,
                )
                self.assertEqual(
                    geometry.execute("SELECT rtreecheck('area_geometry_rtree')").fetchone()[0],
                    "ok",
                )
                core_ids = {
                    row[0] for row in core.execute("SELECT canonical_id FROM entities").fetchall()
                }
                geometry_ids = {
                    row[0]
                    for row in geometry.execute(
                        "SELECT canonical_id FROM area_geometries"
                    ).fetchall()
                }
                self.assertEqual(
                    geometry_ids,
                    {
                        stable_id("golden", "forecast_zone", "polygon-1"),
                        stable_id("golden", "forecast_zone", "multipolygon-1"),
                    },
                )
                self.assertTrue(geometry_ids.issubset(core_ids))
                self.assertEqual(
                    geometry.execute(
                        "SELECT value FROM catalog_metadata WHERE key = 'pack_kind'"
                    ).fetchone()[0],
                    "geometry",
                )
                self.assertEqual(
                    geometry.execute(
                        "SELECT value FROM catalog_metadata WHERE key = 'core_pack_id'"
                    ).fetchone()[0],
                    "fixture",
                )
                self.assertEqual(
                    geometry.execute(
                        "SELECT value FROM catalog_metadata WHERE key = 'count.geometries'"
                    ).fetchone()[0],
                    "2",
                )
            finally:
                core.close()
                geometry.close()

            core_digest = hashlib.sha256(core_path.read_bytes()).hexdigest()
            geometry_digest = hashlib.sha256(geometry_path.read_bytes()).hexdigest()
            paired_geometry = sqlite3.connect(geometry_path)
            try:
                self.assertEqual(
                    paired_geometry.execute(
                        "SELECT value FROM catalog_metadata WHERE key = 'core_sha256'"
                    ).fetchone()[0],
                    core_digest,
                )
            finally:
                paired_geometry.close()
            self.assertEqual(
                core_path.with_suffix(".sqlite.sha256").read_text(encoding="ascii"),
                f"{core_digest}  {core_path.name}\n",
            )
            self.assertEqual(
                geometry_path.with_suffix(".sqlite.sha256").read_text(encoding="ascii"),
                f"{geometry_digest}  {geometry_path.name}\n",
            )
            fragment = json.loads(member_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(fragment["schema_version"], 1)
            self.assertEqual(fragment["pack_id"], "fixture")
            self.assertEqual(fragment["members"]["core"]["sha256"], core_digest)
            self.assertEqual(fragment["members"]["geometry"]["sha256"], geometry_digest)
            self.assertEqual(fragment["members"]["geometry"]["counts"]["area_geometries"], 2)
            first_fragment = member_manifest_path.read_bytes()

            build(
                manifest_path,
                "fixture",
                core_path,
                retrieved_at,
                False,
                geometry_path,
                member_manifest_path,
            )
            self.assertEqual(member_manifest_path.read_bytes(), first_fragment)

            build(manifest_path, "fixture", combined_path, retrieved_at, False)
            combined = sqlite3.connect(combined_path)
            try:
                self.assertEqual(
                    combined.execute(
                        "SELECT geometry_type FROM geometries ORDER BY geometry_type"
                    ).fetchall(),
                    [("multipolygon",), ("point",), ("polygon",)],
                )
                self.assertIsNone(
                    combined.execute(
                        "SELECT value FROM catalog_metadata WHERE key = 'pack_kind'"
                    ).fetchone()
                )
            finally:
                combined.close()

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
