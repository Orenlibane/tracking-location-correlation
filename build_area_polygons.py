#!/usr/bin/env python3
"""
Build area polygon boundaries from municipality GeoJSON files.
For each area in locations.json, finds which municipality polygons contain
the area's locations, then merges those polygons into a single boundary.

Key rule: each municipality is assigned to exactly ONE area (the one with
the most locations inside it) to prevent overlapping borders.
"""

import json
import os
import glob
from collections import defaultdict
from shapely.geometry import shape, Point, MultiPolygon, Polygon
from shapely.ops import unary_union

POLY_DIR = "/Users/orentzezana/Downloads/poly/israel-municipalities-polygons"
LOCATIONS_FILE = "/Users/orentzezana/Desktop/my-test-rpg-dungeon/map-tracking/locations.json"


def load_all_municipalities():
    """Load all municipality GeoJSON polygons."""
    munis = []
    for folder in os.listdir(POLY_DIR):
        folder_path = os.path.join(POLY_DIR, folder)
        if not os.path.isdir(folder_path):
            continue
        geojson_files = glob.glob(os.path.join(folder_path, "*.geojson"))
        for gf in geojson_files:
            try:
                with open(gf, 'r') as f:
                    data = json.load(f)
                for feature in data.get("features", []):
                    geom = shape(feature["geometry"])
                    if not geom.is_valid:
                        geom = geom.buffer(0)
                    props = feature.get("properties", {})
                    munis.append({
                        "name_heb": props.get("MUN_HEB", ""),
                        "name_eng": props.get("MUN_ENG", ""),
                        "geometry": geom,
                        "file": gf
                    })
            except Exception as e:
                print(f"  Warning: Failed to load {gf}: {e}")
    return munis


def find_municipality_for_point(lat, lon, municipalities):
    """Find which municipality polygon contains a given point."""
    point = Point(lon, lat)  # GeoJSON is lon,lat
    for muni in municipalities:
        if muni["geometry"].contains(point):
            return muni
    return None


def polygon_to_leaflet_coords(geom, simplify_tolerance=0.002):
    """Convert a shapely geometry to Leaflet-compatible [lat, lon] coordinate arrays."""
    if simplify_tolerance:
        geom = geom.simplify(simplify_tolerance, preserve_topology=True)

    if isinstance(geom, Polygon):
        coords = list(geom.exterior.coords)
        return [[round(lat, 6), round(lon, 6)] for lon, lat in coords]
    elif isinstance(geom, MultiPolygon):
        result = []
        for poly in geom.geoms:
            coords = list(poly.exterior.coords)
            result.append([[round(lat, 6), round(lon, 6)] for lon, lat in coords])
        return result
    else:
        raise ValueError(f"Unexpected geometry type: {type(geom)}")


def main():
    print("Loading locations...")
    with open(LOCATIONS_FILE, 'r') as f:
        data = json.load(f)
    locations = data["locations"]

    by_area = {}
    for loc in locations:
        area = loc["area"]
        if area not in by_area:
            by_area[area] = []
        by_area[area].append(loc)

    print(f"Found {len(locations)} locations in {len(by_area)} areas")

    print("Loading municipality polygons...")
    municipalities = load_all_municipalities()
    print(f"Loaded {len(municipalities)} municipality polygons")

    # Step 1: For each location, find its municipality
    # Track: municipality -> {area: count} to resolve conflicts
    muni_area_counts = defaultdict(lambda: defaultdict(int))
    muni_geometries = {}  # muni_name -> geometry
    unmatched_locs = []

    for loc in locations:
        muni = find_municipality_for_point(loc["lat"], loc["lon"], municipalities)
        if muni:
            name = muni["name_heb"]
            muni_area_counts[name][loc["area"]] += 1
            muni_geometries[name] = muni["geometry"]
        else:
            unmatched_locs.append(loc)

    if unmatched_locs:
        print(f"\n  {len(unmatched_locs)} locations not matched to any municipality:")
        for loc in unmatched_locs:
            print(f"    - {loc['name']} ({loc['area']}) at {loc['lat']}, {loc['lon']}")

    # Step 2: Assign each municipality to exactly ONE area (majority vote)
    muni_to_area = {}
    conflicts = []
    for muni_name, area_counts in muni_area_counts.items():
        if len(area_counts) > 1:
            conflicts.append((muni_name, dict(area_counts)))
        # Assign to area with most locations
        winner = max(area_counts, key=area_counts.get)
        muni_to_area[muni_name] = winner

    if conflicts:
        print(f"\n  {len(conflicts)} municipalities claimed by multiple areas (resolved by majority):")
        for muni_name, counts in conflicts:
            winner = muni_to_area[muni_name]
            parts = ', '.join(f'{a}({c})' for a, c in sorted(counts.items(), key=lambda x: -x[1]))
            print(f"    {muni_name}: {parts} -> assigned to {winner}")

    # Step 3: Group municipalities by their assigned area
    area_municipalities = defaultdict(dict)
    for muni_name, area in muni_to_area.items():
        area_municipalities[area][muni_name] = muni_geometries[muni_name]

    print("\nArea municipality assignments:")
    for area in by_area:
        munis = area_municipalities.get(area, {})
        print(f"  {area}: {len(munis)} municipalities: {', '.join(munis.keys())}")

    # Step 4: Merge municipality polygons per area
    print("\nMerging polygons per area...")
    area_boundaries = {}
    for area in by_area:
        munis = area_municipalities.get(area, {})
        if not munis:
            print(f"  {area}: No municipalities matched, skipping")
            continue

        geometries = list(munis.values())
        merged = unary_union(geometries)

        if not merged.is_valid:
            merged = merged.buffer(0)

        leaflet_coords = polygon_to_leaflet_coords(merged, simplify_tolerance=0.003)
        area_boundaries[area] = leaflet_coords

        if isinstance(leaflet_coords[0][0], list):
            total_pts = sum(len(ring) for ring in leaflet_coords)
        else:
            total_pts = len(leaflet_coords)

        if isinstance(merged, MultiPolygon):
            print(f"  {area}: {len(munis)} municipalities -> MultiPolygon with {len(merged.geoms)} parts, {total_pts} points")
        else:
            print(f"  {area}: {len(munis)} municipalities -> Polygon, {total_pts} points")

    # Output as compact JavaScript
    print("\nGenerating JavaScript...")
    js_output = "const AREA_BOUNDARIES = "
    js_output += json.dumps(area_boundaries, ensure_ascii=False, separators=(',', ':'))
    js_output += ";\n"

    output_file = os.path.join(os.path.dirname(LOCATIONS_FILE), "area_boundaries.js")
    with open(output_file, 'w') as f:
        f.write(js_output)
    print(f"Written to {output_file}")


if __name__ == "__main__":
    main()
