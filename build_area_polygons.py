#!/usr/bin/env python3
"""
Build area polygon boundaries from municipality GeoJSON files.
For each area in locations.json, finds which municipality polygons contain
the area's locations, then merges those polygons into a single boundary.
"""

import json
import os
import glob
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
    # If no exact match, find nearest
    return None


def polygon_to_leaflet_coords(geom, simplify_tolerance=0.002):
    """Convert a shapely geometry to Leaflet-compatible [lat, lon] coordinate arrays."""
    if simplify_tolerance:
        geom = geom.simplify(simplify_tolerance, preserve_topology=True)

    if isinstance(geom, Polygon):
        # Single polygon - return array of [lat, lon] pairs
        coords = list(geom.exterior.coords)
        return [[round(lat, 6), round(lon, 6)] for lon, lat in coords]
    elif isinstance(geom, MultiPolygon):
        # Multi polygon - return array of arrays
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

    # Group locations by area
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

    # For each area, find which municipalities contain its locations
    area_municipalities = {}  # area -> set of municipality geometries
    unmatched_locs = []

    for area, locs in by_area.items():
        area_munis = {}  # muni_name -> geometry
        for loc in locs:
            muni = find_municipality_for_point(loc["lat"], loc["lon"], municipalities)
            if muni:
                key = muni["name_heb"]
                if key not in area_munis:
                    area_munis[key] = muni["geometry"]
            else:
                unmatched_locs.append(loc)
        area_municipalities[area] = area_munis
        print(f"  {area}: {len(locs)} locations -> {len(area_munis)} municipalities: {', '.join(area_munis.keys())}")

    if unmatched_locs:
        print(f"\n  {len(unmatched_locs)} locations not matched to any municipality:")
        for loc in unmatched_locs:
            print(f"    - {loc['name']} ({loc['area']}) at {loc['lat']}, {loc['lon']}")

    # Merge municipality polygons per area
    print("\nMerging polygons per area...")
    area_boundaries = {}
    for area, munis in area_municipalities.items():
        if not munis:
            print(f"  {area}: No municipalities matched, skipping")
            continue

        geometries = list(munis.values())
        merged = unary_union(geometries)

        # Make sure result is valid
        if not merged.is_valid:
            merged = merged.buffer(0)

        # Simplify more aggressively: ~0.003 degrees ≈ ~300m, good visual quality
        leaflet_coords = polygon_to_leaflet_coords(merged, simplify_tolerance=0.003)
        area_boundaries[area] = leaflet_coords

        # Count total points
        if isinstance(leaflet_coords[0][0], list):
            total_pts = sum(len(ring) for ring in leaflet_coords)
        else:
            total_pts = len(leaflet_coords)

        if isinstance(merged, MultiPolygon):
            print(f"  {area}: Merged {len(munis)} municipalities -> MultiPolygon with {len(merged.geoms)} parts, {total_pts} points")
        else:
            print(f"  {area}: Merged {len(munis)} municipalities -> Polygon, {total_pts} points")

    # Output as compact JavaScript (no indent to save space)
    print("\nGenerating JavaScript...")
    js_output = "const AREA_BOUNDARIES = "
    js_output += json.dumps(area_boundaries, ensure_ascii=False, separators=(',', ':'))
    js_output += ";\n"

    output_file = os.path.join(os.path.dirname(LOCATIONS_FILE), "area_boundaries.js")
    with open(output_file, 'w') as f:
        f.write(js_output)
    print(f"Written to {output_file}")

    # Also print for easy copy
    print("\n" + "=" * 60)
    print("AREA_BOUNDARIES for index.html:")
    print("=" * 60)
    print(js_output)


if __name__ == "__main__":
    main()
