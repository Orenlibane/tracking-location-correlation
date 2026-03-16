#!/usr/bin/env python3
"""
Build area polygon boundaries from municipality GeoJSON files.
Each municipality is shown as its own polygon, colored by area.
Each municipality is assigned to exactly ONE area (majority vote).
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
    munis = []
    for folder in os.listdir(POLY_DIR):
        folder_path = os.path.join(POLY_DIR, folder)
        if not os.path.isdir(folder_path):
            continue
        for gf in glob.glob(os.path.join(folder_path, "*.geojson")):
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
                    })
            except Exception as e:
                print(f"  Warning: Failed to load {gf}: {e}")
    return munis


def geom_to_leaflet(geom, simplify_tolerance=0.002):
    """Convert shapely geometry to Leaflet [lat, lon] arrays."""
    if simplify_tolerance:
        geom = geom.simplify(simplify_tolerance, preserve_topology=True)

    if isinstance(geom, Polygon):
        coords = list(geom.exterior.coords)
        return [[round(lat, 5), round(lon, 5)] for lon, lat in coords]
    elif isinstance(geom, MultiPolygon):
        result = []
        for poly in geom.geoms:
            coords = list(poly.exterior.coords)
            result.append([[round(lat, 5), round(lon, 5)] for lon, lat in coords])
        return result
    return None


def main():
    print("Loading locations...")
    with open(LOCATIONS_FILE, 'r') as f:
        data = json.load(f)
    locations = data["locations"]

    by_area = {}
    for loc in locations:
        by_area.setdefault(loc["area"], []).append(loc)
    print(f"Found {len(locations)} locations in {len(by_area)} areas")

    print("Loading municipality polygons...")
    municipalities = load_all_municipalities()
    print(f"Loaded {len(municipalities)} municipality polygons")

    # For each location, find its municipality
    muni_area_counts = defaultdict(lambda: defaultdict(int))
    muni_geometries = {}
    unmatched = []

    for loc in locations:
        pt = Point(loc["lon"], loc["lat"])
        found = False
        for m in municipalities:
            if m["geometry"].contains(pt):
                name = m["name_heb"]
                muni_area_counts[name][loc["area"]] += 1
                muni_geometries[name] = m["geometry"]
                found = True
                break
        if not found:
            unmatched.append(loc)

    if unmatched:
        print(f"\n  {len(unmatched)} locations not in any municipality:")
        for loc in unmatched:
            print(f"    - {loc['name']} ({loc['area']}) at {loc['lat']}, {loc['lon']}")

    # Assign each municipality to ONE area (majority vote)
    muni_to_area = {}
    for muni_name, area_counts in muni_area_counts.items():
        winner = max(area_counts, key=area_counts.get)
        muni_to_area[muni_name] = winner
        if len(area_counts) > 1:
            parts = ', '.join(f'{a}({c})' for a, c in sorted(area_counts.items(), key=lambda x: -x[1]))
            print(f"  Conflict: {muni_name}: {parts} -> {winner}")

    # Group by area, output individual municipality polygons
    area_munis = defaultdict(list)
    for muni_name, area in muni_to_area.items():
        coords = geom_to_leaflet(muni_geometries[muni_name], simplify_tolerance=0.002)
        if coords:
            area_munis[area].append({"name": muni_name, "coords": coords})

    print("\nArea municipality counts:")
    for area in by_area:
        munis = area_munis.get(area, [])
        print(f"  {area}: {len(munis)} municipalities")

    # Output as JavaScript
    js_output = "const AREA_MUNICIPALITIES = "
    js_output += json.dumps(dict(area_munis), ensure_ascii=False, separators=(',', ':'))
    js_output += ";\n"

    output_file = os.path.join(os.path.dirname(LOCATIONS_FILE), "area_boundaries.js")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(js_output)
    print(f"\nWritten to {output_file} ({len(js_output)} bytes)")


if __name__ == "__main__":
    main()
