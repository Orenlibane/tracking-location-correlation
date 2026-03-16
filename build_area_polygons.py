#!/usr/bin/env python3
"""
Build area polygon boundaries from municipality GeoJSON files.
Uses district-level boundaries to constrain which area each municipality
can be assigned to, preventing cross-region assignment errors.
"""

import json
import os
import glob
from collections import defaultdict
from shapely.geometry import shape, Point, MultiPolygon, Polygon
from shapely.ops import unary_union

POLY_DIR = "/Users/orentzezana/Downloads/poly/israel-municipalities-polygons"
LOCATIONS_FILE = "/Users/orentzezana/Desktop/my-test-rpg-dungeon/map-tracking/locations.json"
DISTRICTS_FILE = "/Users/orentzezana/Desktop/my-test-rpg-dungeon/map-tracking/districts.geojson"

# Which app areas are allowed in each district
DISTRICT_TO_AREAS = {
    "tel_aviv":  ["תל אביב"],
    "jerusalem": ["ירושלים"],
    "center":    ["המרכז", "השרון", "הדרום", "תל אביב"],
    "haifa":     ["המפרץ", "הצפון", "השרון"],
    "north":     ["הצפון", "המפרץ", "השרון"],
    "south":     ["אשדוד", "הדרום"],
}


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


def load_districts():
    with open(DISTRICTS_FILE, 'r') as f:
        data = json.load(f)
    districts = {}
    for feature in data["features"]:
        geom = shape(feature["geometry"])
        districts[feature["id"]] = geom
    return districts


def find_district(lat, lon, districts):
    """Find which district a point falls in."""
    pt = Point(lon, lat)
    for dist_id, geom in districts.items():
        if geom.contains(pt):
            return dist_id
    return None


def geom_to_leaflet(geom, simplify_tolerance=0.002):
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

    print("Loading districts...")
    districts = load_districts()
    print(f"Loaded {len(districts)} districts")

    print("Loading municipality polygons...")
    municipalities = load_all_municipalities()
    print(f"Loaded {len(municipalities)} municipality polygons")

    # Step 1: For each location, find its municipality AND district
    muni_area_counts = defaultdict(lambda: defaultdict(int))
    muni_geometries = {}
    muni_districts = defaultdict(set)  # track which districts a muni spans
    unmatched = []

    for loc in locations:
        pt = Point(loc["lon"], loc["lat"])
        found_muni = False
        for m in municipalities:
            if m["geometry"].contains(pt):
                name = m["name_heb"]
                muni_area_counts[name][loc["area"]] += 1
                muni_geometries[name] = m["geometry"]
                found_muni = True
                # Track district for this municipality (use centroid)
                centroid = m["geometry"].centroid
                dist = find_district(centroid.y, centroid.x, districts)
                if dist:
                    muni_districts[name].add(dist)
                break
        if not found_muni:
            unmatched.append(loc)

    if unmatched:
        print(f"\n  {len(unmatched)} locations not in any municipality:")
        for loc in unmatched:
            print(f"    - {loc['name']} ({loc['area']}) at {loc['lat']}, {loc['lon']}")

    # Step 2: Assign each municipality to ONE area, constrained by district
    muni_to_area = {}
    for muni_name, area_counts in muni_area_counts.items():
        dists = muni_districts.get(muni_name, set())

        # Get allowed areas based on district
        allowed_areas = set()
        for d in dists:
            allowed_areas.update(DISTRICT_TO_AREAS.get(d, []))

        if allowed_areas:
            # Filter counts to only allowed areas
            filtered = {a: c for a, c in area_counts.items() if a in allowed_areas}
            if filtered:
                winner = max(filtered, key=filtered.get)
            else:
                # No match with district constraint - fall back to majority
                winner = max(area_counts, key=area_counts.get)
                print(f"  No district match for {muni_name} (district: {dists}, areas: {list(area_counts.keys())}) -> fallback to {winner}")
        else:
            # No district found - fall back to majority
            winner = max(area_counts, key=area_counts.get)

        muni_to_area[muni_name] = winner

        if len(area_counts) > 1:
            parts = ', '.join(f'{a}({c})' for a, c in sorted(area_counts.items(), key=lambda x: -x[1]))
            print(f"  Conflict: {muni_name}: {parts} -> {winner}")

    # Step 3: Group by area, output individual municipality polygons
    area_munis = defaultdict(list)
    for muni_name, area in muni_to_area.items():
        coords = geom_to_leaflet(muni_geometries[muni_name], simplify_tolerance=0.002)
        if coords:
            area_munis[area].append({"name": muni_name, "coords": coords})

    print("\nArea municipality counts:")
    for area in by_area:
        munis = area_munis.get(area, [])
        names = [m["name"] for m in munis]
        print(f"  {area}: {len(munis)} municipalities: {', '.join(names)}")

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
