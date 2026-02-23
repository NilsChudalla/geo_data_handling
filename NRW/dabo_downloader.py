"""
DABO NRW – Downloader für vollständige Schichtverzeichnisse (boreholeML)
Generiert mit Claude Sonnet 4.6, 23.02.2026, Nils Chudalla
========================================================================
Quelle: Web Feature Service „boreholeML Full"
URL:    https://www.bml3.nrw.de/service/bml
Lizenz: Datenlizenz Deutschland – Namensnennung – Version 2.0
        https://www.govdata.de/dl-de/by-2-0

Installation:
    pip install requests pyproj

Verwendung:
    python dabo_downloader.py
    → selection.geojson im gleichen Verzeichnis ablegen
"""



import requests
import json
import xml.etree.ElementTree as ET
import os
import time
import sys
from datetime import datetime

from pyproj import Transformer

# =============================================================================
# KONFIGURATION
# =============================================================================

BASE_URL       = "https://www.bml3.nrw.de/service/bml"
WFS_VERSION    = "2.0.0"
OUTPUT_FORMAT  = "text/xml; subtype=gml/3.2.1"
OUTPUT_FILE    = None        # None = automatischer Name mit Zeitstempel

SELECTION_FILE = "selection.geojson"   # bnum-Attribut wird als ID verwendet
ID_PREFIX      = "DABO_"               # bnum → "DABO_<bnum>"
STATUS_FILTER  = 1                     # Nur Bohrungen mit status == 1
MIN_LAENGE     = 50                    # Nur Bohrungen mit laenge > MIN_LAENGE

CHUNK_SIZE     = 200                   # IDs pro WFS-Request
PAUSE          = 0.0                   # Sekunden zwischen Requests

# =============================================================================
# XML-NAMESPACES
# =============================================================================

NS = {
    "wfs": "http://www.opengis.net/wfs/2.0",
    "ows": "http://www.opengis.net/ows/1.1",
    "bml": "http://www.infogeo.de/boreholeml/3.0",
    "gmd": "http://www.isotc211.org/2005/gmd",
}

# =============================================================================
# KOORDINATENTRANSFORMATION
# =============================================================================

_transformers = {}

def to_wgs84(coords, srs_name):
    srs = str(srs_name).split(":")[-1]
    if not srs or len(coords) < 2:
        return None
    try:
        if srs not in _transformers:
            _transformers[srs] = Transformer.from_crs(
                f"EPSG:{int(srs)}", "EPSG:4326", always_xy=True)
        lon, lat = _transformers[srs].transform(coords[0], coords[1])
        if 5.0 <= lon <= 10.5 and 49.5 <= lat <= 53.0:
            return [round(lon, 7), round(lat, 7)]
    except Exception:
        pass
    return None

# =============================================================================
# GML PARSING → GeoJSON
# =============================================================================

def txt(el, tag):
    found = el.find(tag, NS)
    return found.text.strip() if found is not None and found.text else None


def parse_borehole(bh):
    p = {}
    p["id"]                    = txt(bh, "bml:id")
    p["fullName"]              = bh.findtext(
        f"{{{NS['bml']}}}fullName/{{{NS['gmd']}}}LocalisedCharacterString")
    p["totalLength_m"]         = txt(bh, "bml:totalLength")
    p["drillingDate"]          = txt(bh, "bml:drillingDate")
    p["exportDate"]            = txt(bh, "bml:exportDate")
    p["boreholeStatus"]        = txt(bh, "bml:boreholeStatus")
    p["drillingMethod"]        = txt(bh, "bml:drillingMethod")
    p["drillingPurpose"]       = txt(bh, "bml:drillingPurpose")
    p["lastHorizon"]           = txt(bh, "bml:lastHorizon")
    p["groundwaterEncountered"] = txt(bh, "bml:groundwaterEncountered")
    p["archiveDataLegalAvail"] = txt(bh, "bml:archiveDataLegalAvail")
    p["archiveDataTechAvail"]  = txt(bh, "bml:archiveDataTechAvail")
    p["scansLegalAvail"]       = txt(bh, "bml:scansLegalAvail")
    p["scansTechAvail"]        = txt(bh, "bml:scansTechAvail")
    p["labDataLegalAvail"]     = txt(bh, "bml:labDataLegalAvail")
    p["labDataTechAvail"]      = txt(bh, "bml:labDataTechAvail")

    # Lage → WGS84
    geom = None
    origin_el = bh.find("bml:origin/bml:Origin", NS)
    if origin_el is not None:
        loc_el = origin_el.find("bml:originalLocation", NS)
        if loc_el is not None and loc_el.text:
            srs = loc_el.attrib.get("srsName", "")
            p["location_srsName"] = srs
            try:
                c = [float(x) for x in loc_el.text.strip().split()]
                wgs = to_wgs84(c, srs)
                geom = {"type": "Point", "coordinates": wgs if wgs else c}
            except ValueError:
                pass
        elev_el = origin_el.find("bml:elevation", NS)
        if elev_el is not None and elev_el.text:
            p["elevation_m"] = elev_el.text.strip()

    # Schichtverzeichnis
    layers = []
    for series in bh.findall("bml:intervalSeries/bml:IntervalSeries", NS):
        series_desc = series.findtext(
            f"{{{NS['bml']}}}description/{{{NS['gmd']}}}LocalisedCharacterString", "")
        for interval in series.findall("bml:layer/bml:Interval", NS):
            layer = {
                "series":           series_desc,
                "from_m":           txt(interval, "bml:from"),
                "to_m":             txt(interval, "bml:to"),
                "rockCode":         txt(interval, "bml:rockCode"),
                "rockNameText":     interval.findtext(
                    f"{{{NS['bml']}}}rockNameText/{{{NS['gmd']}}}LocalisedCharacterString"),
                "geoGenesis":       txt(interval, "bml:geoGenesis"),
                "carbonateContent": txt(interval, "bml:carbonateContent"),
                "stratigraphy":     [],
                "lithology":        [],
            }
            for strat in interval.findall("bml:stratigraphy/bml:Stratigraphy", NS):
                s = {child.tag.split("}")[-1]:
                     (child.text.strip() if child.text
                      else child.attrib.get("codeListValue", ""))
                     for child in strat}
                layer["stratigraphy"].append(s)
            for lith in interval.findall("bml:lithology/bml:Lithology", NS):
                l = {child.tag.split("}")[-1]:
                     (child.text.strip() if child.text
                      else child.attrib.get("codeListValue", ""))
                     for child in lith}
                layer["lithology"].append(l)
            layers.append({k: v for k, v in layer.items()
                           if v is not None and v != "" and v != []})
    if layers:
        p["layers"]      = layers
        p["layer_count"] = len(layers)

    p = {k: v for k, v in p.items() if v is not None and v != ""}
    return {"type": "Feature", "geometry": geom, "properties": p}


def parse_feature_collection(xml_bytes):
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        print(f"  ❌ XML-Fehler: {e}")
        return [], 0
    features = [parse_borehole(child)
                for member in root.findall("wfs:member", NS)
                for child in member]
    return features, int(root.attrib.get("numberReturned", len(features)))

# =============================================================================
# HTTP
# =============================================================================

def do_request(params, label=""):
    for attempt in range(1, 4):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=300)
            if not resp.ok:
                try:
                    err = ET.fromstring(resp.content).findtext(
                        ".//{http://www.opengis.net/ows/1.1}ExceptionText", "")
                    print(f"  ❌ HTTP {resp.status_code} {label}: {err.strip()}")
                except Exception:
                    print(f"  ❌ HTTP {resp.status_code} {label}: {resp.text[:200]}")
                return None
            return resp.content
        except requests.exceptions.Timeout:
            print(f"  ⚠️  Timeout {label} (Versuch {attempt}/3)...")
            time.sleep(10 * attempt)
        except requests.RequestException as e:
            print(f"  ❌ Netzwerkfehler: {e}")
            return None
    return None


def fetch_by_ids(typename, ids):
    # Teste beim ersten Aufruf welcher Filter-Parameter funktioniert
    working_key, working_fmt = None, None
    for key, fmt in [
        ("RESOURCEID", f"{typename}.{ids[0]}"),
        ("RESOURCEID", ids[0]),
        ("FEATUREID",  f"{typename}.{ids[0]}"),
        ("FEATUREID",  ids[0]),
    ]:
        print(f"  🔍 Teste {key}={fmt}...")
        xml = do_request({"SERVICE":"WFS","VERSION":WFS_VERSION,"REQUEST":"GetFeature",
                          "TYPENAMES":typename,"OUTPUTFORMAT":OUTPUT_FORMAT, key: fmt})
        if xml:
            batch, _ = parse_feature_collection(xml)
            if batch:
                working_key = key
                working_fmt = (lambda t: lambda id_: f"{t}.{id_}")(typename) \
                              if "." in fmt else (lambda id_: id_)
                print(f"     ✅ Verwende {key}")
                break
        print(f"     → 0 Ergebnisse")

    if not working_key:
        print("  ❌ Kein ID-Filter funktioniert.")
        return []

    all_features = []
    for i in range(0, len(ids), CHUNK_SIZE):
        chunk = ids[i:i+CHUNK_SIZE]
        label = f"{i+1}–{min(i+CHUNK_SIZE, len(ids))} / {len(ids)}"
        print(f"  📥 IDs {label}...")
        xml = do_request({"SERVICE":"WFS","VERSION":WFS_VERSION,"REQUEST":"GetFeature",
                          "TYPENAMES":typename,"OUTPUTFORMAT":OUTPUT_FORMAT,
                          working_key: ",".join(working_fmt(id_) for id_ in chunk)}, label)
        if xml:
            batch, _ = parse_feature_collection(xml)
            all_features.extend(batch)
            n_layers = sum(f["properties"].get("layer_count", 0) for f in batch)
            print(f"     → {len(batch)} Bohrungen, {n_layers} Schichten ({len(xml)//1024} KB)")
        if PAUSE > 0:
            time.sleep(PAUSE)

    return all_features

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("  DABO NRW – Schichtverzeichnis-Downloader")
    print("=" * 60)

    # selection.geojson lesen
    try:
        with open(SELECTION_FILE, "r", encoding="utf-8") as f:
            sel = json.load(f)
    except FileNotFoundError:
        print(f"\n❌ Datei nicht gefunden: {SELECTION_FILE}"); sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"\n❌ Ungültiges JSON in {SELECTION_FILE}: {e}"); sys.exit(1)

    all_bnums = [feat["properties"]["bnum"]
                 for feat in sel["features"]
                 if feat.get("properties", {}).get("bnum")]
    filtered  = [feat for feat in sel["features"]
                 if feat.get("properties", {}).get("bnum")
                 and feat["properties"].get("status") == STATUS_FILTER
                 and (feat["properties"].get("laenge") or 0) > MIN_LAENGE]
    fetch_ids = [f"{ID_PREFIX}{feat['properties']['bnum']}" for feat in filtered]

    skipped = len(all_bnums) - len(fetch_ids)
    print(f"\n📂 {SELECTION_FILE}: {len(all_bnums)} Bohrungen gelesen")
    print(f"   Status {STATUS_FILTER}, Länge > {MIN_LAENGE}m: {len(fetch_ids)} Bohrungen"
          + (f" ({skipped} übersprungen)" if skipped else ""))
    print(f"   Beispiele: {fetch_ids[:3]}{'...' if len(fetch_ids) > 3 else ''}")

    if not fetch_ids:
        print("⚠️  Keine Bohrungen nach Filter – Abbruch."); return

    # WFS-Layer ermitteln
    print("\n📋 Rufe WFS-Capabilities ab...")
    cap_xml = do_request({"SERVICE":"WFS","VERSION":WFS_VERSION,"REQUEST":"GetCapabilities"})
    if not cap_xml:
        sys.exit(1)
    root = ET.fromstring(cap_xml)
    ft   = root.find(".//wfs:FeatureType", NS)
    typename = ft.findtext("wfs:Name", default="bml:Borehole", namespaces=NS)
    print(f"✅ Layer: {typename}")

    # Download
    print()
    all_features = fetch_by_ids(typename, fetch_ids)

    if not all_features:
        print("\n⚠️  Keine Features heruntergeladen."); return

    total_layers = sum(f["properties"].get("layer_count", 0) for f in all_features)
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    outfile = OUTPUT_FILE or f"dabo_bohrungen_selection_{ts}.geojson"

    geojson = {
        "type": "FeatureCollection",
        "name": "DABO_Bohrungen_NRW",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "features": all_features,
        "metadata": {
            "source":        BASE_URL,
            "selection_file": SELECTION_FILE,
            "status_filter": STATUS_FILTER,
            "min_laenge":    MIN_LAENGE,
            "feature_count": len(all_features),
            "total_layers":  total_layers,
            "download_date": datetime.now().isoformat(),
            "license":       "Datenlizenz Deutschland – Namensnennung – Version 2.0",
            "license_url":   "https://www.govdata.de/dl-de/by-2-0",
            "citation": (
                f"Bohrungen NRW ({BASE_URL}), Geologischer Dienst NRW, "
                f"Abrufdatum: {datetime.now().strftime('%d.%m.%Y')}. "
                f"Nutzung gemäß Datenlizenz Deutschland – Namensnennung – "
                f"Version 2.0 (https://www.govdata.de/dl-de/by-2-0)"
            ),
        },
    }

    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Fertig!")
    print(f"   Bohrungen:      {len(all_features):,}")
    print(f"   Schichten ges.: {total_layers:,}")
    print(f"   Dateigröße:     {os.path.getsize(outfile)/1_048_576:.2f} MB")
    print(f"   Datei:          {outfile}")
    print(f"\n📌 Zitierung:\n   {geojson['metadata']['citation']}")


if __name__ == "__main__":
    main()
