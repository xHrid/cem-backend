import ee
import json
import uuid
import shutil
from pathlib import Path
from .settings import get_settings

_initialized = False

def _init_gee():
    global _initialized
    if _initialized:
        return
    import os
    project = os.environ.get("GEE_PROJECT", "ee-geeapi")
    sa_key = os.environ.get("GEE_SERVICE_ACCOUNT_KEY", "")
    if sa_key and Path(sa_key).is_file():
        credentials = ee.ServiceAccountCredentials(
            os.environ.get("GEE_SERVICE_ACCOUNT", ""),
            sa_key,
        )
        ee.Initialize(credentials, project=project)
    else:
        ee.Initialize(project=project)
    _initialized = True


def _parse_kml_to_ee_geometry(kml_bytes: bytes):
    from xml.etree import ElementTree
    root = ElementTree.fromstring(kml_bytes)
    ns = {'kml': 'http://www.opengis.net/kml/2.2'}
    coord_el = root.find('.//kml:coordinates', ns)
    if coord_el is None:
        coord_el = root.find('.//{http://www.opengis.net/kml/2.2}coordinates')
    if coord_el is None:
        for el in root.iter():
            if el.tag.endswith('coordinates'):
                coord_el = el
                break
    if coord_el is None:
        raise ValueError("No <coordinates> in KML")

    raw = coord_el.text.strip()
    coords = []
    for triple in raw.split():
        parts = triple.split(',')
        lon, lat = float(parts[0]), float(parts[1])
        coords.append([lon, lat])

    if len(coords) < 3:
        raise ValueError("Polygon needs >= 3 coords")

    return ee.Geometry.Polygon([coords])


def _get_annual_embedding(aoi, year):
    start = ee.Date(f'{year}-01-01')
    end = start.advance(1, 'year')
    col = ee.ImageCollection('GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL')
    return col.filterDate(start, end).filterBounds(aoi).first()


def generate_stratification(
    kml_bytes: bytes,
    max_clusters: int = 5,
    year: int = 2024,
    scale: int = 10,
    num_pixels: int = 1000,
):
    _init_gee()

    aoi = _parse_kml_to_ee_geometry(kml_bytes)
    image = _get_annual_embedding(aoi, year).clip(aoi)

    training = image.sample(region=aoi, scale=scale, numPixels=num_pixels)

    bounds_info = aoi.bounds().getInfo()['coordinates'][0]
    bounds = [
        [bounds_info[0][1], bounds_info[0][0]],
        [bounds_info[2][1], bounds_info[2][0]],
    ]

    s = get_settings()
    overlays_dir = s.DATA_DIR / "stratification_overlays"
    overlays_dir.mkdir(parents=True, exist_ok=True)

    palette = ['FF0000', '00FF00', '0000FF', 'FFFF00', 'FF00FF', '00FFFF', 'FFA500', '808080']
    results = []

    for k in range(2, max_clusters + 1):
        clusterer = ee.Clusterer.wekaKMeans(k).train(
            features=training,
            inputProperties=image.bandNames(),
        )
        classified = image.cluster(clusterer)
        vis = classified.visualize(min=0, max=k - 1, palette=palette[:k])
        final = vis.clip(aoi).reproject('EPSG:4326', None, scale)

        thumb_url = final.getThumbUrl({
            'region': bounds_info,
            'format': 'png',
        })

        import requests as req
        resp = req.get(thumb_url, timeout=120)
        resp.raise_for_status()

        fname = f"{uuid.uuid4()}.png"
        fpath = overlays_dir / fname
        fpath.write_bytes(resp.content)

        results.append({
            "overlay_id": fname.replace('.png', ''),
            "bounds": bounds,
            "cluster_count": k,
        })

    return results
