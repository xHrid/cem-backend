"""
STAC 1.0 provenance items (item 9 + item 12).

For every result file a pipeline step produces, we write a sibling STAC Item
JSON named "<file>.stac.json". Each item records:

  * a stable asset id           (job + step + filename)
  * datetime                    (the file's modification time)
  * a short description         (from the manifest, + the filename)
  * the algorithm + API version (item 12)
  * the input parameters used   (the run params)
  * the spot geolocation        (geometry + bbox from spot lat/lon, item 9)

Anything whose exact shape depends on the drone / STAC-B team is configurable
(STAC_COLLECTION, STAC_ASSET_BASE_URL). With the defaults this emits a valid
STAC 1.0 Item. We deliberately keep the CEM-specific fields under a "cem:"
prefix so they don't collide with whatever extension schema STAC-B settles on.
"""
import json
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import pipeline_meta as meta
from .settings import get_settings

STAC_VERSION = "1.0.0"
SIDECAR_SUFFIX = ".stac.json"

# Map a file extension to a STAC asset role.
_ROLE_BY_EXT = {
    ".png": "thumbnail", ".jpg": "thumbnail", ".jpeg": "thumbnail",
    ".csv": "data", ".json": "metadata", ".txt": "metadata", ".log": "metadata",
}


def is_sidecar(name: str) -> bool:
    return name.endswith(SIDECAR_SUFFIX)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sanitize(text: str) -> str:
    return "".join(c if (c.isalnum() or c in "-_.") else "_" for c in text)


def geometry_from_geo(geo: Optional[list]) -> tuple[Optional[dict], Optional[list]]:
    """geo = [{"name","lat","lon"}, ...] -> (geometry, bbox) or (None, None)."""
    pts: list[tuple[float, float]] = []  # (lon, lat)
    for g in geo or []:
        try:
            lat = float(g.get("lat"))
            lon = float(g.get("lon"))
        except (TypeError, ValueError, AttributeError):
            continue
        pts.append((lon, lat))
    if not pts:
        return None, None
    lons = [p[0] for p in pts]
    lats = [p[1] for p in pts]
    bbox = [min(lons), min(lats), max(lons), max(lats)]
    if len(pts) == 1:
        geom = {"type": "Point", "coordinates": [pts[0][0], pts[0][1]]}
    else:
        geom = {"type": "MultiPoint", "coordinates": [[x, y] for x, y in pts]}
    return geom, bbox


def _asset_href(job_id: str, rel_path: str) -> str:
    s = get_settings()
    if s.STAC_ASSET_BASE_URL:
        return f"{s.STAC_ASSET_BASE_URL}/jobs/{job_id}/{rel_path}"
    # Relative href (resolved against the item's own location in the data dir).
    return Path(rel_path).name


def build_item(job_id: str, step: str, rel_path: str, abs_path: Path,
               params: dict, geo: Optional[list], created_at: Optional[str] = None) -> dict:
    """Return a STAC 1.0 Item dict for one result file."""
    s = get_settings()
    man = meta.load_manifest().get(step, {})
    fname = abs_path.name
    geom, bbox = geometry_from_geo(geo)
    dt = _iso(abs_path.stat().st_mtime) if abs_path.is_file() else _now_iso()
    ext = abs_path.suffix.lower()
    media_type = mimetypes.guess_type(fname)[0] or "application/octet-stream"
    role = _ROLE_BY_EXT.get(ext, "data")

    item = {
        "type": "Feature",
        "stac_version": STAC_VERSION,
        "stac_extensions": [],
        "id": _sanitize(f"{job_id}_{step}_{fname}"),
        "collection": s.STAC_COLLECTION,
        "geometry": geom,
        "bbox": bbox,
        "properties": {
            "datetime": dt,
            "created": created_at or _now_iso(),
            "description": f"{man.get('name', step)} — {fname}. "
                           f"{man.get('description', '')}".strip(),
            "processing:software": {"cem-bioacoustics-api": s.API_VERSION},
            "cem:job_id": job_id,
            "cem:step": step,
            "cem:algorithm": man.get("script_file", f"{step}.py"),
            "cem:algorithm_version": meta.step_version(step),
            "cem:api_version": s.API_VERSION,
            "cem:parameters": params or {},
            "cem:spots": geo or [],
        },
        "assets": {
            "data": {
                "href": _asset_href(job_id, rel_path),
                "type": media_type,
                "title": fname,
                "roles": [role],
            }
        },
        "links": [],
    }
    return item


# --------------------------------------------------------------------------- #
# STACD / Airflow synchronous-API items (STAC 1.1.0).
#
# Separate from the 1.0.0 sidecars above (do not change those). These are shaped
# to match the CoreStack/STACD catalog example items: Polygon geometry, bbox,
# start/end_datetime, assets.data with an href, a collection, and links.
# --------------------------------------------------------------------------- #
def _polygon_from_bbox(bbox: list) -> dict:
    minx, miny, maxx, maxy = bbox
    return {"type": "Polygon", "coordinates": [[
        [minx, miny], [minx, maxy], [maxx, maxy], [maxx, miny], [minx, miny],
    ]]}


def _date_iso(value, default=None):
    """'20251101' or '2025-11-01' -> '2025-11-01T00:00:00Z'."""
    if not value:
        return default
    v = str(value).replace("-", "")
    if len(v) == 8 and v.isdigit():
        return f"{v[0:4]}-{v[4:6]}-{v[6:8]}T00:00:00Z"
    return default


def build_stacd_item(asset_id: str, step: str, abs_path: Path, params: dict,
                     geo: Optional[list], browse_href: Optional[str] = None) -> dict:
    """STAC 1.1.0 Item for one output, matching the STACD response shape."""
    s = get_settings()
    man = meta.load_manifest().get(step, {})
    fname = abs_path.name
    geom, bbox = geometry_from_geo(geo)
    # STACD/CoreStack items use Polygon geometry; promote a multi-spot bbox to a
    # Polygon. A single spot stays a Point.
    if bbox and geom and geom.get("type") == "MultiPoint":
        geom = _polygon_from_bbox(bbox)
    ext = abs_path.suffix.lower()
    media_type = mimetypes.guess_type(fname)[0] or "application/octet-stream"
    assets = {"data": {
        "href": browse_href or fname,
        "type": media_type,
        "title": fname,
        "roles": ["data"],
    }}
    if ext in (".png", ".jpg", ".jpeg"):
        assets["data"]["roles"] = ["data", "thumbnail"]
    props = {
        "title": man.get("name", step),
        "description": man.get("description", ""),
        "datetime": _now_iso(),
        "cem:algorithm": man.get("script_file", f"{step}.py"),
        "cem:algorithm_version": meta.step_version(step),
        "cem:api_version": s.API_VERSION,
        "cem:parameters": params or {},
        "cem:spots": geo or [],
    }
    sd = _date_iso((params or {}).get("start_date"))
    ed = _date_iso((params or {}).get("end_date"))
    if sd:
        props["start_datetime"] = sd
    if ed:
        props["end_datetime"] = ed
    return {
        "type": "Feature",
        "stac_version": s.STACD_STAC_VERSION,
        "stac_extensions": [],
        "id": _sanitize(asset_id),
        "geometry": geom,
        "bbox": bbox,
        "properties": props,
        "links": [
            {"rel": "collection", "href": "../collection.json",
             "type": "application/json", "title": s.STAC_COLLECTION},
            {"rel": "parent", "href": "../collection.json",
             "type": "application/json", "title": s.STAC_COLLECTION},
        ],
        "assets": assets,
        "collection": s.STAC_COLLECTION,
    }


def write_items(job_root: Path, job_id: str, step: str, rel_paths: list[str],
                params: dict, geo: Optional[list]) -> list[str]:
    """Write a <file>.stac.json sidecar next to each result file.

    Returns the rel paths of the sidecar files created (so the caller can list
    them as job results). No-op when STAC_ENABLED is false. Never raises — STAC
    failure must not fail the compute job.
    """
    if not get_settings().STAC_ENABLED:
        return []
    created: list[str] = []
    now = _now_iso()
    for rel in rel_paths:
        if is_sidecar(rel):
            continue
        abs_path = job_root / rel
        if not abs_path.is_file():
            continue
        try:
            item = build_item(job_id, step, rel, abs_path, params, geo, created_at=now)
            sidecar = abs_path.with_name(abs_path.name + SIDECAR_SUFFIX)
            sidecar.write_text(json.dumps(item, indent=2))
            created.append(str(sidecar.relative_to(job_root)).replace("\\", "/"))
        except Exception as e:  # provenance is best-effort
            print(f"[stac] could not write item for {rel}: {e}")
    return created
