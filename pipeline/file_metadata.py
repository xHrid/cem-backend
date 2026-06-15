"""
Unified audio-file metadata.
========================================================================
One job: turn an audio file into a single, source-agnostic record so the
analysis scripts never parse raw filenames themselves.

Unified record (JSON-serialisable — these become the aggregate CSV columns):

    {
      "filename": "CRIMESPOT3_20251130_093100.wav",
      "filepath": "/abs/path/.../CRIMESPOT3_20251130_093100.wav",
      "spot":     "CRIMESPOT3",     # recorder / site label
      "date":     "2025-11-30",     # ISO date (YYYY-MM-DD)
      "hour":     9,                # 0-23
      "minute":   31,
      "second":   0,
      "source":   "song_meter"      # how the metadata was derived
    }

Song Meter style names  ->  SPOT_YYYYMMDD_HHMMSS.<ext>  parse automatically.

Audio from other sources / odd names:
  - pass spot= explicitly (e.g. the spot a reference file is attached to), and/or
  - pass overrides=<dict of any record fields> to inject date/hour/etc.
This keeps every downstream script dependent only on the unified record, never
on a particular naming convention.
"""

import os
import re
from datetime import date as _date

# Song Meter / CEM convention: SPOT_YYYYMMDD_HHMMSS.<ext>  (24h clock)
# Spot label may start with a digit (e.g. recorder serials like "04213SPOT1").
_FILENAME_RE = re.compile(
    r"^(?P<spot>[A-Za-z0-9][A-Za-z0-9_-]*)_"
    r"(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})_"
    r"(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})"
    r"\.[A-Za-z0-9]+$",
    re.IGNORECASE,
)

RECORD_FIELDS = ("filename", "filepath", "spot", "date", "hour", "minute", "second", "source")


def parse_filename(name: str) -> dict | None:
    """Parse a Song Meter style name. Returns dict with a real datetime.date
    (for range comparisons) or None if the name does not match / is invalid.

    Validates month, day, hour, minute, second. Year is NOT range-checked
    (too tricky) beyond forming a valid date.
    """
    m = _FILENAME_RE.match(os.path.basename(name))
    if not m:
        return None
    month, day = int(m.group("month")), int(m.group("day"))
    hour, minute, second = int(m.group("hour")), int(m.group("minute")), int(m.group("second"))
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
        return None
    try:
        d = _date(int(m.group("year")), month, day)
    except ValueError:
        return None
    return {"spot": m.group("spot"), "date": d, "hour": hour, "minute": minute, "second": second}


def build_record(filepath: str, spot: str | None = None,
                 source: str = "song_meter", overrides: dict | None = None) -> dict:
    """Build the unified metadata record for one file.

    Precedence (low -> high): filename parse  <  spot arg  <  overrides dict.
    `date` in the returned record is an ISO string (CSV/JSON friendly).
    """
    filename = os.path.basename(filepath)
    rec = {f: None for f in RECORD_FIELDS}
    rec["filename"] = filename
    rec["filepath"] = filepath
    rec["source"] = source

    parsed = parse_filename(filename)
    if parsed:
        rec["spot"] = parsed["spot"]
        rec["date"] = parsed["date"].isoformat()
        rec["hour"] = parsed["hour"]
        rec["minute"] = parsed["minute"]
        rec["second"] = parsed["second"]
    else:
        rec["source"] = "external"   # name didn't follow the convention

    if spot:
        rec["spot"] = spot           # attached spot (e.g. reference import) wins

    if overrides:
        for k, v in overrides.items():
            if k in RECORD_FIELDS and v is not None:
                rec[k] = v

    return rec
