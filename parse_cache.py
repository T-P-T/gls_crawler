from __future__ import annotations
"""
Analizza i file HTML già scaricati nella cache (``cache/raw``) senza effettuare
nuove richieste HTTP. Estrae gli eventi e ricostruisce le soste come nello
script principale ``gls_crawler.py``.

Uso:
    python parse_cache.py [shipments.csv] [events_out.csv] [stops_out.csv]

- ``shipments.csv`` (opzionale): CSV con una colonna contenente gli URL di
  tracking. Serve solo per associare l'ID di tracking all'URL originale.
- ``events_out.csv`` e ``stops_out.csv`` (opzionali) sono i percorsi di output;
  se omessi vengono creati ``out/events_from_cache.csv`` e
  ``out/stops_from_cache.csv``.

Lo script legge tutti i file ``*.html`` presenti nella cartella ``cache/raw`` e
produce due CSV analoghi a quelli generati dal crawler: uno per gli eventi e uno
per le soste.
"""
from pathlib import Path
import sys
import pandas as pd
from tqdm import tqdm

# Importa le funzioni di parsing dallo script principale
from gls_crawler import (
    RAW_DIR,
    OUT_DIR,
    parse_events_from_html,
    stops_from_events,
    extract_tracking_id,
    now_utc,
)


def load_url_map(csv_path: str | None) -> dict[str, str]:
    """Restituisce una mappa tracking_id -> URL a partire dal CSV fornito."""
    mapping: dict[str, str] = {}
    if not csv_path:
        return mapping
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        try:
            df = pd.read_csv(csv_path, sep=";")
        except Exception:
            df = pd.read_csv(csv_path, engine="python")
    col = "url" if "url" in df.columns else df.columns[0]
    for u in df[col].dropna().astype(str):
        tid = extract_tracking_id(u)
        mapping[tid] = u
    return mapping


def parse_cache(csv_path: str | None, events_out: Path, stops_out: Path) -> None:
    url_map = load_url_map(csv_path)
    events_rows: list[dict] = []
    stops_rows: list[dict] = []

    for html_file in tqdm(sorted(RAW_DIR.glob("*.html")), desc="Parsing cache"):
        tid = html_file.stem
        url = url_map.get(tid, "")
        html = html_file.read_text(errors="ignore")
        events = parse_events_from_html(html)
        stops = stops_from_events(events)

        print(f"== {tid} ==")
        for st in stops:
            checkpoint = st.get("checkpoint")
            print(f"spedizione verso: {checkpoint}")
            print(f"arrivo a {checkpoint} alle h {st.get('arrival_at')}")
            print(f"partenza da {checkpoint} alle h {st.get('departure_at')}")
            print("---")

        for ev in events:
            events_rows.append({
                "tracking_id": tid,
                "url": url,
                "event_time_iso": ev.get("event_time_iso"),
                "description": ev.get("description"),
                "location": ev.get("location"),
                "ingested_at": now_utc(),
            })
        for st in stops:
            stops_rows.append({
                "tracking_id": tid,
                "url": url,
                "checkpoint": st.get("checkpoint"),
                "arrival_at": st.get("arrival_at"),
                "departure_at": st.get("departure_at"),
                "dwell_hours": st.get("dwell_hours"),
                "ingested_at": now_utc(),
            })

    ev_cols = ["tracking_id", "url", "event_time_iso", "description", "location", "ingested_at"]
    st_cols = ["tracking_id", "url", "checkpoint", "arrival_at", "departure_at", "dwell_hours", "ingested_at"]

    events_out.parent.mkdir(parents=True, exist_ok=True)
    stops_out.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(events_rows, columns=ev_cols).to_csv(events_out, index=False)
    pd.DataFrame(stops_rows, columns=st_cols).to_csv(stops_out, index=False)


if __name__ == "__main__":
    shipments = sys.argv[1] if len(sys.argv) > 1 else None
    events_out = Path(sys.argv[2]) if len(sys.argv) > 2 else OUT_DIR / "events_from_cache.csv"
    stops_out = Path(sys.argv[3]) if len(sys.argv) > 3 else OUT_DIR / "stops_from_cache.csv"
    parse_cache(shipments, events_out, stops_out)
