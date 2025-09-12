import asyncio, aiohttp, async_timeout, random, re, os, csv, json, sys, signal, hashlib
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
from dateutil import parser as dtparse
import pandas as pd
from tqdm import tqdm
import math

# ---------------------- Config ----------------------
CONCURRENCY = int(os.getenv("CONCURRENCY", "6"))          # max tasks in parallel
GLOBAL_RPS   = float(os.getenv("GLOBAL_RPS", "1.5"))       # richieste/secondo totali
PER_HOST_RPS = float(os.getenv("PER_HOST_RPS", "0.8"))
TIMEOUT_S    = float(os.getenv("TIMEOUT_S", "12"))
MAX_RETRIES  = int(os.getenv("MAX_RETRIES", "6"))
JITTER_S     = float(os.getenv("JITTER_S", "0.4"))

CACHE_DIR = Path(os.getenv("CACHE_DIR", "cache"))
RAW_DIR   = CACHE_DIR / "raw"
OUT_DIR   = Path(os.getenv("OUT_DIR", "out"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

UA_LIST = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]

# ---------------------- Helpers ----------------------
def extract_tracking_id(url: str) -> str:
    """
    Prova a ricavare un ID stabile per cache/filename.
    Preferisce ultimo segmento numerico o parametro rf.
    """
    q = parse_qs(urlparse(url).query)
    if "rf" in q and q["rf"]:
        return q["rf"][0]
    # fallback: ultimo path segment
    seg = urlparse(url).path.rstrip("/").split("/")[-1]
    if seg and re.fullmatch(r"[A-Za-z0-9_-]+", seg):
        return seg
    # hash come extrema ratio
    return hashlib.sha1(url.encode()).hexdigest()[:16]

def now_utc():
    return datetime.now(timezone.utc).isoformat()

class TokenBucket:
    """Semplice rate limiter (token bucket)"""
    def __init__(self, rps: float):
        self.capacity = max(1.0, rps)
        self.tokens = self.capacity
        self.rps = rps
        self.updated = asyncio.get_event_loop().time()
    async def wait(self):
        if self.rps <= 0:
            await asyncio.sleep(1)
            return
        while True:
            now = asyncio.get_event_loop().time()
            delta = now - self.updated
            self.updated = now
            self.tokens = min(self.capacity, self.tokens + delta * self.rps)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            await asyncio.sleep(max(0.01, (1.0 - self.tokens) / self.rps))

GLOBAL_BUCKET = TokenBucket(GLOBAL_RPS)
HOST_BUCKETS = {}

def get_host_bucket(host: str) -> TokenBucket:
    if host not in HOST_BUCKETS:
        HOST_BUCKETS[host] = TokenBucket(PER_HOST_RPS)
    return HOST_BUCKETS[host]

# ---------------------- Networking ----------------------
async def polite_get(session: aiohttp.ClientSession, url: str) -> aiohttp.ClientResponse:
    """GET con rate-limit globale e per host, retry/backoff/jitter e rispetto Retry-After."""
    host = urlparse(url).netloc
    backoff = 1.0
    for attempt in range(1, MAX_RETRIES + 1):
        await GLOBAL_BUCKET.wait()
        await get_host_bucket(host).wait()
        headers = {
            "User-Agent": random.choice(UA_LIST),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        }
        try:
            async with async_timeout.timeout(TIMEOUT_S):
                resp = await session.get(url, headers=headers, allow_redirects=True)
            if resp.status == 200:
                return resp
            if resp.status in (429, 500, 502, 503, 504):
                # rispetto Retry-After se presente
                ra = resp.headers.get("Retry-After")
                wait_s = float(ra) if ra and ra.isdigit() else backoff + random.uniform(0, JITTER_S)
                await asyncio.sleep(min(60, wait_s))
                backoff = min(backoff * 2, 30)
            else:
                text = await resp.text(errors="ignore")
                raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
        except Exception as e:
            # timeout / reset / ecc.
            await asyncio.sleep(backoff + random.uniform(0, JITTER_S))
            backoff = min(backoff * 2, 30)
    raise RuntimeError(f"Max retries exceeded for {url}")

# ---------------------- Parsing ----------------------
RE_DATE = re.compile(r"(\d{1,2}\s+[A-Za-zàéìòù]{3,}\s+\d{4}\s+\d{1,2}:\d{2})")  # es. 30 giu 2025 13:35
RE_CITY = re.compile(r"(?:a|in)\s+(?:sede|hub|filiale|deposito)?\s*([A-Za-zÀ-ÖØ-öø-ÿ'\\- ]{2,})", re.I)

def parse_events_from_html(html: str):
    """
    Tenta più strategie:
    1) JSON incorporato in <script> (molti siti iniettano un oggetto con gli eventi)
    2) Tabella/timeline HTML con classi comuni
    3) Fallback: regex su timestamp + descrizione vicina
    Ritorna lista di dict: {event_time_iso, description, location}
    """
    soup = BeautifulSoup(html, "lxml")
    events = []

    # 1) Script JSON embedded
    for sc in soup.find_all("script"):
        txt = sc.string or sc.text or ""
        if "event" in txt.lower() and "timestamp" in txt.lower():
            try:
                # estrai il primo oggetto JSON plausibile
                by_brace = re.search(r"\{.*\}", txt, re.S)
                if by_brace:
                    obj = json.loads(by_brace.group(0))
                    # cerca arrays chiamati events / history
                    cand = obj.get("events") or obj.get("history") or []
                    for ev in cand:
                        ts = ev.get("timestamp") or ev.get("time")
                        desc = ev.get("description") or ev.get("status") or ""
                        loc = ev.get("location") or ev.get("city") or ""
                        if ts:
                            try:
                                t_iso = dtparse.parse(ts).astimezone(timezone.utc).isoformat()
                                events.append({"event_time_iso": t_iso, "description": desc, "location": loc})
                            except:  # noqa
                                pass
                    if events:
                        break
            except Exception:
                pass

    # 2) timeline/table
    if not events:
        rows = soup.select("tr, li, div.timeline-row, div.track-row")
        for r in rows:
            # timestamp
            ttxt = " ".join([x.get_text(" ", strip=True) for x in r.select(".time,.date,.timestamp,.gls-time")]) or r.get_text(" ", strip=True)
            m = RE_DATE.search(ttxt)
            ts = None
            if m:
                try:
                    ts = dtparse.parse(m.group(1), dayfirst=True)
                except Exception:
                    ts = None
            # desc/location
            desc = (r.select_one(".desc,.text,.status,.gls-status") or r).get_text(" ", strip=True)
            loc_elm = r.select_one(".place,.location,.city,.office,.center")
            loc = (loc_elm.get_text(" ", strip=True) if loc_elm else None)
            if not loc:
                # tenta regex su frase
                mm = RE_CITY.search(desc)
                if mm:
                    loc = mm.group(1).strip()
            if ts:
                events.append({
                    "event_time_iso": ts.astimezone(timezone.utc).isoformat(),
                    "description": desc,
                    "location": loc or ""
                })

    # 3) fallback grezzo su regex
    if not events:
        for m in RE_DATE.finditer(html):
            try:
                ts = dtparse.parse(m.group(1), dayfirst=True)
                # prendi 120 char intorno come descrizione
                start = max(0, m.start() - 120)
                end = min(len(html), m.end() + 120)
                ctx = re.sub(r"<[^>]+>", " ", html[start:end])
                ctx = re.sub(r"\s+", " ", ctx)
                mm = RE_CITY.search(ctx)
                loc = mm.group(1).strip() if mm else ""
                events.append({
                    "event_time_iso": ts.astimezone(timezone.utc).isoformat(),
                    "description": ctx.strip()[:280],
                    "location": loc
                })
            except Exception:
                pass

    # dedup + sort
    seen = set()
    cleaned = []
    for ev in events:
        key = (ev["event_time_iso"], ev["description"][:120])
        if key not in seen:
            seen.add(key)
            cleaned.append(ev)
    cleaned.sort(key=lambda x: x["event_time_iso"])
    return cleaned

ARR_PAT = re.compile(r"\b(arrivat[oa]|arrived|arrivo)\b", re.I)
DEP_PAT = re.compile(r"\b(partit[oa]|uscit[oa]|departed|in transito|shipped)\b", re.I)

def stops_from_events(events):
    """
    Costruisce arrivo/partenza per checkpoint:
    - quando cambia 'location' => arrivo nuovo checkpoint
    - prima occorrenza con DEP_PAT nello stesso checkpoint => partenza
    Se non trovato, usa arrivo checkpoint successivo come partenza implicita.
    """
    if not events:
        return []

    # normalizza location
    def norm_loc(s):
        s = (s or "").strip()
        s = re.sub(r"(?i)(sede|filiale|hub|centro|deposito|gls|centro smistamento|parcel center)", "", s)
        s = re.sub(r"\s*[-–]\s*", " ", s)
        return re.sub(r"\s+", " ", s).strip().title()

    enriched = [{"t": ev["event_time_iso"], "desc": ev["description"], "loc": norm_loc(ev["location"])} for ev in events]

    stops = []
    if not enriched[0]["loc"]:
        # prova a ereditare dalla prima con loc disponibile
        for e in enriched:
            if e["loc"]:
                enriched[0]["loc"] = e["loc"]; break

    current_loc = enriched[0]["loc"]
    arrival = enriched[0]["t"]
    departure = None

    for i in range(1, len(enriched)):
        e = enriched[i]
        if e["loc"] != current_loc and e["loc"]:
            # cambio checkpoint => chiudo il precedente
            departure = e["t"]  # partenza implicita
            stops.append({"checkpoint": current_loc, "arrival_at": arrival, "departure_at": departure})
            # apertura nuovo
            current_loc = e["loc"]
            arrival = e["t"]
            departure = None
        else:
            # stesso checkpoint: cerca uscita esplicita
            if DEP_PAT.search(e["desc"]) and not departure:
                departure = e["t"]
                stops.append({"checkpoint": current_loc, "arrival_at": arrival, "departure_at": departure})
                # in molti flussi si rimane nello stesso loc ma in “transito”: riapriamo solo se cambia poi
                arrival = e["t"]  # opzionale; qui manteniamo semplice

    # ultimo aperto senza chiusura
    if arrival and (not stops or stops[-1]["checkpoint"] != current_loc):
        stops.append({"checkpoint": current_loc, "arrival_at": arrival, "departure_at": None})

    # calcola dwell
    for s in stops:
        if s["arrival_at"] and s["departure_at"]:
            t1 = datetime.fromisoformat(s["arrival_at"].replace("Z","")).timestamp()
            t2 = datetime.fromisoformat(s["departure_at"].replace("Z","")).timestamp()
            s["dwell_hours"] = max(0.0, (t2 - t1) / 3600.0)
        else:
            s["dwell_hours"] = None
    return stops

# ---------------------- Pipeline ----------------------
stop_flag = False
def _sigint(*args):
    global stop_flag
    stop_flag = True
signal.signal(signal.SIGINT, _sigint)

async def fetch_and_parse(session, url: str):
    """Scarica con cache e ritorna dict con events e stops."""
    tid = extract_tracking_id(url)
    cache_file = RAW_DIR / f"{tid}.html"
    if cache_file.exists():
        html = cache_file.read_text(errors="ignore")
    else:
        resp = await polite_get(session, url)
        html = await resp.text(errors="ignore")
        cache_file.write_text(html, encoding="utf-8", errors="ignore")
        await asyncio.sleep(random.uniform(0.05, JITTER_S))  # micro-pausa

    events = parse_events_from_html(html)
    stops = stops_from_events(events)
    return {"tracking_id": tid, "url": url, "events": events, "stops": stops}

async def run_csv(in_csv: str, out_events_csv: str, out_stops_csv: str):
    # leggi lista URL (colonna 'url' o prima colonna senza header)
    # pandas per robustezza
    try:
        df = pd.read_csv(in_csv)
        if "url" in df.columns:
            urls = df["url"].dropna().astype(str).tolist()
        else:
            urls = df.iloc[:,0].dropna().astype(str).tolist()
    except Exception:
        # CSV separato da ; eventualmente
        df = pd.read_csv(in_csv, sep=";")
        urls = (df["url"] if "url" in df.columns else df.iloc[:,0]).dropna().astype(str).tolist()

    # dedup & ordina per tracking id (stabilità)
    seen = set(); ordered = []
    for u in urls:
        tid = extract_tracking_id(u)
        if tid not in seen:
            seen.add(tid); ordered.append(u)

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=12, sock_read=12)
    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        sem = asyncio.Semaphore(CONCURRENCY)
        results = []

        pbar = tqdm(total=len(ordered), desc="Download/Parse", unit="trk")
        async def worker(u):
            nonlocal results
            async with sem:
                try:
                    data = await fetch_and_parse(session, u)
                    results.append(data)
                except Exception as e:
                    results.append({"tracking_id": extract_tracking_id(u), "url": u, "error": str(e)})
                finally:
                    pbar.update(1)

        tasks = [asyncio.create_task(worker(u)) for u in ordered]
        await asyncio.gather(*tasks)
        pbar.close()

    # salva events
    ev_rows = []
    st_rows = []
    for r in results:
        tid = r.get("tracking_id")
        url = r.get("url")
        if "events" in r and r["events"]:
            for ev in r["events"]:
                ev_rows.append({
                    "tracking_id": tid,
                    "url": url,
                    "event_time_iso": ev.get("event_time_iso"),
                    "description": ev.get("description"),
                    "location": ev.get("location"),
                    "ingested_at": now_utc()
                })
        if "stops" in r and r["stops"]:
            for s in r["stops"]:
                st_rows.append({
                    "tracking_id": tid,
                    "url": url,
                    "checkpoint": s.get("checkpoint"),
                    "arrival_at": s.get("arrival_at"),
                    "departure_at": s.get("departure_at"),
                    "dwell_hours": s.get("dwell_hours"),
                    "ingested_at": now_utc()
                })

    pd.DataFrame(ev_rows).to_csv(out_events_csv, index=False)
    pd.DataFrame(st_rows).to_csv(out_stops_csv, index=False)

# ---------------------- Entrypoint ----------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python gls_crawler.py shipments.csv [events_out.csv] [stops_out.csv]")
        sys.exit(1)
    in_csv = sys.argv[1]
    events_out = sys.argv[2] if len(sys.argv) > 2 else str(OUT_DIR / "events.csv")
    stops_out  = sys.argv[3] if len(sys.argv) > 3 else str(OUT_DIR / "stops.csv")
    asyncio.run(run_csv(in_csv, events_out, stops_out))
