#!/usr/bin/env python3
"""
Franklin County, Ohio — Motivated Seller Lead Scraper
Target: https://franklin.oh.publicsearch.us  (Franklin County Recorder)
Parcel enrichment: https://audr-api.franklincountyohio.gov
"""

import csv
import io
import json
import logging
import os
import re
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")

SEARCH_BASE   = "https://franklin.oh.publicsearch.us"
AUDITOR_API   = "https://audr-api.franklincountyohio.gov/api/Parcel"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

DOC_TYPE_SEARCHES: dict[str, tuple[str, str]] = {
    "LP":       ("pre_foreclosure",  "Lis Pendens"),
    "NOFC":     ("pre_foreclosure",  "Notice of Foreclosure"),
    "TAXDEED":  ("tax_distress",     "Tax Deed"),
    "JUD":      ("judgment",         "Judgment"),
    "CCJ":      ("judgment",         "Certified Court Judgment"),
    "DRJUD":    ("judgment",         "Domestic Relations Judgment"),
    "LNCORPTX": ("tax_lien",         "Corp Tax Lien"),
    "LNIRS":    ("tax_lien",         "IRS Lien"),
    "LNFED":    ("tax_lien",         "Federal Tax Lien"),
    "LN":       ("lien",             "Lien"),
    "LNMECH":   ("lien",             "Mechanic Lien"),
    "LNHOA":    ("lien",             "HOA Lien"),
    "MEDLN":    ("lien",             "Medicaid Lien"),
    "PRO":      ("probate",          "Probate"),
    "NOC":      ("commencement",     "Notice of Commencement"),
    "RELLP":    ("release",          "Release Lis Pendens"),
}

DOC_SEARCH_TERMS = {
    "LP":       ["Lis Pendens"],
    "NOFC":     ["Notice of Foreclosure", "Foreclosure"],
    "TAXDEED":  ["Tax Deed"],
    "JUD":      ["Judgment"],
    "CCJ":      ["Certified Judgment"],
    "DRJUD":    ["Domestic Relations Judgment"],
    "LNCORPTX": ["Corporation Tax Lien", "Corp Tax Lien"],
    "LNIRS":    ["IRS Lien"],
    "LNFED":    ["Federal Lien", "Federal Tax Lien"],
    "LN":       ["Lien"],
    "LNMECH":   ["Mechanic Lien", "Mechanics Lien"],
    "LNHOA":    ["HOA Lien", "Homeowners Association Lien"],
    "MEDLN":    ["Medicaid Lien"],
    "PRO":      ["Probate"],
    "NOC":      ["Notice of Commencement"],
    "RELLP":    ["Release Lis Pendens", "Release of Lis Pendens"],
}

REPO_ROOT   = Path(__file__).resolve().parent.parent
OUTPUT_DIRS = [REPO_ROOT / "dashboard", REPO_ROOT / "data"]
REQUEST_TIMEOUT = 10
HEADERS = {
    "User-Agent":      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         SEARCH_BASE + "/",
}


# ══════════════════════════════════════════════════════════════════════════════
# HTTP HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        s.get(SEARCH_BASE, timeout=10)
        log.info("Session established with recorder portal")
    except Exception as e:
        log.warning("Could not pre-load session: %s", e)
    return s


def _retry_get(session, url, params=None, retries=3, **kwargs) -> Optional[requests.Response]:
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=10, **kwargs)
            if r.status_code == 200:
                return r
            log.warning("HTTP %d on attempt %d: %s", r.status_code, attempt, url)
        except Exception as exc:
            log.warning("Request error attempt %d: %s", attempt, exc)
        time.sleep(2 * attempt)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# SEARCH + PARSING
# ══════════════════════════════════════════════════════════════════════════════

def _normalise_date(raw) -> str:
    if not raw:
        return ""
    raw = str(raw).split("T")[0].strip()
    try:
        return datetime.strptime(raw, "%Y-%m-%d").strftime("%m/%d/%Y")
    except Exception:
        return raw


def _clean_amount(raw: str) -> str:
    raw = str(raw).strip()
    if raw in ("", "0", "None", "null"):
        return ""
    try:
        v = float(re.sub(r"[^\d.]", "", raw))
        return f"{v:.2f}" if v > 0 else ""
    except Exception:
        return raw


def _join_names(val) -> str:
    if isinstance(val, list):
        return "; ".join(
            v.get("name", v.get("fullName", str(v))) if isinstance(v, dict) else str(v)
            for v in val
        )
    return str(val) if val else ""


def _parse_json_response(data: dict, doc_code: str) -> list[dict]:
    records = []
    items = (
        data.get("hits", {}).get("hits", [])
        or data.get("hits", [])
        or data.get("results", [])
        or data.get("data", [])
        or data.get("instruments", [])
        or data.get("documents", [])
    )
    if isinstance(items, dict):
        items = list(items.values())

    for item in items:
        try:
            src = item.get("_source", item)
            inst_num = str(src.get("instrumentNumber",
                           src.get("docNumber",
                           src.get("instrument_number",
                           src.get("id", "")))))
            filed    = _normalise_date(src.get("recordedDate",
                                       src.get("filedDate",
                                       src.get("recorded_date", ""))))
            grantor  = _join_names(src.get("grantors", src.get("grantor", "")))
            grantee  = _join_names(src.get("grantees", src.get("grantee", "")))
            legal    = str(src.get("legalDescription", src.get("legal", "")))[:300]
            amount   = _clean_amount(str(src.get("consideration",
                                         src.get("amount",
                                         src.get("totalAmount", "")))))
            doc_id   = str(src.get("id", src.get("_id", inst_num)))
            clerk_url = f"{SEARCH_BASE}/result/index/{doc_id}" if doc_id else ""

            if inst_num:
                records.append({
                    "doc_num":   inst_num,
                    "doc_type":  doc_code,
                    "filed":     filed,
                    "grantor":   grantor,
                    "grantee":   grantee,
                    "legal":     legal,
                    "amount":    amount,
                    "clerk_url": clerk_url,
                })
        except Exception as exc:
            log.debug("Skip item: %s", exc)

    return records


def _parse_html_response(html: str, doc_code: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    records = []

    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if not any(k in " ".join(headers) for k in ("instrument", "grantor", "recorded", "doc")):
            continue
        for tr in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if not cells:
                continue
            cm = {h: cells[i] for i, h in enumerate(headers) if i < len(cells)}

            def g(*keys):
                for k in keys:
                    for ck, cv in cm.items():
                        if k in ck:
                            return cv
                return ""

            link = ""
            a = tr.find("a", href=True)
            if a:
                href = a["href"]
                link = href if href.startswith("http") else SEARCH_BASE + href

            doc_num = g("instrument", "doc #", "number", "record")
            if not doc_num and cells:
                doc_num = cells[0]

            records.append({
                "doc_num":   doc_num,
                "doc_type":  doc_code,
                "filed":     _normalise_date(g("recorded", "filed", "date")),
                "grantor":   g("grantor", "owner"),
                "grantee":   g("grantee"),
                "legal":     g("legal", "description"),
                "amount":    _clean_amount(g("consideration", "amount")),
                "clerk_url": link,
            })

    return records


def search_one_type(session: requests.Session, doc_code: str,
                    start_dt: datetime, end_dt: datetime) -> list[dict]:
    records: list[dict] = []
    search_terms = DOC_SEARCH_TERMS.get(doc_code, [doc_code])

    for term in search_terms:
        page = 1
        while True:
            found_any = False

            for api_path in ["/api/search/instruments", "/api/instruments/search", "/api/search"]:
                params = {
                    "q":          term,
                    "startDate":  start_dt.strftime("%Y-%m-%d"),
                    "endDate":    end_dt.strftime("%Y-%m-%d"),
                    "department": "RP",
                    "page":       page,
                    "size":       200,
                    "per_page":   200,
                }
                r = _retry_get(session, SEARCH_BASE + api_path, params=params)
                if r is None:
                    continue
                try:
                    data = r.json()
                    batch = _parse_json_response(data, doc_code)
                    if batch:
                        records.extend(batch)
                        log.debug("  %s '%s' p%d → %d (JSON %s)", doc_code, term, page, len(batch), api_path)
                        total = data.get("total", data.get("totalHits", 0))
                        if isinstance(total, dict):
                            total = total.get("value", 0)
                        found_any = True
                        if page * 200 >= int(total or 0):
                            break
                        page += 1
                        break
                except Exception:
                    continue

            if found_any:
                continue

            html_params = {
                "searchTerm":  term,
                "docType":     doc_code,
                "startDate":   start_dt.strftime("%m/%d/%Y"),
                "endDate":     end_dt.strftime("%m/%d/%Y"),
                "department":  "RP",
                "page":        page,
            }
            r2 = _retry_get(session, f"{SEARCH_BASE}/search/results", params=html_params)
            if r2 is not None:
                batch = _parse_html_response(r2.text, doc_code)
                if batch:
                    records.extend(batch)
                    log.debug("  %s '%s' p%d → %d (HTML)", doc_code, term, page, len(batch))
                    page += 1
                    continue

            break

        time.sleep(0.4)

    return records


def scrape_recorder(start_dt: datetime, end_dt: datetime) -> list[dict]:
    session  = make_session()
    all_recs : list[dict] = []
    for code in DOC_TYPE_SEARCHES:
        log.info("Searching: %s  (%s)", code, DOC_TYPE_SEARCHES[code][1])
        try:
            recs = search_one_type(session, code, start_dt, end_dt)
            log.info("  → %d records", len(recs))
            all_recs.extend(recs)
        except Exception as exc:
            log.error("Error scraping %s: %s", code, exc)
    return all_recs


# ══════════════════════════════════════════════════════════════════════════════
# PARCEL LOOKUP
# ══════════════════════════════════════════════════════════════════════════════

class ParcelLookup:
    _OWNER_COLS  = ["OWNER", "OWN1", "OWNERNAME"]
    _SITE_COLS   = ["SITE_ADDR", "SITEADDR", "SITE_ADDRESS"]
    _SCITY_COLS  = ["SITE_CITY", "SITECITY"]
    _SZIP_COLS   = ["SITE_ZIP",  "SITEZIP"]
    _MAIL1_COLS  = ["ADDR_1", "MAILADR1", "MAIL_ADDR1"]
    _MCITY_COLS  = ["CITY", "MAILCITY", "MAIL_CITY"]
    _MSTATE_COLS = ["STATE", "MAILSTATE"]
    _MZIP_COLS   = ["ZIP", "MAILZIP", "MAIL_ZIP"]

    def __init__(self):
        self._by_owner: dict[str, dict] = {}
        self._loaded = False
        self._api_session = requests.Session()
        self._api_session.headers["User-Agent"] = HEADERS["User-Agent"]

    @staticmethod
    def _pick(row, cols):
        for c in cols:
            v = row.get(c) or row.get(c.lower()) or ""
            if v:
                return str(v).strip()
        return ""

    @staticmethod
    def _name_variants(raw: str) -> list[str]:
        raw = raw.strip().upper()
        variants = {raw}
        if "," in raw:
            parts = [p.strip() for p in raw.split(",", 1)]
            if len(parts) == 2:
                variants.add(f"{parts[1]} {parts[0]}")
        else:
            words = raw.split()
            if len(words) >= 2:
                variants.add(f"{words[-1]} {' '.join(words[:-1])}")
                variants.add(f"{words[-1]}, {' '.join(words[:-1])}")
        return [v for v in variants if v]

    def _index(self, row: dict) -> None:
        owner = self._pick(row, self._OWNER_COLS)
        if not owner:
            return
        p = {
            "prop_address": self._pick(row, self._SITE_COLS),
            "prop_city":    self._pick(row, self._SCITY_COLS) or "Columbus",
            "prop_state":   "OH",
            "prop_zip":     self._pick(row, self._SZIP_COLS),
            "mail_address": self._pick(row, self._MAIL1_COLS),
            "mail_city":    self._pick(row, self._MCITY_COLS),
            "mail_state":   self._pick(row, self._MSTATE_COLS) or "OH",
            "mail_zip":     self._pick(row, self._MZIP_COLS),
        }
        for v in self._name_variants(owner):
            self._by_owner.setdefault(v, p)

    def load_file(self, path: Path) -> int:
        count = 0
        if path.suffix.lower() == ".dbf" and HAS_DBF:
            try:
                for rec in DBF(str(path), encoding="latin-1", ignore_missing_memofile=True):
                    self._index(dict(rec)); count += 1
            except Exception as e:
                log.error("DBF error: %s", e)
        elif path.suffix.lower() in (".csv", ".txt"):
            try:
                with open(path, newline="", encoding="utf-8-sig", errors="replace") as fh:
                    for row in csv.DictReader(fh):
                        self._index(row); count += 1
            except Exception as e:
                log.error("CSV error: %s", e)
        self._loaded = count > 0
        return count

    def _api_lookup(self, owner: str) -> Optional[dict]:
        try:
            r = self._api_session.get(
                AUDITOR_API,
                params={"ownerName": owner, "pageSize": 1},
                timeout=8,
            )
            if r.status_code == 200:
                items = r.json()
                if isinstance(items, list) and items:
                    p = items[0]
                elif isinstance(items, dict):
                    p = (items.get("data") or items.get("parcels") or [{}])[0]
                else:
                    return None
                return {
                    "prop_address": str(p.get("siteAddress", p.get("address", ""))),
                    "prop_city":    str(p.get("siteCity", "Columbus")),
                    "prop_state":   "OH",
                    "prop_zip":     str(p.get("siteZip", p.get("zip", ""))),
                    "mail_address": str(p.get("mailAddress", "")),
                    "mail_city":    str(p.get("mailCity", "")),
                    "mail_state":   str(p.get("mailState", "OH")),
                    "mail_zip":     str(p.get("mailZip", "")),
                }
        except Exception:
            pass
        return None

    def lookup(self, owner: str) -> Optional[dict]:
        if not owner:
            return None
        for v in self._name_variants(owner.upper()):
            hit = self._by_owner.get(v)
            if hit:
                return hit
        return self._api_lookup(owner)

    @property
    def loaded(self) -> bool:
        return self._loaded


# ══════════════════════════════════════════════════════════════════════════════
# SCORING
# ══════════════════════════════════════════════════════════════════════════════

def _parse_amount(raw: str) -> float:
    try:
        return float(re.sub(r"[^\d.]", "", raw))
    except Exception:
        return 0.0


def _is_new_this_week(filed: str) -> bool:
    try:
        return (datetime.now() - datetime.strptime(filed.strip(), "%m/%d/%Y")).days <= 7
    except Exception:
        return False


def compute_score_and_flags(rec: dict) -> tuple[int, list[str]]:
    flags    = []
    doc_type = rec.get("doc_type", "")
    cat      = rec.get("cat", "")
    amount   = _parse_amount(rec.get("amount", ""))
    owner    = rec.get("owner", "")
    filed    = rec.get("filed", "")

    if doc_type == "LP":    flags.append("Lis pendens")
    if doc_type == "NOFC":  flags.append("Pre-foreclosure")
    if cat == "judgment":   flags.append("Judgment lien")
    if cat == "tax_lien":   flags.append("Tax lien")
    if doc_type == "LNMECH": flags.append("Mechanic lien")
    if cat == "probate":    flags.append("Probate / estate")
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bLTD\b", owner.upper()):
        flags.append("LLC / corp owner")
    if _is_new_this_week(filed): flags.append("New this week")

    seen = set()
    flags = [f for f in flags if not (f in seen or seen.add(f))]

    score = 30 + 10 * len(flags)
    if "Lis pendens" in flags and "Pre-foreclosure" in flags: score += 20
    if amount > 100_000:  score += 15
    elif amount > 50_000: score += 10
    if "New this week" in flags: score += 5
    if rec.get("prop_address") or rec.get("mail_address"): score += 5

    return min(score, 100), flags


# ══════════════════════════════════════════════════════════════════════════════
# ENRICH + OUTPUT
# ══════════════════════════════════════════════════════════════════════════════

def enrich_record(raw: dict, parcel: ParcelLookup) -> dict:
    code = raw.get("doc_type", "")
    cat, cat_label = DOC_TYPE_SEARCHES.get(code, ("other", code))
    owner = raw.get("grantor", "")
    p = parcel.lookup(owner) or {}
    rec = {
        "doc_num":      raw.get("doc_num", ""),
        "doc_type":     code,
        "filed":        raw.get("filed", ""),
        "cat":          cat,
        "cat_label":    cat_label,
        "owner":        owner,
        "grantee":      raw.get("grantee", ""),
        "amount":       raw.get("amount", ""),
        "legal":        raw.get("legal", ""),
        "prop_address": p.get("prop_address", ""),
        "prop_city":    p.get("prop_city", ""),
        "prop_state":   p.get("prop_state", "OH"),
        "prop_zip":     p.get("prop_zip", ""),
        "mail_address": p.get("mail_address", ""),
        "mail_city":    p.get("mail_city", ""),
        "mail_state":   p.get("mail_state", "OH"),
        "mail_zip":     p.get("mail_zip", ""),
        "clerk_url":    raw.get("clerk_url", ""),
        "flags":        [],
        "score":        30,
    }
    rec["score"], rec["flags"] = compute_score_and_flags(rec)
    return rec


def _split_name(full: str) -> tuple[str, str]:
    full = full.strip()
    if "," in full:
        parts = [p.strip() for p in full.split(",", 1)]
        return parts[1].title(), parts[0].title()
    parts = full.split()
    return (parts[0].title(), " ".join(parts[1:]).title()) if len(parts) >= 2 else (full.title(), "")


def build_ghl_csv(records: list[dict]) -> str:
    fields = [
        "First Name", "Last Name", "Mailing Address", "Mailing City",
        "Mailing State", "Mailing Zip", "Property Address", "Property City",
        "Property State", "Property Zip", "Lead Type", "Document Type",
        "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score",
        "Motivated Seller Flags", "Source", "Public Records URL",
    ]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    w.writeheader()
    for r in records:
        first, last = _split_name(r.get("owner", ""))
        w.writerow({
            "First Name": first, "Last Name": last,
            "Mailing Address": r.get("mail_address", ""),
            "Mailing City":    r.get("mail_city", ""),
            "Mailing State":   r.get("mail_state", ""),
            "Mailing Zip":     r.get("mail_zip", ""),
            "Property Address": r.get("prop_address", ""),
            "Property City":    r.get("prop_city", ""),
            "Property State":   r.get("prop_state", ""),
            "Property Zip":     r.get("prop_zip", ""),
            "Lead Type":        r.get("cat_label", ""),
            "Document Type":    r.get("doc_type", ""),
            "Date Filed":       r.get("filed", ""),
            "Document Number":  r.get("doc_num", ""),
            "Amount/Debt Owed": r.get("amount", ""),
            "Seller Score":     r.get("score", 0),
            "Motivated Seller Flags": "; ".join(r.get("flags", [])),
            "Source":           "Franklin County Recorder",
            "Public Records URL": r.get("clerk_url", ""),
        })
    return buf.getvalue()


def save_output(records: list[dict], start_str: str, end_str: str) -> None:
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Franklin County Recorder",
        "date_range":   {"from": start_str, "to": end_str},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address") or r.get("mail_address")),
        "records":      sorted(records, key=lambda r: r.get("score", 0), reverse=True),
    }
    for d in OUTPUT_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        with open(d / "records.json", "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
        with open(d / "leads_ghl.csv", "w", encoding="utf-8", newline="") as fh:
            fh.write(build_ghl_csv(records))
        log.info("Saved %d records → %s", len(records), d)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    end_dt    = datetime.now()
    start_dt  = end_dt - timedelta(days=LOOKBACK_DAYS)
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    log.info("╔══════════════════════════════════════════════╗")
    log.info("║  Franklin County Motivated Seller Scraper   ║")
    log.info("╚══════════════════════════════════════════════╝")
    log.info("Portal : %s", SEARCH_BASE)
    log.info("Range  : %s → %s (%d days)", start_str, end_str, LOOKBACK_DAYS)

    parcel = ParcelLookup()
    pdir   = REPO_ROOT / "data" / "parcels"
    pdir.mkdir(parents=True, exist_ok=True)
    for existing in list(pdir.glob("**/*.dbf")) + list(pdir.glob("**/*.csv")):
        if time.time() - existing.stat().st_mtime < 86400:
            parcel.load_file(existing)
            break
    log.info("Parcel file lookup: %s | live API fallback: enabled",
             "ready" if parcel.loaded else "not loaded")

    log.info("Scraping …")
    raw_records = scrape_recorder(start_dt, end_dt)
    log.info("Raw records: %d", len(raw_records))

    seen: set = set()
    enriched: list[dict] = []
    for raw in raw_records:
        try:
            rec = enrich_record(raw, parcel)
            key = (rec["doc_num"], rec["doc_type"])
            if key in seen or not rec["doc_num"]:
                continue
            seen.add(key)
            enriched.append(rec)
        except Exception as exc:
            log.warning("Skipping bad record: %s", exc)

    log.info("Unique enriched records: %d", len(enriched))
    save_output(enriched, start_str, end_str)

    with_addr = sum(1 for r in enriched if r.get("prop_address") or r.get("mail_address"))
    avg_score = sum(r["score"] for r in enriched) / max(len(enriched), 1)
    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log.info("Total leads   : %d", len(enriched))
    log.info("With address  : %d (%.0f%%)", with_addr, 100 * with_addr / max(len(enriched), 1))
    log.info("Avg score     : %.1f / 100", avg_score)
    if enriched:
        log.info("Top 5 leads:")
        for r in sorted(enriched, key=lambda x: x["score"], reverse=True)[:5]:
            log.info("  [%d] %s  %s  %s", r["score"], r["doc_type"], r["owner"][:40], r["filed"])


if __name__ == "__main__":
    main()
