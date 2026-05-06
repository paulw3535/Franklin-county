#!/usr/bin/env python3
"""
Franklin County, Ohio — Motivated Seller Lead Scraper  v3
Sources:
  1. ArcGIS / Auditor open-data feature service  (no auth, fully public)
     https://services2.arcgis.com/ziXVKVy3BiopMCCU/arcgis/rest/services/
  2. Franklin County Auditor owner-lookup API   (address enrichment)
     https://audr-api.franklincountyohio.gov/v1/parcels/ByOwner
  3. Franklin County Auditor daily-conveyance page (HTML fallback)
     https://property.franklincountyauditor.com
"""

import csv
import io
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")

# ── constants ──────────────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
REPO_ROOT     = Path(__file__).resolve().parent.parent
OUTPUT_DIRS   = [REPO_ROOT / "dashboard", REPO_ROOT / "data"]

# Auditor owner/address API  (confirmed working, no auth)
AUDITOR_OWNER_API = "https://audr-api.franklincountyohio.gov/v1/parcels/ByOwner"
AUDITOR_ADDR_API  = "https://audr-api.franklincountyohio.gov/v1/parcels/ByAddress"

# ArcGIS feature service — Franklin County Auditor open data
# Layer 0 = Parcel Boundaries with owner + transfer attributes
ARCGIS_BASE   = (
    "https://services2.arcgis.com/ziXVKVy3BiopMCCU/arcgis/rest/services"
    "/Parcel_Boundaries/FeatureServer/0/query"
)

# Auditor conveyance (transfer) page — HTML fallback
CONV_URL = "https://property.franklincountyauditor.com/_web/search/commonsearch.aspx?mode=conveyance"

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# ── distress keyword filters ───────────────────────────────────────────────────
# We query the ArcGIS layer for transfers, then classify by deed type / remarks
DISTRESS_PATTERNS: list[tuple[str, str, str]] = [
    # (regex pattern, doc_type code, cat)
    (r"lis\s*pendens",                        "LP",       "pre_foreclosure"),
    (r"notice\s+of\s+foreclos",               "NOFC",     "pre_foreclosure"),
    (r"foreclos",                              "NOFC",     "pre_foreclosure"),
    (r"tax\s+deed",                            "TAXDEED",  "tax_distress"),
    (r"\bjudgment\b|\bjudgement\b",           "JUD",      "judgment"),
    (r"certified.{0,10}judgment",             "CCJ",      "judgment"),
    (r"domestic.{0,15}judgment",              "DRJUD",    "judgment"),
    (r"corp.{0,10}tax\s+lien|corporation.*lien","LNCORPTX","tax_lien"),
    (r"\birs\b.*lien|internal\s+revenue.*lien","LNIRS",   "tax_lien"),
    (r"federal.*lien|fed.*tax.*lien",          "LNFED",   "tax_lien"),
    (r"mechanic.{0,5}lien|mechanic.{0,5}s lien","LNMECH", "lien"),
    (r"\bhoa\b.*lien|homeowner.{0,15}lien",   "LNHOA",   "lien"),
    (r"medicaid.*lien",                        "MEDLN",   "lien"),
    (r"\blien\b",                              "LN",      "lien"),
    (r"\bprobate\b|\bestate\b",                "PRO",     "probate"),
    (r"notice\s+of\s+commencement",            "NOC",     "commencement"),
    (r"release.{0,10}lis\s*pendens",           "RELLP",   "release"),
]

DOC_META: dict[str, tuple[str, str]] = {
    "LP":       ("pre_foreclosure", "Lis Pendens"),
    "NOFC":     ("pre_foreclosure", "Notice of Foreclosure"),
    "TAXDEED":  ("tax_distress",    "Tax Deed"),
    "JUD":      ("judgment",        "Judgment"),
    "CCJ":      ("judgment",        "Certified Court Judgment"),
    "DRJUD":    ("judgment",        "Domestic Relations Judgment"),
    "LNCORPTX": ("tax_lien",        "Corp Tax Lien"),
    "LNIRS":    ("tax_lien",        "IRS Lien"),
    "LNFED":    ("tax_lien",        "Federal Tax Lien"),
    "LN":       ("lien",            "Lien"),
    "LNMECH":   ("lien",            "Mechanic Lien"),
    "LNHOA":    ("lien",            "HOA Lien"),
    "MEDLN":    ("lien",            "Medicaid Lien"),
    "PRO":      ("probate",         "Probate"),
    "NOC":      ("commencement",    "Notice of Commencement"),
    "RELLP":    ("release",         "Release Lis Pendens"),
}


# ══════════════════════════════════════════════════════════════════════════════
# HTTP
# ══════════════════════════════════════════════════════════════════════════════

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = UA
    s.headers["Accept"]     = "application/json, text/html, */*"
    return s


def get_json(session, url, params=None, retries=3) -> Optional[dict]:
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
            log.warning("HTTP %d attempt %d  %s", r.status_code, attempt, url[:80])
        except Exception as e:
            log.warning("Error attempt %d: %s", attempt, e)
        time.sleep(3 * attempt)
    return None


def get_html(session, url, params=None, retries=3) -> Optional[str]:
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=15)
            if r.status_code == 200:
                return r.text
        except Exception as e:
            log.warning("HTML error attempt %d: %s", attempt, e)
        time.sleep(3 * attempt)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# CLASSIFIER
# ══════════════════════════════════════════════════════════════════════════════

def classify(text: str) -> Optional[tuple[str, str, str]]:
    """Return (doc_type, cat, cat_label) if text matches a distress pattern."""
    t = text.lower()
    for pattern, code, cat in DISTRESS_PATTERNS:
        if re.search(pattern, t):
            _, label = DOC_META.get(code, (cat, code))
            return code, cat, label
    return None


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 1 — ArcGIS Auditor open-data (property transfers with deed type)
# ══════════════════════════════════════════════════════════════════════════════

def _arcgis_where(start_dt: datetime, end_dt: datetime) -> str:
    s = start_dt.strftime("%Y-%m-%d")
    e = end_dt.strftime("%Y-%m-%d")
    # CONVEY_DATE or TRANSFER_DATE field holds transfer date
    return (
        f"(CONVEY_DATE >= DATE '{s}' AND CONVEY_DATE <= DATE '{e}') OR "
        f"(TRANSFER_DATE >= DATE '{s}' AND TRANSFER_DATE <= DATE '{e}')"
    )


def fetch_arcgis(session: requests.Session,
                 start_dt: datetime, end_dt: datetime) -> list[dict]:
    """Query ArcGIS feature service for recent transfers."""
    records: list[dict] = []
    offset   = 0
    page_sz  = 1000

    # Fields we want — ArcGIS returns all if we omit outFields
    fields = ("OBJECTID,OWNER,OWN1,SITE_ADDR,SITEADDR,SITE_CITY,SITECITY,"
              "SITE_ZIP,SITEZIP,ADDR_1,MAILADR1,CITY,MAILCITY,STATE,MAILSTATE,"
              "ZIP,MAILZIP,CONVEY_DATE,TRANSFER_DATE,DEED_TYPE,GRANTOR,GRANTEE,"
              "LEGAL_DESC,LEGAL,SALE_PRICE,AMOUNT,PARCEL_ID,PARCELID,PIN")

    while True:
        params = {
            "where":         _arcgis_where(start_dt, end_dt),
            "outFields":     fields,
            "f":             "json",
            "resultOffset":  offset,
            "resultRecordCount": page_sz,
            "orderByFields": "OBJECTID ASC",
        }
        data = get_json(session, ARCGIS_BASE, params=params)
        if not data:
            break

        features = data.get("features", [])
        if not features:
            break

        for feat in features:
            a = feat.get("attributes", {})
            # Pull all text fields together for classification
            deed_type = str(a.get("DEED_TYPE", "") or "")
            grantor   = str(a.get("GRANTOR", a.get("OWNER", a.get("OWN1", ""))) or "")
            grantee   = str(a.get("GRANTEE", "") or "")
            legal     = str(a.get("LEGAL_DESC", a.get("LEGAL", "")) or "")[:300]
            combined  = f"{deed_type} {grantor} {grantee} {legal}"

            match = classify(combined)
            if not match:
                continue

            doc_type, cat, cat_label = match

            # Date
            raw_date = a.get("CONVEY_DATE") or a.get("TRANSFER_DATE")
            filed = ""
            if raw_date:
                try:
                    # ArcGIS returns epoch ms
                    filed = datetime.utcfromtimestamp(int(raw_date) / 1000).strftime("%m/%d/%Y")
                except Exception:
                    filed = str(raw_date)

            # Amount
            amount_raw = a.get("SALE_PRICE", a.get("AMOUNT", ""))
            amount = _clean_amount(str(amount_raw))

            # Addresses
            prop_addr  = str(a.get("SITE_ADDR", a.get("SITEADDR", "")) or "")
            prop_city  = str(a.get("SITE_CITY", a.get("SITECITY", "Columbus")) or "Columbus")
            prop_zip   = str(a.get("SITE_ZIP",  a.get("SITEZIP",  "")) or "")
            mail_addr  = str(a.get("ADDR_1",    a.get("MAILADR1", "")) or "")
            mail_city  = str(a.get("CITY",      a.get("MAILCITY", "")) or "")
            mail_state = str(a.get("STATE",     a.get("MAILSTATE","OH")) or "OH")
            mail_zip   = str(a.get("ZIP",       a.get("MAILZIP",  "")) or "")

            parcel_id  = str(a.get("PARCEL_ID", a.get("PARCELID", a.get("PIN", ""))) or "")
            obj_id     = str(a.get("OBJECTID", ""))
            clerk_url  = (f"https://property.franklincountyauditor.com"
                          f"/_web/datalets/datalet.aspx?pin={parcel_id}"
                          if parcel_id else "")

            records.append({
                "doc_num":      parcel_id or obj_id,
                "doc_type":     doc_type,
                "filed":        filed,
                "cat":          cat,
                "cat_label":    cat_label,
                "owner":        grantor,
                "grantee":      grantee,
                "amount":       amount,
                "legal":        legal,
                "prop_address": prop_addr,
                "prop_city":    prop_city,
                "prop_state":   "OH",
                "prop_zip":     prop_zip,
                "mail_address": mail_addr,
                "mail_city":    mail_city,
                "mail_state":   mail_state,
                "mail_zip":     mail_zip,
                "clerk_url":    clerk_url,
            })

        log.info("  ArcGIS offset %d → %d features (%d matched so far)",
                 offset, len(features), len(records))

        if len(features) < page_sz:
            break
        offset += page_sz
        time.sleep(0.3)

    return records


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 2 — Auditor Daily Conveyances page (HTML)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_conveyances(session: requests.Session,
                      start_dt: datetime, end_dt: datetime) -> list[dict]:
    """Scrape the auditor's daily conveyance list for distress deed types."""
    records: list[dict] = []
    current = start_dt

    while current <= end_dt:
        date_str = current.strftime("%m/%d/%Y")
        params   = {"searchValue": date_str, "searchType": "conveyance"}
        html     = get_html(session, CONV_URL, params=params)

        if html:
            soup = BeautifulSoup(html, "lxml")
            for table in soup.find_all("table"):
                headers = [th.get_text(strip=True).lower()
                           for th in table.find_all("th")]
                if not any(k in " ".join(headers)
                           for k in ("grantor", "deed", "parcel", "owner")):
                    continue
                for tr in table.find_all("tr")[1:]:
                    cells = [td.get_text(strip=True)
                             for td in tr.find_all("td")]
                    if not cells:
                        continue
                    cm = {h: cells[i] for i, h in enumerate(headers)
                          if i < len(cells)}

                    def g(*keys):
                        for k in keys:
                            for ck, cv in cm.items():
                                if k in ck:
                                    return cv
                        return ""

                    deed    = g("deed", "type", "instrument")
                    grantor = g("grantor", "seller", "owner")
                    grantee = g("grantee", "buyer")
                    parcel  = g("parcel", "pin", "id")
                    amount  = _clean_amount(g("price", "amount", "consideration"))
                    combined = f"{deed} {grantor} {grantee}"

                    match = classify(combined)
                    if not match:
                        continue
                    doc_type, cat, cat_label = match

                    link = ""
                    a_tag = tr.find("a", href=True)
                    if a_tag:
                        href = a_tag["href"]
                        link = (href if href.startswith("http")
                                else "https://property.franklincountyauditor.com" + href)

                    records.append({
                        "doc_num":      parcel or deed,
                        "doc_type":     doc_type,
                        "filed":        date_str,
                        "cat":          cat,
                        "cat_label":    cat_label,
                        "owner":        grantor,
                        "grantee":      grantee,
                        "amount":       amount,
                        "legal":        "",
                        "prop_address": "",
                        "prop_city":    "",
                        "prop_state":   "OH",
                        "prop_zip":     "",
                        "mail_address": "",
                        "mail_city":    "",
                        "mail_state":   "OH",
                        "mail_zip":     "",
                        "clerk_url":    link,
                    })

        log.debug("Conveyances %s → %d matched", date_str, len(records))
        current += timedelta(days=1)
        time.sleep(0.5)

    return records


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 3 — Auditor owner-lookup API  (address enrichment)
# ══════════════════════════════════════════════════════════════════════════════

_addr_cache: dict[str, Optional[dict]] = {}


def lookup_address(session: requests.Session, owner: str) -> Optional[dict]:
    if not owner:
        return None
    key = owner.strip().upper()
    if key in _addr_cache:
        return _addr_cache[key]

    data = get_json(session, AUDITOR_OWNER_API,
                    params={"ownerName": owner, "pageSize": 1, "pageNumber": 1})
    result = None
    if data:
        items = (data if isinstance(data, list)
                 else data.get("parcels", data.get("data", data.get("results", []))))
        if items:
            p = items[0]
            result = {
                "prop_address": str(p.get("siteAddress",  p.get("address",    ""))),
                "prop_city":    str(p.get("siteCity",     "Columbus")),
                "prop_state":   "OH",
                "prop_zip":     str(p.get("siteZip",      p.get("zip",        ""))),
                "mail_address": str(p.get("mailAddress",  p.get("mailingAddress",""))),
                "mail_city":    str(p.get("mailCity",     "")),
                "mail_state":   str(p.get("mailState",    "OH")),
                "mail_zip":     str(p.get("mailZip",      "")),
            }

    _addr_cache[key] = result
    time.sleep(0.1)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _clean_amount(raw: str) -> str:
    raw = str(raw).strip()
    if raw in ("", "0", "None", "null", "0.0", "0.00"):
        return ""
    try:
        v = float(re.sub(r"[^\d.]", "", raw))
        return f"{v:.2f}" if v > 0 else ""
    except Exception:
        return raw


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


# ══════════════════════════════════════════════════════════════════════════════
# SCORING
# ══════════════════════════════════════════════════════════════════════════════

def compute_score_and_flags(rec: dict) -> tuple[int, list[str]]:
    flags    = []
    doc_type = rec.get("doc_type", "")
    cat      = rec.get("cat", "")
    amount   = _parse_amount(rec.get("amount", ""))
    owner    = rec.get("owner", "")
    filed    = rec.get("filed", "")

    if doc_type == "LP":     flags.append("Lis pendens")
    if doc_type == "NOFC":   flags.append("Pre-foreclosure")
    if cat == "judgment":    flags.append("Judgment lien")
    if cat == "tax_lien":    flags.append("Tax lien")
    if doc_type == "LNMECH": flags.append("Mechanic lien")
    if cat == "probate":     flags.append("Probate / estate")
    if cat == "tax_distress": flags.append("Tax lien")
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
# OUTPUT
# ══════════════════════════════════════════════════════════════════════════════

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
            "Source":           "Franklin County Auditor",
            "Public Records URL": r.get("clerk_url", ""),
        })
    return buf.getvalue()


def save_output(records: list[dict], start_str: str, end_str: str) -> None:
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Franklin County Auditor (ArcGIS + Conveyances)",
        "date_range":   {"from": start_str, "to": end_str},
        "total":        len(records),
        "with_address": sum(1 for r in records
                            if r.get("prop_address") or r.get("mail_address")),
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
    log.info("Range  : %s → %s (%d days)", start_str, end_str, LOOKBACK_DAYS)

    session = make_session()
    all_recs: list[dict] = []

    # ── Source 1: ArcGIS open data ─────────────────────────────────────────────
    log.info("── Source 1: ArcGIS Auditor open-data ──")
    try:
        arcgis_recs = fetch_arcgis(session, start_dt, end_dt)
        log.info("ArcGIS records matched: %d", len(arcgis_recs))
        all_recs.extend(arcgis_recs)
    except Exception as e:
        log.error("ArcGIS fetch failed: %s", e)

    # ── Source 2: Auditor daily conveyances ────────────────────────────────────
    log.info("── Source 2: Auditor daily conveyances ──")
    try:
        conv_recs = fetch_conveyances(session, start_dt, end_dt)
        log.info("Conveyance records matched: %d", len(conv_recs))
        all_recs.extend(conv_recs)
    except Exception as e:
        log.error("Conveyance fetch failed: %s", e)

    # ── Deduplicate ────────────────────────────────────────────────────────────
    seen: set = set()
    unique: list[dict] = []
    for r in all_recs:
        key = (r.get("doc_num", ""), r.get("owner", ""), r.get("filed", ""))
        if key in seen or not (r.get("doc_num") or r.get("owner")):
            continue
        seen.add(key)
        unique.append(r)

    log.info("Unique records after dedup: %d", len(unique))

    # ── Enrich addresses where missing ────────────────────────────────────────
    log.info("── Enriching addresses via Auditor API ──")
    enriched_count = 0
    for r in unique:
        if not r.get("prop_address") and not r.get("mail_address") and r.get("owner"):
            addr = lookup_address(session, r["owner"])
            if addr:
                r.update({k: v for k, v in addr.items() if v})
                enriched_count += 1
    log.info("Address-enriched: %d records", enriched_count)

    # ── Score ──────────────────────────────────────────────────────────────────
    for r in unique:
        r["score"], r["flags"] = compute_score_and_flags(r)

    # ── Save ───────────────────────────────────────────────────────────────────
    save_output(unique, start_str, end_str)

    with_addr = sum(1 for r in unique if r.get("prop_address") or r.get("mail_address"))
    avg_score = sum(r["score"] for r in unique) / max(len(unique), 1)
    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log.info("Total leads   : %d", len(unique))
    log.info("With address  : %d (%.0f%%)", with_addr,
             100 * with_addr / max(len(unique), 1))
    log.info("Avg score     : %.1f / 100", avg_score)
    if unique:
        log.info("Top 5 leads:")
        for r in sorted(unique, key=lambda x: x["score"], reverse=True)[:5]:
            log.info("  [%d] %s  %-40s  %s",
                     r["score"], r["doc_type"], r["owner"][:40], r["filed"])


if __name__ == "__main__":
    main()
