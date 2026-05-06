#!/usr/bin/env python3
"""
Franklin County, Ohio — Motivated Seller Lead Scraper  v4
─────────────────────────────────────────────────────────
Sources (all confirmed public, no auth required):

  1. GIS Tax Parcel layer  – recent sales + owner / address data
     https://gis.franklincountyohio.gov/hosting/rest/services/
             ParcelFeatures/Parcel_Features/MapServer/0/query

  2. Auditor ByOwner API  – address enrichment
     https://audr-api.franklincountyohio.gov/v1/parcels/ByOwner

  3. Auditor Delinquent / tax-distress signals via parcel attributes
     (HOMSTD, OWNEROCCUPIED, RENTAL flags + low SALEPRICE)

Distress detection strategy
───────────────────────────
Because lien/LP documents live behind the blocked recorder portal,
we identify motivated sellers by combining:
  • Recent deed transfer at below-market price  → possible distress sale
  • Non-owner-occupied rental                   → landlord distress
  • Missing homestead exemption on owner-occ    → possible tax issue
  • LLC/Corp owner                              → investor / estate
  • Sale price == 0 or $100                     → sheriff / tax deed
  • Out-of-county mailing address               → absentee owner
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
REPO_ROOT     = Path(__file__).resolve().parent.parent
OUTPUT_DIRS   = [REPO_ROOT / "dashboard", REPO_ROOT / "data"]

GIS_QUERY = (
    "https://gis.franklincountyohio.gov/hosting/rest/services"
    "/ParcelFeatures/Parcel_Features/MapServer/0/query"
)
AUDITOR_OWNER_API = "https://audr-api.franklincountyohio.gov/v1/parcels/ByOwner"

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

DOC_META = {
    "DISTRESS_SALE":  ("tax_distress",    "Distress / Below-Market Sale"),
    "TAX_DEED":       ("tax_distress",    "Possible Tax Deed ($0 / $100)"),
    "ABSENTEE":       ("pre_foreclosure", "Absentee / Out-of-County Owner"),
    "LLC_CORP":       ("pre_foreclosure", "LLC / Corp Owner"),
    "RENTAL":         ("lien",            "Non-Owner-Occupied Rental"),
    "NO_HOMESTEAD":   ("judgment",        "No Homestead – Possible Tax Issue"),
    "RECENT_SALE":    ("commencement",    "Recent Transfer"),
}


# ══════════════════════════════════════════════════════════════════════════════
# HTTP helpers
# ══════════════════════════════════════════════════════════════════════════════

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = UA
    s.headers["Accept"]     = "application/json, */*"
    return s


def get_json(session, url, params=None, retries=3) -> Optional[dict]:
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
            log.warning("HTTP %d attempt %d  %s", r.status_code, attempt, url[:80])
        except Exception as e:
            log.warning("Attempt %d error: %s", attempt, e)
        time.sleep(3 * attempt)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# GIS parcel query
# ══════════════════════════════════════════════════════════════════════════════

# Exact field names from the confirmed layer schema
GIS_FIELDS = (
    "OBJECTID,PARCELID,SITEADDRESS,ZIPCD,"
    "OWNERNME1,OWNERNME2,"
    "PSTLADDRES,PSTLCITYSTZIP,"
    "MAILNME1,MAILNME2,"
    "SALEDATE,SALEPRICE,"
    "OWNEROCCUPIED,HOMSTD,RENTAL,"
    "CLASSCD,CLASSDSCRP,USECD,"
    "PRPRTYDSCRP,PRPRTYDSCRP2"
)


def _epoch_to_date(ms) -> str:
    if not ms:
        return ""
    try:
        return datetime.utcfromtimestamp(int(ms) / 1000).strftime("%m/%d/%Y")
    except Exception:
        return ""


def _sale_where(start_dt: datetime, end_dt: datetime) -> str:
    # SALEDATE is a Date field — ArcGIS accepts ISO strings
    s = start_dt.strftime("%Y-%m-%d")
    e = end_dt.strftime("%Y-%m-%d")
    return f"SALEDATE >= DATE '{s}' AND SALEDATE <= DATE '{e}'"


def fetch_gis_parcels(session: requests.Session,
                      start_dt: datetime, end_dt: datetime) -> list[dict]:
    """Pull all parcels with a sale date in the lookback window."""
    records = []
    offset  = 0
    page_sz = 2000   # layer max is 3000; stay under

    while True:
        params = {
            "where":              _sale_where(start_dt, end_dt),
            "outFields":          GIS_FIELDS,
            "f":                  "json",
            "resultOffset":       offset,
            "resultRecordCount":  page_sz,
            "orderByFields":      "OBJECTID ASC",
            "returnGeometry":     "false",
        }
        data = get_json(session, GIS_QUERY, params=params)
        if not data:
            log.warning("GIS query returned no data at offset %d", offset)
            break

        if "error" in data:
            log.error("GIS API error: %s", data["error"])
            break

        features = data.get("features", [])
        log.info("GIS offset %d → %d features", offset, len(features))

        for feat in features:
            a = feat.get("attributes", {}) or {}

            owner      = str(a.get("OWNERNME1", "") or "").strip()
            owner2     = str(a.get("OWNERNME2", "") or "").strip()
            site_addr  = str(a.get("SITEADDRESS", "") or "").strip()
            zipcd      = str(a.get("ZIPCD", "") or "").strip()
            mail_addr  = str(a.get("PSTLADDRES", a.get("MAILNME2", "")) or "").strip()
            mail_csz   = str(a.get("PSTLCITYSTZIP", a.get("MAILNME1","")) or "").strip()
            sale_date  = _epoch_to_date(a.get("SALEDATE"))
            sale_price = float(a.get("SALEPRICE") or 0)
            parcel_id  = str(a.get("PARCELID", "") or "").strip()
            obj_id     = str(a.get("OBJECTID", "") or "")
            owner_occ  = str(a.get("OWNEROCCUPIED", "") or "").strip().upper()
            homestead  = str(a.get("HOMSTD", "") or "").strip().upper()
            rental     = str(a.get("RENTAL", "") or "").strip().upper()
            legal      = " ".join(filter(None, [
                str(a.get("PRPRTYDSCRP","") or ""),
                str(a.get("PRPRTYDSCRP2","") or ""),
            ]))[:200]

            # Parse mailing city/state/zip from combined field "CITY ST ZIP"
            mail_city, mail_state, mail_zip = _parse_csz(mail_csz)

            # ── Classify distress type ───────────────────────────────────────
            doc_type = _classify_parcel(
                owner=owner, sale_price=sale_price,
                owner_occ=owner_occ, homestead=homestead,
                rental=rental, mail_state=mail_state,
            )

            clerk_url = (
                f"https://property.franklincountyauditor.com"
                f"/_web/datalets/datalet.aspx?pin={parcel_id}&UseSearch=no"
                if parcel_id else ""
            )

            cat, cat_label = DOC_META.get(doc_type, ("other", doc_type))

            records.append({
                "doc_num":      parcel_id or obj_id,
                "doc_type":     doc_type,
                "filed":        sale_date,
                "cat":          cat,
                "cat_label":    cat_label,
                "owner":        owner + (f"; {owner2}" if owner2 else ""),
                "grantee":      "",
                "amount":       f"{sale_price:.2f}" if sale_price > 0 else "",
                "legal":        legal,
                "prop_address": site_addr,
                "prop_city":    "Columbus",
                "prop_state":   "OH",
                "prop_zip":     zipcd,
                "mail_address": mail_addr,
                "mail_city":    mail_city,
                "mail_state":   mail_state or "OH",
                "mail_zip":     mail_zip,
                "clerk_url":    clerk_url,
                "flags":        [],
                "score":        30,
            })

        if len(features) < page_sz:
            break
        offset += page_sz
        time.sleep(0.3)

    return records


def _parse_csz(csz: str) -> tuple[str, str, str]:
    """Parse 'COLUMBUS OH 43215' → ('COLUMBUS', 'OH', '43215')."""
    csz = csz.strip()
    # Try zip at end
    m = re.search(r"(\d{5})(?:-\d{4})?$", csz)
    zip_code = m.group(1) if m else ""
    rest = csz[:m.start()].strip() if m else csz
    # State is last 2 letters before zip
    m2 = re.search(r"\b([A-Z]{2})\s*$", rest)
    state = m2.group(1) if m2 else ""
    city  = rest[:m2.start()].strip().rstrip(",") if m2 else rest
    return city.title(), state, zip_code


def _classify_parcel(owner: str, sale_price: float,
                     owner_occ: str, homestead: str,
                     rental: str, mail_state: str) -> str:
    """Return the primary distress doc_type code."""
    # $0 or $100 → likely sheriff sale / tax deed
    if sale_price <= 100:
        return "TAX_DEED"
    # LLC / Corp
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bLTD\b|\bTRUST\b", owner.upper()):
        return "LLC_CORP"
    # Out-of-state owner
    if mail_state and mail_state.upper() not in ("OH", ""):
        return "ABSENTEE"
    # Rental non-owner-occupied
    if rental in ("Y", "1", "YES") or owner_occ in ("N", "0", "NO"):
        return "RENTAL"
    # No homestead on what should be owner-occ
    if homestead in ("N", "0", "NO", ""):
        return "NO_HOMESTEAD"
    # Below-market (under $50k — possible distress)
    if 100 < sale_price < 50_000:
        return "DISTRESS_SALE"
    # Catch-all recent transfer
    return "RECENT_SALE"


# ══════════════════════════════════════════════════════════════════════════════
# Address enrichment (for records missing mail address)
# ══════════════════════════════════════════════════════════════════════════════

_addr_cache: dict[str, Optional[dict]] = {}


def enrich_address(session: requests.Session, owner: str) -> Optional[dict]:
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
            mail_csz = str(p.get("mailingCityStateZip", "") or "")
            city, state, zp = _parse_csz(mail_csz)
            result = {
                "prop_address": str(p.get("siteAddress", "") or ""),
                "prop_city":    str(p.get("siteCity", "Columbus") or "Columbus"),
                "prop_zip":     str(p.get("siteZip", p.get("zip", "")) or ""),
                "mail_address": str(p.get("mailingAddress", p.get("mailAddress", "")) or ""),
                "mail_city":    city or str(p.get("mailCity", "") or ""),
                "mail_state":   state or str(p.get("mailState", "OH") or "OH"),
                "mail_zip":     zp or str(p.get("mailZip", "") or ""),
            }
    _addr_cache[key] = result
    time.sleep(0.1)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Scoring
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

    # Flags by doc type
    if doc_type == "TAX_DEED":      flags.append("Possible tax deed")
    if doc_type == "DISTRESS_SALE": flags.append("Below-market sale")
    if doc_type == "LLC_CORP":      flags.append("LLC / corp owner")
    if doc_type == "ABSENTEE":      flags.append("Out-of-state owner")
    if doc_type == "RENTAL":        flags.append("Non-owner-occupied")
    if doc_type == "NO_HOMESTEAD":  flags.append("No homestead exemption")
    if doc_type == "RECENT_SALE":   flags.append("Recent transfer")

    # Extra flags
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bLTD\b|\bTRUST\b", owner.upper()):
        if "LLC / corp owner" not in flags:
            flags.append("LLC / corp owner")
    if amount > 0 and amount <= 100:
        if "Possible tax deed" not in flags:
            flags.append("Possible tax deed")
    if _is_new_this_week(filed):
        flags.append("New this week")

    seen = set()
    flags = [f for f in flags if not (f in seen or seen.add(f))]

    score = 30 + 10 * len(flags)

    if doc_type == "TAX_DEED":      score += 20
    if doc_type == "DISTRESS_SALE": score += 15
    if amount > 0 and amount <= 100: score += 20
    elif 100 < amount < 50_000:      score += 10

    if "New this week" in flags:    score += 5
    if rec.get("prop_address"):     score += 5
    if rec.get("mail_address"):     score += 3

    return min(score, 100), flags


# ══════════════════════════════════════════════════════════════════════════════
# Output
# ══════════════════════════════════════════════════════════════════════════════

def _split_name(full: str) -> tuple[str, str]:
    full = full.split(";")[0].strip()
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
            "Mailing Address":  r.get("mail_address", ""),
            "Mailing City":     r.get("mail_city", ""),
            "Mailing State":    r.get("mail_state", ""),
            "Mailing Zip":      r.get("mail_zip", ""),
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
            "Source":           "Franklin County Auditor GIS",
            "Public Records URL": r.get("clerk_url", ""),
        })
    return buf.getvalue()


def save_output(records: list[dict], start_str: str, end_str: str) -> None:
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Franklin County Auditor GIS",
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
# Main
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
    log.info("Source : Franklin County Auditor GIS layer")

    session = make_session()

    # ── Fetch recent sales from GIS ────────────────────────────────────────────
    log.info("Querying GIS parcel layer for recent transfers …")
    records = fetch_gis_parcels(session, start_dt, end_dt)
    log.info("Total parcels retrieved: %d", len(records))

    # ── Enrich missing mail addresses ──────────────────────────────────────────
    missing = [r for r in records if not r.get("mail_address")]
    log.info("Enriching %d records via Auditor ByOwner API …", len(missing))
    enriched = 0
    for r in missing[:200]:   # cap at 200 API calls per run
        addr = enrich_address(session, r["owner"].split(";")[0].strip())
        if addr:
            for k, v in addr.items():
                if v and not r.get(k):
                    r[k] = v
            enriched += 1
    log.info("Addresses enriched: %d", enriched)

    # ── Score ──────────────────────────────────────────────────────────────────
    for r in records:
        r["score"], r["flags"] = compute_score_and_flags(r)

    # ── Filter — keep meaningful leads only ───────────────────────────────────
    leads = [r for r in records if r["score"] >= 35 or r["doc_type"] != "RECENT_SALE"]
    log.info("Leads after score filter: %d", len(leads))

    # ── Save ───────────────────────────────────────────────────────────────────
    save_output(leads, start_str, end_str)

    with_addr = sum(1 for r in leads if r.get("prop_address") or r.get("mail_address"))
    avg_score = sum(r["score"] for r in leads) / max(len(leads), 1)
    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log.info("Total leads   : %d", len(leads))
    log.info("With address  : %d (%.0f%%)", with_addr,
             100 * with_addr / max(len(leads), 1))
    log.info("Avg score     : %.1f / 100", avg_score)
    if leads:
        log.info("Top 5 leads:")
        for r in sorted(leads, key=lambda x: x["score"], reverse=True)[:5]:
            log.info("  [%d] %-12s  %-40s  %s",
                     r["score"], r["doc_type"], r["owner"][:40], r["filed"])


if __name__ == "__main__":
    main()
