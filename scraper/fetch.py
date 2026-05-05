#!/usr/bin/env python3
"""
Franklin County, Ohio — Motivated Seller Lead Scraper
Scrapes clerk portal for distressed-property document types,
enriches with parcel data, scores leads, and writes JSON output.
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── optional dbfread ───────────────────────────────────────────────────────────
try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

# ── logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")

# ── constants ──────────────────────────────────────────────────────────────────
CLERK_BASE        = "https://clerk.franklincountyohio.gov"
CLERK_SEARCH_URL  = f"{CLERK_BASE}/records/document-search"

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

# document codes → (category tag, friendly label)
DOC_TYPES: dict[str, tuple[str, str]] = {
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
    "LNMECH":   ("lien",             "Mechanic's Lien"),
    "LNHOA":    ("lien",             "HOA Lien"),
    "MEDLN":    ("lien",             "Medicaid Lien"),
    "PRO":      ("probate",          "Probate Document"),
    "NOC":      ("commencement",     "Notice of Commencement"),
    "RELLP":    ("release",          "Release of Lis Pendens"),
}

# Franklin County Auditor bulk data (GIS / parcel downloads)
AUDITOR_BULK_URLS = [
    "https://www.franklincountyauditor.com/gis-data/",
    "https://apps.franklincountyauditor.com/downloads/",
    "https://apps2.franklincountyauditor.com/Downloads/",
]

# output paths
REPO_ROOT   = Path(__file__).resolve().parent.parent
OUTPUT_DIRS = [REPO_ROOT / "dashboard", REPO_ROOT / "data"]


# ══════════════════════════════════════════════════════════════════════════════
# PARCEL / OWNER LOOKUP
# ══════════════════════════════════════════════════════════════════════════════

class ParcelLookup:
    """Load Franklin County parcel DBF and index by owner-name variants."""

    # possible column name sets (DBF column names vary by vintage)
    _OWNER_COLS  = ["OWNER", "OWN1", "OWNERNAME", "OWNER_NAME"]
    _SITE_COLS   = ["SITE_ADDR", "SITEADDR", "SITE_ADDRESS"]
    _SCITY_COLS  = ["SITE_CITY", "SITECITY"]
    _SZIP_COLS   = ["SITE_ZIP",  "SITEZIP"]
    _MAIL1_COLS  = ["ADDR_1", "MAILADR1", "MAIL_ADDR1", "MAIL_ADDR"]
    _MCITY_COLS  = ["CITY", "MAILCITY", "MAIL_CITY"]
    _MSTATE_COLS = ["STATE", "MAILSTATE", "MAIL_STATE"]
    _MZIP_COLS   = ["ZIP", "MAILZIP", "MAIL_ZIP"]

    def __init__(self):
        self._by_owner: dict[str, dict] = {}
        self._loaded = False

    # ── helpers ────────────────────────────────────────────────────────────────
    @staticmethod
    def _pick(row: dict, cols: list[str]) -> str:
        for c in cols:
            v = row.get(c) or row.get(c.lower()) or ""
            if v:
                return str(v).strip()
        return ""

    @staticmethod
    def _name_variants(raw: str) -> list[str]:
        """Return search variants for a raw owner string."""
        raw = raw.strip().upper()
        variants = {raw}
        # "LAST, FIRST MI" → "FIRST MI LAST"
        if "," in raw:
            parts = [p.strip() for p in raw.split(",", 1)]
            if len(parts) == 2:
                variants.add(f"{parts[1]} {parts[0]}")
                variants.add(f"{parts[0]} {parts[1]}")
        else:
            # "FIRST LAST" → "LAST FIRST"
            words = raw.split()
            if len(words) >= 2:
                variants.add(f"{words[-1]} {' '.join(words[:-1])}")
                variants.add(f"{words[-1]}, {' '.join(words[:-1])}")
        return [v for v in variants if v]

    def _index_record(self, row: dict) -> None:
        owner_raw = self._pick(row, self._OWNER_COLS)
        if not owner_raw:
            return
        parcel = {
            "prop_address": self._pick(row, self._SITE_COLS),
            "prop_city":    self._pick(row, self._SCITY_COLS) or "Columbus",
            "prop_state":   "OH",
            "prop_zip":     self._pick(row, self._SZIP_COLS),
            "mail_address": self._pick(row, self._MAIL1_COLS),
            "mail_city":    self._pick(row, self._MCITY_COLS),
            "mail_state":   self._pick(row, self._MSTATE_COLS) or "OH",
            "mail_zip":     self._pick(row, self._MZIP_COLS),
        }
        for v in self._name_variants(owner_raw):
            self._by_owner.setdefault(v, parcel)

    # ── loading ────────────────────────────────────────────────────────────────
    def _load_dbf(self, path: Path) -> int:
        if not HAS_DBF:
            log.warning("dbfread not installed — skipping parcel enrichment")
            return 0
        count = 0
        try:
            for rec in DBF(str(path), encoding="latin-1", ignore_missing_memofile=True):
                self._index_record(dict(rec))
                count += 1
        except Exception as exc:
            log.error("DBF read error %s: %s", path, exc)
        return count

    def _load_csv(self, path: Path) -> int:
        count = 0
        try:
            with open(path, newline="", encoding="utf-8-sig", errors="replace") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    self._index_record(row)
                    count += 1
        except Exception as exc:
            log.error("CSV read error %s: %s", path, exc)
        return count

    def load_file(self, path: Path) -> int:
        suffix = path.suffix.lower()
        if suffix == ".dbf":
            n = self._load_dbf(path)
        elif suffix in (".csv", ".txt"):
            n = self._load_csv(path)
        else:
            log.warning("Unknown parcel file format: %s", path)
            return 0
        log.info("Parcel file %s — loaded %d records", path.name, n)
        self._loaded = n > 0
        return n

    def load_directory(self, directory: Path) -> int:
        """Load all DBF/CSV parcel files found in *directory*."""
        total = 0
        for p in sorted(directory.glob("**/*")):
            if p.suffix.lower() in (".dbf", ".csv"):
                total += self.load_file(p)
        return total

    # ── lookup ─────────────────────────────────────────────────────────────────
    def lookup(self, owner: str) -> Optional[dict]:
        if not owner:
            return None
        for v in self._name_variants(owner.upper()):
            hit = self._by_owner.get(v)
            if hit:
                return hit
        return None

    @property
    def loaded(self) -> bool:
        return self._loaded


# ══════════════════════════════════════════════════════════════════════════════
# PARCEL DATA DOWNLOADER
# ══════════════════════════════════════════════════════════════════════════════

def _try_download_parcel_data(dest_dir: Path) -> Optional[Path]:
    """
    Attempt to locate and download Franklin County parcel bulk data.
    Returns the path of the extracted file, or None on failure.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Check if we already have a recent download (within 24 h)
    for existing in dest_dir.glob("**/*.dbf"):
        age = time.time() - existing.stat().st_mtime
        if age < 86400:
            log.info("Using cached parcel file: %s", existing)
            return existing

    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (compatible; FranklinCountyLeadScraper/1.0)"
    )

    # Candidate download links harvested from the auditor pages
    direct_links = [
        "https://www.franklincountyauditor.com/gis-data/data/parcels.zip",
        "https://www.franklincountyauditor.com/gis-data/data/parcel.zip",
        "https://apps.franklincountyauditor.com/downloads/parcels.zip",
        "https://apps2.franklincountyauditor.com/Downloads/ParcelData.zip",
        "https://apps2.franklincountyauditor.com/Downloads/Parcel.zip",
    ]

    # Also scrape the auditor GIS page for dynamic links
    for base in AUDITOR_BULK_URLS:
        try:
            r = session.get(base, timeout=20)
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.search(r"parcel|property|real.?estate", href, re.I):
                    if href.endswith((".zip", ".dbf", ".csv")):
                        full = href if href.startswith("http") else base.rstrip("/") + "/" + href.lstrip("/")
                        if full not in direct_links:
                            direct_links.insert(0, full)
        except Exception:
            pass

    for url in direct_links:
        try:
            log.info("Trying parcel download: %s", url)
            r = session.get(url, timeout=60, stream=True)
            if r.status_code != 200:
                continue
            fname = url.split("/")[-1]
            local = dest_dir / fname
            with open(local, "wb") as fh:
                for chunk in r.iter_content(65536):
                    fh.write(chunk)
            log.info("Downloaded %s (%.1f MB)", fname, local.stat().st_size / 1e6)

            if local.suffix.lower() == ".zip":
                with zipfile.ZipFile(local) as zf:
                    zf.extractall(dest_dir)
                local.unlink()
                # return first DBF or CSV found
                for p in sorted(dest_dir.glob("**/*.dbf")):
                    return p
                for p in sorted(dest_dir.glob("**/*.csv")):
                    return p
            else:
                return local
        except Exception as exc:
            log.warning("Parcel download failed for %s: %s", url, exc)

    log.warning("Could not download parcel data — addresses will be empty")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# CLERK PORTAL SCRAPER  (Playwright)
# ══════════════════════════════════════════════════════════════════════════════

async def _search_doc_type(
    page,
    doc_code: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """
    Navigate the Franklin County Clerk document-search form for *doc_code*
    between *start_date* and *end_date* (MM/DD/YYYY).
    Returns a list of raw record dicts.
    """
    records: list[dict] = []

    for attempt in range(1, 4):
        try:
            await page.goto(CLERK_SEARCH_URL, wait_until="networkidle", timeout=60_000)

            # ── fill the search form ──────────────────────────────────────────
            # Document type selector
            try:
                await page.select_option("select#DocumentType", doc_code, timeout=5_000)
            except Exception:
                # try text-based select or input
                try:
                    await page.fill("input#DocumentType", doc_code, timeout=3_000)
                except Exception:
                    pass

            # Date range
            for sel, val in [
                ("#DateFrom, #BeginDate, input[name*='StartDate'], input[name*='FromDate']", start_date),
                ("#DateTo,   #EndDate,   input[name*='EndDate'],   input[name*='ToDate']",   end_date),
            ]:
                for s in sel.split(","):
                    s = s.strip()
                    try:
                        await page.fill(s, val, timeout=2_000)
                        break
                    except Exception:
                        continue

            # Submit
            for btn_sel in ["button[type='submit']", "input[type='submit']", "#btnSearch", "#SearchButton"]:
                try:
                    await page.click(btn_sel, timeout=3_000)
                    break
                except Exception:
                    continue

            await page.wait_for_load_state("networkidle", timeout=30_000)

            # ── paginate results ─────────────────────────────────────────────
            page_num = 1
            while True:
                html = await page.content()
                soup = BeautifulSoup(html, "lxml")
                batch = _parse_results_table(soup, doc_code)
                records.extend(batch)
                log.debug("  %s page %d → %d rows", doc_code, page_num, len(batch))

                # next page
                next_btn = soup.find("a", string=re.compile(r"next|>", re.I))
                if not next_btn:
                    break
                try:
                    await page.click("a:has-text('Next'), a:has-text('>')", timeout=5_000)
                    await page.wait_for_load_state("networkidle", timeout=20_000)
                    page_num += 1
                except Exception:
                    break

            break  # success

        except PWTimeout:
            log.warning("Timeout on %s attempt %d", doc_code, attempt)
            if attempt == 3:
                log.error("Giving up on %s", doc_code)
        except Exception as exc:
            log.warning("Error on %s attempt %d: %s", doc_code, attempt, exc)
            if attempt == 3:
                log.error("Giving up on %s", doc_code)

    return records


def _parse_results_table(soup: BeautifulSoup, doc_code: str) -> list[dict]:
    """Extract records from search-results HTML."""
    rows = []

    # Look for the main results table
    tables = soup.find_all("table")
    result_table = None
    for t in tables:
        headers = [th.get_text(strip=True).lower() for th in t.find_all("th")]
        if any(k in " ".join(headers) for k in ("document", "grantor", "filed", "book")):
            result_table = t
            break

    if not result_table:
        # Try grid/div layout
        for row_div in soup.select(".result-row, .search-result, tr[data-docnum]"):
            rec = _parse_row_div(row_div, doc_code)
            if rec:
                rows.append(rec)
        return rows

    headers = [th.get_text(strip=True).lower() for th in result_table.find_all("th")]

    for tr in result_table.find_all("tr")[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not cells:
            continue

        # Map cells to headers
        cell_map: dict[str, str] = {}
        for i, h in enumerate(headers):
            if i < len(cells):
                cell_map[h] = cells[i]

        # Extract link
        link = ""
        a_tag = tr.find("a", href=True)
        if a_tag:
            href = a_tag["href"]
            link = href if href.startswith("http") else CLERK_BASE + href

        rec = _build_record(cell_map, cells, doc_code, link)
        if rec:
            rows.append(rec)

    return rows


def _parse_row_div(div, doc_code: str) -> Optional[dict]:
    text = div.get_text(" ", strip=True)
    link = ""
    a = div.find("a", href=True)
    if a:
        href = a["href"]
        link = href if href.startswith("http") else CLERK_BASE + href
    if not text:
        return None
    return {
        "doc_num":  div.get("data-docnum", ""),
        "doc_type": doc_code,
        "filed":    "",
        "grantor":  text[:80],
        "grantee":  "",
        "legal":    "",
        "amount":   "",
        "clerk_url": link,
    }


def _build_record(cell_map: dict, cells: list, doc_code: str, link: str) -> Optional[dict]:
    def g(*keys):
        for k in keys:
            for ck, cv in cell_map.items():
                if k in ck:
                    return cv
        return ""

    doc_num  = g("document number", "doc #", "doc num", "instrument")
    filed    = g("filed", "date filed", "recorded", "file date")
    grantor  = g("grantor", "owner", "seller")
    grantee  = g("grantee", "buyer")
    legal    = g("legal", "description", "property")
    amount   = g("amount", "consideration", "$")

    if not doc_num and cells:
        doc_num = cells[0]

    return {
        "doc_num":   doc_num,
        "doc_type":  doc_code,
        "filed":     filed,
        "grantor":   grantor,
        "grantee":   grantee,
        "legal":     legal,
        "amount":    amount,
        "clerk_url": link,
    }


async def scrape_clerk(start_date: str, end_date: str) -> list[dict]:
    """Run Playwright scraper across all document types."""
    all_records: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        for code in DOC_TYPES:
            log.info("Searching doc type: %s", code)
            recs = await _search_doc_type(page, code, start_date, end_date)
            log.info("  → %d records", len(recs))
            all_records.extend(recs)

        await browser.close()

    return all_records


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
        d = datetime.strptime(filed.strip(), "%m/%d/%Y")
        return (datetime.now() - d).days <= 7
    except Exception:
        try:
            d = datetime.fromisoformat(filed.strip())
            return (datetime.now() - d).days <= 7
        except Exception:
            return False


def compute_score_and_flags(rec: dict) -> tuple[int, list[str]]:
    flags: list[str] = []
    score = 30  # base

    cat = rec.get("cat", "")
    doc_type = rec.get("doc_type", "")
    amount = _parse_amount(rec.get("amount", ""))
    owner = rec.get("owner", "")
    filed = rec.get("filed", "")

    # Category flags
    if doc_type in ("LP", "NOFC"):
        flags.append("Lis pendens" if doc_type == "LP" else "Pre-foreclosure")
    if cat == "pre_foreclosure":
        flags.append("Pre-foreclosure")
    if cat == "judgment":
        flags.append("Judgment lien")
    if cat == "tax_lien":
        flags.append("Tax lien")
    if cat == "lien" and doc_type == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "probate":
        flags.append("Probate / estate")
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bLTD\b|\bCO\b", owner.upper()):
        flags.append("LLC / corp owner")
    if _is_new_this_week(filed):
        flags.append("New this week")

    # Remove duplicates, preserve order
    seen: set[str] = set()
    unique_flags = []
    for f in flags:
        if f not in seen:
            seen.add(f)
            unique_flags.append(f)
    flags = unique_flags

    # Scoring bonuses
    score += 10 * len(flags)

    # LP + foreclosure combo
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20

    # Amount bonuses
    if amount > 100_000:
        score += 15
    elif amount > 50_000:
        score += 10

    # New this week
    if "New this week" in flags:
        score += 5

    # Has address
    if rec.get("prop_address") or rec.get("mail_address"):
        score += 5

    return min(score, 100), flags


# ══════════════════════════════════════════════════════════════════════════════
# RECORD ENRICHMENT + NORMALIZATION
# ══════════════════════════════════════════════════════════════════════════════

def enrich_record(raw: dict, parcel: ParcelLookup) -> dict:
    doc_code = raw.get("doc_type", "")
    cat, cat_label = DOC_TYPES.get(doc_code, ("other", doc_code))
    owner = raw.get("grantor", "")

    # Parcel enrichment
    p = parcel.lookup(owner) or {}

    rec: dict = {
        "doc_num":      raw.get("doc_num", ""),
        "doc_type":     doc_code,
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


# ══════════════════════════════════════════════════════════════════════════════
# GHL CSV EXPORT
# ══════════════════════════════════════════════════════════════════════════════

def _split_name(full: str) -> tuple[str, str]:
    """Best-effort split of 'FIRST LAST' or 'LAST, FIRST'."""
    full = full.strip()
    if "," in full:
        parts = [p.strip() for p in full.split(",", 1)]
        return parts[1], parts[0]  # first, last
    parts = full.split()
    if len(parts) >= 2:
        return parts[0].title(), " ".join(parts[1:]).title()
    return full.title(), ""


def build_ghl_csv(records: list[dict]) -> str:
    fieldnames = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()

    for r in records:
        first, last = _split_name(r.get("owner", ""))
        writer.writerow({
            "First Name":             first,
            "Last Name":              last,
            "Mailing Address":        r.get("mail_address", ""),
            "Mailing City":           r.get("mail_city", ""),
            "Mailing State":          r.get("mail_state", ""),
            "Mailing Zip":            r.get("mail_zip", ""),
            "Property Address":       r.get("prop_address", ""),
            "Property City":          r.get("prop_city", ""),
            "Property State":         r.get("prop_state", ""),
            "Property Zip":           r.get("prop_zip", ""),
            "Lead Type":              r.get("cat_label", ""),
            "Document Type":          r.get("doc_type", ""),
            "Date Filed":             r.get("filed", ""),
            "Document Number":        r.get("doc_num", ""),
            "Amount/Debt Owed":       r.get("amount", ""),
            "Seller Score":           r.get("score", 0),
            "Motivated Seller Flags": "; ".join(r.get("flags", [])),
            "Source":                 "Franklin County Clerk of Courts",
            "Public Records URL":     r.get("clerk_url", ""),
        })

    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════════════
# OUTPUT
# ══════════════════════════════════════════════════════════════════════════════

def save_output(records: list[dict], start_date: str, end_date: str) -> None:
    payload = {
        "fetched_at":    datetime.utcnow().isoformat() + "Z",
        "source":        "Franklin County Clerk of Courts",
        "date_range":    {"from": start_date, "to": end_date},
        "total":         len(records),
        "with_address":  sum(1 for r in records if r.get("prop_address") or r.get("mail_address")),
        "records":       sorted(records, key=lambda r: r.get("score", 0), reverse=True),
    }

    for d in OUTPUT_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        json_path = d / "records.json"
        with open(json_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
        log.info("Saved %d records → %s", len(records), json_path)

        csv_path = d / "leads_ghl.csv"
        with open(csv_path, "w", encoding="utf-8", newline="") as fh:
            fh.write(build_ghl_csv(records))
        log.info("GHL CSV → %s", csv_path)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    log.info("╔══════════════════════════════════════════════╗")
    log.info("║  Franklin County Motivated Seller Scraper   ║")
    log.info("╚══════════════════════════════════════════════╝")
    log.info("Date range: %s → %s (%d days)", start_str, end_str, LOOKBACK_DAYS)

    # ── 1. Load parcel data ────────────────────────────────────────────────────
    parcel = ParcelLookup()
    parcel_dir = REPO_ROOT / "data" / "parcels"
    parcel_file = _try_download_parcel_data(parcel_dir)
    if parcel_file:
        parcel.load_file(parcel_file)
    else:
        parcel.load_directory(parcel_dir)

    if parcel.loaded:
        log.info("Parcel lookup ready")
    else:
        log.warning("Parcel lookup unavailable — addresses will be empty")

    # ── 2. Scrape clerk portal ─────────────────────────────────────────────────
    log.info("Starting Playwright clerk scrape …")
    raw_records = await scrape_clerk(start_str, end_str)
    log.info("Raw records scraped: %d", len(raw_records))

    # ── 3. Enrich + deduplicate ────────────────────────────────────────────────
    seen_doc_nums: set[str] = set()
    enriched: list[dict] = []
    for raw in raw_records:
        try:
            rec = enrich_record(raw, parcel)
            key = (rec["doc_num"], rec["doc_type"])
            if key in seen_doc_nums or not rec["doc_num"]:
                continue
            seen_doc_nums.add(key)
            enriched.append(rec)
        except Exception as exc:
            log.warning("Skipping bad record %s: %s", raw, exc)

    log.info("Enriched unique records: %d", len(enriched))

    # ── 4. Save output ─────────────────────────────────────────────────────────
    save_output(enriched, start_str, end_str)

    # ── Summary ────────────────────────────────────────────────────────────────
    with_addr = sum(1 for r in enriched if r.get("prop_address") or r.get("mail_address"))
    avg_score = (sum(r["score"] for r in enriched) / len(enriched)) if enriched else 0
    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log.info("Total leads   : %d", len(enriched))
    log.info("With address  : %d (%.0f%%)", with_addr, 100 * with_addr / max(len(enriched), 1))
    log.info("Avg score     : %.1f / 100", avg_score)
    log.info("Top 5 leads:")
    for r in sorted(enriched, key=lambda x: x["score"], reverse=True)[:5]:
        log.info("  [%d] %s  %s  %s", r["score"], r["doc_type"], r["owner"][:40], r["filed"])


if __name__ == "__main__":
    asyncio.run(main())
