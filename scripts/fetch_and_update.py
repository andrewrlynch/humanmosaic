#!/usr/bin/env python3
"""
fetch_and_update.py — Daily feed fetch and Google Sheets updater for The Human Mosaic.

Creates the Google Sheet on first run, appends new keyword-matched papers daily.
Deduplicates by DOI (exact) or normalized title.

Usage:
    python scripts/fetch_and_update.py

Environment variables required:
    GOOGLE_SERVICE_ACCOUNT_JSON  — contents of service account JSON key file
    GOOGLE_SHEET_ID              — sheet ID (written to $GITHUB_ENV on first run)

Optional:
    DAYS                         — lookback window in days (default: 2, overlap for safety)
"""

import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path
from email.utils import parsedate_to_datetime

# Add scripts/ to path so we can import from somatic_digest
sys.path.insert(0, str(Path(__file__).parent))

import requests
import feedparser

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    print("ERROR: pip install gspread google-auth", file=sys.stderr)
    sys.exit(1)

# ── Constants ──────────────────────────────────────────────────────────────────

SHEET_NAME   = "The Human Mosaic — Paper Feed"
WORKSHEET    = "Papers"
DAYS         = int(os.environ.get("DAYS", "2"))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

COLUMNS = [
    "key", "title", "authors", "source", "date", "doi", "url",
    "type", "section", "tags", "abstract", "fetched_at",
]

# ── Feeds ──────────────────────────────────────────────────────────────────────

RSS_FEEDS = [
    # Preprint servers
    ("bioRxiv Cancer Biology",              "https://connect.biorxiv.org/biorxiv_xml.php?subject=cancer_biology",         "preprint"),
    ("bioRxiv Genomics",                    "https://connect.biorxiv.org/biorxiv_xml.php?subject=genomics",               "preprint"),
    ("bioRxiv Evolutionary Bio",            "https://connect.biorxiv.org/biorxiv_xml.php?subject=evolutionary_biology",   "preprint"),
    ("medRxiv Oncology",                    "https://connect.medrxiv.org/medrxiv_xml.php?subject=oncology",               "preprint"),
    ("medRxiv Pathology",                   "https://connect.medrxiv.org/medrxiv_xml.php?subject=pathology",              "preprint"),
    ("arXiv q-bio.GN",                      "https://arxiv.org/rss/q-bio.GN",                                             "preprint"),
    ("arXiv q-bio.PE",                      "https://arxiv.org/rss/q-bio.PE",                                             "preprint"),
    ("arXiv q-bio.QM",                      "https://arxiv.org/rss/q-bio.QM",                                             "preprint"),
    ("arXiv q-bio.CB",                      "https://arxiv.org/rss/q-bio.CB",                                             "preprint"),
    ("arXiv cs.LG",                         "https://arxiv.org/rss/cs.LG",                                                "preprint"),
    ("eLife",                               "https://elifesciences.org/rss/recent.xml",                                   "preprint"), 

    # CSHL Press
    ("Genome Research",                     "https://genome.cshlp.org/rss/current.xml",                                   "journal"),
    ("Genes & Development",                 "https://genesdev.cshlp.org/rss/current.xml",                                 "journal"),
    ("Cold Spring Harb Perspect Biol",      "https://cshperspectives.cshlp.org/rss/current.xml",                          "journal"),

    # BMJ
    ("Gut",                                 "https://gut.bmj.com/rss/current.xml",                                        "journal"),
    ("BMJ Clinical Genetics & Genomics",    "https://jmg.bmj.com/rss/current.xml",                                        "journal"),
    ("BMJ Clinical Pathology",              "https://jcp.bmj.com/rss/current.xml",                                        "journal"),
 
    # AACR
    ("Cancer Discovery",                    "https://aacrjournals.org/rss/site_1000003/1000004.xml",                      "journal"),
    ("Cancer Research",                     "https://aacrjournals.org/rss/site_1000011/1000008.xml",                      "journal"),
    ("Clinical Cancer Research",            "https://aacrjournals.org/rss/site_1000013/1000009.xml",                      "journal"),
    ("Cancer Prevention Research",          "https://aacrjournals.org/rss/site_1000009/1000007.xml",                      "journal"),
    ("Molecular Cancer Research",           "https://aacrjournals.org/rss/site_1000015/1000010.xml",                      "journal"),
 
    # Cell Press
    ("Cell",                                "https://www.cell.com/cell/inpress.rss",                                      "journal"),
    ("Cancer Cell",                         "https://www.cell.com/cancer-cell/inpress.rss",                               "journal"),
    ("Cell Genomics",                       "https://www.cell.com/cell-genomics/inpress.rss",                             "journal"),
    ("Cell Cycle",                          "https://www.tandfonline.com/feed/rss/kccy20",  "journal"),
    ("Molecular Cell",                      "https://www.cell.com/molecular-cell/inpress.rss",                            "journal"),
    ("Cell Reports",                        "https://www.cell.com/cell-reports/inpress.rss",                              "journal"),
    ("Cell Reports Medicine",               "https://www.cell.com/cell-reports-medicine/inpress.rss",                     "journal"),
    ("Developmental Cell",                  "https://www.cell.com/developmental-cell/inpress.rss",                        "journal"),
    ("American Journal of Human Genetics",  "https://www.cell.com/ajhg/inpress.rss",                                      "journal"),
    ("iScience",                            "https://www.cell.com/iscience/inpress.rss",                                  "journal"),
    ("Current Opinion in Genetics",         "https://www.cell.com/current-opinion-genetics-development/inpress.rss",      "journal"),
    ("Trends in Genetics",                  "https://www.cell.com/trends/genetics/inpress.rss",                           "journal"),
    ("Trends in Cancer",                    "https://www.cell.com/trends/cancer/inpress.rss",                             "journal"),
    ("Trends in Cell Biology",              "https://www.cell.com/trends/cell-biology/inpress.rss",                       "journal"),
 
    # Nature portfolio
    ("Nature",                    "https://www.nature.com/nature.rss",                                         "journal"),
    ("Nature Genetics",           "https://www.nature.com/ng.rss",                                             "journal"),
    ("Nature Cancer",             "https://www.nature.com/natcancer.rss",                                      "journal"),
    ("Nature Methods",            "https://www.nature.com/nmeth.rss",                                          "journal"),
    ("Nature Communications",     "https://www.nature.com/ncomms.rss",                                         "journal"),
    ("Nature Ecology & Evolution","https://www.nature.com/natecolevol.rss",                                    "journal"),
    ("Nature Medicine",           "https://www.nature.com/nm.rss",                                             "journal"),
    ("Nature Reviews Cancer",     "https://www.nature.com/nrc.rss",                                            "journal"),
    ("Nature Reviews Genetics",   "https://www.nature.com/nrg.rss",                                            "journal"),
    ("Blood Cancer Journal",      "https://www.nature.com/bcj.rss",                                            "journal"),
    ("Leukemia",                  "https://www.nature.com/leu.rss",                                            "journal"),
    ("Oncogene",                  "https://www.nature.com/onc.rss",                                            "journal"),
    ("Scientific Reports",        "https://www.nature.com/srep.rss",                                            "journal"),
    ("Nature Reviews Molecular Cell Bio", "https://www.nature.com/nrm.rss",       "journal"),
    ("Nature Reviews Disease Primers", "https://www.nature.com/nrdp.rss",         "journal"),

    # Annual Reviews
    ("Annual Review of Genetics",      "https://www.annualreviews.org/rss/content/journals/genet/latestarticles", "journal"),
    ("Annual Review of Genomics",      "https://www.annualreviews.org/rss/content/journals/genom/latestarticles", "journal"),
    ("Annual Review of Cancer Biology","https://www.annualreviews.org/rss/content/journals/cancerbio/latestarticles", "journal"),
    ("Annual Review of Cell Dev Biol", "https://www.annualreviews.org/rss/content/journals/cellbio/latestarticles", "journal"),
    ("Annual Review of Medicine",      "https://www.annualreviews.org/rss/content/journals/med/latestarticles",   "journal"),

 
    # Oxford
    ("Bioinformatics",            "https://academic.oup.com/rss/site_5127/advanceAccess_3122.xml",             "journal"),
    ("Briefings in Bioinformatics","https://academic.oup.com/rss/site_5374/advanceAccess_3707.xml",            "journal"),
    ("Nucleic Acids Research",    "https://academic.oup.com/rss/site_5127/advanceAccess_3120.xml",             "journal"),
 
    # PLOS
    ("PLOS Biology",              "https://journals.plos.org/plosbiology/feed/atom",                           "journal"),
    ("PLOS Computational Biology","https://journals.plos.org/ploscompbiol/feed/atom",                          "journal"),
    ("PLOS Genetics",             "https://journals.plos.org/plosgenetics/feed/atom",                          "journal"),
 
    # NEJM / Lancet
    ("NEJM",                      "https://www.nejm.org/action/showFeed?jc=nejm&type=etoc&feed=rss",           "journal"),
    ("Lancet Oncology",           "https://www.thelancet.com/rssfeed/lanonc_current.xml",                      "journal"),
    ("Lancet",                    "https://www.thelancet.com/rssfeed/lancet_current.xml",                       "journal"),
 
    # Science journals (AAAS)
    ("Science",                   "https://www.science.org/action/showFeed?type=etoc&feed=rss&jc=science",     "journal"),
    ("Science Translational Medicine", "https://www.science.org/action/showFeed?type=etoc&feed=rss&jc=stm",    "journal"),
    ("Science Advances",          "https://www.science.org/action/showFeed?type=etoc&feed=rss&jc=sciadv",      "journal"),
    ("Science Immunology",        "https://www.science.org/action/showFeed?type=etoc&feed=rss&jc=sciimmunol",  "journal"),
    ("Science Signaling",         "https://www.science.org/action/showFeed?type=etoc&feed=rss&jc=scisignal",   "journal"),
 
    # ASCB
    ("Molecular Biology of the Cell", "https://www.molbiolcell.org/action/showFeed?type=etoc&feed=rss&jc=mboc", "journal"),

    # Other
    ("Seminars in Cancer Biology",     "https://rss.sciencedirect.com/publication/science/10462804",            "journal"),
    ("Genome Biology",            "https://genomebiology.biomedcentral.com/articles/most-recent/rss.xml",      "journal"),
    ("Journal of Cell Biology",            "https://rupress.org/rss/site_1000001/1000003.xml",                 "journal")
 ]

# ── Keywords ───────────────────────────────────────────────────────────────────

TIER1 = [
    "tumor evolution", "cancer evolution", "somatic evolution", "clonal evolution", "subclonal evolution", "cancer progression", "field cancerization",
    "chromosomal instability", "genomic instability", "genetic instability", "phenotype plasticity", "phenotypic plasticity", "therapeutic resistance", "somatic mosaicism", "clonal hematopoiesis"
]

TIER2 = [
    # Mutation terms
    "somatic mutation", "mutational signature", "mutation signature", "somatic hypermutation", "APOBEC", "MMR", "mismatch repair", "HRD", "homologous recombination deficiency", "mutagenic exposure", "base editing", "DNA repair pathway",
    # ITH terms
    "tumor heterogeneity", "intratumor heterogeneity", 
    # Mitosis and chromosomal instability terms
    "abnormal mitosis", "mitotic fidelity", "chromosome missegregation", "micronucleus", "lagging chromosome", "anaphase bridge", "centrosome amplification", "spindle assembly checkpoint", "cytokinesis failure", "replication stress", "DNA damage", "genome instability", "telomere dysfunction", "telomere catastrophe", "whole genome doubling", "WGD", 
    # Methods terms
    "cancer genome", "whole genome sequencing", "wgs", "single-cell sequencing", "scRNA-seq", "spatial transcriptomics", "ATAC-seq", "ChIP-seq", "lineage tracing", "barcoding", "phylogenetics", "computational modeling", "machine learning", "deep learning", "clustering", "dimensionality reduction",
    # Simple variant terms
    "copy number", "copy number variation", "copy number alteration", "CNA", "aneuploidy", "amplification", "structural variation", "structural variant", "SV", "genomic deletion", "inversion", "translocation", "rearrangement",
    # Complex variant or cluster mutagenesis terms
    "ecDNA", "extrachromosomal", "double minute", "chromothripsis", "chromoplexy", "kataegis", "BFB", "breakage-fusion-bridge", "breakage fusion bridge",
    # Evolution terms
    "cancer phylogeny", "tumor phylogenetics", "phylogenomics", "timing", "ccf", "cancer cell fraction", "clonal dynamics", "subclonal dynamics", "evolutionary trajectory", "evolutionary trajectories",
    # Human genetic diversity
    "population genomics", "human genetic diversity", "pangenome", "polygenic risk", "ancestry", "admixture", "rare variant", "germline structural variation", "gwas", "genome-wide association",
    # Plasticity terms
    "reprogramming", "transdifferentiation", "cell state", "stromal remodeling", "tumor microenvironment", "cancer-associated fibroblast", "neural remodeling", "perineural", "axonogenesis", "neuroendocrine", "epithelial-mesenchymal transition", "EMT", "neuroplasticity", "cell identity", "fate determination", "lineage plasticity", "differentiation", "de-differentiation", "dedifferentiation", "wound healing",
    # Specific cancer terms
    "sarcoma", "lung cancer", "breast cancer", "PDAC", "pancreatic cancer", "colorectal cancer", "glioblastoma", "leukemia", "lymphoma", "melanoma", "prostate cancer", "ovarian cancer", "bladder cancer", "esophageal cancer", "gastric cancer", "liver cancer", "hepatocellular carcinoma", "cancer of unknown primary", "CUP", "metastatic cancer", "advanced cancer", "therapy-resistant cancer"
]

SECTIONS = [
    ("Cancer Evolution",     ["tumor evolution","cancer evolution","clonal evolution","subclonal","tumor heterogeneity","ecDNA","whole genome doubling"]),
    ("Somatic Evolution",    ["somatic evolution","somatic mosaicism","clonal hematopoiesis","aging","non-cancer clonal"]),
    ("Genome Instability",   ["chromosomal instability","genomic instability","chromothripsis","aneuploidy","structural variation","replication stress","DNA damage","breakage fusion bridge","telomere"]),
    ("Mutational Processes", ["mutational signature","APOBEC","HRD","mismatch repair","kataegis","DNA repair"]),
    ("Plasticity & Epigenetics", ["phenotypic plasticity","epigenetic","cell state","dedifferentiation","lineage plasticity","EMT","transdifferentiation"]),
    ("Human Diversity",      ["population genomics","gwas","pangenome","polygenic risk","ancestry","admixture","rare variant"]),
]

# ── Utilities ──────────────────────────────────────────────────────────────────
 
def strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&#[0-9]+;", "", text)
    return " ".join(text.split())
 
 
def clean_doi(doi: str) -> str:
    if not doi:
        return ""
    return doi.split("?")[0].split("#")[0].rstrip("./,;").strip()
 
 
def title_key(title: str) -> str:
    t = (title or "").lower()
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()
 
 
def paper_key(title: str, doi: str) -> str:
    if doi:
        return f"doi:{doi.strip()}"
    return f"title:{title_key(title)[:80]}"
 
 
def parse_date(entry) -> datetime | None:
    for field in ("published", "updated"):
        val = entry.get(f"{field}_parsed")
        if val:
            try:
                return datetime(*val[:6], tzinfo=timezone.utc)
            except Exception:
                pass
        raw = entry.get(field, "")
        if raw:
            try:
                return parsedate_to_datetime(raw).replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return None
 
 
def keyword_passes(text: str) -> bool:
    t = text.lower()
    if any(k.lower() in t for k in TIER1):
        return True
    return sum(1 for k in TIER2 if k.lower() in t) >= 2
 
 
def classify_section(text: str) -> str:
    t = text.lower()
    for section, keywords in SECTIONS:
        if any(k.lower() in t for k in keywords):
            return section
    return "Other"
 
 
def extract_tags(text: str) -> str:
    t = text.lower()
    all_kw = TIER1 + TIER2
    hits = [k for k in all_kw if k.lower() in t]
    unique = list(dict.fromkeys(hits))
    return ", ".join(unique[:5])
 
 
def extract_authors(entry) -> str:
    authors_list = entry.get("authors", [])
    if authors_list and isinstance(authors_list, list):
        names = [a.get("name", "").strip() for a in authors_list if a.get("name")]
        if names:
            suffix = " et al." if len(names) > 3 else ""
            return ", ".join(names[:3]) + suffix
    author_str = entry.get("author", "") or entry.get("dc_creator", "")
    if author_str:
        parts = [a.strip() for a in re.split(r"[;]", author_str) if a.strip()]
        if len(parts) > 1:
            suffix = " et al." if len(parts) > 3 else ""
            return ", ".join(parts[:3]) + suffix
        return author_str.strip()
    return ""
 
# ── Fetch ──────────────────────────────────────────────────────────────────────
 
def fetch_rss(days: int) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    papers = []
    seen_keys: set[str] = set()
 
    for label, url, feed_type in RSS_FEEDS:
        try:
            r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
            feed = feedparser.parse(r.content)
            count = 0
            for entry in feed.entries:
                pub_date = parse_date(entry)
                if pub_date and pub_date < cutoff:
                    continue
 
                link = entry.get("link", "")
                title = strip_html(entry.get("title", "")).strip()
                abstract = strip_html(entry.get("summary", "")).strip()
 
                if not title or len(title) < 10:
                    continue
 
                # DOI extraction
                doi = ""
                for tag in entry.get("tags", []):
                    t = tag.get("term", "")
                    if t.startswith("10."):
                        doi = clean_doi(t)
                        break
                if not doi and entry.get("prism_doi"):
                    doi = clean_doi(entry.get("prism_doi"))
                if not doi:
                    m = re.search(r"10\.\d{4,}/[^\s?#]+", link)
                    if m:
                        doi = clean_doi(m.group(0))
 
                key = paper_key(title, doi)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
 
                combined = title + " " + abstract
                if not keyword_passes(combined):
                    continue
 
                papers.append({
                    "key":        key,
                    "title":      title,
                    "authors":    extract_authors(entry),
                    "source":     label,
                    "date":       pub_date.strftime("%Y-%m-%d") if pub_date else "",
                    "doi":        doi,
                    "url":        link,
                    "type":       feed_type,
                    "section":    classify_section(combined),
                    "tags":       extract_tags(combined),
                    "abstract":   abstract[:600],
                    "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                })
                count += 1
            print(f"  [{label}] {count} papers")
        except Exception as e:
            print(f"  [{label}] FAILED: {e}", file=sys.stderr)
        time.sleep(0.3)
 
    return papers
 
 
def fetch_biorxiv_api(days: int) -> list[dict]:
    end = datetime.now()
    start = end - timedelta(days=days)
    date_str = f"{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}"
    papers = []
    seen: set[str] = set()
    for server in ["biorxiv", "medrxiv"]:
        try:
            r = requests.get(
                f"https://api.biorxiv.org/details/{server}/{date_str}/0/json",
                timeout=30
            )
            r.raise_for_status()
            for p in r.json().get("collection", []):
                doi = p.get("doi", "")
                if doi in seen:
                    continue
                seen.add(doi)
                title    = p.get("title", "")
                abstract = p.get("abstract", "")[:600]
                combined = title + " " + abstract
                if not keyword_passes(combined):
                    continue
                papers.append({
                    "key":        paper_key(title, doi),
                    "title":      title,
                    "authors":    p.get("authors", ""),
                    "source":     f"bioRxiv API ({p.get('category','')})",
                    "date":       p.get("date", ""),
                    "doi":        doi,
                    "url":        f"https://doi.org/{doi}",
                    "type":       "preprint",
                    "section":    classify_section(combined),
                    "tags":       extract_tags(combined),
                    "abstract":   abstract,
                    "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                })
        except Exception as e:
            print(f"  bioRxiv API ({server}) failed: {e}", file=sys.stderr)
    print(f"  [bioRxiv API] {len(papers)} papers")
    return papers
 
# ── Google Sheets ──────────────────────────────────────────────────────────────
 
def get_client() -> gspread.Client:
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_json:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON not set")
    info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)
 
 
def get_or_create_sheet(client: gspread.Client, sheet_id: str | None) -> tuple[gspread.Spreadsheet, bool]:
    created = False
    if sheet_id:
        try:
            sh = client.open_by_key(sheet_id)
            print(f"  Opened existing sheet: {sh.title} ({sheet_id})")
            return sh, created
        except Exception:
            print(f"  Sheet {sheet_id} not found, creating new one...")
 
    sh = client.create(SHEET_NAME)
    sh.share(None, perm_type="anyone", role="reader")
    created = True
    print(f"  Created sheet: {sh.title} (ID: {sh.id})")
    print(f"  *** Set GOOGLE_SHEET_ID={sh.id} in your GitHub repo secrets ***")
 
    # Write to GITHUB_ENV if running in Actions
    github_env = os.environ.get("GITHUB_ENV")
    if github_env:
        with open(github_env, "a") as f:
            f.write(f"GOOGLE_SHEET_ID={sh.id}\n")
        print(f"  Written GOOGLE_SHEET_ID to GITHUB_ENV")
 
    return sh, created
 
 
def ensure_worksheet(sh: gspread.Spreadsheet, created: bool) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(WORKSHEET)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(WORKSHEET, rows=10000, cols=len(COLUMNS))
 
    # Write header if sheet is empty or newly created
    if created or ws.row_count == 0 or not ws.row_values(1):
        ws.clear()
        ws.append_row(COLUMNS, value_input_option="RAW")
        # Bold + freeze header row
        ws.format("1:1", {"textFormat": {"bold": True}})
        sh.batch_update({"requests": [{
            "updateSheetProperties": {
                "properties": {"sheetId": ws.id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount"
            }
        }]})
        print(f"  Header row written")
 
    return ws
 
 
def load_existing_keys(ws: gspread.Worksheet) -> set[str]:
    try:
        keys = ws.col_values(1)
        return set(keys[1:])  # skip header
    except Exception:
        return set()
 
 
def append_papers(ws: gspread.Worksheet, papers: list[dict], existing_keys: set[str]) -> int:
    new_rows = []
    for p in papers:
        if p["key"] in existing_keys:
            continue
        row = [p.get(col, "") for col in COLUMNS]
        new_rows.append(row)
        existing_keys.add(p["key"])
 
    if new_rows:
        ws.append_rows(new_rows, value_input_option="RAW")
        print(f"  Appended {len(new_rows)} new rows")
    else:
        print(f"  No new papers to append")
 
    return len(new_rows)
 
 
def write_sheet_id_to_docs(sheet_id: str) -> None:
    """Write the sheet ID to docs/sheet_id.txt for the dashboard to read."""
    docs_dir = Path(__file__).parent.parent / "docs"
    docs_dir.mkdir(exist_ok=True)
    (docs_dir / "sheet_id.txt").write_text(sheet_id)
 
 
 
# ── GitHub Models classification ───────────────────────────────────────────────
 
CLASSIFY_SYSTEM = """You are classifying genomics papers for The Human Mosaic newsletter.
 
Sections:
- "Cancer Evolution": tumor phylogenetics, subclonal dynamics, clonal selection, WGD, ecDNA, sarcoma, tumor heterogeneity
- "Somatic Evolution": clonal hematopoiesis, somatic mosaicism, aging, non-cancer clonal expansions, developmental mosaicism
- "Genome Instability": CIN, structural variation, chromothripsis, BFB, replication stress, DNA damage, aneuploidy, rearrangements
- "Mutational Processes": mutational signatures, APOBEC, MMR, HRD, mutagenic exposures, repair pathways
- "Plasticity & Epigenetics": cell state transitions, epigenetic reprogramming, lineage plasticity, dedifferentiation, EMT, chromatin remodeling
- "Human Genetic Diversity": population genomics, GWAS, pangenome, polygenic risk, ancestry, admixture, rare variants
- "Other": anything that doesn't clearly fit the above
 
Return ONLY a JSON array — no prose, no fences. One object per paper:
{"idx": number, "section": string, "tags": [2-5 specific biological terms from the abstract]}"""
 
 
def classify_papers_github(papers: list[dict], token: str, batch_size: int = 25) -> list[dict]:
    """
    Classify paper sections and tags using GitHub Models (GPT-4o mini).
    Falls back to keyword classification if the API call fails.
    Uses only stdlib urllib — no extra dependencies.
    """
    if not token:
        print("  No GITHUB_TOKEN — skipping AI classification, using keyword fallback")
        return papers
 
    endpoint = "https://models.github.ai/inference/chat/completions"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2026-03-10",
    }
 
    batches = [papers[i:i+batch_size] for i in range(0, len(papers), batch_size)]
    print(f"  Classifying {len(papers)} papers in {len(batches)} batch(es) via GitHub Models...")
 
    for b_idx, batch in enumerate(batches, 1):
        numbered = "\n\n".join(
            f"{j+1}. {p['title']}\n{(p.get('abstract') or '')[:400]}"
            for j, p in enumerate(batch)
        )
        body = json.dumps({
            "model": "openai/gpt-4o-mini",
            "messages": [
                {"role": "system", "content": CLASSIFY_SYSTEM},
                {"role": "user",   "content": f"Classify these {len(batch)} papers:\n\n{numbered}"},
            ],
            "max_tokens": 4096,
            "temperature": 0,
        }).encode()
 
        try:
            req = urllib.request.Request(endpoint, data=body, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=40) as resp:
                result = json.loads(resp.read())
 
            raw = result["choices"][0]["message"]["content"]
            match = re.search(r'\[[\s\S]*\]', raw)
            if not match:
                print(f"  Batch {b_idx}: no JSON array found, skipping", file=sys.stderr)
                continue
 
            json_str = match.group(0)
            try:
                scored = json.loads(json_str)
            except json.JSONDecodeError:
                cut = json_str.rfind("},")
                if cut == -1:
                    cut = json_str.rfind("}")
                if cut > 0:
                    try:
                        scored = json.loads(json_str[:cut+1] + "]")
                        print(f"  Batch {b_idx}: salvaged {len(scored)} entries from truncated JSON")
                    except json.JSONDecodeError:
                        print(f"  Batch {b_idx}: JSON unrecoverable, skipping", file=sys.stderr)
                        continue
                else:
                    print(f"  Batch {b_idx}: JSON unrecoverable, skipping", file=sys.stderr)
                    continue
            idx_map = {int(e["idx"]): e for e in scored if "idx" in e}
 
            updated = 0
            for j, p in enumerate(batch):
                e = idx_map.get(j + 1)
                if e:
                    p["section"] = e.get("section", p.get("section", "Other"))
                    p["tags"]    = ", ".join(e.get("tags", []))
                    updated += 1
 
            print(f"  Batch {b_idx}/{len(batches)}: classified {updated}/{len(batch)} papers")
 
        except urllib.error.HTTPError as e:
            body_text = e.read().decode()[:300]
            print(f"  Batch {b_idx}: HTTP {e.code} — {body_text}", file=sys.stderr)
        except Exception as e:
            print(f"  Batch {b_idx}: failed — {e}", file=sys.stderr)
 
        time.sleep(1)  # polite rate limiting
 
    return papers
 
 
# ── Main ───────────────────────────────────────────────────────────────────────
 
def main():
    print(f"\n{'='*60}")
    print(f"  The Human Mosaic — Daily Feed Update")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")
 
    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "").strip() or None
 
    # 1. Fetch papers
    print(f"1. Fetching RSS feeds (last {DAYS} days)...")
    papers = fetch_rss(DAYS)
 
    print(f"\n2. bioRxiv API fallback...")
    api_papers = fetch_biorxiv_api(DAYS)
 
    # Merge and deduplicate by key
    all_papers: dict[str, dict] = {}
    for p in papers + api_papers:
        if p["key"] not in all_papers:
            all_papers[p["key"]] = p
 
    print(f"\n   Total unique papers: {len(all_papers)}")
 
    # 2. Classify sections and tags via GitHub Models
    github_token = os.environ.get("GITHUB_TOKEN", "").strip()
    new_papers = list(all_papers.values())
    if github_token:
        print(f"\n2. Classifying papers via GitHub Models...")
        new_papers = classify_papers_github(new_papers, github_token)
    else:
        print("\n2. Skipping AI classification (no GITHUB_TOKEN)")
 
    # 3. Connect to Google Sheets
    print(f"\n3. Connecting to Google Sheets...")
    client = get_client()
    sh, created = get_or_create_sheet(client, sheet_id)
    ws = ensure_worksheet(sh, created)
    existing_keys = load_existing_keys(ws)
    print(f"   {len(existing_keys)} existing rows in sheet")
 
    # 3. Append new papers
    print(f"\n4. Appending new papers...")
    n_new = append_papers(ws, new_papers, existing_keys)
 
    # 4. Write sheet ID for dashboard
    write_sheet_id_to_docs(sh.id)
 
    print(f"\n{'='*60}")
    print(f"  Done. {n_new} new papers added.")
    print(f"  Sheet: https://docs.google.com/spreadsheets/d/{sh.id}")
    print(f"  CSV:   https://docs.google.com/spreadsheets/d/{sh.id}/export?format=csv")
    print(f"{'='*60}\n")
 
 
if __name__ == "__main__":
    main()
