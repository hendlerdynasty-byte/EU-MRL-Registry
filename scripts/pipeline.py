#!/usr/bin/env python3
"""
EU Open Data Portal - Master Pipeline
Runs: import → insights → export → push
Usage: python pipeline.py --niche pesticides
"""

import os, sys, json, csv, hashlib, time, argparse, subprocess
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import requests

# ── Config ────────────────────────────────────────────────────────────────────
NICHES = {
    "pesticides": {
        "db_project":   "eu-pesticides",
        "title":        "EU Pesticides & MRL Database",
        "description":  "Maximum Residue Levels for pesticides in EU food products",
        "data_url":     "https://www.eurl-pesticides.eu/docs/public/tmplt_article.asp?CntID=821&Lang=EN",
        "fallback_csv": "data/pesticides_sample.csv",
        "schema":       "sql/pesticides_schema.sql",
        "site_dir":     "sites/eu-pesticides",
        "tollbit_slug": "eu-pesticides-mrl",
        "price_usd":    "0.02",
    },
    "climate": {
        "db_project":   "eu-climate",
        "title":        "Deutsche & EU Klimadaten",
        "description":  "Klimatrends, Temperaturen und Extremereignisse für Deutschland und die EU",
        "data_url":     "https://opendata.dwd.de/climate_environment/CDC/",
        "fallback_csv": "data/climate_sample.csv",
        "schema":       "sql/climate_schema.sql",
        "site_dir":     "sites/eu-climate",
        "tollbit_slug": "eu-climate-data",
        "price_usd":    "0.015",
    },
    "procurement": {
        "db_project":   "eu-procurement",
        "title":        "EU Public Procurement Database",
        "description":  "Öffentliche Aufträge und Vergabedaten der EU",
        "data_url":     "https://data.europa.eu/data/datasets/ted-1",
        "fallback_csv": "data/procurement_sample.csv",
        "schema":       "sql/procurement_schema.sql",
        "site_dir":     "sites/eu-procurement",
        "tollbit_slug": "eu-procurement-data",
        "price_usd":    "0.025",
    },
}

OPENROUTER_URL   = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "nvidia/nemotron-3-super-120b-a12b:free")
OPENROUTER_KEY   = os.getenv("OPENROUTER_API_KEY", "")

# ── DB Connection ──────────────────────────────────────────────────────────────
def get_conn(niche_key: str):
    """
    Neon connection string stored as GitHub Secret:
    NEON_DB_PESTICIDES, NEON_DB_CLIMATE, NEON_DB_PROCUREMENT …
    Format: postgresql://user:pass@host/dbname?sslmode=require
    """
    env_key = f"NEON_DB_{niche_key.upper()}"
    dsn = os.getenv(env_key)
    if not dsn:
        raise EnvironmentError(f"Missing env var: {env_key}")
    return psycopg2.connect(dsn)

# ── Step 1: Import Data ────────────────────────────────────────────────────────
def import_data(niche_key: str, csv_path: str = None):
    cfg  = NICHES[niche_key]
    conn = get_conn(niche_key)
    cur  = conn.cursor()

    # Apply schema
    schema_file = Path(__file__).parent.parent / cfg["schema"]
    if schema_file.exists():
        cur.execute(schema_file.read_text())
        conn.commit()
        print(f"✓ Schema applied for {niche_key}")

    source = csv_path or cfg.get("fallback_csv")
    if not source or not Path(source).exists():
        print(f"⚠ No CSV found for {niche_key}, skipping import")
        conn.close()
        return

    with open(source, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)

    if niche_key == "pesticides":
        _import_pesticides(cur, rows)
    elif niche_key == "climate":
        _import_climate(cur, rows)
    elif niche_key == "procurement":
        _import_procurement(cur, rows)

    conn.commit()
    conn.close()
    print(f"✓ Imported {len(rows)} rows for {niche_key}")

def _import_pesticides(cur, rows):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pesticides_mrl (
            id SERIAL PRIMARY KEY,
            substance TEXT NOT NULL,
            product TEXT,
            mrl_mg_kg NUMERIC,
            regulation TEXT,
            country TEXT DEFAULT 'EU',
            valid_from DATE,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            row_hash TEXT UNIQUE
        );
    """)
    for r in rows:
        h = hashlib.md5(json.dumps(r, sort_keys=True).encode()).hexdigest()
        cur.execute("""
            INSERT INTO pesticides_mrl
                (substance, product, mrl_mg_kg, regulation, country, valid_from, row_hash)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (row_hash) DO NOTHING
        """, (
            r.get("substance","").strip(),
            r.get("product","").strip(),
            _safe_float(r.get("mrl_mg_kg","")),
            r.get("regulation","").strip(),
            r.get("country","EU"),
            _safe_date(r.get("valid_from","")),
            h,
        ))

def _import_climate(cur, rows):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS climate_data (
            id SERIAL PRIMARY KEY,
            station TEXT,
            date DATE,
            temp_avg NUMERIC,
            temp_max NUMERIC,
            temp_min NUMERIC,
            precipitation_mm NUMERIC,
            region TEXT,
            row_hash TEXT UNIQUE
        );
    """)
    for r in rows:
        h = hashlib.md5(json.dumps(r, sort_keys=True).encode()).hexdigest()
        cur.execute("""
            INSERT INTO climate_data
                (station, date, temp_avg, temp_max, temp_min, precipitation_mm, region, row_hash)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (row_hash) DO NOTHING
        """, (
            r.get("station",""), _safe_date(r.get("date","")),
            _safe_float(r.get("temp_avg","")), _safe_float(r.get("temp_max","")),
            _safe_float(r.get("temp_min","")), _safe_float(r.get("precipitation_mm","")),
            r.get("region",""), h,
        ))

def _import_procurement(cur, rows):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS procurement_notices (
            id SERIAL PRIMARY KEY,
            notice_id TEXT UNIQUE,
            title TEXT,
            contracting_authority TEXT,
            country TEXT,
            cpv_code TEXT,
            value_eur NUMERIC,
            award_date DATE,
            winner TEXT,
            row_hash TEXT UNIQUE
        );
    """)
    for r in rows:
        h = hashlib.md5(json.dumps(r, sort_keys=True).encode()).hexdigest()
        cur.execute("""
            INSERT INTO procurement_notices
                (notice_id, title, contracting_authority, country, cpv_code,
                 value_eur, award_date, winner, row_hash)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (notice_id) DO NOTHING
        """, (
            r.get("notice_id",""), r.get("title",""),
            r.get("contracting_authority",""), r.get("country",""),
            r.get("cpv_code",""), _safe_float(r.get("value_eur","")),
            _safe_date(r.get("award_date","")), r.get("winner",""), h,
        ))

# ── Step 2: Generate LLM Insights ─────────────────────────────────────────────
def generate_insights(niche_key: str) -> dict:
    cfg       = NICHES[niche_key]
    conn      = get_conn(niche_key)
    cur       = conn.cursor()
    data_json = _fetch_sample_data(cur, niche_key)
    conn.close()

    prompt_path = Path(__file__).parent.parent / "prompts" / f"{niche_key}_insights.md"
    if prompt_path.exists():
        prompt_template = prompt_path.read_text()
    else:
        prompt_template = _default_prompt(cfg)

    prompt = prompt_template.replace("{{DATA_JSON}}", json.dumps(data_json, ensure_ascii=False, indent=2))
    prompt = prompt.replace("{{NICHE_TITLE}}", cfg["title"])
    prompt = prompt.replace("{{DATE}}", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    if not OPENROUTER_KEY:
        print("⚠ No OPENROUTER_API_KEY – using placeholder insights")
        return _placeholder_insights(cfg)

    # Respect free-tier: max 20 req/day → we batch into one call
    resp = requests.post(
        OPENROUTER_URL,
        headers={"Authorization": f"Bearer {OPENROUTER_KEY}", "Content-Type": "application/json"},
        json={
            "model": OPENROUTER_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 1800,
            "temperature": 0.4,
        },
        timeout=90,
    )
    resp.raise_for_status()
    raw = resp.json()["choices"][0]["message"]["content"]

    # Parse JSON block from LLM response
    try:
        start = raw.index("{")
        end   = raw.rindex("}") + 1
        insights = json.loads(raw[start:end])
    except (ValueError, json.JSONDecodeError):
        insights = {"summary": raw, "key_findings": [], "trend": "", "action_items": []}

    insights["generated_at"] = datetime.now(timezone.utc).isoformat()
    insights["model"]        = OPENROUTER_MODEL
    print(f"✓ Insights generated for {niche_key}")
    return insights

def _fetch_sample_data(cur, niche_key: str) -> list:
    TABLE_MAP = {
        "pesticides":  "pesticides_mrl",
        "climate":     "climate_data",
        "procurement": "procurement_notices",
    }
    table = TABLE_MAP.get(niche_key)
    if not table:
        return []
    try:
        cur.execute(f"SELECT * FROM {table} ORDER BY id DESC LIMIT 200")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        print(f"⚠ Could not fetch data: {e}")
        return []

def _default_prompt(cfg: dict) -> str:
    return f"""You are a data analyst specializing in EU open data.
Analyze this dataset: {cfg['title']}
Description: {cfg['description']}

Dataset sample (JSON):
{{{{DATA_JSON}}}}

Today's date: {{{{DATE}}}}

Respond ONLY with valid JSON (no markdown fences). Structure:
{{
  "summary": "2-3 sentence executive summary of the dataset",
  "key_findings": [
    {{"finding": "...", "significance": "high|medium|low", "detail": "..."}},
    {{"finding": "...", "significance": "high|medium|low", "detail": "..."}},
    {{"finding": "...", "significance": "high|medium|low", "detail": "..."}}
  ],
  "trend": "One sentence describing the most important trend",
  "statistics": {{
    "total_records": 0,
    "date_range": "...",
    "notable_stat": "..."
  }},
  "action_items": ["...", "...", "..."],
  "data_quality_notes": "..."
}}"""

def _placeholder_insights(cfg: dict) -> dict:
    return {
        "summary": f"This dataset contains structured {cfg['title']} data sourced from official EU open data portals.",
        "key_findings": [{"finding": "Data successfully imported", "significance": "high", "detail": "Records available for analysis"}],
        "trend": "Data pipeline operational – insights pending API key configuration.",
        "statistics": {"total_records": 0, "date_range": "N/A", "notable_stat": "N/A"},
        "action_items": ["Configure OPENROUTER_API_KEY to enable AI insights"],
        "data_quality_notes": "Placeholder – real insights generated once API key is set.",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": "placeholder",
    }

# ── Step 3: Export Static Files ────────────────────────────────────────────────
def export_static(niche_key: str, insights: dict):
    cfg      = NICHES[niche_key]
    conn     = get_conn(niche_key)
    cur      = conn.cursor()
    out_dir  = Path(__file__).parent.parent / cfg["site_dir"] / "public"
    out_dir.mkdir(parents=True, exist_ok=True)

    data = _fetch_sample_data(cur, niche_key)
    conn.close()

    # data.json  (paginated in real use)
    payload = {
        "niche":      niche_key,
        "title":      cfg["title"],
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total":      len(data),
        "records":    data[:500],  # cap at 500 for static file size
    }
    (out_dir / "data.json").write_text(json.dumps(payload, default=str, ensure_ascii=False, indent=2))

    # insights.json
    (out_dir / "insights.json").write_text(json.dumps(insights, default=str, ensure_ascii=False, indent=2))

    # data.csv
    if data:
        with open(out_dir / "data.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=data[0].keys())
            w.writeheader()
            w.writerows(data)

    # llms.txt  (AI-crawler landing page)
    _write_llms_txt(out_dir, cfg, insights)

    # sitemap.xml
    _write_sitemap(out_dir, cfg)

    # schema.org JSON-LD
    _write_jsonld(out_dir, cfg, len(data))

    # robots.txt  with TollBit
    _write_robots(out_dir, cfg)

    print(f"✓ Static files exported to {out_dir}")

def _write_llms_txt(out_dir: Path, cfg: dict, insights: dict):
    content = f"""# {cfg['title']}

> {cfg['description']}

## Dataset Overview
{insights.get('summary', 'EU Open Data structured dataset.')}

## Key Findings
""" + "\n".join(f"- {f['finding']}: {f['detail']}" for f in insights.get("key_findings", [])) + f"""

## Trend
{insights.get('trend', '')}

## Access & Licensing
- **Source**: Official EU Open Data Portal (data.europa.eu)
- **License**: CC0 / Public Domain with Attribution
- **Last Updated**: {insights.get('generated_at', datetime.now().isoformat())[:10]}
- **Format**: JSON, CSV available at /public/data.json and /public/data.csv

## AI Access Pricing
This dataset is licensed for AI summarization via TollBit.
Full-display and summarization licenses available.
Pricing: ${cfg['price_usd']} per request.
Contact: See TollBit portal for automated licensing.

## Attribution
Data sourced from the European Union Open Data Portal.
AI insights generated by automated pipeline. Commercial reuse permitted under original license terms.
"""
    (out_dir / "llms.txt").write_text(content)

def _write_sitemap(out_dir: Path, cfg: dict):
    domain = f"https://{cfg['tollbit_slug']}.vercel.app"
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>{domain}/</loc><changefreq>weekly</changefreq><priority>1.0</priority></url>
  <url><loc>{domain}/public/data.json</loc><changefreq>weekly</changefreq><priority>0.8</priority></url>
  <url><loc>{domain}/public/llms.txt</loc><changefreq>weekly</changefreq><priority>0.9</priority></url>
</urlset>"""
    (out_dir.parent / "sitemap.xml").write_text(xml)

def _write_jsonld(out_dir: Path, cfg: dict, record_count: int):
    jsonld = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": cfg["title"],
        "description": cfg["description"],
        "url": f"https://{cfg['tollbit_slug']}.vercel.app",
        "license": "https://creativecommons.org/publicdomain/zero/1.0/",
        "creator": {"@type": "Organization", "name": "EU Open Data Pipeline"},
        "dateModified": datetime.now(timezone.utc).date().isoformat(),
        "distribution": [
            {"@type": "DataDownload", "encodingFormat": "application/json",
             "contentUrl": f"https://{cfg['tollbit_slug']}.vercel.app/public/data.json"},
            {"@type": "DataDownload", "encodingFormat": "text/csv",
             "contentUrl": f"https://{cfg['tollbit_slug']}.vercel.app/public/data.csv"},
        ],
        "measurementTechnique": "EU Official Open Data",
        "variableMeasured": cfg["title"],
        "size": f"{record_count} records",
    }
    (out_dir / "schema.json").write_text(json.dumps(jsonld, ensure_ascii=False, indent=2))

def _write_robots(out_dir: Path, cfg: dict):
    content = f"""# robots.txt for {cfg['title']}
# TollBit AI Crawler Monetization active

User-agent: *
Allow: /
Sitemap: https://{cfg['tollbit_slug']}.vercel.app/sitemap.xml

# TollBit Pay-Per-Crawl
# AI crawlers must purchase access via TollBit
# Pricing: ${cfg['price_usd']} per request
# Portfolio: https://publisher.tollbit.com

User-agent: GPTBot
User-agent: Claude-Web
User-agent: PerplexityBot
User-agent: anthropic-ai
User-agent: Google-Extended
User-agent: cohere-ai
Disallow: /
# TollBit: PAID_ACCESS_REQUIRED
"""
    (out_dir.parent / "robots.txt").write_text(content)

# ── Step 4: Git Push ───────────────────────────────────────────────────────────
def git_push(niche_key: str):
    cfg     = NICHES[niche_key]
    site_dir = Path(__file__).parent.parent / cfg["site_dir"]
    msg      = f"chore: auto-update {niche_key} data [{datetime.now().strftime('%Y-%m-%d %H:%M')}]"

    subprocess.run(["git", "add", str(site_dir)], check=True)
    subprocess.run(["git", "commit", "-m", msg], check=False)  # no-fail if nothing changed
    subprocess.run(["git", "push"], check=True)
    print(f"✓ Pushed to GitHub for {niche_key}")

# ── Helpers ────────────────────────────────────────────────────────────────────
def _safe_float(v):
    try:    return float(str(v).replace(",", "."))
    except: return None

def _safe_date(v):
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:    return datetime.strptime(str(v), fmt).date()
        except: pass
    return None

# ── Entry Point ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="EU Open Data Pipeline")
    parser.add_argument("--niche",    required=True, choices=list(NICHES.keys()))
    parser.add_argument("--csv",      help="Path to CSV file to import")
    parser.add_argument("--skip-import",   action="store_true")
    parser.add_argument("--skip-insights", action="store_true")
    parser.add_argument("--skip-push",     action="store_true")
    args = parser.parse_args()

    print(f"\n🚀 Pipeline starting for niche: {args.niche}\n{'─'*40}")

    if not args.skip_import:
        print("Step 1/4: Importing data …")
        import_data(args.niche, args.csv)

    if not args.skip_insights:
        print("Step 2/4: Generating AI insights …")
        insights = generate_insights(args.niche)
    else:
        insights = _placeholder_insights(NICHES[args.niche])

    print("Step 3/4: Exporting static files …")
    export_static(args.niche, insights)

    if not args.skip_push:
        print("Step 4/4: Pushing to GitHub …")
        git_push(args.niche)

    print(f"\n✅ Pipeline complete for {args.niche}!")

if __name__ == "__main__":
    main()
