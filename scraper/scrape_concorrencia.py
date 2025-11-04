# -*- coding: utf-8 -*-
"""
Scraper — Concorrência para Residência Médica 2026 (follow button robust)
Fonte: med.estrategia.com
Regras:
  1) Usa WP REST API (content.rendered) quando disponível; fallback para HTML público.
  2) Para cada (H2/H3 + primeira tabela após o título):
       - Procura BOTÃO/LINK a partir da PRÓPRIA TABELA (próximos irmãos),
         até o próximo H2/H3 (ou no máx 15 nós).
       - Se existir, segue o link e extrai TODAS as tabelas da página destino (com títulos).
       - Se não, usa a tabela do bloco principal.
  3) Gera JSON + modelos Excel (consolidado e individuais).
"""

import os
import re
import json
import shutil
from datetime import datetime
from hashlib import md5
from urllib.parse import urljoin

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup, Tag, NavigableString

SCRIPT_VERSION = "2025-11-04-follow-btn-v2"

FONTE_URL = "https://med.estrategia.com/portal/residencia-medica/concorrencia-residencia-medica/"
WP_API_BASE = "https://med.estrategia.com/portal/wp-json/wp/v2"

# paths
SCRAPER_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(SCRAPER_DIR)
OUTPUT_DIR = os.path.join(REPO_DIR, "output")
DATA_DIR = os.path.join(OUTPUT_DIR, "data")
EXCEL_DIR = os.path.join(OUTPUT_DIR, "excel")
SITE_DIR = os.path.join(REPO_DIR, "site")
SITE_DATA_DIR = os.path.join(SITE_DIR, "data")
SITE_DL_DIR = os.path.join(SITE_DIR, "downloads")

JSON_NAME = "concorrencia_2026.json"
XLSX_ALL = "concorrencia_2026.xlsx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": FONTE_URL,
}

# ---------- utils ----------
def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(EXCEL_DIR, exist_ok=True)
    if os.path.isdir(SITE_DIR):
        os.makedirs(SITE_DATA_DIR, exist_ok=True)
        os.makedirs(SITE_DL_DIR, exist_ok=True)

def now_brt():
    return datetime.now(pytz.timezone("America/Sao_Paulo"))

def sanitize_title(t: str) -> str:
    t = (t or "").strip()
    t = re.sub(r"\s+", " ", t)
    return t.rstrip(" .")

def text_of(el: Tag) -> str:
    if not el:
        return ""
    return " ".join(el.get_text(" ", strip=True).split())

def parse_html_table(tbl: Tag):
    cols = []
    thead = tbl.find("thead")
    if thead:
        cols = [text_of(th) for th in thead.find_all(["th", "td"])]
    else:
        first = tbl.find("tr")
        if first:
            ths = first.find_all("th")
            if ths:
                cols = [text_of(th) for th in ths]

    rows = []
    body = tbl.find("tbody") or tbl
    for tr in body.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        row = [text_of(td) for td in cells]
        if not cols and tr == body.find_all("tr")[0]:
            cols = row
            continue
        rows.append(row)

    if not cols and rows:
        m = max(len(r) for r in rows)
        cols = [f"Col{i+1}" for i in range(m)]
    return cols, rows

def is_button_like(a: Tag) -> bool:
    if not a or a.name != "a" or not a.get("href"):
        return False
    txt = text_of(a).lower()
    if re.search(r"(confira|veja|ver|acesse|consulte)", txt):
        return True
    klass = " ".join(a.get("class", [])).lower()
    if any(k in klass for k in ["wp-block-button__link", "btn", "button"]):
        return True
    if (a.get("role") or "").lower() == "button":
        return True
    return False

def follow_url(href: str, base: str) -> str:
    try:
        return urljoin(base, href)
    except Exception:
        return href

def json_hash(data: dict) -> str:
    return md5(json.dumps(data, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

def write_json(path: str, payload: dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def generate_excel(blocks, xlsx_all_path, individuals_dir):
    if not blocks:
        with pd.ExcelWriter(xlsx_all_path, engine="openpyxl") as wr:
            pd.DataFrame([{"mensagem": "Sem dados"}]).to_excel(wr, sheet_name="Sem_dados", index=False)
        return

    with pd.ExcelWriter(xlsx_all_path, engine="openpyxl") as wr:
        for b in blocks:
            df = pd.DataFrame(b["rows"], columns=b["columns"])
            sheet = re.sub(r'[:\\/*?\[\]]', ' ', b["titulo"])[:31] or "Tabela"
            df.to_excel(wr, sheet_name=sheet, index=False)

    for b in blocks:
        df = pd.DataFrame(b["rows"], columns=b["columns"])
        base = re.sub(r"[^\w\s-]", "_", b["titulo"]).strip() or "tabela"
        name = (base[:40] or "tabela") + ".xlsx"
        df.to_excel(os.path.join(individuals_dir, name), index=False)

# ---------- WP API ----------
def fetch_wp_content():
    slug = "concorrencia-residencia-medica"
    # pages
    try:
        url = f"{WP_API_BASE}/pages"
        r = requests.get(url, headers=HEADERS, params={"slug": slug, "_fields": "content.rendered", "per_page": 1}, timeout=60)
        if r.ok:
            arr = r.json()
            if isinstance(arr, list) and arr:
                content = arr[0].get("content", {}).get("rendered")
                if content:
                    print("[SCRAPER] WP API: conteúdo encontrado em /pages.")
                    return content
    except Exception as e:
        print("[SCRAPER] WP API /pages falhou:", e)

    # posts
    try:
        url = f"{WP_API_BASE}/posts"
        r = requests.get(url, headers=HEADERS, params={"slug": slug, "_fields": "content.rendered", "per_page": 1}, timeout=60)
        if r.ok:
            arr = r.json()
            if isinstance(arr, list) and arr:
                content = arr[0].get("content", {}).get("rendered")
                if content:
                    print("[SCRAPER] WP API: conteúdo encontrado em /posts.")
                    return content
    except Exception as e:
        print("[SCRAPER] WP API /posts falhou:", e)

    return None

# ---------- deep page ----------
def collect_from_detail_page(url: str):
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        blocks = []
        headings = soup.find_all(["h1", "h2", "h3"])
        for h in headings:
            titulo = sanitize_title(text_of(h))
            # procura primeira tabela após o heading
            pn = h.next_sibling
            found = None
            while pn and isinstance(pn, (Tag, NavigableString)):
                if isinstance(pn, Tag) and pn.name in ["h1", "h2", "h3"]:
                    break
                if isinstance(pn, Tag):
                    if pn.name == "table":
                        found = pn
                        break
                    maybe = pn.find("table")
                    if maybe:
                        found = maybe
                        break
                pn = pn.next_sibling

            if found:
                cols, rows = parse_html_table(found)
                if rows:
                    blocks.append({"titulo": titulo, "columns": cols, "rows": rows})

        if blocks:
            print(f"[SCRAPER] Deep '{url}': {len(blocks)} tabela(s) via headings.")
            return blocks

        # fallback: todas as tabelas
        all_tbls = soup.find_all("table")
        if all_tbls:
            t0 = sanitize_title(text_of(soup.find("h1"))) or "Tabela"
            out = []
            for i, tb in enumerate(all_tbls, 1):
                cols, rows = parse_html_table(tb)
                if rows:
                    out.append({"titulo": f"{t0} {i}", "columns": cols, "rows": rows})
            if out:
                print(f"[SCRAPER] Deep '{url}': {len(out)} tabela(s) sem headings.")
                return out

    except Exception as e:
        print(f"[SCRAPER] Falha deep '{url}': {e}")

    return []

def prefix_if_needed(parent: str, blocks: list):
    out = []
    pl = (parent or "").lower()
    for b in blocks:
        t = b.get("titulo") or parent
        if pl and pl not in (t or "").lower():
            t = f"{parent} — {t}"
        out.append({"titulo": sanitize_title(t), "columns": b["columns"], "rows": b["rows"]})
    return out

# ---------- coleta principal ----------
def find_first_table_after_heading(h: Tag) -> Tag | None:
    pn = h.next_sibling
    while pn and isinstance(pn, (Tag, NavigableString)):
        if isinstance(pn, Tag) and pn.name in ["h2", "h3"]:
            break
        if isinstance(pn, Tag):
            if pn.name == "table":
                return pn
            maybe = pn.find("table")
            if maybe:
                return maybe
        pn = pn.next_sibling
    return None

def find_button_after_table(tbl: Tag, base_url: str) -> str | None:
    """
    Varre a partir da PRÓPRIA TABELA para frente, até o próximo H2/H3 ou 15 nós,
    procurando por <a> que pareça botão.
    """
    steps = 0
    pn = tbl.next_sibling
    while pn and isinstance(pn, (Tag, NavigableString)) and steps < 15:
        if isinstance(pn, Tag) and pn.name in ["h2", "h3"]:
            break
        if isinstance(pn, Tag):
            for a in pn.find_all("a", href=True):
                if is_button_like(a):
                    return follow_url(a["href"], base_url)
        pn = pn.next_sibling
        steps += 1
    return None

def collect_blocks_from_soup(soup: BeautifulSoup, base_url: str):
    results = []
    for h in soup.find_all(["h2", "h3"]):
        titulo = sanitize_title(text_of(h))
        if not titulo:
            continue

        tbl = find_first_table_after_heading(h)
        if not tbl:
            continue

        # novo: procurar botão a partir da TABELA
        btn_link = find_button_after_table(tbl, base_url)
        if btn_link:
            print(f"[SCRAPER] '{titulo}': botão detectado → {btn_link}")
            deep_blocks = collect_from_detail_page(btn_link)
            if deep_blocks:
                results.extend(prefix_if_needed(titulo, deep_blocks))
                continue  # substitui a tabela resumida

        cols, rows = parse_html_table(tbl)
        if rows:
            results.append({"titulo": titulo, "columns": cols, "rows": rows})

    return results

# ---------- main ----------
def main():
    print(f"[SCRAPER] Iniciando scraping de concorrência 2026… (SCRIPT_VERSION={SCRIPT_VERSION})")
    ensure_dirs()

    html = fetch_wp_content()
    if html:
        print("[SCRAPER] Usando conteúdo do WordPress REST API.")
        soup = BeautifulSoup(html, "lxml")
        base = FONTE_URL
    else:
        print("[SCRAPER] WP API indisponível — usando HTML público.")
        r = requests.get(FONTE_URL, headers=HEADERS, timeout=60)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        base = r.url

    blocks = collect_blocks_from_soup(soup, base)
    print(f"[SCRAPER] Tabelas consolidadas (após follow-button): {len(blocks)}")

    dt = now_brt()
    payload = {
        "fonte_url": FONTE_URL,
        "updated_at_iso": dt.isoformat(),
        "updated_at_br": dt.strftime("%d/%m/%Y %H:%M"),
        "tabelas": blocks,
    }

    json_path = os.path.join(DATA_DIR, JSON_NAME)
    old_hash = None
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                old_hash = json_hash(json.load(f))
        except Exception:
            pass
    new_hash = json_hash(payload)

    write_json(json_path, payload)
    print(f"[SCRAPER] JSON atualizado em: {json_path}")
    if os.path.isdir(SITE_DIR):
        write_json(os.path.join(SITE_DATA_DIR, JSON_NAME), payload)
        print(f"[SCRAPER] JSON copiado para site/.")

    xlsx_all = os.path.join(EXCEL_DIR, XLSX_ALL)
    if old_hash != new_hash:
        generate_excel(blocks, xlsx_all, EXCEL_DIR)
        print(f"[SCRAPER] Modelos Excel gerados em: {EXCEL_DIR}")
        if os.path.isdir(SITE_DIR):
            shutil.copy2(xlsx_all, os.path.join(SITE_DL_DIR, XLSX_ALL))
            for f in os.listdir(EXCEL_DIR):
                if f.lower().endswith(".xlsx"):
                    shutil.copy2(os.path.join(EXCEL_DIR, f), os.path.join(SITE_DL_DIR, f))
            print("[SCRAPER] Modelos copiados para site/downloads.")
    else:
        print("[SCRAPER] Sem alterações — mantendo modelos atuais.")

    print("[SCRAPER] Concluído com sucesso.")

if __name__ == "__main__":
    main()
