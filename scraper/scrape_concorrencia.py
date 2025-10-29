# -*- coding: utf-8 -*-
"""
Scraper — Concorrência para Residência Médica 2026
Blindado para zero-tabelas + geração de modelos Excel
"""

import os
import json
import re
from datetime import datetime
import pytz
import requests
from bs4 import BeautifulSoup, NavigableString, Tag
import pandas as pd
from hashlib import md5
import shutil

SCRIPT_VERSION = "2025-10-29b"
FONTE_URL = "https://med.estrategia.com/portal/residencia-medica/concorrencia-residencia-medica/"

# === Caminhos ===
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # /scraper
OUTPUT_DIR = os.path.join(ROOT_DIR, "output")
DATA_DIR = os.path.join(OUTPUT_DIR, "data")
EXCEL_DIR = os.path.join(OUTPUT_DIR, "excel")
SITE_DIR = os.path.join(os.path.dirname(ROOT_DIR), "site")
SITE_DATA_DIR = os.path.join(SITE_DIR, "data")
SITE_DOWNLOADS_DIR = os.path.join(SITE_DIR, "downloads")

JSON_FILENAME = "concorrencia_2026.json"
XLSX_FILENAME = "concorrencia_2026.xlsx"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

def sanitize_title(text: str) -> str:
    t = re.sub(r"\s+", " ", (text or "").strip())
    return t.rstrip(" .")

def text_of(el: Tag) -> str:
    if el is None:
        return ""
    return " ".join(el.get_text(separator=" ", strip=True).split())

def parse_html_table(table_tag: Tag):
    columns = []
    thead = table_tag.find("thead")
    if thead:
        ths = thead.find_all(["th", "td"])
        columns = [text_of(th) for th in ths]
    else:
        first_tr = table_tag.find("tr")
        if first_tr:
            ths = first_tr.find_all("th")
            if ths:
                columns = [text_of(th) for th in ths]

    rows = []
    tbody = table_tag.find("tbody") or table_tag
    trs = tbody.find_all("tr")
    for tr in trs:
        if tr.find("th") and not tr.find("td"):
            continue
        tds = tr.find_all(["td", "th"])
        if not tds:
            continue
        row = [text_of(td) for td in tds]
        if not columns and tr == trs[0]:
            columns = row
            continue
        rows.append(row)

    if not columns and rows:
        max_len = max(len(r) for r in rows)
        columns = [f"Col{i+1}" for i in range(max_len)]

    return columns, rows

def find_blocks(soup: BeautifulSoup):
    results = []
    main = soup.find(class_=re.compile(r"(entry-content|single-content|post-content|content)")) or soup
    headings = main.find_all(["h2", "h3"])
    for h in headings:
        titulo = sanitize_title(text_of(h))
        if not titulo:
            continue

        pn = h.next_sibling
        found_table = None
        while pn and isinstance(pn, (Tag, NavigableString)):
            if isinstance(pn, Tag) and pn.name in ["h2", "h3"]:
                break
            if isinstance(pn, Tag):
                if pn.name == "table":
                    found_table = pn
                    break
                maybe = pn.find("table")
                if maybe:
                    found_table = maybe
                    break
            pn = pn.next_sibling

        if found_table:
            cols, rows = parse_html_table(found_table)
            if rows:
                results.append({"titulo": titulo, "columns": cols, "rows": rows})
    return results

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(EXCEL_DIR, exist_ok=True)
    if os.path.isdir(SITE_DIR):
        os.makedirs(SITE_DATA_DIR, exist_ok=True)
        os.makedirs(SITE_DOWNLOADS_DIR, exist_ok=True)

def now_brt():
    return datetime.now(pytz.timezone("America/Sao_Paulo"))

def write_json(payload: dict, dest_path: str):
    with open(dest_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def json_hash(data: dict) -> str:
    return md5(json.dumps(data, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

def generate_excel_files(blocks, dest_all, dest_individuals_dir):
    """Gera modelos Excel. Se blocks==0, cria um workbook 'Sem_dados'."""
    if not blocks:
        with pd.ExcelWriter(dest_all, engine="openpyxl") as writer:
            df = pd.DataFrame([{"mensagem": "Sem dados no momento"}])
            df.to_excel(writer, sheet_name="Sem_dados", index=False)
        return

    with pd.ExcelWriter(dest_all, engine="openpyxl") as writer:
        for b in blocks:
            df = pd.DataFrame(b["rows"], columns=b["columns"])
            sheet_name = re.sub(r'[:\\/*?\[\]]', ' ', b["titulo"])[:31] or "Tabela"
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    for b in blocks:
        df = pd.DataFrame(b["rows"], columns=b["columns"])
        safe_name = re.sub(r"[^\w\s-]", "_", b["titulo"]).strip() or "tabela"
        file_name = (safe_name[:40] or "tabela") + ".xlsx"
        df.to_excel(os.path.join(dest_individuals_dir, file_name), index=False)

def main():
    print(f"[SCRAPER] Iniciando scraping de concorrência 2026… (SCRIPT_VERSION={SCRIPT_VERSION})")
    ensure_dirs()

    resp = requests.get(FONTE_URL, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    blocks = find_blocks(soup)
    print(f"[SCRAPER] Tabelas encontradas: {len(blocks)}")

    dt = now_brt()
    payload = {
        "fonte_url": FONTE_URL,
        "updated_at_iso": dt.isoformat(),
        "updated_at_br": dt.strftime("%d/%m/%Y %H:%M"),
        "tabelas": blocks
    }

    json_path = os.path.join(DATA_DIR, JSON_FILENAME)
    old_hash = None
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                old_hash = json_hash(json.load(f))
        except Exception:
            pass
    new_hash = json_hash(payload)

    # Grava sempre o JSON (mesmo vazio) e copia para o site
    write_json(payload, json_path)
    print(f"[SCRAPER] JSON atualizado em: {json_path}")
    if os.path.isdir(SITE_DIR):
        write_json(payload, os.path.join(SITE_DATA_DIR, JSON_FILENAME))
        print(f"[SCRAPER] JSON copiado para o site: {os.path.join(SITE_DATA_DIR, JSON_FILENAME)}")

    # --- CURTO-CIRCUITO: se não há tabelas, cria apenas o consolidado 'Sem_dados' e encerra ---
    if len(blocks) == 0:
        consolidated_excel = os.path.join(EXCEL_DIR, XLSX_FILENAME)
        generate_excel_files([], consolidated_excel, EXCEL_DIR)  # cria Sem_dados
        if os.path.isdir(SITE_DIR):
            os.makedirs(SITE_DOWNLOADS_DIR, exist_ok=True)
            shutil.copy2(consolidated_excel, os.path.join(SITE_DOWNLOADS_DIR, XLSX_FILENAME))
            print(f"[SCRAPER] Placeholder de Excel (Sem_dados) copiado para o site.")
        print("[SCRAPER] Nenhuma tabela encontrada. Finalizado sem erro.")
        return

    # Se mudou, (re)gera modelos e copia
    if old_hash != new_hash:
        consolidated_excel = os.path.join(EXCEL_DIR, XLSX_FILENAME)
        generate_excel_files(blocks, consolidated_excel, EXCEL_DIR)
        print(f"[SCRAPER] Modelos Excel gerados em: {EXCEL_DIR}")

        if os.path.isdir(SITE_DIR):
            os.makedirs(SITE_DOWNLOADS_DIR, exist_ok=True)
            shutil.copy2(consolidated_excel, os.path.join(SITE_DOWNLOADS_DIR, XLSX_FILENAME))
            for f in os.listdir(EXCEL_DIR):
                if f.lower().endswith(".xlsx"):
                    shutil.copy2(os.path.join(EXCEL_DIR, f), os.path.join(SITE_DOWNLOADS_DIR, f))
            print(f"[SCRAPER] Modelos copiados para o site: {SITE_DOWNLOADS_DIR}")
    else:
        print("[SCRAPER] Dados idênticos ao último run — mantendo modelos atuais.")

    print("[SCRAPER] Concluído com sucesso.")

if __name__ == "__main__":
    main()
