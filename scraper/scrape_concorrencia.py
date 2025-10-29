# -*- coding: utf-8 -*-
"""
Scraper — Concorrência para Residência Médica 2026
Autor: ricardo.cadete — versão com geração de modelos Excel

Atualizações:
- Continua fazendo o scraping e salvando o JSON.
- Gera um modelo Excel consolidado (todas as tabelas, cada uma em uma aba).
- Gera modelos individuais (1 Excel por instituição).
- Copia todos os arquivos para site/downloads/.
- Se não houver alteração real nos dados, não regrava nada.
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

FONTE_URL = "https://med.estrategia.com/portal/residencia-medica/concorrencia-residencia-medica/"

# === Caminhos principais ===
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # pasta "scraper"
OUTPUT_DIR = os.path.join(ROOT_DIR, "output")
DATA_DIR = os.path.join(OUTPUT_DIR, "data")
EXCEL_DIR = os.path.join(OUTPUT_DIR, "excel")
SITE_DIR = os.path.join(os.path.dirname(ROOT_DIR), "site")
SITE_DATA_DIR = os.path.join(SITE_DIR, "data")
SITE_DOWNLOADS_DIR = os.path.join(SITE_DIR, "downloads")

JSON_FILENAME = "concorrencia_2026.json"
XLSX_FILENAME = "concorrencia_2026.xlsx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}


# === Funções auxiliares ===
def sanitize_title(text: str) -> str:
    t = re.sub(r"\s+", " ", (text or "").strip())
    return t.rstrip(" .")


def text_of(el: Tag) -> str:
    if el is None:
        return ""
    txt = " ".join(el.get_text(separator=" ", strip=True).split())
    return txt


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
    main = soup.find(class_=re.compile(r"(entry-content|single-content|content)")) or soup
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
                if not found_table:
                    maybe_table = pn.find("table")
                    if maybe_table:
                        found_table = maybe_table
                        break
            pn = pn.next_sibling

        if found_table:
            cols, rows = parse_html_table(found_table)
            if rows:
                results.append({
                    "titulo": titulo,
                    "columns": cols,
                    "rows": rows
                })
    return results


def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(EXCEL_DIR, exist_ok=True)
    if os.path.isdir(SITE_DIR):
        os.makedirs(SITE_DATA_DIR, exist_ok=True)
        os.makedirs(SITE_DOWNLOADS_DIR, exist_ok=True)


def now_brt():
    tz = pytz.timezone("America/Sao_Paulo")
    return datetime.now(tz)


def write_json(payload: dict, dest_path: str):
    with open(dest_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def json_hash(data: dict) -> str:
    """Cria um hash MD5 do conteúdo do JSON (para evitar regravar se não mudou)."""
    text = json.dumps(data, ensure_ascii=False, sort_keys=True)
    return md5(text.encode("utf-8")).hexdigest()


def generate_excel_files(blocks, dest_all, dest_individuals_dir):
    """Gera o Excel consolidado e os individuais."""
    # Consolidado
    with pd.ExcelWriter(dest_all, engine="openpyxl") as writer:
        for b in blocks:
            df = pd.DataFrame(b["rows"], columns=b["columns"])
            sheet_name = re.sub(r'[:\\/*?\[\]]', ' ', b["titulo"])[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    # Individuais
    for b in blocks:
        df = pd.DataFrame(b["rows"], columns=b["columns"])
        safe_name = re.sub(r"[^\w\s-]", "_", b["titulo"]).strip() or "tabela"
        file_name = safe_name[:40] + ".xlsx"
        path = os.path.join(dest_individuals_dir, file_name)
        df.to_excel(path, index=False)


def main():
    print("[SCRAPER] Iniciando scraping de concorrência 2026…")
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

    # Verifica se houve alteração no JSON anterior
    json_path = os.path.join(DATA_DIR, JSON_FILENAME)
    old_hash = None
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                old_hash = json_hash(json.load(f))
        except Exception:
            pass

    new_hash = json_hash(payload)
    if old_hash == new_hash:
        print("[SCRAPER] Nenhuma alteração nos dados detectada — nada será regravado.")
        return

    # Salvar novo JSON
    write_json(payload, json_path)
    print(f"[SCRAPER] JSON atualizado em: {json_path}")

    # Gera Excel consolidado e individuais
    consolidated_excel = os.path.join(EXCEL_DIR, XLSX_FILENAME)
    generate_excel_files(blocks, consolidated_excel, EXCEL_DIR)
    print(f"[SCRAPER] Modelos Excel gerados em: {EXCEL_DIR}")

    # Copiar para site/
    if os.path.isdir(SITE_DIR):
        write_json(payload, os.path.join(SITE_DATA_DIR, JSON_FILENAME))
        # copia o Excel consolidado
        os.makedirs(SITE_DOWNLOADS_DIR, exist_ok=True)
        import shutil
        shutil.copy2(consolidated_excel, os.path.join(SITE_DOWNLOADS_DIR, XLSX_FILENAME))
        # copia também os individuais
        for f in os.listdir(EXCEL_DIR):
            if f.lower().endswith(".xlsx"):
                shutil.copy2(os.path.join(EXCEL_DIR, f), os.path.join(SITE_DOWNLOADS_DIR, f))
        print(f"[SCRAPER] Modelos copiados para o site: {SITE_DOWNLOADS_DIR}")
    else:
        print("[SCRAPER] Pasta do site não encontrada — pulando cópia.")

    print("[SCRAPER] Concluído com sucesso.")


if __name__ == "__main__":
    main()
