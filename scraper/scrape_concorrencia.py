# -*- coding: utf-8 -*-
"""
Scraper Concorrência 2026 — captura tabelas da página principal do Estratégia MED
e, quando existir botão "ver completo", segue o link e extrai as tabelas de lá.
Cada tabela recebe:
- titulo: título específico da tabela (ex.: "HCPA — Acesso Direto")
- card_title: TÍTULO-RAIZ da página principal (ex.: "HCPA")  << usado nos cards
- columns, rows
Também gera modelos Excel e o JSON final em output/.
"""

import os
import re
import json
import io
import datetime as dt
import requests
import pandas as pd
from bs4 import BeautifulSoup

SCRIPT_VERSION = "2025-11-04-card-title-root"

# Fonte
SOURCE_URL = "https://med.estrategia.com/portal/residencia-medica/concorrencia-residencia-medica/"
# Tentamos via WP REST primeiro (conteúdo completo sem bloqueios dinâmicos)
WP_API_CANDIDATES = [
    # páginas
    "https://med.estrategia.com/wp-json/wp/v2/pages?search=concorrencia-residencia-medica&per_page=1&_fields=content.rendered,link,title",
    # posts (fallback)
    "https://med.estrategia.com/wp-json/wp/v2/posts?search=concorrencia-residencia-medica&per_page=1&_fields=content.rendered,link,title",
]

# Pastas de saída no repo
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(ROOT_DIR, "output")
DATA_DIR = os.path.join(OUT_DIR, "data")
EXCEL_DIR = os.path.join(OUT_DIR, "excel")
SITE_DIR = os.path.join(ROOT_DIR, "site")
SITE_DATA_DIR = os.path.join(SITE_DIR, "data")
SITE_DL_DIR = os.path.join(SITE_DIR, "downloads")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(EXCEL_DIR, exist_ok=True)
os.makedirs(SITE_DATA_DIR, exist_ok=True)
os.makedirs(SITE_DL_DIR, exist_ok=True)

SEPARATOR = re.compile(r"\s+[—-]\s+")  # separadores " — " ou " - "

def get_html_from_wp():
    for url in WP_API_CANDIDATES:
        try:
            r = requests.get(url, timeout=30)
            if r.ok:
                data = r.json()
                if isinstance(data, list) and data:
                    html = data[0].get("content", {}).get("rendered", "")
                    if html:
                        return html
        except Exception:
            pass
    # fallback direto à página
    r = requests.get(SOURCE_URL, timeout=30)
    r.raise_for_status()
    return r.text

def text_clean(s):
    return re.sub(r"\s+", " ", (s or "").strip())

def heading_text(el):
    return text_clean(el.get_text(" ", strip=True))

def extract_tables_from_soup(soup):
    """Retorna lista de dicts {title, table} percorrendo a página principal.
    Regra pedida: o título-raiz é o heading imediatamente ANTES do parágrafo
    descritivo e da primeira tabela correspondente.
    """
    blocks = []

    # Estratégia: percorrer elementos, quando achar H2/H3 -> guardar como 'card_title';
    # Em seguida, se vier um parágrafo descritivo e <table>, capturar tabela sob esse "card_title".
    headings = soup.find_all(["h2", "h3"])
    for h in headings:
        card_title = heading_text(h)
        if not card_title:
            continue

        # Avança pelos irmãos até achar parágrafo e/ou tabela/botão
        p_desc = None
        link_btn = None
        local_tables = []

        ptr = h.find_next_sibling()
        while ptr and ptr.name not in ["h2", "h3"]:
            if ptr.name in ["p", "div"]:
                # procura botão "ver completo" dentro
                maybe_link = ptr.find("a", href=True)
                if not p_desc and ptr.name == "p":
                    p_desc = ptr
                if maybe_link and maybe_link.get_text(strip=True):
                    link_btn = maybe_link["href"]
            if ptr.name == "table":
                local_tables.append(ptr)
            ptr = ptr.find_next_sibling()

        # 1) Se houver tabelas locais, adiciona todas com card_title = heading atual
        for tb in local_tables:
            columns = [text_clean(th.get_text(" ", strip=True)) for th in tb.find_all("th")]
            rows = []
            for tr in tb.find_all("tr"):
                tds = tr.find_all(["td"])
                if not tds:
                    continue
                row = [text_clean(td.get_text(" ", strip=True)) for td in tds]
                # ignora linha que é exatamente igual ao header
                if columns and row and row == columns:
                    continue
                rows.append(row)
            if not columns:
                # tenta cabeçalho a partir da primeira linha
                thead = tb.find("thead")
                if thead:
                    ths = thead.find_all("th")
                    columns = [text_clean(t.get_text(" ", strip=True)) for t in ths]

            titulo = card_title  # a tabela local tem como título o heading
            if "estratégia med" in titulo.lower():
                continue  # regra do cliente: não extrair publicidade

            blocks.append({
                "titulo": titulo,
                "card_title": card_title,
                "columns": columns,
                "rows": rows,
            })

        # 2) Se houver botão/link, seguir e extrair tabelas da página interna
        if link_btn:
            try:
                r = requests.get(link_btn, timeout=30)
                if r.ok:
                    inner = BeautifulSoup(r.text, "html.parser")
                    # pega todas as tabelas
                    for tb in inner.find_all("table"):
                        # título específico da tabela (procura heading antes dela)
                        inner_title = card_title
                        prev_h = tb.find_previous(["h2", "h3"])
                        if prev_h:
                            ttxt = heading_text(prev_h)
                            # monta "card_title — subtítulo" (mas card_title permanece)
                            # para título da tabela:
                            if ttxt and ttxt != card_title:
                                inner_title = f"{card_title} — {ttxt}"

                        columns = [text_clean(th.get_text(" ", strip=True)) for th in tb.find_all("th")]
                        rows = []
                        for tr in tb.find_all("tr"):
                            tds = tr.find_all(["td"])
                            if not tds:
                                continue
                            row = [text_clean(td.get_text(" ", strip=True)) for td in tds]
                            if columns and row and row == columns:
                                continue
                            rows.append(row)

                        if "estratégia med" in (inner_title or "").lower():
                            continue

                        blocks.append({
                            "titulo": inner_title or card_title,
                            "card_title": card_title,   # << mantém o heading raiz para os CARDS
                            "columns": columns,
                            "rows": rows,
                        })
            except Exception:
                pass

    return blocks

def save_json(blocks, dest_json):
    now = dt.datetime.now(dt.timezone(dt.timedelta(hours=-3)))  # Brasília em -03
    payload = {
        "source": SOURCE_URL,
        "updated_at": now.isoformat(),
        "updated_at_br": now.strftime("%d/%m/%Y %H:%M"),
        "tabelas": blocks,
    }
    with io.open(dest_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def filename_from_title(title):
    base = re.sub(r"[^\w\s-]", "_", (title or "").strip()) or "tabela"
    if len(base) > 40:
        base = base[:40]
    return f"{base}.xlsx"

def generate_excel_files(blocks, consolidated_path, per_table_dir):
    # Consolidado
    with pd.ExcelWriter(consolidated_path, engine="openpyxl") as writer:
        for i, blk in enumerate(blocks, start=1):
            name = re.sub(r"[\\/*?:\[\]]", "_", blk.get("titulo") or f"Tabela {i}")
            name = (name[:28] + "…") if len(name) > 31 else name  # Excel sheet name max 31
            df = pd.DataFrame(blk.get("rows") or [])
            if blk.get("columns"):
                df.columns = blk["columns"]
            if df.empty:
                df = pd.DataFrame({"(sem dados)": []})
            df.to_excel(writer, sheet_name=name or f"Tab{i}", index=False)

    # Por tabela (modelos)
    for i, blk in enumerate(blocks, start=1):
        df = pd.DataFrame(blk.get("rows") or [])
        if blk.get("columns"):
            df.columns = blk["columns"]
        if df.empty:
            df = pd.DataFrame({"(sem dados)": []})
        fname = filename_from_title(blk.get("titulo"))
        dest = os.path.join(per_table_dir, fname)
        with pd.ExcelWriter(dest, engine="openpyxl") as w:
            df.to_excel(w, sheet_name="Modelo", index=False)

def main():
    print(f"[SCRAPER] Iniciando… (SCRIPT_VERSION={SCRIPT_VERSION})")
    html = get_html_from_wp()
    soup = BeautifulSoup(html, "html.parser")

    blocks = extract_tables_from_soup(soup)
    print(f"[SCRAPER] Tabelas capturadas: {len(blocks)}")

    # Salva JSON
    dest_json = os.path.join(DATA_DIR, "concorrencia_2026.json")
    save_json(blocks, dest_json)
    print(f"[SCRAPER] JSON: {dest_json}")

    # Copia para site/data
    site_json = os.path.join(SITE_DATA_DIR, "concorrencia_2026.json")
    os.replace(dest_json, site_json)
    print(f"[SCRAPER] JSON copiado para site/: {site_json}")

    # Excel
    consolidated = os.path.join(EXCEL_DIR, "concorrencia_2026.xlsx")
    generate_excel_files(blocks, consolidated, EXCEL_DIR)
    print(f"[SCRAPER] Modelos Excel em: {EXCEL_DIR}")

    # Copia excel p/ downloads (todos + individuais)
    # (move o consolidado)
    os.replace(consolidated, os.path.join(SITE_DL_DIR, "concorrencia_2026.xlsx"))
    for fname in os.listdir(EXCEL_DIR):
        if fname.lower().endswith(".xlsx"):
            src = os.path.join(EXCEL_DIR, fname)
            dst = os.path.join(SITE_DL_DIR, fname)
            os.replace(src, dst)
    print("[SCRAPER] Downloads atualizados.")

if __name__ == "__main__":
    main()
