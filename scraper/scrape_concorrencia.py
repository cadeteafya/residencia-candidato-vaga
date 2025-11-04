# -*- coding: utf-8 -*-
"""
Scraper — Concorrência para Residência Médica 2026 (follow button)
Fonte: med.estrategia.com
Estratégia:
  1) Prioriza o conteúdo renderizado via WordPress REST API (content.rendered).
  2) Fallback: HTML público.
  3) Para cada (H2/H3 + tabela):
      - Procura "botão" imediatamente depois da tabela (mesma seção, antes do próximo H2/H3).
      - Se existir, acessa o link e extrai TODAS as tabelas da página de destino (com seus títulos).
      - Se não existir, usa a própria tabela.
  4) Gera JSON + modelos Excel (consolidado e individuais).
"""

import os
import json
import re
from datetime import datetime
import pytz
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup, NavigableString, Tag
import pandas as pd
from hashlib import md5
import shutil

SCRIPT_VERSION = "2025-11-04-follow-btn"

# URL pública
FONTE_URL = "https://med.estrategia.com/portal/residencia-medica/concorrencia-residencia-medica/"

# Base do REST API do WP (área /portal/)
WP_API_BASE = "https://med.estrategia.com/portal/wp-json/wp/v2"

# === Caminhos ===
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # /scraper
REPO_ROOT = os.path.dirname(ROOT_DIR)  # raiz do repositório
OUTPUT_DIR = os.path.join(REPO_ROOT, "output")
DATA_DIR = os.path.join(OUTPUT_DIR, "data")
EXCEL_DIR = os.path.join(OUTPUT_DIR, "excel")
SITE_DIR = os.path.join(REPO_ROOT, "site")
SITE_DATA_DIR = os.path.join(SITE_DIR, "data")
SITE_DOWNLOADS_DIR = os.path.join(SITE_DIR, "downloads")

JSON_FILENAME = "concorrencia_2026.json"
XLSX_FILENAME = "concorrencia_2026.xlsx"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": FONTE_URL,
}

# ---------- Utilidades ----------
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

def is_button_like(a: Tag) -> bool:
    """
    Heurística para identificar "botão" após a tabela que leva ao conteúdo completo.
    """
    if a is None or a.name != "a":
        return False
    txt = text_of(a).lower()
    if re.search(r"(confira|ver|veja|acesse|consulte)", txt):
        return True
    cl = " ".join(a.get("class", [])).lower()
    if any(k in cl for k in ["btn", "button", "wp-block-button__link"]):
        return True
    # role/button
    if (a.get("role") or "").lower() == "button":
        return True
    return False

def resolve_url(href: str, base_url: str) -> str:
    try:
        return urljoin(base_url, href)
    except Exception:
        return href

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

# ---------- Coleta via WordPress REST ----------
def fetch_wp_content():
    """
    Tenta recuperar o HTML renderizado via WP REST API:
    1) /pages?slug=concorrencia-residencia-medica
    2) /posts?slug=concorrencia-residencia-medica
    3) /search?subtype=page,post
    Retorna string HTML (content.rendered) ou None.
    """
    slug = "concorrencia-residencia-medica"

    # 1) pages by slug
    try:
        url = f"{WP_API_BASE}/pages"
        params = {"slug": slug, "_fields": "content.rendered,link,title.rendered", "per_page": 1}
        r = requests.get(url, headers=HEADERS, params=params, timeout=60)
        if r.ok:
            arr = r.json()
            if isinstance(arr, list) and arr:
                content = arr[0].get("content", {}).get("rendered")
                if content:
                    print("[SCRAPER] WP API: conteúdo encontrado em /pages.")
                    return content
    except Exception as e:
        print(f"[SCRAPER] WP API /pages falhou: {e}")

    # 2) posts by slug
    try:
        url = f"{WP_API_BASE}/posts"
        params = {"slug": slug, "_fields": "content.rendered,link,title.rendered", "per_page": 1}
        r = requests.get(url, headers=HEADERS, params=params, timeout=60)
        if r.ok:
            arr = r.json()
            if isinstance(arr, list) and arr:
                content = arr[0].get("content", {}).get("rendered")
                if content:
                    print("[SCRAPER] WP API: conteúdo encontrado em /posts.")
                    return content
    except Exception as e:
        print(f"[SCRAPER] WP API /posts falhou: {e}")

    # 3) search
    try:
        url = f"{WP_API_BASE}/search"
        params = {"search": slug, "subtype": "page,post", "per_page": 1}
        r = requests.get(url, headers=HEADERS, params=params, timeout=60)
        if r.ok:
            arr = r.json()
            if isinstance(arr, list) and arr:
                item = arr[0]
                subtype = item.get("subtype")
                obj_id = item.get("id")
                if subtype in ("page", "post") and obj_id:
                    endpoint = "pages" if subtype == "page" else "posts"
                    url = f"{WP_API_BASE}/{endpoint}/{obj_id}"
                    params = {"_fields": "content.rendered,link,title.rendered"}
                    r2 = requests.get(url, headers=HEADERS, params=params, timeout=60)
                    if r2.ok:
                        data = r2.json()
                        content = data.get("content", {}).get("rendered")
                        if content:
                            print(f"[SCRAPER] WP API: conteúdo encontrado via search em /{endpoint}/{obj_id}.")
                            return content
    except Exception as e:
        print(f"[SCRAPER] WP API /search falhou: {e}")

    return None

# ---------- Parse de tabelas (página principal e páginas detalhadas) ----------
def collect_blocks_from_soup(soup: BeautifulSoup, base_url: str):
    """
    Retorna lista de dicts:
      - se NÃO houver botão após a tabela: {"titulo", "columns", "rows"}
      - se HOUVER botão: coleta tabelas da página destino e retorna os blocos detalhados.
    """
    results = []
    headings = soup.find_all(["h2", "h3"])
    for h in headings:
        titulo = sanitize_title(text_of(h))
        if not titulo:
            continue

        # caminha nos irmãos até próxima heading
        pn = h.next_sibling
        found_table = None
        button_link = None

        while pn and isinstance(pn, (Tag, NavigableString)):
            if isinstance(pn, Tag) and pn.name in ["h2", "h3"]:
                break
            if isinstance(pn, Tag):
                # tabela
                if pn.name == "table":
                    found_table = pn
                else:
                    maybe = pn.find("table")
                    if maybe and not found_table:
                        found_table = maybe
                # botão
                for a in pn.find_all("a", href=True):
                    if is_button_like(a):
                        button_link = resolve_url(a["href"], base_url)
                        break
            if button_link:
                # não precisamos varrer mais
                pass
            pn = pn.next_sibling

        if found_table is None:
            continue

        if button_link:
            print(f"[SCRAPER] '{titulo}': botão detectado → seguindo link: {button_link}")
            deep_blocks = collect_from_detail_page(button_link)
            # se nada veio do deep, usa a tabela resumida como fallback
            if deep_blocks:
                results.extend(prefix_titles_if_needed(titulo, deep_blocks))
                continue

        # sem botão (ou deep vazio) → usa a própria tabela
        cols, rows = parse_html_table(found_table)
        if rows:
            results.append({"titulo": titulo, "columns": cols, "rows": rows})

    return results

def prefix_titles_if_needed(parent_title: str, blocks: list):
    """
    Em algumas páginas detalhadas o título das tabelas já é autoexplicativo.
    Se o título do bloco já contém o nome da instituição, mantém.
    Senão, prefixa como 'INSTITUIÇÃO — Subtítulo'.
    """
    out = []
    pt = (parent_title or "").lower()
    for b in blocks:
        t = b.get("titulo") or parent_title
        if pt and pt not in (t or "").lower():
            t = f"{parent_title} — {t}"
        out.append({"titulo": sanitize_title(t), "columns": b["columns"], "rows": b["rows"]})
    return out

def collect_from_detail_page(url: str):
    """
    Abre a página detalhada e extrai todas as (H2/H3 + tabela).
    Se não houver headings, tenta todas as <table> e usa o <h1> como título base.
    """
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # tentativa 1: headings + tabela
        blocks = []
        headings = soup.find_all(["h1", "h2", "h3"])
        for h in headings:
            titulo = sanitize_title(text_of(h))
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
            print(f"[SCRAPER] Deep page '{url}': {len(blocks)} tabela(s) via headings.")
            return blocks

        # tentativa 2: nenhuma heading → pega todas as tabelas
        all_tables = soup.find_all("table")
        if all_tables:
            base_title = None
            h1 = soup.find("h1")
            if h1:
                base_title = sanitize_title(text_of(h1))
            tmp = []
            for i, tb in enumerate(all_tables, start=1):
                cols, rows = parse_html_table(tb)
                if not rows:
                    continue
                t = f"{base_title or 'Tabela'} {i}"
                tmp.append({"titulo": t, "columns": cols, "rows": rows})
            if tmp:
                print(f"[SCRAPER] Deep page '{url}': {len(tmp)} tabela(s) sem headings.")
                return tmp

    except Exception as e:
        print(f"[SCRAPER] Falha ao coletar deep page '{url}': {e}")

    return []

# ---------- Main ----------
def main():
    print(f"[SCRAPER] Iniciando scraping de concorrência 2026… (SCRIPT_VERSION={SCRIPT_VERSION})")
    ensure_dirs()

    html = fetch_wp_content()
    used_wpapi = False

    if html:
        used_wpapi = True
        print("[SCRAPER] Usando conteúdo do WordPress REST API.")
        soup = BeautifulSoup(html, "lxml")
        base_url = FONTE_URL
    else:
        print("[SCRAPER] WP API não retornou conteúdo — usando HTML público.")
        resp = requests.get(FONTE_URL, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        base_url = resp.url

    blocks = collect_blocks_from_soup(soup, base_url)
    print(f"[SCRAPER] Tabelas consolidadas (após follow-button): {len(blocks)}")

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

    # Sempre grava o JSON (mesmo vazio) e copia para o site
    write_json(payload, json_path)
    print(f"[SCRAPER] JSON atualizado em: {json_path}")
    if os.path.isdir(SITE_DIR):
        write_json(payload, os.path.join(SITE_DATA_DIR, JSON_FILENAME))
        print(f"[SCRAPER] JSON copiado para o site: {os.path.join(SITE_DATA_DIR, JSON_FILENAME)}")

    consolidated_excel = os.path.join(EXCEL_DIR, XLSX_FILENAME)

    if len(blocks) == 0:
        with pd.ExcelWriter(consolidated_excel, engine="openpyxl") as writer:
            df = pd.DataFrame([{"mensagem": "Sem dados no momento"}])
            df.to_excel(writer, sheet_name="Sem_dados", index=False)

        if os.path.isdir(SITE_DIR):
            os.makedirs(SITE_DOWNLOADS_DIR, exist_ok=True)
            shutil.copy2(consolidated_excel, os.path.join(SITE_DOWNLOADS_DIR, XLSX_FILENAME))
            print(f"[SCRAPER] Placeholder de Excel (Sem_dados) copiado para o site.")
        print("[SCRAPER] Nenhuma tabela encontrada. Finalizado sem erro.")
        return

    # (Re)gera modelos somente se mudou
    if old_hash != new_hash:
        generate_excel_files(blocks, consolidated_excel, EXCEL_DIR)
        print(f"[SCRAPER] Modelos Excel gerados em: {EXCEL_DIR}")

        if os.path.isdir(SITE_DIR):
            os.makedirs(SITE_DOWNLOADS_DIR, exist_ok=True)
            shutil.copy2(consolidated_excel, os.path.join(SITE_DOWNLOADS_DIR, XLSX_FILENAME))
            # individuais
            for f in os.listdir(EXCEL_DIR):
                if f.lower().endswith(".xlsx"):
                    shutil.copy2(os.path.join(EXCEL_DIR, f), os.path.join(SITE_DOWNLOADS_DIR, f))
            print(f"[SCRAPER] Modelos copiados para o site: {SITE_DOWNLOADS_DIR}")
    else:
        print("[SCRAPER] Dados idênticos ao último run — mantendo modelos atuais.")

    print("[SCRAPER] Concluído com sucesso.")

if __name__ == "__main__":
    main()
