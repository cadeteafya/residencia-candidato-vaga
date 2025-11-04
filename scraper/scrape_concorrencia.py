# -*- coding: utf-8 -*-
"""
Scraper — Concorrência 2026 (WP-API + HTML público; follow-button + filtros)
Regras novas:
  • Ignorar qualquer bloco cuja heading/título contenha "Estratégia MED".
  • Home agrupa por instituição (lógica de agrupamento está no site).
"""

import os, re, json, shutil
from datetime import datetime
from hashlib import md5
from urllib.parse import urljoin

import pandas as pd
import pytz, requests
from bs4 import BeautifulSoup, Tag, NavigableString

SCRIPT_VERSION = "2025-11-04-mix-v4-skip-estrategia"

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

# ---------------- utils ----------------
def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(EXCEL_DIR, exist_ok=True)
    if os.path.isdir(SITE_DIR):
        os.makedirs(SITE_DATA_DIR, exist_ok=True)
        os.makedirs(SITE_DL_DIR, exist_ok=True)

def now_brt(): return datetime.now(pytz.timezone("America/Sao_Paulo"))

def nrm(s: str) -> str:
    s = (s or "").strip()
    return re.sub(r"\s+", " ", s)

def text_of(el: Tag) -> str:
    if not el: return ""
    return " ".join(el.get_text(" ", strip=True).split())

def parse_html_table(tbl: Tag):
    cols = []
    thead = tbl.find("thead")
    if thead:
        cols = [text_of(th) for th in thead.find_all(["th","td"])]
    rows = []
    body = tbl.find("tbody") or tbl
    trs = body.find_all("tr")
    for i, tr in enumerate(trs):
        cells = tr.find_all(["td","th"])
        if not cells: continue
        row = [text_of(td) for td in cells]
        if not cols and i == 0:
            cols = row; continue
        rows.append(row)
    if not cols and rows:
        m = max(len(r) for r in rows)
        cols = [f"Col{i+1}" for i in range(m)]
    return cols, rows

def is_button_like(a: Tag) -> bool:
    if not a or a.name != "a" or not a.get("href"): return False
    t = text_of(a).lower()
    if re.search(r"(confira|ver|veja|acesse|consulte)", t): return True
    k = " ".join(a.get("class", [])).lower()
    if any(x in k for x in ["wp-block-button__link","btn","button"]): return True
    if (a.get("role") or "").lower() == "button": return True
    return False

def follow_url(href: str, base: str) -> str:
    try: return urljoin(base, href)
    except Exception: return href

def json_hash(data: dict) -> str:
    return md5(json.dumps(data, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

def write_json(path: str, payload: dict):
    with open(path,"w",encoding="utf-8") as f:
        json.dump(payload,f,ensure_ascii=False,indent=2)

def generate_excel(blocks, xlsx_all_path, individuals_dir):
    if not blocks:
        with pd.ExcelWriter(xlsx_all_path, engine="openpyxl") as wr:
            pd.DataFrame([{"mensagem":"Sem dados"}]).to_excel(wr,"Sem_dados",index=False)
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

# ---------------- filtros ----------------
def should_skip_title(title: str) -> bool:
    # ignora qualquer heading/título promocional
    return "estratégia med" in (title or "").lower()

# ---------------- WP API ----------------
def fetch_wp_content():
    slug = "concorrencia-residencia-medica"
    for endpoint in ("pages","posts"):
        try:
            url = f"{WP_API_BASE}/{endpoint}"
            r = requests.get(url, headers=HEADERS,
                params={"slug": slug, "_fields": "content.rendered,link,title.rendered", "per_page": 1}, timeout=60)
            if r.ok:
                arr = r.json()
                if isinstance(arr, list) and arr:
                    c = arr[0].get("content", {}).get("rendered")
                    if c:
                        print(f"[SCRAPER] WP API: conteúdo encontrado em /{endpoint}.")
                        return c
        except Exception as e:
            print(f"[SCRAPER] WP API /{endpoint} falhou:", e)
    return None

# ---------------- público ----------------
def fetch_public_soup():
    r = requests.get(FONTE_URL, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml"), r.url

# ---------------- deep page ----------------
def collect_from_detail_page(url: str):
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        blocks = []
        for h in soup.find_all(["h1","h2","h3"]):
            titulo = nrm(text_of(h))
            if should_skip_title(titulo):  # filtro novo
                continue
            # pega a primeira tabela depois do heading
            pn, found, steps = h.next_sibling, None, 0
            while pn and isinstance(pn,(Tag,NavigableString)) and steps < 300:
                if isinstance(pn,Tag) and pn.name in ["h1","h2","h3"]: break
                if isinstance(pn,Tag):
                    if pn.name == "table": found = pn; break
                    maybe = pn.find("table")
                    if maybe: found = maybe; break
                pn = pn.next_sibling; steps += 1
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
            base_t = nrm(text_of(soup.find("h1"))) or "Tabela"
            out=[]
            for i,tb in enumerate(all_tbls,1):
                cols, rows = parse_html_table(tb)
                if rows:
                    out.append({"titulo": f"{base_t} {i}", "columns": cols, "rows": rows})
            if out:
                print(f"[SCRAPER] Deep '{url}': {len(out)} tabela(s) sem headings.")
                return out
    except Exception as e:
        print(f"[SCRAPER] Falha deep '{url}': {e}")
    return []

# ---------------- helpers de varredura ----------------
def first_table_after(h: Tag):
    pn = h.next_sibling
    while pn and isinstance(pn,(Tag,NavigableString)):
        if isinstance(pn,Tag) and pn.name in ["h2","h3"]: break
        if isinstance(pn,Tag):
            if pn.name=="table": return pn
            maybe = pn.find("table")
            if maybe: return maybe
        pn = pn.next_sibling
    return None

def scan_for_button_from(node: Tag, base_url: str, limit=200):
    steps, pn = 0, node.next_sibling
    while pn and isinstance(pn,(Tag,NavigableString)) and steps < limit:
        if isinstance(pn,Tag) and pn.name in ["h2","h3"]: break
        if isinstance(pn,Tag):
            for a in pn.find_all("a", href=True):
                if is_button_like(a):
                    return follow_url(a["href"], base_url)
        pn = pn.next_sibling; steps += 1
    return None

def find_button_near_title(public_soup: BeautifulSoup, base_url: str, title: str):
    tgt = nrm(title).lower()
    cand = []
    for h in public_soup.find_all(["h2","h3"]):
        ht = nrm(text_of(h)).lower()
        if ht == tgt or tgt in ht or ht in tgt:
            cand.append(h)
    for h in cand:
        tbl = first_table_after(h)
        # varre a partir do heading
        btn = scan_for_button_from(h, base_url, 200)
        if btn: return btn
        # varre a partir da tabela (se houver)
        if tbl:
            btn = scan_for_button_from(tbl, base_url, 200)
            if btn: return btn
        # container pai
        parent = h.find_parent(["section","div","article"])
        if parent:
            for a in parent.find_all("a", href=True):
                if is_button_like(a):
                    return follow_url(a["href"], base_url)
    return None

# ---------------- coleta blocos ----------------
def collect_from_wpapi(html_wp: str):
    soup = BeautifulSoup(html_wp, "lxml")
    out=[]
    for h in soup.find_all(["h2","h3"]):
        titulo = nrm(text_of(h))
        if not titulo or should_skip_title(titulo):  # filtro novo
            continue
        tbl = first_table_after(h)
        if not tbl: continue
        cols, rows = parse_html_table(tbl)
        if rows:
            out.append({"titulo": titulo, "columns": cols, "rows": rows})
    return out

def collect_from_public(public_soup: BeautifulSoup):
    out=[]
    for h in public_soup.find_all(["h2","h3"]):
        titulo = nrm(text_of(h))
        if not titulo or should_skip_title(titulo):  # filtro novo
            continue
        tbl = first_table_after(h)
        if not tbl: continue
        cols, rows = parse_html_table(tbl)
        if rows:
            out.append({"titulo": titulo, "columns": cols, "rows": rows})
    return out

def prefix_if_needed(parent: str, blocks: list):
    out=[]
    pl = nrm(parent).lower()
    for b in blocks:
        t = b.get("titulo") or parent
        if pl and pl not in nrm(t).lower():
            t = f"{parent} — {nrm(t)}"
        out.append({"titulo": nrm(t), "columns": b["columns"], "rows": b["rows"]})
    return out

# ---------------- main ----------------
def main():
    print(f"[SCRAPER] Iniciando scraping de concorrência 2026… (SCRIPT_VERSION={SCRIPT_VERSION})")
    ensure_dirs()

    html_wp = fetch_wp_content()
    public_soup, public_base = fetch_public_soup()

    blocks = collect_from_wpapi(html_wp) if html_wp else collect_from_public(public_soup)
    print(f"[SCRAPER] Blocos iniciais: {len(blocks)}")

    enriched=[]
    for b in blocks:
        titulo = b["titulo"]
        btn = find_button_near_title(public_soup, public_base, titulo)
        if btn:
            print(f"[SCRAPER] '{titulo}': botão detectado → {btn}")
            deep = collect_from_detail_page(btn)
            if deep:
                enriched.extend(prefix_if_needed(titulo, deep))
                continue
        enriched.append(b)

    print(f"[SCRAPER] Tabelas consolidadas (após follow-button): {len(enriched)}")

    dt = now_brt()
    payload = {
        "fonte_url": FONTE_URL,
        "updated_at_iso": dt.isoformat(),
        "updated_at_br": dt.strftime("%d/%m/%Y %H:%M"),
        "tabelas": enriched,
    }

    json_path = os.path.join(DATA_DIR, JSON_NAME)
    old_hash = None
    if os.path.exists(json_path):
        try:
            with open(json_path,"r",encoding="utf-8") as f:
                old_hash = json_hash(json.load(f))
        except Exception: pass
    new_hash = json_hash(payload)

    write_json(json_path, payload)
    print(f"[SCRAPER] JSON atualizado em: {json_path}")
    if os.path.isdir(SITE_DIR):
        write_json(os.path.join(SITE_DATA_DIR, JSON_NAME), payload)
        print("[SCRAPER] JSON copiado para site/.")

    xlsx_all = os.path.join(EXCEL_DIR, XLSX_ALL)
    if old_hash != new_hash:
        generate_excel(enriched, xlsx_all, EXCEL_DIR)
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
