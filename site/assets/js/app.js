/* site/assets/js/app.js
 * Renderização da página e download dos modelos Excel (estáticos em /downloads),
 * com fallback para geração client-side via SheetJS se o arquivo não existir.
 */

(function () {
  const cfg = (window.CONCORRENCIA_CONFIG || {});
  const JSON_PATH = cfg.jsonPath || "data/concorrencia_2026.json";
  const ALL_XLSX_NAME = cfg.allExcelFilename || "concorrencia_2026.xlsx";
  const DOWNLOADS_PATH = cfg.downloadsPath || "downloads/"; // novo: onde ficam os modelos

  const $updatedAt = document.getElementById("updatedAt");
  const $blocks = document.getElementById("blocks");
  const $btnExtrairTodos = document.getElementById("btnExtrairTodos");
  const $linkFonte = document.getElementById("linkFonte");

  // ---------- Utils ----------
  function el(tag, attrs = {}, children = []) {
    const node = document.createElement(tag);
    Object.entries(attrs).forEach(([k, v]) => {
      if (k === "class") node.className = v;
      else if (k === "text") node.textContent = v;
      else if (k === "html") node.innerHTML = v;
      else node.setAttribute(k, v);
    });
    (Array.isArray(children) ? children : [children]).forEach((c) => {
      if (c == null) return;
      if (typeof c === "string") node.appendChild(document.createTextNode(c));
      else node.appendChild(c);
    });
    return node;
  }

  // precisa **casar** com o Python:
  // file_name = (re.sub(r"[^\w\s-]", "_", titulo).strip() or "tabela")[:40] + ".xlsx"
  function sanitizeFileNameFromTitle(title) {
    const base = (title || "").toString().replace(/[^\w\s-]/g, "_").trim() || "tabela";
    const cut = base.length > 40 ? base.slice(0, 40) : base;
    return `${cut}.xlsx`;
  }

  function tableToAOA(columns, rows) {
    const header = [columns];
    const body = rows.map(r => {
      if (r.length < columns.length) return r.concat(Array(columns.length - r.length).fill(""));
      if (r.length > columns.length) return r.slice(0, columns.length);
      return r;
    });
    return header.concat(body);
  }

  function sanitizeSheetName(name, used = new Set()) {
    // apenas para fallback SheetJS
    let n = (name || "Planilha").toString().replace(/[:\\\/\?\*\[\]]/g, " ").trim();
    if (!n) n = "Planilha";
    if (n.length > 31) n = n.slice(0, 31).trim();
    let base = n, i = 1;
    while (used.has(n)) {
      const suffix = ` (${i++})`;
      const max = 31 - suffix.length;
      n = (base.length > max ? base.slice(0, max) : base) + suffix;
    }
    used.add(n);
    return n;
  }

  function aoaToWorkbook(sheetsMap) {
    const wb = XLSX.utils.book_new();
    const used = new Set();
    Object.entries(sheetsMap).forEach(([name, aoa]) => {
      const ws = XLSX.utils.aoa_to_sheet(aoa);
      const safe = sanitizeSheetName(name, used);
      XLSX.utils.book_append_sheet(wb, ws, safe);
    });
    return wb;
  }

  function downloadWorkbook(wb, filename) {
    XLSX.writeFile(wb, filename, { compression: true });
  }

  // Tenta baixar o arquivo estático; se não existir (404), usa fallback SheetJS
  async function tryDownloadStaticOrFallback(url, fallbackFn) {
    try {
      const resp = await fetch(url, { method: "HEAD", cache: "no-store" });
      if (resp.ok) {
        window.location.href = url; // baixa o arquivo pronto
        return;
      }
      // 404 ou outro erro -> fallback
      if (typeof fallbackFn === "function") fallbackFn();
    } catch (e) {
      if (typeof fallbackFn === "function") fallbackFn();
    }
  }

  // ---------- Render ----------
  function renderPage(payload) {
    // Atualizado em + link fonte
    if ($updatedAt) {
      const txt = payload.updated_at_br || "—";
      $updatedAt.textContent = `Atualizado em: ${txt}`;
    }
    if ($linkFonte && payload.fonte_url) {
      $linkFonte.href = payload.fonte_url;
    }

    // Render blocks
    $blocks.innerHTML = "";
    const blocks = Array.isArray(payload.tabelas) ? payload.tabelas : [];

    if (!blocks.length) {
      $blocks.appendChild(el("div", { class: "muted" , text: "Nenhuma tabela encontrada no momento." }));
      return;
    }

    blocks.forEach((b, idx) => {
      const titulo = b.titulo || `Bloco ${idx + 1}`;
      const columns = Array.isArray(b.columns) ? b.columns : [];
      const rows = Array.isArray(b.rows) ? b.rows : [];

      const $title = el("h2", { text: titulo });

      // Botão: baixar modelo individual (estático em /downloads)
      const $btn = el("button", { class: "btn", text: "Extrair esta tabela", type: "button" });
      const $actions = el("div", { class: "block-actions" }, [$btn]);

      // Tabela visual
      const $table = el("table");
      const $thead = el("thead");
      const $trHead = el("tr");
      columns.forEach((c) => $trHead.appendChild(el("th", { text: c })));
      $thead.appendChild($trHead);

      const $tbody = el("tbody");
      rows.forEach((r) => {
        const $tr = el("tr");
        columns.forEach((_, i) => $tr.appendChild(el("td", { text: (r[i] ?? "") })));
        $tbody.appendChild($tr);
      });

      $table.appendChild($thead);
      $table.appendChild($tbody);

      const $wrap = el("div", { class: "table-wrap" }, [$table]);
      const $block = el("section", { class: "block", "aria-label": titulo }, [$title, $actions, $wrap]);
      $blocks.appendChild($block);

      // Handler: baixar estático ou gerar fallback
      $btn.addEventListener("click", async () => {
        const filename = sanitizeFileNameFromTitle(titulo); // deve bater com o Python
        const url = `${DOWNLOADS_PATH}${filename}`;

        await tryDownloadStaticOrFallback(url, () => {
          // Fallback: gerar no client apenas esta tabela
          if (typeof XLSX === "undefined") {
            alert("Arquivo estático não encontrado e SheetJS indisponível para fallback.");
            return;
          }
          const aoa = tableToAOA(columns, rows);
          const wb = aoaToWorkbook({ [titulo]: aoa });
          const dlName = filename; // usa mesmo nome
          downloadWorkbook(wb, dlName);
        });
      });
    });

    // Extrair todos: baixar modelo consolidado estático; fallback = gerar no client
    if ($btnExtrairTodos) {
      $btnExtrairTodos.onclick = async () => {
        const url = `${DOWNLOADS_PATH}${ALL_XLSX_NAME}`;

        await tryDownloadStaticOrFallback(url, () => {
          // Fallback: gerar workbook com todas as tabelas
          if (typeof XLSX === "undefined") {
            alert("Arquivo estático não encontrado e SheetJS indisponível para fallback.");
            return;
          }
          const map = {};
          blocks.forEach((b) => {
            const columns = Array.isArray(b.columns) ? b.columns : [];
            const rows = Array.isArray(b.rows) ? b.rows : [];
            const aoa = tableToAOA(columns, rows);
            map[b.titulo || "Tabela"] = aoa;
          });
          const wb = aoaToWorkbook(map);
          downloadWorkbook(wb, ALL_XLSX_NAME);
        });
      };
    }
  }

  async function boot() {
    try {
      const resp = await fetch(JSON_PATH, { cache: "no-store" });
      if (!resp.ok) throw new Error(`Falha ao carregar JSON (${resp.status})`);
      const payload = await resp.json();

      // torna downloadsPath configurável em runtime, se quiser
      if (!window.CONCORRENCIA_CONFIG) window.CONCORRENCIA_CONFIG = {};
      window.CONCORRENCIA_CONFIG.downloadsPath = DOWNLOADS_PATH;

      renderPage(payload);
    } catch (err) {
      console.error(err);
      if ($blocks) {
        $blocks.innerHTML = "";
        $blocks.appendChild(
          el("div", { class: "muted", text: "Erro ao carregar os dados. Tente novamente mais tarde." })
        );
      }
    }
  }

  document.readyState === "loading"
    ? document.addEventListener("DOMContentLoaded", boot)
    : boot();
})();
