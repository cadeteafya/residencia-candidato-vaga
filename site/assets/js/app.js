/* site/assets/js/app.js
 * Renderização e exportação client-side (SheetJS) para a página
 * "Concorrência para Residência Médica 2026"
 */

(function () {
  const cfg = (window.CONCORRENCIA_CONFIG || {});
  const JSON_PATH = cfg.jsonPath || "data/concorrencia_2026.json";
  const ALL_XLSX_NAME = cfg.allExcelFilename || "concorrencia_2026.xlsx";

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

  function sanitizeSheetName(name, used = new Set()) {
    // Excel sheet name constraints: max 31 chars, cannot contain: : \ / ? * [ ]
    let n = (name || "Planilha").toString()
      .replace(/[:\\\/\?\*\[\]]/g, " ")
      .trim();
    if (n.length === 0) n = "Planilha";
    if (n.length > 31) n = n.slice(0, 31).trim();

    // desambiguar duplicados
    let base = n;
    let i = 1;
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

  function tableToAOA(columns, rows) {
    const header = [columns];
    const body = rows.map(r => {
      // Garante que o número de colunas se mantenha
      if (r.length < columns.length) {
        return r.concat(Array(columns.length - r.length).fill(""));
      } else if (r.length > columns.length) {
        return r.slice(0, columns.length);
      }
      return r;
    });
    return header.concat(body);
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

      // Handler: exportar somente esta tabela
      $btn.addEventListener("click", () => {
        const aoa = tableToAOA(columns, rows);
        const wb = aoaToWorkbook({ [titulo]: aoa });
        const filename = `${titulo.replace(/[^\w\s.-]/g, "_").trim() || "tabela"}.xlsx`;
        downloadWorkbook(wb, filename);
      });
    });

    // Extrair todos: um workbook com uma aba por instituição
    if ($btnExtrairTodos) {
      $btnExtrairTodos.onclick = () => {
        const map = {};
        blocks.forEach((b) => {
          const t = b.titulo || "Tabela";
          map[t] = tableToAOA(b.columns || [], b.rows || []);
        });
        const wb = aoaToWorkbook(map);
        downloadWorkbook(wb, ALL_XLSX_NAME);
      };
    }
  }

  async function boot() {
    try {
      // Garantir que SheetJS foi carregado
      if (typeof XLSX === "undefined") {
        console.error("SheetJS (xlsx.full.min.js) não encontrado.");
      }
      const resp = await fetch(JSON_PATH, { cache: "no-store" });
      if (!resp.ok) throw new Error(`Falha ao carregar JSON (${resp.status})`);
      const payload = await resp.json();
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

  // Start
  document.readyState === "loading"
    ? document.addEventListener("DOMContentLoaded", boot)
    : boot();
})();
