/* site/assets/js/app.js — versão sem SheetJS (sem fallback)
 * Baixa modelos estáticos em /downloads e renderiza as tabelas do JSON.
 */

(function () {
  const cfg = (window.CONCORRENCIA_CONFIG || {});
  const JSON_PATH = cfg.jsonPath || "data/concorrencia_2026.json";
  const ALL_XLSX_NAME = cfg.allExcelFilename || "concorrencia_2026.xlsx";
  const DOWNLOADS_PATH = cfg.downloadsPath || "downloads/";

  const $updatedAt = document.getElementById("updatedAt");
  const $blocks = document.getElementById("blocks");
  const $btnExtrairTodos = document.getElementById("btnExtrairTodos");
  const $linkFonte = document.getElementById("linkFonte");

  // utils
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

  // mesmo algoritmo de nome do Python (garante que o link bata com o arquivo gerado)
  function sanitizeFileNameFromTitle(title) {
    const base = (title || "").toString().replace(/[^\w\s-]/g, "_").trim() || "tabela";
    const cut = base.length > 40 ? base.slice(0, 40) : base;
    return `${cut}.xlsx`;
  }

  async function ensureAndDownload(url) {
    try {
      const resp = await fetch(url, { method: "HEAD", cache: "no-store" });
      if (resp.ok) {
        window.location.href = url;
      } else {
        alert("Arquivo ainda não está disponível. Tente novamente em alguns instantes.");
      }
    } catch (e) {
      alert("Não foi possível acessar o arquivo agora. Tente novamente mais tarde.");
    }
  }

  // render
  function renderPage(payload) {
    if ($updatedAt) {
      const txt = payload.updated_at_br || "—";
      $updatedAt.textContent = `Atualizado em: ${txt}`;
    }
    if ($linkFonte && payload.fonte_url) {
      $linkFonte.href = payload.fonte_url;
    }

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

      // tabela visual
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

      // download do modelo individual
      $btn.addEventListener("click", () => {
        const filename = sanitizeFileNameFromTitle(titulo);
        ensureAndDownload(`${DOWNLOADS_PATH}${filename}`);
      });
    });

    // download do modelo consolidado
    if ($btnExtrairTodos) {
      $btnExtrairTodos.onclick = () => ensureAndDownload(`${DOWNLOADS_PATH}${ALL_XLSX_NAME}`);
    }
  }

  async function boot() {
    try {
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

  document.readyState === "loading"
    ? document.addEventListener("DOMContentLoaded", boot)
    : boot();
})();
