// crm/public/js/label_overrides.js
// Frontend-only label override: "Comment" -> "Feedback" (no core server changes)

(() => {
  // Safety nets
  window.frappe = window.frappe || {};
  frappe._messages = frappe._messages || {};

  // 1) Central dictionary for exact-string overrides (case-sensitive)
  const OVERRIDES = {
    // base
    "Comment": "Feedback",
    "Comments": "Feedback",
    "Add Comment": "Add Feedback",
    "New Comment": "New Feedback",
    "Add a comment": "Add feedback",
    "Write a comment...": "Write feedback...",
    "View Comments": "View Feedback",
    "Show Comments": "Show Feedback",
    "Hide Comments": "Hide Feedback",
    "Comment count": "Feedback count",
    "Latest Comments": "Latest Feedback",
    "No Comments": "No Feedback",
    "Post Comment": "Post Feedback",
    "Edit Comment": "Edit Feedback",
    "Delete Comment": "Delete Feedback",

    // timeline / composer common
    "Add a comment to this document": "Add feedback to this document",
    "Comment added": "Feedback added",
    "Comment removed": "Feedback removed",
    "Reply to comment": "Reply to feedback",

    // button/placeholder variants sometimes used in CRM Vue
    "Write a comment": "Write feedback",
    "Add your comment": "Add your feedback",
    "Add your comment...": "Add your feedback...",

    // lower-case fallbacks (rare in product strings, but harmless)
    "comment": "feedback",
    "comments": "feedback",
  };

  // 2) Feed the translation dictionary used by frappe.__ at runtime
  Object.assign(frappe._messages, OVERRIDES);

  // 3) Wrap the global translator so future calls use the override first
  const originalTranslate =
    (typeof frappe !== "undefined" && typeof frappe.__ === "function" && frappe.__) ||
    (typeof window.__ === "function" && window.__) ||
    ((s) => s);

  function translated(txt, ...rest) {
    if (typeof txt === "string" && OVERRIDES.hasOwnProperty(txt)) {
      return OVERRIDES[txt];
    }
    return originalTranslate(txt, ...rest);
  }

  // expose wrapped translator
  frappe.__ = translated;
  window.__ = translated;

  // 4) Post-render DOM swaps for static nodes already printed
  //    (covers labels not passed through __, or third-party components)
  const EXACTS = new Map(Object.entries(OVERRIDES));

  // Text-node swapper (exact, preserves other content)
  function swapTextNode(node) {
    const t = node.nodeValue?.trim();
    if (!t) return;
    if (EXACTS.has(t)) {
      node.nodeValue = node.nodeValue.replace(t, EXACTS.get(t));
    }
  }

  // Attribute swapper for title/aria-label/tooltips
  function swapAttr(el, attr) {
    const v = el.getAttribute(attr);
    if (!v) return;
    const trimmed = v.trim();
    if (EXACTS.has(trimmed)) {
      el.setAttribute(attr, EXACTS.get(trimmed));
    }
  }

  function sweep(root = document) {
    // text nodes
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, null);
    let n;
    while ((n = walker.nextNode())) swapTextNode(n);

    // common attributes on interactive elements
    root.querySelectorAll("[title],[aria-label]").forEach((el) => {
      swapAttr(el, "title");
      swapAttr(el, "aria-label");
    });
  }

  // Initial sweep
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => sweep(document));
  } else {
    sweep(document);
  }

  // 5) MutationObserver to catch SPA/Vue renders (CRM frontend)
  const mo = new MutationObserver((mutations) => {
    for (const m of mutations) {
      if (m.type === "childList") {
        m.addedNodes.forEach((node) => {
          if (node.nodeType === Node.TEXT_NODE) {
            swapTextNode(node);
          } else if (node.nodeType === Node.ELEMENT_NODE) {
            sweep(node);
          }
        });
      } else if (m.type === "attributes") {
        if (m.target && (m.attributeName === "title" || m.attributeName === "aria-label")) {
          swapAttr(m.target, m.attributeName);
        }
      }
    }
  });

  mo.observe(document.documentElement, {
    childList: true,
    subtree: true,
    attributes: true,
    attributeFilter: ["title", "aria-label"],
  });

  // 6) Also patch common component label getters if present (defensive)
  //    Some components read from frappe.utils or desk internals; keep calls routed to our wrapper.
  if (frappe.utils) {
    // no-op placeholder to keep future hooks routed through frappe.__
    frappe.utils._original__ = originalTranslate;
  }

  // Console hint (once)
  if (!window.__COMMENT_OVERRIDE_BANNER__) {
    window.__COMMENT_OVERRIDE_BANNER__ = true;
    // eslint-disable-next-line no-console
    console.info("[CRM] UI label override active: “Comment/Comments” → “Feedback”.");
  }
})();
