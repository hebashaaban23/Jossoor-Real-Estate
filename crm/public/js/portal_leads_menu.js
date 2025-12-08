// portal_leads_menu.js
// Injects a "Leads ▼" dropdown into the portal sidebar.
// Put this file under your_app/public/js/ and include it via hooks (web_include_js).

(function () {
  function createLeadsDropdown() {
    // Define menu items (change hrefs to match your routes)
    const items = [
      { label: "New Lead", href: "/leads/new" },
      { label: "Buyer Leads", href: "/leads?type=buyer" },
      { label: "Seller Leads", href: "/leads?type=seller" },
      { label: "Renter Leads", href: "/leads?type=renter" },
      { label: "Unassigned Leads", href: "/leads?status=unassigned" },
      { label: "Hot / Priority Leads", href: "/leads?priority=high" },
      { label: "Follow-up Today", href: "/leads?follow_up=today" },
      { label: "Converted Leads", href: "/leads?status=converted" },
      { label: "Lost Leads", href: "/leads?status=lost" }
    ];

    // Find sidebar container - try a few selectors for compatibility
    const sidebarSelectors = [
      ".portal-sidebar",        // custom portals
      "#sidebar",               // older templates
      ".sidebar",               // generic
      ".layout-side"            // alternate layouts
    ];

    let sidebar;
    for (const sel of sidebarSelectors) {
      sidebar = document.querySelector(sel);
      if (sidebar) break;
    }

    // If no sidebar element found, try inserting near any nav element
    if (!sidebar) {
      sidebar = document.querySelector("nav") || document.body;
    }

    // Create dropdown wrapper
    const wrapper = document.createElement("div");
    wrapper.className = "portal-leads-dropdown";

    // Header (click to toggle)
    const header = document.createElement("a");
    header.href = "#";
    header.className = "portal-leads-header";
    header.innerHTML = 'Leads <span class="leads-caret">▾</span>';
    header.setAttribute("aria-expanded", "false");
    wrapper.appendChild(header);

    // Menu (hidden by default)
    const menu = document.createElement("ul");
    menu.className = "portal-leads-menu";
    menu.style.display = "none";

    items.forEach(i => {
      const li = document.createElement("li");
      const a = document.createElement("a");
      a.textContent = i.label;
      a.href = i.href;
      a.className = "portal-leads-item";
      li.appendChild(a);
      menu.appendChild(li);
    });

    wrapper.appendChild(menu);

    // Insert dropdown: place after an existing 'Leads' link if exists, otherwise append
    const existingLeadsLink = Array.from(document.querySelectorAll("a"))
      .find(a => a.textContent && a.textContent.trim().toLowerCase() === "leads");

    if (existingLeadsLink && existingLeadsLink.parentElement) {
      // replace or insert after existing link
      existingLeadsLink.parentElement.insertAdjacentElement("afterend", wrapper);
      // optionally hide the original link:
      // existingLeadsLink.style.display = "none";
    } else {
      // append to sidebar
      sidebar.appendChild(wrapper);
    }

    // Toggle behavior
    header.addEventListener("click", function (ev) {
      ev.preventDefault();
      const open = menu.style.display !== "none";
      menu.style.display = open ? "none" : "block";
      header.setAttribute("aria-expanded", String(!open));
      wrapper.classList.toggle("open", !open);
    });

    // Close when clicking outside
    document.addEventListener("click", function (ev) {
      if (!wrapper.contains(ev.target)) {
        menu.style.display = "none";
        header.setAttribute("aria-expanded", "false");
        wrapper.classList.remove("open");
      }
    });
  }

  // Run when DOM is ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", createLeadsDropdown);
  } else {
    createLeadsDropdown();
  }
})();
