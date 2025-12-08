frappe.pages['new-leads-dashboard'].on_page_load = function (wrapper) {
    const root = document.createElement("div");
    root.id = "new-leads-app";
    root.style.padding = "16px";
    root.style.fontFamily = "system-ui,-apple-system,BlinkMacSystemFont,'Inter',sans-serif";
    wrapper.appendChild(root);

    root.innerHTML = `
        <div style="max-width:900px">
            <h2 style="font-size:18px; font-weight:600; margin:0 0 12px;">
                New Leads Dashboard
            </h2>

            <div style="
                display:flex;
                flex-wrap:wrap;
                gap:12px;
                margin-bottom:16px;
                align-items:flex-end;
            ">
                <div style="display:flex; flex-direction:column;">
                    <label style="font-size:12px;color:#555;">From Date</label>
                    <input type="date" id="nl-from" style="padding:6px 8px;border:1px solid #ccc;border-radius:6px;min-width:140px;">
                </div>

                <div style="display:flex; flex-direction:column;">
                    <label style="font-size:12px;color:#555;">To Date</label>
                    <input type="date" id="nl-to" style="padding:6px 8px;border:1px solid #ccc;border-radius:6px;min-width:140px;">
                </div>

                <div style="display:flex; flex-direction:column;">
                    <label style="font-size:12px;color:#555;">Owner (optional)</label>
                    <input type="text" id="nl-owner" placeholder="user@email.com" style="padding:6px 8px;border:1px solid #ccc;border-radius:6px;min-width:180px;">
                </div>

                <button id="nl-run" style="
                    background:#3b82f6;
                    color:#fff;
                    border:0;
                    border-radius:6px;
                    padding:8px 12px;
                    font-size:13px;
                    font-weight:500;
                    cursor:pointer;
                ">
                    Run
                </button>
            </div>

            <div id="nl-summary" style="
                background:#fff;
                border:1px solid #eee;
                box-shadow:0 4px 16px rgba(0,0,0,0.05);
                border-radius:10px;
                padding:16px;
                margin-bottom:16px;
                font-size:14px;
                line-height:1.4;
            ">
                <div><strong>Total New Leads:</strong> <span id="nl-total">-</span></div>
                <div style="color:#666;margin-top:4px;font-size:12px;">
                    <span id="nl-range"></span>
                </div>
            </div>

            <table id="nl-table" style="border-collapse:collapse;width:100%;background:#fff;border:1px solid #eee;border-radius:10px;overflow:hidden;">
                <thead style="background:#fafafa;">
                    <tr>
                        <th style="text-align:left;padding:8px 12px;border-bottom:1px solid #eee;font-size:12px;color:#555;">Date</th>
                        <th style="text-align:right;padding:8px 12px;border-bottom:1px solid #eee;font-size:12px;color:#555;">New Leads</th>
                    </tr>
                </thead>
                <tbody id="nl-body"></tbody>
            </table>
        </div>
    `;

    // defaults
    const today = frappe.datetime.get_today();
    const last7 = frappe.datetime.add_days(today, -7);

    root.querySelector('#nl-from').value = last7;
    root.querySelector('#nl-to').value = today;

    // run button
    root.querySelector('#nl-run').addEventListener('click', async () => {
        const from_date = root.querySelector('#nl-from').value || null;
        const to_date = root.querySelector('#nl-to').value || null;
        const owner = root.querySelector('#nl-owner').value || null;

        // call backend KPI (which we already tested in console)
        const r = await frappe.call({
            method: "crm.fcrm.api.new_leads_kpi.total_new_leads",
            args: { from_date, to_date, owner }
        });

        if (!r.message) {
            return;
        }

        const msg = r.message;
        const tbody = root.querySelector('#nl-body');
        tbody.innerHTML = "";

        // summary header
        root.querySelector('#nl-total').textContent = msg.total_new_leads || 0;

        let rangeText = "";
        if (msg.from_date && msg.to_date) {
            rangeText = `${msg.from_date} → ${msg.to_date}`;
        } else if (msg.from_date) {
            rangeText = `Since ${msg.from_date}`;
        } else if (msg.to_date) {
            rangeText = `Until ${msg.to_date}`;
        }
        root.querySelector('#nl-range').textContent = rangeText;

        // daily breakdown rows
        const breakdown = msg.daily_breakdown || [];
        breakdown.forEach(row => {
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td style="padding:8px 12px;border-bottom:1px solid #eee;font-size:13px;color:#333;">
                    ${row.creation_date}
                </td>
                <td style="padding:8px 12px;border-bottom:1px solid #eee;font-size:13px;color:#333;text-align:right;">
                    ${row.lead_count}
                </td>
            `;
            tbody.appendChild(tr);
        });

        // TOTAL row
        const totalTr = document.createElement("tr");
        totalTr.innerHTML = `
            <td style="padding:8px 12px;font-weight:600;background:#fafafa;">TOTAL</td>
            <td style="padding:8px 12px;font-weight:600;background:#fafafa;text-align:right;">${msg.total_new_leads || 0}</td>
        `;
        tbody.appendChild(totalTr);
    });
};
