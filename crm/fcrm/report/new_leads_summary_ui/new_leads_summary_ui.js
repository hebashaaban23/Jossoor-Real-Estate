/* global frappe */

frappe.query_reports["New Leads Summary (UI)"] = {
    filters: [
        {
            fieldname: "from_date",
            label: "From Date",
            fieldtype: "Date",
            default: frappe.datetime.add_days(frappe.datetime.get_today(), -7)
        },
        {
            fieldname: "to_date",
            label: "To Date",
            fieldtype: "Date",
            default: frappe.datetime.get_today()
        },
        {
            fieldname: "owner",
            label: "Owner",
            fieldtype: "Link",
            options: "User",
            reqd: 0
        }
    ],

    get_datatable_options(columns) {
        // optional styling control if you want
        return {};
    },

    async get_data(filters) {
        // call our backend KPI function
        const r = await frappe.call({
            method: "crm.fcrm.api.new_leads_kpi.total_new_leads",
            args: {
                from_date: filters.from_date,
                to_date: filters.to_date,
                owner: filters.owner || null
            }
        });

        if (!r.message) {
            return [];
        }

        const breakdown = r.message.daily_breakdown || [];
        const total = r.message.total_new_leads || 0;

        // convert daily_breakdown rows into what the table expects
        const rows = breakdown.map(row => {
            return {
                creation_date: row.creation_date,
                lead_count: row.lead_count
            };
        });

        // push TOTAL row at the end
        rows.push({
            creation_date: "TOTAL",
            lead_count: total
        });

        return rows;
    },

    get_columns() {
        return [
            {
                label: "Date",
                fieldname: "creation_date",
                fieldtype: "Data",
                width: 140
            },
            {
                label: "New Leads",
                fieldname: "lead_count",
                fieldtype: "Int",
                width: 120
            }
        ];
    }
};

