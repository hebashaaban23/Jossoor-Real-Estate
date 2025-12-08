frappe.query_reports["New Leads Summary"] = {
    filters: [
        {
            fieldname: "from_date",
            label: "From Date",
            fieldtype: "Date",
            reqd: 0,
            default: frappe.datetime.add_days(frappe.datetime.get_today(), -7),
        },
        {
            fieldname: "to_date",
            label: "To Date",
            fieldtype: "Date",
            reqd: 0,
            default: frappe.datetime.get_today(),
        },
        {
            fieldname: "owner",
            label: "Owner",
            fieldtype: "Link",
            options: "User",
            reqd: 0,
        },
    ],
};

