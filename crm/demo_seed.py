import frappe
from frappe.utils import nowdate, add_days


def make_demo():
    frappe.flags.in_test = False

    company = frappe.db.get_single_value("Global Defaults", "default_company")
    if not company:
        raise Exception("No Default Company is set. Please set it in Global Defaults.")

    # ---------- Defaults ----------
    # Item Group
    default_item_group = (
        frappe.db.get_value("Item Group", {"item_group_name": "All Item Groups"})
        or frappe.db.get_value("Item Group", {}, "name")
    )

    # Customer Group / Supplier Group / Territory
    default_customer_group = (
        frappe.db.get_value("Customer Group", {"customer_group_name": "All Customer Groups"})
        or frappe.db.get_value("Customer Group", {}, "name")
    )
    default_supplier_group = (
        frappe.db.get_value("Supplier Group", {"supplier_group_name": "All Supplier Groups"})
        or frappe.db.get_value("Supplier Group", {}, "name")
    )
    default_territory = (
        frappe.db.get_value("Territory", {"territory_name": "All Territories"})
        or frappe.db.get_value("Territory", {}, "name")
    )

    # ---------- Warehouses ----------
    warehouses = [
        "Main Warehouse - Demo",
        "Finished Goods - Demo",
        "Raw Materials - Demo",
    ]

    wh_names = []
    for wh in warehouses:
        if not frappe.db.exists("Warehouse", {"warehouse_name": wh, "company": company}):
            doc = frappe.get_doc({
                "doctype": "Warehouse",
                "warehouse_name": wh,
                "company": company,
            })
            doc.insert(ignore_permissions=True)
            wh_names.append(doc.name)
        else:
            wh_names.append(frappe.get_value("Warehouse", {"warehouse_name": wh, "company": company}))

    main_wh = wh_names[0]
    fg_wh = wh_names[1]
    rm_wh = wh_names[2]

    # ---------- Items ----------
    items = [
        {"item_code": "DEM-ITEM-001", "item_name": "Demo Phone", "stock_uom": "Nos"},
        {"item_code": "DEM-ITEM-002", "item_name": "Demo Laptop", "stock_uom": "Nos"},
        {"item_code": "DEM-ITEM-003", "item_name": "Demo Headphones", "stock_uom": "Nos"},
        {"item_code": "RAW-ITEM-001", "item_name": "Raw Material A", "stock_uom": "Kg"},
        {"item_code": "RAW-ITEM-002", "item_name": "Raw Material B", "stock_uom": "Kg"},
    ]

    item_codes = []
    for it in items:
        if not frappe.db.exists("Item", it["item_code"]):
            doc = frappe.get_doc({
                "doctype": "Item",
                "item_code": it["item_code"],
                "item_name": it["item_name"],
                "stock_uom": it["stock_uom"],
                "is_stock_item": 1,
                "include_item_in_manufacturing": 1,
                "default_warehouse": main_wh,
                "item_group": default_item_group,  # ✅ IMPORTANT
            })
            doc.insert(ignore_permissions=True)
            item_codes.append(doc.name)
        else:
            item_codes.append(it["item_code"])

    # ---------- Customers ----------
    customers = ["ABC Trading", "Global Stores", "FastMart"]
    customer_names = []
    for c in customers:
        if not frappe.db.exists("Customer", {"customer_name": c}):
            doc = frappe.get_doc({
                "doctype": "Customer",
                "customer_name": c,
                "customer_group": default_customer_group,
                "territory": default_territory,
            })
            doc.insert(ignore_permissions=True)
            customer_names.append(doc.name)
        else:
            customer_names.append(
                frappe.get_value("Customer", {"customer_name": c})
            )

    # ---------- Suppliers ----------
    suppliers = ["SupplyCo", "Mega Suppliers", "RawSource"]
    supplier_names = []
    for s in suppliers:
        if not frappe.db.exists("Supplier", {"supplier_name": s}):
            doc = frappe.get_doc({
                "doctype": "Supplier",
                "supplier_name": s,
                "supplier_group": default_supplier_group,
            })
            doc.insert(ignore_permissions=True)
            supplier_names.append(doc.name)
        else:
            supplier_names.append(
                frappe.get_value("Supplier", {"supplier_name": s})
            )

    today = nowdate()

    # ---------- Opening Stock ----------
    if not frappe.db.exists(
        "Stock Entry",
        {
            "purpose": "Material Receipt",
            "company": company,
            "remarks": "Demo Opening Stock",
        },
    ):
        se = frappe.get_doc({
            "doctype": "Stock Entry",
            "stock_entry_type": "Material Receipt",
            "company": company,
            "posting_date": today,
            "remarks": "Demo Opening Stock",
            "items": [
                {
                    "item_code": item_codes[0],
                    "t_warehouse": main_wh,
                    "qty": 50,
                    "basic_rate": 300,
                },
                {
                    "item_code": item_codes[1],
                    "t_warehouse": main_wh,
                    "qty": 20,
                    "basic_rate": 800,
                },
                {
                    "item_code": item_codes[2],
                    "t_warehouse": main_wh,
                    "qty": 80,
                    "basic_rate": 50,
                },
                {
                    "item_code": item_codes[3],
                    "t_warehouse": rm_wh,
                    "qty": 100,
                    "basic_rate": 10,
                },
                {
                    "item_code": item_codes[4],
                    "t_warehouse": rm_wh,
                    "qty": 120,
                    "basic_rate": 12,
                },
            ],
        })
        se.insert(ignore_permissions=True)
        se.submit()

    # ---------- Buying: Purchase Order -> Purchase Invoice ----------
    if not frappe.db.exists("Purchase Order", {"company": company, "title": "Demo PO 0001"}):
        po = frappe.get_doc({
            "doctype": "Purchase Order",
            "company": company,
            "supplier": supplier_names[0],
            "schedule_date": today,
            "title": "Demo PO 0001",
            "items": [
                {
                    "item_code": item_codes[3],
                    "qty": 50,
                    "uom": "Kg",
                    "schedule_date": today,
                    "rate": 11,
                    "warehouse": rm_wh,
                },
                {
                    "item_code": item_codes[4],
                    "qty": 60,
                    "uom": "Kg",
                    "schedule_date": today,
                    "rate": 13,
                    "warehouse": rm_wh,
                },
            ],
        })
        po.insert(ignore_permissions=True)
        po.submit()

        # Purchase Receipt
        pr = frappe.get_doc(po.make_purchase_receipt())
        pr.posting_date = today
        pr.insert(ignore_permissions=True)
        pr.submit()

        # Purchase Invoice
        pi = frappe.get_doc(pr.make_purchase_invoice())
        pi.posting_date = today
        pi.insert(ignore_permissions=True)
        pi.submit()

    # ---------- Selling: Sales Order -> Sales Invoice ----------
    if not frappe.db.exists("Sales Order", {"company": company, "title": "Demo SO 0001"}):
        so = frappe.get_doc({
            "doctype": "Sales Order",
            "company": company,
            "customer": customer_names[0],
            "delivery_date": add_days(today, 3),
            "title": "Demo SO 0001",
            "items": [
                {
                    "item_code": item_codes[0],
                    "qty": 5,
                    "uom": "Nos",
                    "rate": 400,
                    "warehouse": main_wh,
                },
                {
                    "item_code": item_codes[1],
                    "qty": 3,
                    "uom": "Nos",
                    "rate": 1000,
                    "warehouse": main_wh,
                },
            ],
        })
        so.insert(ignore_permissions=True)
        so.submit()

        # Delivery Note
        dn = frappe.get_doc(so.make_delivery_note())
        dn.posting_date = today
        dn.insert(ignore_permissions=True)
        dn.submit()

        # Sales Invoice
        si = frappe.get_doc(dn.make_sales_invoice())
        si.posting_date = today
        si.insert(ignore_permissions=True)
        si.submit()

        # Payment Entry (for the SI)
        paid_from = frappe.db.get_value(
            "Account", {"account_type": "Bank", "company": company}, "name"
        )
        paid_to = frappe.db.get_value(
            "Account", {"account_type": "Receivable", "company": company}, "name"
        )

        if paid_from and paid_to:
            pe = frappe.get_doc({
                "doctype": "Payment Entry",
                "payment_type": "Receive",
                "company": company,
                "posting_date": today,
                "party_type": "Customer",
                "party": customer_names[0],
                "paid_from": paid_from,
                "paid_to": paid_to,
                "paid_amount": si.outstanding_amount,
                "received_amount": si.outstanding_amount,
                "references": [
                    {
                        "reference_doctype": "Sales Invoice",
                        "reference_name": si.name,
                        "allocated_amount": si.outstanding_amount,
                    }
                ],
            })
            pe.insert(ignore_permissions=True)
            pe.submit()

    frappe.db.commit()
    frappe.clear_cache()
