import frappe

DT = "Saved Filter"
MODULE_NAME = "fcrm"   # module label to show in Desk (we'll create Module Def if missing)
APP_NAME = "crm"       # put the module under the 'crm' app you already have

def ensure_module_def(module_name: str, app_name: str):
    # Create Module Def if not exists, scoped to 'crm' app
    if not frappe.db.exists("Module Def", module_name):
        md = frappe.new_doc("Module Def")
        md.module_name = module_name
        md.app_name = app_name
        md.insert(ignore_permissions=True)
        frappe.db.commit()
        print(f"✅ Created Module Def: {module_name} in app {app_name}")
    else:
        print(f"ℹ️ Module Def already exists: {module_name}")

def upsert_doctype():
    ensure_module_def(MODULE_NAME, APP_NAME)

    if frappe.db.exists("DocType", DT):
        doc = frappe.get_doc("DocType", DT)
        # if it was created in another module previously, move it
        doc.module = MODULE_NAME
    else:
        doc = frappe.new_doc("DocType")
        doc.name = DT
        doc.module = MODULE_NAME
        doc.custom = 1
        doc.istable = 0
        doc.is_tree = 0
        doc.track_changes = 1
        doc.allow_import = 1
        doc.editable_grid = 1
        doc.autoname = "hash"
        doc.engine = "InnoDB"

    def ensure_field(field):
        for i, f in enumerate(doc.fields or []):
            if f.fieldname == field["fieldname"]:
                doc.fields[i].update(field); return
        doc.append("fields", field)

    ensure_field(dict(fieldname="title", label="Title", fieldtype="Data", reqd=1, in_list_view=1))
    ensure_field(dict(fieldname="reference_doctype", label="Reference DocType",
                      fieldtype="Link", options="DocType", reqd=1, in_list_view=1))
    ensure_field(dict(fieldname="owner_user", label="Owner User",
                      fieldtype="Link", options="User", hidden=1, read_only=1))
    ensure_field(dict(fieldname="is_public", label="Is Public", fieldtype="Check",
                      default="0", in_list_view=1))
    ensure_field(dict(fieldname="is_favorite", label="Favorite", fieldtype="Check",
                      default="0", in_list_view=1))
    ensure_field(dict(fieldname="filters_json", label="Filters JSON", fieldtype="Long Text"))
    ensure_field(dict(fieldname="quick_state_json", label="Quick State JSON", fieldtype="Long Text"))
    ensure_field(dict(fieldname="sort_by", label="Sort By", fieldtype="Data"))
    ensure_field(dict(fieldname="sort_order", label="Sort Order", fieldtype="Select",
                      options="desc\nasc", default="desc"))
    ensure_field(dict(fieldname="limit", label="Limit", fieldtype="Int"))

    # reset permissions to desired set
    doc.permissions = []
    doc.append("permissions", dict(role="System Manager",
                                   read=1, write=1, create=1, delete=1, export=1, print=1, share=1))
    doc.append("permissions", dict(role="CRM User",
                                   read=1, write=1, create=1, delete=1, if_owner=1))

    doc.save(ignore_permissions=True)
    frappe.db.commit()
    frappe.clear_cache(doctype=DT)
    print(f"✅ Created/updated DocType '{DT}' in module '{MODULE_NAME}' (app '{APP_NAME}')")
    return doc

if __name__ == "__main__":
    doc = upsert_doctype()
    # sanity insert
    try:
        d = frappe.get_doc({
            "doctype": DT,
            "title": "Sample Filter View",
            "reference_doctype": "Lead",
            "is_public": 0,
            "is_favorite": 1,
            "filters_json": "[]",
        })
        d.owner_user = frappe.session.user
        d.insert()
        print(f"✅ Sample Saved Filter inserted: {d.name}")
    except Exception as e:
        print("ℹ️ Sample insert skipped:", e)
