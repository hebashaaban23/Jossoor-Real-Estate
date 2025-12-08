frappe.ui.form.on('Reservation', {
  onload(frm) {
    lock_lead_fields(frm);
    set_unit_link_target(frm);
    set_unit_query(frm);
    set_payment_plan_query(frm);
  },

  refresh(frm) {
    lock_lead_fields(frm);
    set_unit_link_target(frm);
    set_unit_query(frm);
  },

  async lead(frm) {
    if (!frm.doc.lead) {
      clear_lead_fields(frm);
      return;
    }
    try {
      const { message: lead } = await frappe.call({
        method: 'frappe.client.get',
        args: { doctype: 'CRM Lead', name: frm.doc.lead }
      });
      if (!lead) return;

      frm.set_value('lead_name', lead.lead_name || '');
      frm.set_value('phone', lead.mobile_no || lead.phone || '');
      frm.set_value('source', lead.source || '');
      frm.set_value('campaign', lead.campaign || '');

      lock_lead_fields(frm);
      set_payment_plan_query(frm);
    } catch (e) {
      console.warn('Lead fetch failed:', e);
    }
  },

  project(frm) {
    // reset unit when project changes
    frm.set_value('unit', '');
    set_unit_link_target(frm);
    set_unit_query(frm);
    set_payment_plan_query(frm);

    // clear dependent fields (optional)
    frm.set_value({
      property_type: '',
      area: '',
      price: '',
      price_per_meter: '',
      maintenance_fees: '',
      availability: '',
      floor: '',
      view: '',
      orientation: '',
      furnished: '',
      finishing: '',
      parking: '',
      bedrooms: '',
      bathrooms: '',
      master_bed_rooms: '',
      categories: '',
      property_image: '',
      floor_plan: '',
      brochure: '',
      video_url: ''
    });
    frm.refresh_fields();
  },

  async unit(frm) {
    const picked = frm.doc.unit;
    if (!picked) return;

    try {
      const { message: meta } = await frappe.call({
        method: 'crm.fcrm.doctype.reservation.reservation.get_unit_meta',
        args: { name: picked, project: frm.doc.project || null }
      });
      if (!meta) return;

      // Map into Reservation
      const updates = {};

      if (meta.project && !frm.doc.project) updates.project = meta.project;

      // Basic / price / area
      if (meta.property_type)     updates.property_type = meta.property_type;
      if (meta.area)              updates.area = meta.area;
      if (meta.price)             updates.price = meta.price;
      if (meta.price_per_meter)   updates.price_per_meter = meta.price_per_meter;
      if (meta.maintenance_fees)  updates.maintenance_fees = meta.maintenance_fees;

      // status & details
      if (meta.availability)      updates.availability = meta.availability;
      if (meta.floor)             updates.floor = meta.floor;
      if (meta.view)              updates.view = meta.view;
      if (meta.orientation)       updates.orientation = meta.orientation;
      if (meta.furnished)         updates.furnished = meta.furnished;
      if (meta.finishing)         updates.finishing = meta.finishing;
      if (meta.parking)           updates.parking = meta.parking;
      if (meta.bedrooms)          updates.bedrooms = meta.bedrooms;
      if (meta.bathrooms)         updates.bathrooms = meta.bathrooms;
      if (meta.master_bed_rooms)  updates.master_bed_rooms = meta.master_bed_rooms;
      if (meta.categories)        updates.categories = meta.categories;

      // media
      if (meta.property_image)    updates.property_image = meta.property_image;
      if (meta.floor_plan)        updates.floor_plan = meta.floor_plan;
      if (meta.brochure)          updates.brochure = meta.brochure;
      if (meta.video_url)         updates.video_url = meta.video_url;

      // Default total_cost to price if empty
      if (!frm.doc.total_cost && meta.price) updates.total_cost = meta.price;

      if (Object.keys(updates).length) {
        frm.set_value(updates);
        frm.refresh_fields();
      }

      set_payment_plan_query(frm);
    } catch (e) {
      console.warn('get_unit_meta failed:', e);
    }
  },

  // finance
  total_cost: compute,
  years: compute,
  frequency: compute,

  validate(frm) {
    compute(frm);
  }
});

/* ---------------- helpers ---------------- */
function lock_lead_fields(frm) {
  ['lead_name', 'phone', 'source', 'campaign'].forEach(f => {
    frm.set_df_property(f, 'read_only', 1);
  });
  frm.refresh_fields(['lead_name', 'phone', 'source', 'campaign']);
}

function clear_lead_fields(frm) {
  frm.set_value({ lead_name: '', phone: '', source: '', campaign: '' });
}

function set_unit_link_target(frm) {
  const target = frm.doc.project ? 'Project Unit' : 'Unit';
  const df = frm.get_docfield('unit');
  if (df && df.options !== target) {
    frm.set_df_property('unit', 'options', target);
    frm.refresh_field('unit');
  }
}

function set_unit_query(frm) {
  frm.set_query('unit', function () {
    const target_dt = frm.doc.project ? 'Project Unit' : 'Unit';
    const filters = { target_dt };
    if (target_dt === 'Project Unit' && frm.doc.project) {
      filters.project = frm.doc.project;
    }
    return {
      query: 'crm.fcrm.doctype.reservation.reservation.search_units_by_title',
      filters,
      page_length: 20,
      ignore_user_permissions: 1
    };
  });
}

function set_payment_plan_query(frm) {
  frm.set_query('payment_plan', async function () {
    const filters = {};
    if (frm.doc.lead) filters.lead = frm.doc.lead;

    if (frm.doc.unit) {
      const isPU = await quick_exists('Project Unit', frm.doc.unit);
      if (isPU) filters.project_unit = frm.doc.unit;
      else      filters.unit = frm.doc.unit;
    }
    return { filters };
  });
}

async function quick_exists(doctype, name) {
  try {
    const { message } = await frappe.call({
      method: 'frappe.client.get_value',
      args: { doctype, filters: { name }, fieldname: 'name' }
    });
    return !!message?.name;
  } catch {
    return false;
  }
}

function compute(frm) {
  const map = { Monthly: 12, Quarterly: 4, Biannual: 2, Annual: 1 };
  const perYear = map[frm.doc.frequency || 'Monthly'] || 12;
  const years = cint(frm.doc.years) || 0;
  const n = years * perYear;
  frm.set_value('installments', n);
  const total = flt(frm.doc.total_cost) || 0;
  frm.set_value('per_installment', total > 0 && n > 0 ? total / n : 0);
}
