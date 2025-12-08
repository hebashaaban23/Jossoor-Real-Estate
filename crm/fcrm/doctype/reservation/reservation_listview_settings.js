frappe.listview_settings['Reservation'] = {
  add_fields: ['lead_name','total_cost','per_installment','installments','reservation_date','sales_agent','project'],
  get_indicator(doc) {
    if (doc.docstatus === 1) return [__('Submitted'), 'green', 'docstatus,=,1'];
    return [__('Draft'), 'gray', 'docstatus,=,0'];
  },
  onload(listview) {
    listview.filter_area.add([
      ['Reservation','reservation_date','Between','Today','+30d'],
    ]);
  }
}
