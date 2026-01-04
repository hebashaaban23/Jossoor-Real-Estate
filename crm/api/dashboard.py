import json

import frappe
from frappe import _

from crm.fcrm.doctype.crm_dashboard.crm_dashboard import create_default_manager_dashboard
from crm.utils import sales_user_only


@frappe.whitelist()
def reset_to_default():
	frappe.only_for("System Manager")
	create_default_manager_dashboard(force=True)


def _add_links_to_layout_items(layout):
	"""
	Add navigation links to dashboard items that don't have them.
	This ensures backward compatibility with existing dashboards.
	"""
	import json
	
	for item in layout:
		# Skip if link already exists
		if 'link' in item and item['link']:
			continue
		
		# Add links based on item name
		if item.get('name') == 'total_leads':
			item['link'] = {"name": "Leads"}
		elif item.get('name') == 'delayed_leads':
			item['link'] = {"name": "Leads", "query": {"delayed": "1"}}
		elif item.get('name') == 'total_deals':
			item['link'] = {"name": "Deals"}
		elif item.get('name', '').startswith('lead_status_') and 'status' in item:
			# Dynamic status card
			status_name = item['status']
			item['link'] = {"name": "Leads", "query": {"status": status_name}}
	
	return layout


@frappe.whitelist()
@sales_user_only
def get_dashboard(from_date="", to_date="", user="", project=""):
	"""
	Get the dashboard data for the CRM dashboard.
	"""
	# If empty strings, keep them as empty (all time)
	# Only set to current month if explicitly not provided (None/empty but not empty string)
	if from_date == "" and to_date == "":
		# All time - keep empty strings
		pass
	elif not from_date or not to_date:
		# Default to current month if not specified
		from_date = frappe.utils.get_first_day(from_date or frappe.utils.nowdate())
		to_date = frappe.utils.get_last_day(to_date or frappe.utils.nowdate())

	roles = frappe.get_roles(frappe.session.user)
	is_sales_user = "Sales User" in roles and "Sales Manager" not in roles and "System Manager" not in roles
	is_sales_manager = "Sales Manager" in roles and "System Manager" not in roles
	
	if is_sales_user and not user:
		user = frappe.session.user
	
	# For Sales Manager, if no user filter is specified, filter to only their team
	team_users = None
	if is_sales_manager and not user:
		# Get team members + leader
		team_members = _get_team_members_for_leader(frappe.session.user)
		team_users = team_members + [frappe.session.user]
		# Pass special marker to indicate team filtering
		user = "__TEAM__"

	# Normalize project - ensure it's a string or None
	if project and project.strip():
		project = project.strip()
	else:
		project = ""

	dashboard = frappe.db.exists("CRM Dashboard", "Manager Dashboard")

	layout = []

	if not dashboard:
		layout = json.loads(create_default_manager_dashboard())
		frappe.db.commit()
	else:
		layout = json.loads(frappe.db.get_value("CRM Dashboard", "Manager Dashboard", "layout") or "[]")
	
	# Add links to items that don't have them (for backward compatibility)
	layout = _add_links_to_layout_items(layout)

	# List of methods that support empty dates (all time)
	methods_supporting_empty_dates = [
		'get_total_leads',
		'get_delayed_leads',
		'get_lead_status_count',
		'get_leads_by_status',
		'get_leads_by_status_chart',
		'get_total_deals',
	]
	
	for l in layout:
		# Check if it's a dynamic lead status card
		if l['name'].startswith('lead_status_') and 'status' in l:
			# Dynamic status card
			status_name = l['status']
			if team_users:
				l["data"] = get_lead_status_count(from_date, to_date, user, status_name, project, team_users=team_users)
			else:
				l["data"] = get_lead_status_count(from_date, to_date, user, status_name, project)
		else:
			# Regular method-based card
			method_name = f"get_{l['name']}"
			if hasattr(frappe.get_attr("crm.api.dashboard"), method_name):
				method = getattr(frappe.get_attr("crm.api.dashboard"), method_name)
				# Check if method accepts project parameter
				import inspect
				sig = inspect.signature(method)
				
				# For methods that don't support empty dates, use current month as default
				if method_name not in methods_supporting_empty_dates:
					# Use current month if dates are empty
					if not from_date or not to_date:
						call_from_date = frappe.utils.get_first_day(frappe.utils.nowdate())
						call_to_date = frappe.utils.get_last_day(frappe.utils.nowdate())
					else:
						call_from_date = from_date
						call_to_date = to_date
				else:
					# Methods that support empty dates - pass as is
					call_from_date = from_date
					call_to_date = to_date
				
				# Pass team_users if available for team filtering
				if 'project' in sig.parameters:
					if team_users and 'team_users' in sig.parameters:
						l["data"] = method(call_from_date, call_to_date, user, project, team_users=team_users)
					else:
						l["data"] = method(call_from_date, call_to_date, user, project)
				else:
					if team_users and 'team_users' in sig.parameters:
						l["data"] = method(call_from_date, call_to_date, user, team_users=team_users)
					else:
						l["data"] = method(call_from_date, call_to_date, user)
			else:
				l["data"] = None

	return layout


@frappe.whitelist()
@sales_user_only
def get_chart(name, type, from_date="", to_date="", user="", status=None, project=""):
	"""
	Get number chart data for the dashboard.
	"""
	if not from_date or not to_date:
		from_date = frappe.utils.get_first_day(from_date or frappe.utils.nowdate())
		to_date = frappe.utils.get_last_day(to_date or frappe.utils.nowdate())

	roles = frappe.get_roles(frappe.session.user)
	is_sales_user = "Sales User" in roles and "Sales Manager" not in roles and "System Manager" not in roles
	if is_sales_user and not user:
		user = frappe.session.user

	# Check if it's a dynamic lead status card
	if name.startswith('lead_status_') and status:
		return get_lead_status_count(from_date, to_date, user, status, project)
	
	method_name = f"get_{name}"
	if hasattr(frappe.get_attr("crm.api.dashboard"), method_name):
		method = getattr(frappe.get_attr("crm.api.dashboard"), method_name)
		return method(from_date, to_date, user, project)
	else:
		return {"error": _("Invalid chart name")}


def get_total_leads(from_date, to_date, user="", project="", team_users=None):
	"""
	Get lead count for the dashboard.
	"""
	conds = []
	params = {}
	
	# Handle empty dates (all time)
	if from_date and to_date:
		params["from_date"] = from_date
		params["to_date"] = to_date
		diff = frappe.utils.date_diff(to_date, from_date)
		if diff == 0:
			diff = 1
		params["prev_from_date"] = frappe.utils.add_days(from_date, -diff)
	else:
		# All time - no date filtering
		from_date = None
		to_date = None
		params["prev_from_date"] = None

	# Handle team filtering for Sales Manager
	if user == "__TEAM__" and team_users:
		placeholders = ", ".join([f"%(team_user_{i})s" for i in range(len(team_users))])
		conds.append(f"lead_owner IN ({placeholders})")
		for i, team_user in enumerate(team_users):
			params[f"team_user_{i}"] = team_user
	elif user and user != "__TEAM__":
		conds.append("lead_owner = %(user)s")
		params["user"] = user
	
	if project:
		conds.append("project = %(project)s")
		params["project"] = project
		# Exclude Duplicate leads when filtering by project
		# Use is_duplicate field (Check field) - exclude where is_duplicate = 1
		conds.append("COALESCE(is_duplicate, 0) = 0")

	where_clause = ""
	if conds:
		where_clause = "WHERE " + " AND ".join(conds)

	if from_date and to_date:
		# Date range specified
		result = frappe.db.sql(
			f"""
			SELECT
				COUNT(CASE
					WHEN creation >= %(from_date)s AND creation < DATE_ADD(%(to_date)s, INTERVAL 1 DAY)
					THEN name
					ELSE NULL
				END) as current_month_leads,

				COUNT(CASE
					WHEN creation >= %(prev_from_date)s AND creation < %(from_date)s
					THEN name
					ELSE NULL
				END) as prev_month_leads
			FROM `tabCRM Lead`
			{where_clause}
			""",
			params,
			as_dict=1,
		)
	else:
		# All time - count all leads
		result = frappe.db.sql(
			f"""
			SELECT
				COUNT(*) as current_month_leads,
				0 as prev_month_leads
			FROM `tabCRM Lead`
			{where_clause}
			""",
			params,
			as_dict=1,
		)

	current_month_leads = result[0].current_month_leads or 0

	return {
		"title": _("Total leads"),
		"tooltip": _("Total number of leads"),
		"value": current_month_leads,
	}


def get_feedback_comments(from_date, to_date, user=""):
	"""
	Get feedback comments count for CRM Lead (comment_type = 'Comment').
	"""
	conds = ""

	diff = frappe.utils.date_diff(to_date, from_date)
	if diff == 0:
		diff = 1

	if user:
		conds += f" AND owner = '{user}'"

	result = frappe.db.sql(
		f"""
		SELECT
            COUNT(CASE
                WHEN creation >= %(from_date)s AND creation < DATE_ADD(%(to_date)s, INTERVAL 1 DAY)
                    AND comment_type = 'Comment'
                    AND reference_doctype = 'CRM Lead'
                    {conds}
                THEN name
                ELSE NULL
            END) as current_feedback,

            COUNT(CASE
                WHEN creation >= %(prev_from_date)s AND creation < %(from_date)s
                    AND comment_type = 'Comment'
                    AND reference_doctype = 'CRM Lead'
                    {conds}
                THEN name
                ELSE NULL
            END) as prev_feedback
		FROM `tabComment`
    """,
		{
			"from_date": from_date,
			"to_date": to_date,
			"prev_from_date": frappe.utils.add_days(from_date, -diff),
		},
		as_dict=1,
	)

	current_feedback = result[0].current_feedback or 0

	return {
		"title": _("FeedBack"),
		"tooltip": _("Total number of feedback"),
		"value": current_feedback,
	}


def get_ongoing_deals(from_date, to_date, user=""):
	"""
	Get ongoing deal count for the dashboard, and also calculate average deal value for ongoing deals.
	"""
	conds = ""

	diff = frappe.utils.date_diff(to_date, from_date)
	if diff == 0:
		diff = 1

	if user:
		conds += f" AND d.deal_owner = '{user}'"

	result = frappe.db.sql(
		f"""
		SELECT
			COUNT(CASE
				WHEN d.creation >= %(from_date)s AND d.creation < DATE_ADD(%(to_date)s, INTERVAL 1 DAY)
					AND s.type NOT IN ('Won', 'Lost')
					{conds}
				THEN d.name
				ELSE NULL
			END) as current_month_deals,

			COUNT(CASE
				WHEN d.creation >= %(prev_from_date)s AND d.creation < %(from_date)s
					AND s.type NOT IN ('Won', 'Lost')
					{conds}
				THEN d.name
				ELSE NULL
			END) as prev_month_deals
		FROM `tabCRM Deal` d
		JOIN `tabCRM Deal Status` s ON d.status = s.name
	""",
		{
			"from_date": from_date,
			"to_date": to_date,
			"prev_from_date": frappe.utils.add_days(from_date, -diff),
		},
		as_dict=1,
	)

	current_month_deals = result[0].current_month_deals or 0

	return {
		"title": _("Ongoing deals"),
		"tooltip": _("Total number of non won/lost deals"),
		"value": current_month_deals,
	}


def get_average_ongoing_deal_value(from_date, to_date, user=""):
	"""
	Get ongoing deal count for the dashboard, and also calculate average deal value for ongoing deals.
	"""
	conds = ""

	diff = frappe.utils.date_diff(to_date, from_date)
	if diff == 0:
		diff = 1

	if user:
		conds += f" AND d.deal_owner = '{user}'"

	result = frappe.db.sql(
		f"""
		SELECT
			AVG(CASE
				WHEN d.creation >= %(from_date)s AND d.creation < DATE_ADD(%(to_date)s, INTERVAL 1 DAY)
					AND s.type NOT IN ('Won', 'Lost')
					{conds}
				THEN d.deal_value * IFNULL(d.exchange_rate, 1)
				ELSE NULL
			END) as current_month_avg_value,

			AVG(CASE
				WHEN d.creation >= %(prev_from_date)s AND d.creation < %(from_date)s
					AND s.type NOT IN ('Won', 'Lost')
					{conds}
				THEN d.deal_value * IFNULL(d.exchange_rate, 1)
				ELSE NULL
			END) as prev_month_avg_value
		FROM `tabCRM Deal` d
		JOIN `tabCRM Deal Status` s ON d.status = s.name
    """,
		{
			"from_date": from_date,
			"to_date": to_date,
			"prev_from_date": frappe.utils.add_days(from_date, -diff),
		},
		as_dict=1,
	)

	current_month_avg_value = result[0].current_month_avg_value or 0
	prev_month_avg_value = result[0].prev_month_avg_value or 0

	avg_value_delta = current_month_avg_value - prev_month_avg_value if prev_month_avg_value else 0

	return {
		"title": _("Avg. ongoing deal value"),
		"tooltip": _("Average deal value of non won/lost deals"),
		"value": current_month_avg_value,
		"delta": avg_value_delta,
		"prefix": get_base_currency_symbol(),
	}


def get_won_deals(from_date, to_date, user=""):
	"""
	Get won deal count for the dashboard, and also calculate average deal value for won deals.
	"""

	diff = frappe.utils.date_diff(to_date, from_date)
	if diff == 0:
		diff = 1

	conds = ""

	if user:
		conds += f" AND d.deal_owner = '{user}'"

	result = frappe.db.sql(
		f"""
		SELECT
			COUNT(CASE
				WHEN d.closed_date >= %(from_date)s AND d.closed_date < DATE_ADD(%(to_date)s, INTERVAL 1 DAY)
					AND s.type = 'Won'
					{conds}
				THEN d.name
				ELSE NULL
			END) as current_month_deals,

			COUNT(CASE
				WHEN d.closed_date >= %(prev_from_date)s AND d.closed_date < %(from_date)s
					AND s.type = 'Won'
					{conds}
				THEN d.name
				ELSE NULL
			END) as prev_month_deals
		FROM `tabCRM Deal` d
		JOIN `tabCRM Deal Status` s ON d.status = s.name
		""",
		{
			"from_date": from_date,
			"to_date": to_date,
			"prev_from_date": frappe.utils.add_days(from_date, -diff),
		},
		as_dict=1,
	)

	current_month_deals = result[0].current_month_deals or 0

	return {
		"title": _("Won deals"),
		"tooltip": _("Total number of won deals based on its closure date"),
		"value": current_month_deals,
	}


def get_average_won_deal_value(from_date, to_date, user=""):
	"""
	Get won deal count for the dashboard, and also calculate average deal value for won deals.
	"""

	diff = frappe.utils.date_diff(to_date, from_date)
	if diff == 0:
		diff = 1

	conds = ""

	if user:
		conds += f" AND d.deal_owner = '{user}'"

	result = frappe.db.sql(
		f"""
		SELECT
			AVG(CASE
				WHEN d.closed_date >= %(from_date)s AND d.closed_date < DATE_ADD(%(to_date)s, INTERVAL 1 DAY)
					AND s.type = 'Won'
					{conds}
				THEN d.deal_value * IFNULL(d.exchange_rate, 1)
				ELSE NULL
			END) as current_month_avg_value,

			AVG(CASE
				WHEN d.closed_date >= %(prev_from_date)s AND d.closed_date < %(from_date)s
					AND s.type = 'Won'
					{conds}
				THEN d.deal_value * IFNULL(d.exchange_rate, 1)
				ELSE NULL
			END) as prev_month_avg_value
		FROM `tabCRM Deal` d
		JOIN `tabCRM Deal Status` s ON d.status = s.name
		""",
		{
			"from_date": from_date,
			"to_date": to_date,
			"prev_from_date": frappe.utils.add_days(from_date, -diff),
		},
		as_dict=1,
	)

	current_month_avg_value = result[0].current_month_avg_value or 0
	prev_month_avg_value = result[0].prev_month_avg_value or 0

	avg_value_delta = current_month_avg_value - prev_month_avg_value if prev_month_avg_value else 0

	return {
		"title": _("Avg. won deal value"),
		"tooltip": _("Average deal value of won deals"),
		"value": current_month_avg_value,
		"delta": avg_value_delta,
		"prefix": get_base_currency_symbol(),
	}


def get_average_deal_value(from_date, to_date, user=""):
	"""
	Get average deal value for the dashboard.
	"""

	diff = frappe.utils.date_diff(to_date, from_date)
	if diff == 0:
		diff = 1

	conds = ""

	if user:
		conds += f" AND d.deal_owner = '{user}'"

	result = frappe.db.sql(
		f"""
		SELECT
			AVG(CASE
				WHEN d.creation >= %(from_date)s AND d.creation < DATE_ADD(%(to_date)s, INTERVAL 1 DAY)
					AND s.type != 'Lost'
					{conds}
				THEN d.deal_value * IFNULL(d.exchange_rate, 1)
				ELSE NULL
			END) as current_month_avg,

			AVG(CASE
				WHEN d.creation >= %(prev_from_date)s AND d.creation < %(from_date)s
					AND s.type != 'Lost'
					{conds}
				THEN d.deal_value * IFNULL(d.exchange_rate, 1)
				ELSE NULL
			END) as prev_month_avg
		FROM `tabCRM Deal` AS d
		JOIN `tabCRM Deal Status` s ON d.status = s.name
		""",
		{
			"from_date": from_date,
			"to_date": to_date,
			"prev_from_date": frappe.utils.add_days(from_date, -diff),
		},
		as_dict=1,
	)

	current_month_avg = result[0].current_month_avg or 0
	prev_month_avg = result[0].prev_month_avg or 0

	delta = current_month_avg - prev_month_avg if prev_month_avg else 0

	return {
		"title": _("Avg. deal value"),
		"tooltip": _("Average deal value of ongoing & won deals"),
		"value": current_month_avg,
		"prefix": get_base_currency_symbol(),
		"delta": delta,
		"deltaSuffix": "%",
	}


def get_average_time_to_close_a_lead(from_date, to_date, user=""):
	"""
	Get average time to close deals for the dashboard.
	"""

	diff = frappe.utils.date_diff(to_date, from_date)
	if diff == 0:
		diff = 1

	conds = ""

	if user:
		conds += f" AND d.deal_owner = '{user}'"

	prev_from_date = frappe.utils.add_days(from_date, -diff)
	prev_to_date = from_date

	result = frappe.db.sql(
		f"""
		SELECT
			AVG(CASE WHEN d.closed_date >= %(from_date)s AND d.closed_date < DATE_ADD(%(to_date)s, INTERVAL 1 DAY)
				THEN TIMESTAMPDIFF(DAY, COALESCE(l.creation, d.creation), d.closed_date) END) as current_avg_lead,
			AVG(CASE WHEN d.closed_date >= %(prev_from_date)s AND d.closed_date < %(prev_to_date)s
				THEN TIMESTAMPDIFF(DAY, COALESCE(l.creation, d.creation), d.closed_date) END) as prev_avg_lead
		FROM `tabCRM Deal` AS d
		JOIN `tabCRM Deal Status` s ON d.status = s.name
		LEFT JOIN `tabCRM Lead` l ON d.lead = l.name
		WHERE d.closed_date IS NOT NULL AND s.type = 'Won'
			{conds}
		""",
		{
			"from_date": from_date,
			"to_date": to_date,
			"prev_from_date": prev_from_date,
			"prev_to_date": prev_to_date,
		},
		as_dict=1,
	)

	current_avg_lead = result[0].current_avg_lead or 0
	prev_avg_lead = result[0].prev_avg_lead or 0
	delta_lead = current_avg_lead - prev_avg_lead if prev_avg_lead else 0

	return {
		"title": _("Avg. time to close a lead"),
		"tooltip": _("Average time taken from lead creation to deal closure"),
		"value": current_avg_lead,
		"suffix": " days",
		"delta": delta_lead,
		"deltaSuffix": " days",
		"negativeIsBetter": True,
	}


def get_average_time_to_close_a_deal(from_date, to_date, user=""):
	"""
	Get average time to close deals for the dashboard.
	"""

	diff = frappe.utils.date_diff(to_date, from_date)
	if diff == 0:
		diff = 1

	conds = ""

	if user:
		conds += f" AND d.deal_owner = '{user}'"

	prev_from_date = frappe.utils.add_days(from_date, -diff)
	prev_to_date = from_date

	result = frappe.db.sql(
		f"""
		SELECT
			AVG(CASE WHEN d.closed_date >= %(from_date)s AND d.closed_date < DATE_ADD(%(to_date)s, INTERVAL 1 DAY)
				THEN TIMESTAMPDIFF(DAY, d.creation, d.closed_date) END) as current_avg_deal,
			AVG(CASE WHEN d.closed_date >= %(prev_from_date)s AND d.closed_date < %(prev_to_date)s
				THEN TIMESTAMPDIFF(DAY, d.creation, d.closed_date) END) as prev_avg_deal
		FROM `tabCRM Deal` AS d
		JOIN `tabCRM Deal Status` s ON d.status = s.name
		LEFT JOIN `tabCRM Lead` l ON d.lead = l.name
		WHERE d.closed_date IS NOT NULL AND s.type = 'Won'
			{conds}
		""",
		{
			"from_date": from_date,
			"to_date": to_date,
			"prev_from_date": prev_from_date,
			"prev_to_date": prev_to_date,
		},
		as_dict=1,
	)

	current_avg_deal = result[0].current_avg_deal or 0
	prev_avg_deal = result[0].prev_avg_deal or 0
	delta_deal = current_avg_deal - prev_avg_deal if prev_avg_deal else 0

	return {
		"title": _("Avg. time to close a deal"),
		"tooltip": _("Average time taken from deal creation to deal closure"),
		"value": current_avg_deal,
		"suffix": " days",
		"delta": delta_deal,
		"deltaSuffix": " days",
		"negativeIsBetter": True,
	}


def get_sales_trend(from_date="", to_date="", user="", project=""):
	"""
	Get sales trend data for the dashboard.
	[
		{ date: new Date('2024-05-01'), leads: 45, deals: 23, won_deals: 12 },
		{ date: new Date('2024-05-02'), leads: 50, deals: 30, won_deals: 15 },
		...
	]
	"""

	lead_conds = []
	deal_conds = ""
	params = {}

	if not from_date or not to_date:
		from_date = frappe.utils.get_first_day(from_date or frappe.utils.nowdate())
		to_date = frappe.utils.get_last_day(to_date or frappe.utils.nowdate())

	params["from"] = from_date
	params["to"] = to_date

	if user:
		lead_conds.append("lead_owner = %(user)s")
		deal_conds += f" AND deal_owner = '{user}'"
		params["user"] = user
	
	if project:
		lead_conds.append("project = %(project)s")
		params["project"] = project
		# Exclude Duplicate leads when filtering by project
		# Use is_duplicate field (Check field) - exclude where is_duplicate = 1
		lead_conds.append("COALESCE(is_duplicate, 0) = 0")

	lead_where = "DATE(creation) BETWEEN %(from)s AND %(to)s"
	if lead_conds:
		lead_where += " AND " + " AND ".join(lead_conds)

	result = frappe.db.sql(
		f"""
		SELECT
			DATE_FORMAT(date, '%%Y-%%m-%%d') AS date,
			SUM(leads) AS leads,
			SUM(deals) AS deals,
			SUM(won_deals) AS won_deals
		FROM (
			SELECT
				DATE(creation) AS date,
				COUNT(*) AS leads,
				0 AS deals,
				0 AS won_deals
			FROM `tabCRM Lead`
			WHERE {lead_where}
			GROUP BY DATE(creation)

			UNION ALL

			SELECT
				DATE(d.creation) AS date,
				0 AS leads,
				COUNT(*) AS deals,
				SUM(CASE WHEN s.type = 'Won' THEN 1 ELSE 0 END) AS won_deals
			FROM `tabCRM Deal` d
			JOIN `tabCRM Deal Status` s ON d.status = s.name
			WHERE DATE(d.creation) BETWEEN %(from)s AND %(to)s
			{deal_conds}
			GROUP BY DATE(d.creation)
		) AS daily
		GROUP BY date
		ORDER BY date
		""",
		params,
		as_dict=True,
	)

	sales_trend = [
		{
			"date": frappe.utils.get_datetime(row.date).strftime("%Y-%m-%d"),
			"leads": row.leads or 0,
			"deals": row.deals or 0,
			"won_deals": row.won_deals or 0,
		}
		for row in result
	]

	return {
		"data": sales_trend,
		"title": _("Sales trend"),
		"subtitle": _("Daily performance of leads, deals, and wins"),
		"xAxis": {
			"title": _("Date"),
			"key": "date",
			"type": "time",
			"timeGrain": "day",
		},
		"yAxis": {
			"title": _("Count"),
		},
		"series": [
			{"name": "leads", "type": "line", "showDataPoints": True},
			{"name": "deals", "type": "line", "showDataPoints": True},
			{"name": "won_deals", "type": "line", "showDataPoints": True},
		],
	}


def get_forecasted_revenue(from_date="", to_date="", user=""):
	"""
	Get forecasted revenue for the dashboard.
	[
		{ date: new Date('2024-05-01'), forecasted: 1200000, actual: 980000 },
		{ date: new Date('2024-06-01'), forecasted: 1350000, actual: 1120000 },
		{ date: new Date('2024-07-01'), forecasted: 1600000, actual: "" },
		{ date: new Date('2024-08-01'), forecasted: 1500000, actual: "" },
		...
	]
	"""
	deal_conds = ""

	if user:
		deal_conds += f" AND d.deal_owner = '{user}'"

	result = frappe.db.sql(
		f"""
		SELECT
			DATE_FORMAT(d.expected_closure_date, '%Y-%m')                        AS month,
			SUM(
				CASE
					WHEN s.type = 'Lost' THEN d.expected_deal_value * IFNULL(d.exchange_rate, 1)
					ELSE d.expected_deal_value * IFNULL(d.probability, 0) / 100 * IFNULL(d.exchange_rate, 1)  -- forecasted
				END
			)                                                       AS forecasted,
			SUM(
				CASE
					WHEN s.type = 'Won' THEN d.deal_value * IFNULL(d.exchange_rate, 1)            -- actual
					ELSE 0
				END
			)                                                       AS actual
		FROM `tabCRM Deal` AS d
		JOIN `tabCRM Deal Status` s ON d.status = s.name
		WHERE d.expected_closure_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
		{deal_conds}
		GROUP BY DATE_FORMAT(d.expected_closure_date, '%Y-%m')
		ORDER BY month
		""",
		as_dict=True,
	)

	for row in result:
		row["month"] = frappe.utils.get_datetime(row["month"]).strftime("%Y-%m-01")
		row["forecasted"] = row["forecasted"] or ""
		row["actual"] = row["actual"] or ""

	return {
		"data": result or [],
		"title": _("Forecasted revenue"),
		"subtitle": _("Projected vs actual revenue based on deal probability"),
		"xAxis": {
			"title": _("Month"),
			"key": "month",
			"type": "time",
			"timeGrain": "month",
		},
		"yAxis": {
			"title": _("Revenue") + f" ({get_base_currency_symbol()})",
		},
		"series": [
			{"name": "forecasted", "type": "line", "showDataPoints": True},
			{"name": "actual", "type": "line", "showDataPoints": True},
		],
	}


def get_funnel_conversion(from_date="", to_date="", user="", project=""):
	"""
	Get funnel conversion data for the dashboard.
	[
		{ stage: 'Leads', count: 120 },
		{ stage: 'Qualification', count: 100 },
		{ stage: 'Negotiation', count: 80 },
		{ stage: 'Ready to Close', count: 60 },
		{ stage: 'Won', count: 30 },
		...
	]
	"""
	lead_conds = []
	deal_conds = ""
	params = {}

	if not from_date or not to_date:
		from_date = frappe.utils.get_first_day(from_date or frappe.utils.nowdate())
		to_date = frappe.utils.get_last_day(to_date or frappe.utils.nowdate())

	params["from"] = from_date
	params["to"] = to_date

	if user:
		lead_conds.append("lead_owner = %(user)s")
		deal_conds += f" AND deal_owner = '{user}'"
		params["user"] = user
	
	if project:
		lead_conds.append("project = %(project)s")
		params["project"] = project
		# Exclude Duplicate leads when filtering by project
		# Use is_duplicate field (Check field) - exclude where is_duplicate = 1
		lead_conds.append("COALESCE(is_duplicate, 0) = 0")

	lead_where = "DATE(creation) BETWEEN %(from)s AND %(to)s"
	if lead_conds:
		lead_where += " AND " + " AND ".join(lead_conds)

	result = []

	# Get total leads
	total_leads = frappe.db.sql(
		f"""
			SELECT COUNT(*) AS count
			FROM `tabCRM Lead`
			WHERE {lead_where}
		""",
		params,
		as_dict=True,
	)
	total_leads_count = total_leads[0].count if total_leads else 0

	result.append({"stage": "Leads", "count": total_leads_count})

	result += get_deal_status_change_counts(from_date, to_date, deal_conds)

	return {
		"data": result or [],
		"title": _("Funnel conversion"),
		"subtitle": _("Lead to deal conversion pipeline"),
		"xAxis": {
			"title": _("Stage"),
			"key": "stage",
			"type": "category",
		},
		"yAxis": {
			"title": _("Count"),
		},
		"swapXY": True,
		"series": [
			{
				"name": "count",
				"type": "bar",
				"echartOptions": {
					"colorBy": "data",
				},
			},
		],
	}


def get_deals_by_stage_axis(from_date="", to_date="", user=""):
	"""
	Get deal data by stage for the dashboard.
	[
		{ stage: 'Prospecting', count: 120 },
		{ stage: 'Negotiation', count: 45 },
		...
	]
	"""
	deal_conds = ""

	if not from_date or not to_date:
		from_date = frappe.utils.get_first_day(from_date or frappe.utils.nowdate())
		to_date = frappe.utils.get_last_day(to_date or frappe.utils.nowdate())

	if user:
		deal_conds += f" AND d.deal_owner = '{user}'"

	result = frappe.db.sql(
		f"""
		SELECT
			d.status AS stage,
			COUNT(*) AS count,
			s.type AS status_type
		FROM `tabCRM Deal` AS d
		JOIN `tabCRM Deal Status` s ON d.status = s.name
		WHERE DATE(d.creation) BETWEEN %(from)s AND %(to)s AND s.type NOT IN ('Lost')
		{deal_conds}
		GROUP BY d.status
		ORDER BY count DESC
		""",
		{"from": from_date, "to": to_date},
		as_dict=True,
	)

	return {
		"data": result or [],
		"title": _("Deals by ongoing & won stage"),
		"xAxis": {
			"title": _("Stage"),
			"key": "stage",
			"type": "category",
		},
		"yAxis": {"title": _("Count")},
		"series": [
			{"name": "count", "type": "bar"},
		],
	}


def get_deals_by_stage_donut(from_date="", to_date="", user=""):
	"""
	Get deal data by stage for the dashboard.
	[
		{ stage: 'Prospecting', count: 120 },
		{ stage: 'Negotiation', count: 45 },
		...
	]
	"""
	deal_conds = ""

	if not from_date or not to_date:
		from_date = frappe.utils.get_first_day(from_date or frappe.utils.nowdate())
		to_date = frappe.utils.get_last_day(to_date or frappe.utils.nowdate())

	if user:
		deal_conds += f" AND d.deal_owner = '{user}'"

	result = frappe.db.sql(
		f"""
		SELECT
			d.status AS stage,
			COUNT(*) AS count,
			s.type AS status_type
		FROM `tabCRM Deal` AS d
		JOIN `tabCRM Deal Status` s ON d.status = s.name
		WHERE DATE(d.creation) BETWEEN %(from)s AND %(to)s
		{deal_conds}
		GROUP BY d.status
		ORDER BY count DESC
		""",
		{"from": from_date, "to": to_date},
		as_dict=True,
	)

	return {
		"data": result or [],
		"title": _("Deals by stage"),
		"subtitle": _("Current pipeline distribution"),
		"categoryColumn": "stage",
		"valueColumn": "count",
	}


def get_lost_deal_reasons(from_date="", to_date="", user=""):
	"""
	Get lost deal reasons for the dashboard.
	[
		{ reason: 'Price too high', count: 20 },
		{ reason: 'Competitor won', count: 15 },
		...
	]
	"""

	deal_conds = ""

	if not from_date or not to_date:
		from_date = frappe.utils.get_first_day(from_date or frappe.utils.nowdate())
		to_date = frappe.utils.get_last_day(to_date or frappe.utils.nowdate())

	if user:
		deal_conds += f" AND d.deal_owner = '{user}'"

	result = frappe.db.sql(
		f"""
		SELECT
			d.lost_reason AS reason,
			COUNT(*) AS count
		FROM `tabCRM Deal` AS d
		JOIN `tabCRM Deal Status` s ON d.status = s.name
		WHERE DATE(d.creation) BETWEEN %(from)s AND %(to)s AND s.type = 'Lost'
		{deal_conds}
		GROUP BY d.lost_reason
		HAVING reason IS NOT NULL AND reason != ''
		ORDER BY count DESC
		""",
		{"from": from_date, "to": to_date},
		as_dict=True,
	)

	return {
		"data": result or [],
		"title": _("Lost deal reasons"),
		"subtitle": _("Common reasons for losing deals"),
		"xAxis": {
			"title": _("Reason"),
			"key": "reason",
			"type": "category",
		},
		"yAxis": {
			"title": _("Count"),
		},
		"series": [
			{"name": "count", "type": "bar"},
		],
	}


def get_leads_by_source(from_date="", to_date="", user="", project=""):
	"""
	Get lead data by source for the dashboard.
	[
		{ source: 'Website', count: 120 },
		{ source: 'Referral', count: 45 },
		...
	]
	"""
	lead_conds = []
	params = {}

	if not from_date or not to_date:
		from_date = frappe.utils.get_first_day(from_date or frappe.utils.nowdate())
		to_date = frappe.utils.get_last_day(to_date or frappe.utils.nowdate())

	params["from"] = from_date
	params["to"] = to_date

	if user:
		lead_conds.append("lead_owner = %(user)s")
		params["user"] = user
	
	if project:
		lead_conds.append("project = %(project)s")
		params["project"] = project
		# Exclude Duplicate leads when filtering by project
		# Use is_duplicate field (Check field) - exclude where is_duplicate = 1
		lead_conds.append("COALESCE(is_duplicate, 0) = 0")

	lead_where = "DATE(creation) BETWEEN %(from)s AND %(to)s"
	if lead_conds:
		lead_where += " AND " + " AND ".join(lead_conds)

	result = frappe.db.sql(
		f"""
		SELECT
			IFNULL(source, 'Empty') AS source,
			COUNT(*) AS count
		FROM `tabCRM Lead`
		WHERE {lead_where}
		GROUP BY source
		ORDER BY count DESC
		""",
		params,
		as_dict=True,
	)

	return {
		"data": result or [],
		"title": _("Leads by source"),
		"subtitle": _("Lead generation channel analysis"),
		"categoryColumn": "source",
		"valueColumn": "count",
	}


def get_deals_by_source(from_date="", to_date="", user=""):
	"""
	Get deal data by source for the dashboard.
	[
		{ source: 'Website', count: 120 },
		{ source: 'Referral', count: 45 },
		...
	]
	"""
	deal_conds = ""

	if not from_date or not to_date:
		from_date = frappe.utils.get_first_day(from_date or frappe.utils.nowdate())
		to_date = frappe.utils.get_last_day(to_date or frappe.utils.nowdate())

	if user:
		deal_conds += f" AND deal_owner = '{user}'"

	result = frappe.db.sql(
		f"""
		SELECT
			IFNULL(source, 'Empty') AS source,
			COUNT(*) AS count
		FROM `tabCRM Deal`
		WHERE DATE(creation) BETWEEN %(from)s AND %(to)s
		{deal_conds}
		GROUP BY source
		ORDER BY count DESC
		""",
		{"from": from_date, "to": to_date},
		as_dict=True,
	)

	return {
		"data": result or [],
		"title": _("Deals by source"),
		"subtitle": _("Deal generation channel analysis"),
		"categoryColumn": "source",
		"valueColumn": "count",
	}


def get_deals_by_territory(from_date="", to_date="", user=""):
	"""
	Get deal data by territory for the dashboard.
	[
		{ territory: 'North America', deals: 45, value: 2300000 },
		{ territory: 'Europe', deals: 30, value: 1500000 },
		...
	]
	"""
	deal_conds = ""

	if not from_date or not to_date:
		from_date = frappe.utils.get_first_day(from_date or frappe.utils.nowdate())
		to_date = frappe.utils.get_last_day(to_date or frappe.utils.nowdate())

	if user:
		deal_conds += f" AND d.deal_owner = '{user}'"

	result = frappe.db.sql(
		f"""
		SELECT
			IFNULL(d.territory, 'Empty') AS territory,
			COUNT(*) AS deals,
			SUM(COALESCE(d.deal_value, 0) * IFNULL(d.exchange_rate, 1)) AS value
		FROM `tabCRM Deal` AS d
		WHERE DATE(d.creation) BETWEEN %(from)s AND %(to)s
		{deal_conds}
		GROUP BY d.territory
		ORDER BY value DESC
		""",
		{"from": from_date, "to": to_date},
		as_dict=True,
	)

	return {
		"data": result or [],
		"title": _("Deals by territory"),
		"subtitle": _("Geographic distribution of deals and revenue"),
		"xAxis": {
			"title": _("Territory"),
			"key": "territory",
			"type": "category",
		},
		"yAxis": {
			"title": _("Number of deals"),
		},
		"y2Axis": {
			"title": _("Deal value") + f" ({get_base_currency_symbol()})",
		},
		"series": [
			{"name": "deals", "type": "bar"},
			{"name": "value", "type": "line", "showDataPoints": True, "axis": "y2"},
		],
	}


def get_deals_by_salesperson(from_date="", to_date="", user=""):
	"""
	Get deal data by salesperson for the dashboard.
	[
		{ salesperson: 'John Smith', deals: 45, value: 2300000 },
		{ salesperson: 'Jane Doe', deals: 30, value: 1500000 },
		...
	]
	"""
	deal_conds = ""

	if not from_date or not to_date:
		from_date = frappe.utils.get_first_day(from_date or frappe.utils.nowdate())
		to_date = frappe.utils.get_last_day(to_date or frappe.utils.nowdate())

	if user:
		deal_conds += f" AND d.deal_owner = '{user}'"

	result = frappe.db.sql(
		f"""
		SELECT
			IFNULL(u.full_name, d.deal_owner) AS salesperson,
			COUNT(*)                           AS deals,
			SUM(COALESCE(d.deal_value, 0) * IFNULL(d.exchange_rate, 1)) AS value
		FROM `tabCRM Deal` AS d
		LEFT JOIN `tabUser` AS u ON u.name = d.deal_owner
		WHERE DATE(d.creation) BETWEEN %(from)s AND %(to)s
		{deal_conds}
		GROUP BY d.deal_owner
		ORDER BY value DESC
		""",
		{"from": from_date, "to": to_date},
		as_dict=True,
	)

	return {
		"data": result or [],
		"title": _("Deals by salesperson"),
		"subtitle": _("Number of deals and total value per salesperson"),
		"xAxis": {
			"title": _("Salesperson"),
			"key": "salesperson",
			"type": "category",
		},
		"yAxis": {
			"title": _("Number of deals"),
		},
		"y2Axis": {
			"title": _("Deal value") + f" ({get_base_currency_symbol()})",
		},
		"series": [
			{"name": "deals", "type": "bar"},
			{"name": "value", "type": "line", "showDataPoints": True, "axis": "y2"},
		],
	}


def get_base_currency_symbol():
	"""
	Get the base currency symbol from the system settings.
	"""
	base_currency = frappe.db.get_single_value("FCRM Settings", "currency") or "USD"
	return frappe.db.get_value("Currency", base_currency, "symbol") or ""


def get_deal_status_change_counts(from_date, to_date, deal_conds=""):
	"""
	Get count of each status change (to) for each deal, excluding deals with current status type 'Lost'.
	Order results by status position.
	Returns:
	[
	  {"status": "Qualification", "count": 120},
	  {"status": "Negotiation", "count": 85},
	  ...
	]
	"""
	result = frappe.db.sql(
		f"""
		SELECT
			scl.to AS stage,
			COUNT(*) AS count
		FROM
			`tabCRM Status Change Log` scl
		JOIN
			`tabCRM Deal` d ON scl.parent = d.name
		JOIN
			`tabCRM Deal Status` s ON d.status = s.name
		JOIN
			`tabCRM Deal Status` st ON scl.to = st.name
		WHERE
			scl.to IS NOT NULL
			AND scl.to != ''
			AND s.type != 'Lost'
			AND DATE(d.creation) BETWEEN %(from)s AND %(to)s
			{deal_conds}
		GROUP BY
			scl.to, st.position
		ORDER BY
			st.position ASC
		""",
		{"from": from_date, "to": to_date},
		as_dict=True,
	)
	return result or []


def get_leads_by_status(from_date, to_date, user="", project="", team_users=None):
	"""
	Get lead count by status for the dashboard.
	Returns data for both number chart and donut chart.
	"""
	base_conds = []
	base_params = {}
	
	# Handle empty dates (all time)
	if from_date and to_date:
		base_params["from_date"] = from_date
		base_params["to_date"] = to_date
		diff = frappe.utils.date_diff(to_date, from_date)
		if diff == 0:
			diff = 1
		base_params["prev_from_date"] = frappe.utils.add_days(from_date, -diff)
	else:
		# All time - no date filtering
		from_date = None
		to_date = None
		base_params["prev_from_date"] = None

	# Handle team filtering for Sales Manager
	if user == "__TEAM__" and team_users:
		placeholders = ", ".join([f"%(team_user_{i})s" for i in range(len(team_users))])
		base_conds.append(f"lead_owner IN ({placeholders})")
		for i, team_user in enumerate(team_users):
			base_params[f"team_user_{i}"] = team_user
	elif user and user != "__TEAM__":
		base_conds.append("lead_owner = %(user)s")
		base_params["user"] = user
	
	if project:
		base_conds.append("project = %(project)s")
		base_params["project"] = project
		# Exclude Duplicate leads when filtering by project
		# Use is_duplicate field (Check field) - exclude where is_duplicate = 1
		base_conds.append("COALESCE(is_duplicate, 0) = 0")

	# Get all statuses ordered by position
	statuses = frappe.db.sql(
		"""
		SELECT lead_status, color, position
		FROM `tabCRM Lead Status`
		ORDER BY position ASC
		""",
		as_dict=1,
	)

	result = []
	for status in statuses:
		status_name = status.lead_status
		
		# Build conditions for this status
		conds = base_conds.copy()
		conds.append("status = %(status)s")
		params = base_params.copy()
		params["status"] = status_name
		
		where_clause = "WHERE " + " AND ".join(conds)
		
		# Get current and previous period counts
		if from_date and to_date:
			# Date range specified
			counts = frappe.db.sql(
				f"""
				SELECT
					COUNT(CASE
						WHEN creation >= %(from_date)s AND creation < DATE_ADD(%(to_date)s, INTERVAL 1 DAY)
						THEN name
						ELSE NULL
					END) as current_count,

					COUNT(CASE
						WHEN creation >= %(prev_from_date)s AND creation < %(from_date)s
						THEN name
						ELSE NULL
					END) as prev_count
				FROM `tabCRM Lead`
				{where_clause}
				""",
				params,
				as_dict=1,
			)
		else:
			# All time - count all leads with this status
			counts = frappe.db.sql(
				f"""
				SELECT
					COUNT(*) as current_count,
					0 as prev_count
				FROM `tabCRM Lead`
				{where_clause}
				""",
				params,
				as_dict=1,
			)

		current_count = counts[0].current_count or 0
		prev_count = counts[0].prev_count or 0

		delta_in_percentage = (
			(current_count - prev_count) / prev_count * 100 if prev_count else 0
		)

		result.append({
			"status": status_name,
			"count": current_count,
			"color": status.color,
			"delta": delta_in_percentage,
		})

	return {
		"data": result,
		"title": _("Leads by status"),
		"subtitle": _("Lead count breakdown by status"),
		"categoryColumn": "status",
		"valueColumn": "count",
	}


def get_delayed_leads(from_date, to_date, user="", project="", team_users=None):
	"""
	Get delayed lead count for the dashboard.
	Note: Returns 0 if 'delayed' field doesn't exist in CRM Lead.
	"""
	# Check if 'delayed' field exists in CRM Lead
	doctype_meta = frappe.get_meta("CRM Lead")
	has_delayed_field = doctype_meta.has_field("delayed")
	
	if not has_delayed_field:
		# Field doesn't exist, return zero with appropriate message
		return {
			"title": _("Delayed leads"),
			"tooltip": _("Field 'delayed' not available in CRM Lead"),
			"value": 0,
		}
	
	conds = []
	params = {}
	
	# Handle empty dates (all time)
	if from_date and to_date:
		params["from_date"] = from_date
		params["to_date"] = to_date
		diff = frappe.utils.date_diff(to_date, from_date)
		if diff == 0:
			diff = 1
		params["prev_from_date"] = frappe.utils.add_days(from_date, -diff)
	else:
		# All time - no date filtering
		from_date = None
		to_date = None
		params["prev_from_date"] = None

	# Handle team filtering for Sales Manager
	if user == "__TEAM__" and team_users:
		placeholders = ", ".join([f"%(team_user_{i})s" for i in range(len(team_users))])
		conds.append(f"lead_owner IN ({placeholders})")
		for i, team_user in enumerate(team_users):
			params[f"team_user_{i}"] = team_user
	elif user and user != "__TEAM__":
		conds.append("lead_owner = %(user)s")
		params["user"] = user
	
	if project:
		conds.append("project = %(project)s")
		params["project"] = project
		# Exclude Duplicate leads when filtering by project
		# Use is_duplicate field (Check field) - exclude where is_duplicate = 1
		conds.append("COALESCE(is_duplicate, 0) = 0")

	where_clause = ""
	if conds:
		where_clause = "WHERE " + " AND ".join(conds)

	if from_date and to_date:
		# Date range specified
		result = frappe.db.sql(
			f"""
			SELECT
				COUNT(CASE
					WHEN creation >= %(from_date)s AND creation < DATE_ADD(%(to_date)s, INTERVAL 1 DAY)
						AND `delayed` = 1
					THEN name
					ELSE NULL
				END) as current_delayed,

				COUNT(CASE
					WHEN creation >= %(prev_from_date)s AND creation < %(from_date)s
						AND `delayed` = 1
					THEN name
					ELSE NULL
				END) as prev_delayed
			FROM `tabCRM Lead`
			{where_clause}
			""",
			params,
			as_dict=1,
		)
	else:
		# All time - count all delayed leads
		result = frappe.db.sql(
			f"""
			SELECT
				COUNT(CASE
					WHEN `delayed` = 1
					THEN name
					ELSE NULL
				END) as current_delayed,
				0 as prev_delayed
			FROM `tabCRM Lead`
			{where_clause}
			""",
			params,
			as_dict=1,
		)

	current_delayed = result[0].current_delayed or 0

	return {
		"title": _("Delayed leads"),
		"tooltip": _("Total number of delayed leads"),
		"value": current_delayed,
	}


def get_total_deals(from_date, to_date, user=""):
	"""
	Get total deal count for the dashboard.
	"""
	conds = []
	params = {}
	
	# Handle empty dates (all time)
	if from_date and to_date:
		params["from_date"] = from_date
		params["to_date"] = to_date
		diff = frappe.utils.date_diff(to_date, from_date)
		if diff == 0:
			diff = 1
		params["prev_from_date"] = frappe.utils.add_days(from_date, -diff)
	else:
		# All time - no date filtering
		from_date = None
		to_date = None
		params["prev_from_date"] = None

	if user:
		conds.append(f"deal_owner = '{user}'")

	where_clause = ""
	if conds:
		where_clause = "WHERE " + " AND ".join(conds)

	if from_date and to_date:
		# Date range specified
		result = frappe.db.sql(
			f"""
			SELECT
				COUNT(CASE
					WHEN creation >= %(from_date)s AND creation < DATE_ADD(%(to_date)s, INTERVAL 1 DAY)
					THEN name
					ELSE NULL
				END) as current_deals,

				COUNT(CASE
					WHEN creation >= %(prev_from_date)s AND creation < %(from_date)s
					THEN name
					ELSE NULL
				END) as prev_deals
			FROM `tabCRM Deal`
			{where_clause}
			""",
			params,
			as_dict=1,
		)
	else:
		# All time - count all deals
		result = frappe.db.sql(
			f"""
			SELECT
				COUNT(*) as current_deals,
				0 as prev_deals
			FROM `tabCRM Deal`
			{where_clause}
			""",
			params,
			as_dict=1,
		)

	current_deals = result[0].current_deals or 0

	return {
		"title": _("Total deals"),
		"tooltip": _("Total number of deals"),
		"value": current_deals,
	}


def get_leads_by_status_chart(from_date="", to_date="", user="", project=""):
	"""
	Get lead data by status for chart display.
	"""
	lead_conds = []
	params = {}

	# Handle empty dates (all time)
	if from_date and to_date:
		params["from"] = from_date
		params["to"] = to_date
	else:
		# All time - no date filtering
		from_date = None
		to_date = None

	if user:
		lead_conds.append("l.lead_owner = %(user)s")
		params["user"] = user
	
	if project:
		lead_conds.append("l.project = %(project)s")
		params["project"] = project
		# Exclude Duplicate leads when filtering by project
		# Use is_duplicate field (Check field) - exclude where is_duplicate = 1
		lead_conds.append("COALESCE(l.is_duplicate, 0) = 0")

	if from_date and to_date:
		# Date range specified
		date_cond = "DATE(l.creation) BETWEEN %(from)s AND %(to)s"
		if lead_conds:
			where_clause = f"WHERE {date_cond} AND " + " AND ".join(lead_conds)
		else:
			where_clause = f"WHERE {date_cond}"
	else:
		# All time - no date filtering
		if lead_conds:
			where_clause = "WHERE " + " AND ".join(lead_conds)
		else:
			where_clause = ""

	result = frappe.db.sql(
		f"""
		SELECT
			l.status AS status,
			COUNT(*) AS count,
			s.color AS color
		FROM `tabCRM Lead` AS l
		JOIN `tabCRM Lead Status` s ON l.status = s.lead_status
		{where_clause}
		GROUP BY l.status, s.position, s.color
		ORDER BY s.position ASC
		""",
		params,
		as_dict=True,
	)

	return {
		"data": result or [],
		"title": _("Leads by status"),
		"subtitle": _("Lead count breakdown by status"),
		"xAxis": {
			"title": _("Status"),
			"key": "status",
			"type": "category",
		},
		"yAxis": {
			"title": _("Count"),
		},
		"series": [
			{"name": "count", "type": "bar"},
		],
	}


def get_lead_status_count(from_date, to_date, user, status_name, project="", team_users=None):
	"""
	Helper function to get count for a specific status.
	"""
	conds = []
	params = {
		"status": status_name,
	}
	
	# Handle empty dates (all time)
	if from_date and to_date:
		params["from_date"] = from_date
		params["to_date"] = to_date
		diff = frappe.utils.date_diff(to_date, from_date)
		if diff == 0:
			diff = 1
		params["prev_from_date"] = frappe.utils.add_days(from_date, -diff)
	else:
		# All time - no date filtering
		from_date = None
		to_date = None
		params["prev_from_date"] = None

	# Handle team filtering for Sales Manager
	if user == "__TEAM__" and team_users:
		placeholders = ", ".join([f"%(team_user_{i})s" for i in range(len(team_users))])
		conds.append(f"lead_owner IN ({placeholders})")
		for i, team_user in enumerate(team_users):
			params[f"team_user_{i}"] = team_user
	elif user and user != "__TEAM__":
		conds.append("lead_owner = %(user)s")
		params["user"] = user
	
	if project:
		conds.append("project = %(project)s")
		params["project"] = project
		# Exclude Duplicate leads when filtering by project
		# Use is_duplicate field (Check field) - exclude where is_duplicate = 1
		conds.append("COALESCE(is_duplicate, 0) = 0")

	# Always filter by status
	conds.append("status = %(status)s")

	where_clause = "WHERE " + " AND ".join(conds)

	if from_date and to_date:
		# Date range specified
		result = frappe.db.sql(
			f"""
			SELECT
				COUNT(CASE
					WHEN creation >= %(from_date)s AND creation < DATE_ADD(%(to_date)s, INTERVAL 1 DAY)
					THEN name
					ELSE NULL
				END) as current_count,

				COUNT(CASE
					WHEN creation >= %(prev_from_date)s AND creation < %(from_date)s
					THEN name
					ELSE NULL
				END) as prev_count
			FROM `tabCRM Lead`
			{where_clause}
			""",
			params,
			as_dict=1,
		)
	else:
		# All time - count all leads with this status
		result = frappe.db.sql(
			f"""
			SELECT
				COUNT(*) as current_count,
				0 as prev_count
			FROM `tabCRM Lead`
			{where_clause}
			""",
			params,
			as_dict=1,
		)

	current_count = result[0].current_count or 0

	return {
		"title": _(status_name),
		"tooltip": _(f"{status_name} leads"),
		"value": current_count,
	}


def get_all_lead_statuses():
	"""
	Get all lead statuses from CRM Lead Status doctype.
	Returns list of status names ordered by position.
	"""
	statuses = frappe.db.sql(
		"""
		SELECT lead_status, color, position
		FROM `tabCRM Lead Status`
		ORDER BY position ASC
		""",
		as_dict=1,
	)
	return statuses


@frappe.whitelist()
def test_project_filter(project="", from_date="", to_date=""):
	"""
	Test function to verify that Duplicate leads are excluded when filtering by project.
	Returns counts for verification.
	"""
	if not project:
		return {"error": "Project parameter is required"}
	
	# Use current month if dates not provided
	if not from_date or not to_date:
		from_date = frappe.utils.get_first_day(frappe.utils.nowdate())
		to_date = frappe.utils.get_last_day(frappe.utils.nowdate())
	
	# Get total leads with project in date range (including duplicates)
	total_with_duplicates = frappe.db.sql(
		"""
		SELECT COUNT(*) as count
		FROM `tabCRM Lead`
		WHERE project = %s
		AND creation >= %s
		AND creation < DATE_ADD(%s, INTERVAL 1 DAY)
		""",
		(project, from_date, to_date),
		as_dict=1,
	)[0].count or 0
	
	# Get duplicate leads count in date range
	duplicate_count = frappe.db.sql(
		"""
		SELECT COUNT(*) as count
		FROM `tabCRM Lead`
		WHERE project = %s
		AND is_duplicate = 1
		AND creation >= %s
		AND creation < DATE_ADD(%s, INTERVAL 1 DAY)
		""",
		(project, from_date, to_date),
		as_dict=1,
	)[0].count or 0
	
	# Get count excluding duplicates in date range (what dashboard should show)
	excluded_duplicates = frappe.db.sql(
		"""
		SELECT COUNT(*) as count
		FROM `tabCRM Lead`
		WHERE project = %s
		AND COALESCE(is_duplicate, 0) = 0
		AND creation >= %s
		AND creation < DATE_ADD(%s, INTERVAL 1 DAY)
		""",
		(project, from_date, to_date),
		as_dict=1,
	)[0].count or 0
	
	# Get dashboard result
	dashboard_result = get_total_leads(from_date, to_date, "", project)
	
	return {
		"project": project,
		"from_date": str(from_date),
		"to_date": str(to_date),
		"total_leads_with_duplicates": total_with_duplicates,
		"duplicate_leads_count": duplicate_count,
		"leads_excluding_duplicates": excluded_duplicates,
		"dashboard_shows": dashboard_result.get("value", 0),
		"match": excluded_duplicates == dashboard_result.get("value", 0),
		"message": "✓ Match - Duplicate leads are correctly excluded" if excluded_duplicates == dashboard_result.get("value", 0) else "✗ Mismatch - Check the filter logic"
	}


@frappe.whitelist()
@sales_user_only
def get_all_projects():
	"""
	Get all Real Estate Projects ordered by creation date (newest first).
	Returns all projects without limit.
	"""
	# Use SQL query to get all projects without limit
	projects = frappe.db.sql(
		"""
		SELECT name, project_name
		FROM `tabReal Estate Project`
		ORDER BY creation DESC
		""",
		as_dict=True,
	)
	
	return projects


@frappe.whitelist()
@sales_user_only
def get_all_crm_users():
	"""
	Get all CRM Users (Sales User or Sales Manager roles) ordered by creation date (newest first).
	Returns all users without limit.
	For Sales Managers, returns only themselves and their team members.
	"""
	current_user = frappe.session.user
	roles = frappe.get_roles(current_user)
	is_sales_manager = "Sales Manager" in roles and "System Manager" not in roles
	
	# If Sales Manager, get only themselves and team members
	if is_sales_manager:
		# Get team members
		team_members = _get_team_members_for_leader(current_user)
		# Include the leader themselves
		team_members.append(current_user)
		
		# Get user details for team members
		crm_users = []
		for user_email in team_members:
			try:
				user_doc = frappe.get_doc("User", user_email)
				user_roles = frappe.get_roles(user_email)
				# Only include if they are Sales User or Sales Manager
				if "Sales User" in user_roles or "Sales Manager" in user_roles:
					crm_users.append({
						"name": user_doc.name,
						"full_name": user_doc.full_name or user_doc.name,
						"creation": user_doc.creation,
					})
			except frappe.DoesNotExistError:
				continue
			except Exception:
				continue
		
		# Sort by creation desc
		crm_users.sort(key=lambda x: x.get("creation", ""), reverse=True)
		return crm_users
	
	# For non-Sales Managers, return all CRM users
	# Use SQL query to get all enabled users
	all_users = frappe.db.sql(
		"""
		SELECT name, full_name, creation
		FROM `tabUser`
		WHERE enabled = 1
		ORDER BY creation DESC
		""",
		as_dict=True,
	)
	
	# Filter to only CRM users (Sales User or Sales Manager roles)
	crm_users = []
	for user in all_users:
		roles = frappe.get_roles(user.name)
		if "Sales User" in roles or "Sales Manager" in roles:
			crm_users.append(user)
	
	return crm_users


def _get_team_members_for_leader(team_leader):
	"""
	Get list of team member emails for a given team leader.
	
	Args:
		team_leader: Email/username of the team leader
		
	Returns:
		List of team member emails (excluding the leader)
	"""
	if not team_leader:
		return []
	
	# Find Team where current user is team_leader
	teams = frappe.get_all(
		"Team",
		filters={"team_leader": team_leader},
		fields=["name"],
		limit=1
	)
	
	if not teams:
		return []
	
	team_name = teams[0].name
	
	# Get team members from Member child table
	members = frappe.get_all(
		"Member",
		filters={
			"parent": team_name,
			"parenttype": "Team"
		},
		fields=["member"],
		pluck="member"
	)
	
	# Filter out None/empty values
	return [m for m in members if m]
