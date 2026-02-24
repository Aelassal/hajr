from odoo import models, fields


class ResPartner(models.Model):
    _inherit = 'res.partner'

    x_sql_vendor_id = fields.Integer(string='SQL Vendor ID', index=True)
    x_sql_customer_id = fields.Integer(string='SQL Customer ID', index=True)


