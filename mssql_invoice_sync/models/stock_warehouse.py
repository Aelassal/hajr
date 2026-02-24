from odoo import models, fields


class StockWarehouse(models.Model):
    _inherit = 'stock.warehouse'

    x_sql_branch_id = fields.Integer(string='SQL Branch ID', index=True)


