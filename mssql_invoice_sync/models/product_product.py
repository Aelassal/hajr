from odoo import models, fields


class ProductProduct(models.Model):
    _inherit = 'product.product'

    x_sql_item_id = fields.Integer(string='SQL Item ID', index=True)
    x_english_name = fields.Char(string='English Name')



