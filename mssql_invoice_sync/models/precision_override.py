from odoo import models, fields


class SaleOrderLine(models.Model):
    _inherit = 'sale.order.line'

    # Override price_subtotal to use 6 decimal places for precision
    # This prevents rounding errors before tax calculation
    price_subtotal = fields.Monetary(
        string="Subtotal",
        compute='_compute_amount',
        store=True, precompute=True , digits=(0, 6))


class AccountMoveLine(models.Model):
    _inherit = 'account.move.line'

    # Override price_subtotal to use 6 decimal places for precision
    # This prevents rounding errors before tax calculation
    price_subtotal = fields.Monetary(
        string='Subtotal',
        compute='_compute_totals', store=True,
        currency_field='currency_id', digits=(0, 6)
    )
