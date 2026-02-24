from odoo import models, fields


class PurchaseOrderLine(models.Model):
    _inherit = 'purchase.order.line'

    def _prepare_account_move_line(self, move=False):
        """Override to ensure invoice quantity doesn't exceed product_qty"""
        res = super()._prepare_account_move_line(move)
        
        # Ensure invoice quantity doesn't exceed product_qty
        if res.get('quantity', 0) > self.product_qty:
            res['quantity'] = self.product_qty
        
        return res

