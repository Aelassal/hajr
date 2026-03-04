from odoo import models, fields, api
from odoo.exceptions import UserError
from datetime import timedelta
import logging

_logger = logging.getLogger(__name__)


class MssqlOperations(models.TransientModel):
    _name = 'mssql.operations'
    _description = 'MSSQL Sync Operations Wizard'

    sync_config_id = fields.Many2one(
        'mssql.sync', string='Sync Configuration', required=True,
        default=lambda self: self.env['mssql.sync'].search([], limit=1).id)

    operation = fields.Selection([
        ('sync_products', 'Import Products'),
        ('sync_vendors', 'Import Vendors'),
        ('sync_customers', 'Import Customers'),
        ('sync_sales_invoices', 'Import Sales Invoices (POS Sessions)'),
        ('sync_purchase_invoices', 'Import Purchase Invoices'),
        ('sync_stock_transfers', 'Import Stock Transfers'),
        ('sync_stock_adjustments', 'Import Stock Adjustments'),
        ('sync_stock_scrap', 'Import Scrap/Write-offs'),
        ('reconcile_stock', 'Reconcile Stock Levels'),
        ('initial_stock', 'Set Initial Stock from Current Balances'),
        ('update_products', 'Update Products (Prices, Qty, Barcode)'),
        ('migrate_storable', 'Migrate Products to Storable'),
    ], string='Operation', required=True)

    date_from = fields.Date(string='Date From')
    date_to = fields.Date(string='Date To')

    def execute(self):
        """Execute the selected operation."""
        self.ensure_one()
        config = self.sync_config_id
        if not config:
            raise UserError('Please select a Sync Configuration.')

        op = self.operation
        _logger.info(f"Operations Wizard: executing '{op}' on config '{config.name}'")

        if op == 'sync_products':
            return config.sync_products()

        elif op == 'sync_vendors':
            return config.sync_vendors()

        elif op == 'sync_customers':
            return config.sync_customers()

        elif op == 'sync_sales_invoices':
            if not self.date_from:
                raise UserError('Please specify a Date From for sales invoice import.')
            # Process each date in the range
            date_from = self.date_from
            date_to = self.date_to or self.date_from
            current_date = date_from
            results = []
            while current_date <= date_to:
                try:
                    result = config.create_session_based_invoices(current_date)
                    results.append(f"{current_date}: OK")
                except Exception as e:
                    results.append(f"{current_date}: Error - {str(e)}")
                current_date += timedelta(days=1)
            summary = '\n'.join(results)
            return config._success_notification('Sales Invoice Import', summary)

        elif op == 'sync_purchase_invoices':
            if not self.date_from:
                raise UserError('Please specify a Date From for purchase invoice import.')
            date_from = self.date_from
            date_to = self.date_to or self.date_from
            current_date = date_from
            results = []
            while current_date <= date_to:
                try:
                    config.write({'purchase_invoice_date': current_date})
                    result = config.sync_purchase_invoices()
                    results.append(f"{current_date}: OK")
                except Exception as e:
                    results.append(f"{current_date}: Error - {str(e)}")
                current_date += timedelta(days=1)
            summary = '\n'.join(results)
            return config._success_notification('Purchase Invoice Import', summary)

        elif op == 'sync_stock_transfers':
            return config.sync_stock_transfers(
                date_from=self.date_from, date_to=self.date_to)

        elif op == 'sync_stock_adjustments':
            return config.sync_stock_adjustments(
                date_from=self.date_from, date_to=self.date_to)

        elif op == 'sync_stock_scrap':
            return config.sync_stock_scrap(
                date_from=self.date_from, date_to=self.date_to)

        elif op == 'reconcile_stock':
            return config.action_reconcile_stock()

        elif op == 'initial_stock':
            config._set_initial_stock_levels()
            return config._success_notification(
                'Initial Stock Set', 'Stock levels set from MSSQL current balances.')

        elif op == 'update_products':
            return config.action_update_products()

        elif op == 'migrate_storable':
            return config.action_migrate_products_to_storable()

        else:
            raise UserError(f'Unknown operation: {op}')
