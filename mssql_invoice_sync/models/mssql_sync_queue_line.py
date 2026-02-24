from odoo import models, fields
import json
import logging

_logger = logging.getLogger(__name__)


class MssqlSyncQueueLine(models.Model):
    _name = 'mssql.sync.queue.line'
    _description = 'MSSQL Sync Queue Line'
    _order = 'id'

    queue_id = fields.Many2one(
        'mssql.sync.queue', string='Queue',
        required=True, ondelete='cascade', index=True)
    name = fields.Char(string='Name', required=True)
    sync_type = fields.Selection(
        related='queue_id.sync_type', store=True, index=True)
    mssql_id = fields.Char(
        string='MSSQL ID', required=True, index=True)
    mssql_table = fields.Char(string='MSSQL Table')
    record_data = fields.Text(
        string='Record Data',
        help='JSON blob of raw MSSQL data for retry')
    state = fields.Selection([
        ('draft', 'Draft'),
        ('done', 'Done'),
        ('failed', 'Failed'),
        ('cancel', 'Cancelled'),
    ], string='State', default='draft', required=True, index=True)
    error_message = fields.Text(string='Error Message')
    retry_count = fields.Integer(string='Retry Count', default=0)
    processed_at = fields.Datetime(string='Processed At')
    odoo_model = fields.Char(string='Odoo Model')
    odoo_record_id = fields.Integer(string='Odoo Record ID')

    def process_line(self):
        """Dispatch to type-specific processor on the sync config.

        Each sync type has a corresponding _process_queue_* method on
        mssql.sync (defined in the domain-specific files).

        Raises:
            Exception: re-raised from the processor so the queue
            can catch it and mark this line as failed.
        """
        self.ensure_one()
        config = self.queue_id.sync_config_id
        data = json.loads(self.record_data)
        sync_type = self.queue_id.sync_type

        processor_map = {
            'sales_session': '_process_queue_sales_session',
            'purchase_invoice': '_process_queue_purchase_invoice',
            'stock_transfer': '_process_queue_stock_transfer',
            'stock_adjustment': '_process_queue_stock_adjustment',
            'stock_scrap': '_process_queue_stock_scrap',
            'initial_stock': '_process_queue_initial_stock',
        }

        method_name = processor_map.get(sync_type)
        if not method_name:
            raise ValueError(f"Unknown sync type: {sync_type}")

        method = getattr(config, method_name, None)
        if not method:
            raise ValueError(
                f"Processor method {method_name} not found on mssql.sync")

        result = method(data, self)

        # Store created record reference
        if result and isinstance(result, dict):
            vals = {}
            if result.get('model'):
                vals['odoo_model'] = result['model']
            if result.get('id'):
                vals['odoo_record_id'] = result['id']
            if vals:
                self.write(vals)

    def action_retry_line(self):
        """Manual retry of a single failed line."""
        self.ensure_one()
        if self.state == 'failed':
            self.write({'state': 'draft'})
            self.queue_id.action_process_queue()

    def action_view_record(self):
        """Open the created Odoo record."""
        self.ensure_one()
        if self.odoo_model and self.odoo_record_id:
            return {
                'type': 'ir.actions.act_window',
                'res_model': self.odoo_model,
                'res_id': self.odoo_record_id,
                'view_mode': 'form',
                'target': 'current',
            }
