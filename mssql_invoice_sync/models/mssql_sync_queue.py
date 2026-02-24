from odoo import models, fields, api
import logging

_logger = logging.getLogger(__name__)

SYNC_TYPE_SELECTION = [
    ('sales_session', 'Sales Session'),
    ('purchase_invoice', 'Purchase Invoice'),
    ('stock_transfer', 'Stock Transfer'),
    ('stock_adjustment', 'Stock Adjustment'),
    ('stock_scrap', 'Stock Scrap'),
    ('initial_stock', 'Initial Stock'),
]


class MssqlSyncQueue(models.Model):
    _name = 'mssql.sync.queue'
    _inherit = ['mail.thread']
    _description = 'MSSQL Sync Queue'
    _order = 'id desc'

    name = fields.Char(string='Name', readonly=True, default='/', copy=False)
    sync_config_id = fields.Many2one(
        'mssql.sync', string='Sync Configuration',
        required=True, ondelete='cascade')
    sync_type = fields.Selection(
        SYNC_TYPE_SELECTION, string='Sync Type',
        required=True, index=True)
    sync_date = fields.Date(string='Sync Date')

    state = fields.Selection([
        ('draft', 'Draft'),
        ('processing', 'Processing'),
        ('partially_completed', 'Partially Completed'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ], string='State', compute='_compute_state', store=True, default='draft')

    is_processing = fields.Boolean(string='Processing', default=False)
    queue_process_count = fields.Integer(string='Process Count', default=0)
    is_action_require = fields.Boolean(string='Action Required', default=False)

    line_ids = fields.One2many(
        'mssql.sync.queue.line', 'queue_id', string='Queue Lines')

    total_count = fields.Integer(
        string='Total', compute='_compute_counts', store=True)
    draft_count = fields.Integer(
        string='Draft', compute='_compute_counts', store=True)
    done_count = fields.Integer(
        string='Done', compute='_compute_counts', store=True)
    failed_count = fields.Integer(
        string='Failed', compute='_compute_counts', store=True)
    cancel_count = fields.Integer(
        string='Cancelled', compute='_compute_counts', store=True)

    # ── Computed Fields ────────────────────────────────────────────────

    @api.depends('line_ids.state', 'is_processing')
    def _compute_state(self):
        for record in self:
            if record.is_processing:
                record.state = 'processing'
                continue
            if not record.line_ids:
                record.state = 'draft'
                continue

            states = record.line_ids.mapped('state')
            total = len(states)
            done = states.count('done')
            cancel = states.count('cancel')
            draft = states.count('draft')
            failed = states.count('failed')

            if done + cancel == total:
                record.state = 'completed'
            elif draft == total:
                record.state = 'draft'
            elif failed + cancel == total:
                record.state = 'failed'
            else:
                record.state = 'partially_completed'

    @api.depends('line_ids.state')
    def _compute_counts(self):
        for record in self:
            lines = record.line_ids
            record.total_count = len(lines)
            record.draft_count = len(lines.filtered(lambda l: l.state == 'draft'))
            record.done_count = len(lines.filtered(lambda l: l.state == 'done'))
            record.failed_count = len(lines.filtered(lambda l: l.state == 'failed'))
            record.cancel_count = len(lines.filtered(lambda l: l.state == 'cancel'))

    # ── Create Override ────────────────────────────────────────────────

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('name', '/') == '/':
                vals['name'] = self.env['ir.sequence'].next_by_code(
                    'mssql.sync.queue') or '/'
        return super().create(vals_list)

    # ── Queue Actions ──────────────────────────────────────────────────

    def action_process_queue(self):
        """Process all draft and failed lines in this queue."""
        self.ensure_one()
        self.write({'is_processing': True})

        lines_to_process = self.line_ids.filtered(
            lambda l: l.state in ('draft', 'failed'))

        if not lines_to_process:
            self.write({'is_processing': False})
            return

        _logger.info(f"Processing queue {self.name}: "
                     f"{len(lines_to_process)} lines to process")

        for line in lines_to_process:
            try:
                # Use savepoint to isolate each line's processing
                with self.env.cr.savepoint():
                    line.process_line()
                # Mark success outside savepoint so it persists
                line.write({
                    'state': 'done',
                    'processed_at': fields.Datetime.now(),
                    'error_message': False,
                })
            except Exception as e:
                # Savepoint already rolled back the failed processing
                line.write({
                    'state': 'failed',
                    'error_message': str(e),
                    'retry_count': line.retry_count + 1,
                    'processed_at': fields.Datetime.now(),
                })
                _logger.error(f"Queue line {line.name} failed: {str(e)}")

        self.write({'is_processing': False})
        _logger.info(f"Queue {self.name} processing complete: "
                     f"{self.done_count} done, {self.failed_count} failed")

    def action_retry_failed(self):
        """Reset failed lines to draft and reprocess."""
        self.ensure_one()
        failed_lines = self.line_ids.filtered(lambda l: l.state == 'failed')
        if failed_lines:
            failed_lines.write({'state': 'draft'})
            self.write({'is_action_require': False})
            self.action_process_queue()

    def action_set_to_completed(self):
        """Cancel remaining draft/failed lines."""
        self.ensure_one()
        remaining = self.line_ids.filtered(
            lambda l: l.state in ('draft', 'failed'))
        if remaining:
            remaining.write({'state': 'cancel'})

    # ── Cron ───────────────────────────────────────────────────────────

    @api.model
    def cron_process_sync_queues(self):
        """Cron: auto-retry queues with draft/failed lines (max 3 attempts)."""
        queues = self.search([
            ('state', 'in', ('draft', 'partially_completed', 'failed')),
            ('is_action_require', '=', False),
            ('is_processing', '=', False),
        ])

        for queue in queues:
            queue.queue_process_count += 1

            if queue.queue_process_count > 3:
                queue.write({'is_action_require': True})
                queue.message_post(
                    body='This queue requires manual processing. '
                         '3 automatic retry attempts have been exhausted.',
                    message_type='comment',
                    subtype_xmlid='mail.mt_note',
                )
                _logger.warning(
                    f"Queue {queue.name} marked as action required "
                    f"after 3 attempts")
                continue

            _logger.info(f"Auto-retry queue {queue.name} "
                         f"(attempt {queue.queue_process_count})")

            # Reset failed lines to draft for retry
            failed_lines = queue.line_ids.filtered(
                lambda l: l.state == 'failed')
            if failed_lines:
                failed_lines.write({'state': 'draft'})

            try:
                queue.action_process_queue()
            except Exception as e:
                _logger.error(
                    f"Cron: queue {queue.name} processing failed: {str(e)}")

            self.env.cr.commit()
