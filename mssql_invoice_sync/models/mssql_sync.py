from odoo import models, fields
from odoo.exceptions import UserError
import pymssql
import logging

_logger = logging.getLogger(__name__)


class MssqlSync(models.Model):
    _name = 'mssql.sync'
    _description = 'SQL Server Sync Configuration'

    # ── Connection Fields ─────────────────────────────────────────────
    name = fields.Char(string='Name', default='SQL Server Connection', required=True)
    server = fields.Char(string='Server', default='localhost', required=True)
    port = fields.Integer(string='Port', default=1433, required=True)
    database = fields.Char(string='Database', default='EPOSData', required=True)
    username = fields.Char(string='Username', default='SA', required=True)
    password = fields.Char(string='Password', required=True)
    trust_cert = fields.Boolean(string='Trust Server Certificate', default=True)

    # ── Shared Tracking Fields ────────────────────────────────────────
    products_fetched = fields.Boolean(string='Products Fetched', default=False)
    vendors_fetched = fields.Boolean(string='Vendors Fetched', default=False)
    customers_fetched = fields.Boolean(string='Customers Fetched', default=False)

    # ── Connection ────────────────────────────────────────────────────

    def _get_connection(self):
        """Create and return SQL Server connection"""
        try:
            conn = pymssql.connect(
                server=self.server,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database
            )
            return conn
        except Exception as e:
            raise UserError(f'Connection failed: {str(e)}')

    def test_connection(self):
        """Test SQL Server connection"""
        try:
            conn = self._get_connection()
            conn.close()
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Success',
                    'message': 'Connection successful!',
                    'type': 'success',
                    'sticky': False,
                }
            }
        except Exception as e:
            raise UserError(f'Connection test failed: {str(e)}')

    # ── Shared UI Helpers ─────────────────────────────────────────────

    def _prepare_invoice_list_view(self, invoice_ids):
        """Prepare return view for invoice list

        Returns:
            dict: Action dict for opening invoice list view
        """
        return {
            'type': 'ir.actions.act_window',
            'name': 'Sales Invoices',
            'res_model': 'account.move',
            'domain': [('id', 'in', invoice_ids)],
            'view_mode': 'list,form',
            'target': 'current',
        }

    def _success_notification(self, title, message):
        """Prepare success notification dict

        Returns:
            dict: Notification action dict
        """
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': title,
                'message': message,
                'type': 'success',
                'sticky': False,
            }
        }

    # ── Shared Product Helpers ────────────────────────────────────────

    def _get_or_create_decimal_product(self):
        """Get or create a 'Decimal' product for handling rounding adjustments

        # ============================================================================
        # DECIMAL ADJUSTMENT PRODUCT
        # Used to handle small rounding differences between MSSQL totals and
        # Odoo calculated totals. This ensures invoice totals match exactly.
        # ============================================================================
        """
        product = self.env['product.product'].search([
            ('name', '=', 'Decimal'),
            ('type', '=', 'service'),
        ], limit=1)

        if not product:
            product = self.env['product.product'].create({
                'name': 'Decimal',
                'type': 'service',  # Service type - no stock impact
                'invoice_policy': 'order',
            })
            _logger.info(f"Created 'Decimal' product with ID {product.id}")

        return product

    # ── Sync Log Helpers ──────────────────────────────────────────────

    def _is_already_synced(self, sync_type, mssql_id, mssql_table):
        """Check if a record has already been synced (idempotency check).

        Args:
            sync_type: One of product/vendor/customer/transfer/adjustment/scrap/reconciliation
            mssql_id: The MSSQL primary key value (as string)
            mssql_table: The MSSQL source table name
        Returns:
            bool: True if already synced successfully
        """
        return bool(self.env['mssql.sync.log'].search_count([
            ('sync_type', '=', sync_type),
            ('mssql_id', '=', str(mssql_id)),
            ('mssql_table', '=', mssql_table),
            ('status', '=', 'success'),
        ], limit=1))

    def _log_sync(self, sync_type, mssql_id, mssql_table, odoo_model=False,
                  odoo_record_id=False, status='success', error_message=False, notes=False):
        """Create a sync log entry.

        Args:
            sync_type: One of product/vendor/customer/transfer/adjustment/scrap/reconciliation
            mssql_id: The MSSQL primary key value (as string)
            mssql_table: The MSSQL source table name
            odoo_model: Name of the Odoo model created (e.g. 'stock.picking')
            odoo_record_id: ID of the Odoo record created
            status: success/error/skipped
            error_message: Error details if status is error
            notes: Additional notes
        Returns:
            mssql.sync.log record
        """
        return self.env['mssql.sync.log'].create({
            'sync_type': sync_type,
            'mssql_id': str(mssql_id),
            'mssql_table': mssql_table,
            'odoo_model': odoo_model or '',
            'odoo_record_id': odoo_record_id or 0,
            'status': status,
            'error_message': error_message or '',
            'notes': notes or '',
        })

    def _get_synced_ids(self, sync_type, mssql_table):
        """Get all successfully synced MSSQL IDs for a given type/table.

        Returns:
            set: Set of mssql_id strings that have been synced
        """
        logs = self.env['mssql.sync.log'].search([
            ('sync_type', '=', sync_type),
            ('mssql_table', '=', mssql_table),
            ('status', '=', 'success'),
        ])
        return set(logs.mapped('mssql_id'))

    # ── Change Detection Helper ───────────────────────────────────────

    def _has_record_changed(self, record, new_vals, skip_fields=None):
        """Check if record values have actually changed (DRY helper)

        Args:
            record: Odoo record (partner or product)
            new_vals: dict of new values to compare
            skip_fields: list of field names to skip in comparison

        Returns:
            bool: True if any field has changed
        """
        if skip_fields is None:
            skip_fields = []

        for field, new_value in new_vals.items():
            if field in skip_fields:
                continue

            current_value = getattr(record, field, None)

            # Handle None/False comparison
            if current_value in [None, False] and new_value in [None, False]:
                continue

            # Handle float comparison with tolerance
            if isinstance(new_value, (int, float)):
                if abs(float(current_value or 0) - float(new_value)) > 0.01:
                    return True
            elif str(current_value or '').strip() != str(new_value or '').strip():
                return True

        return False

    # ── Generic Partner Sync ──────────────────────────────────────────

    def _generic_partner_sync(self, sql_records, sql_id_field, odoo_id_field, partner_type, field_mapping, only_new=False):
        """Generic partner sync logic - works for vendors and customers

        Args:
            sql_records: List of records from MSSQL
            sql_id_field: Name of ID field in SQL (e.g., 'SupplierID')
            odoo_id_field: Name of Odoo custom field (e.g., 'x_sql_vendor_id')
            partner_type: 'supplier' or 'customer'
            field_mapping: Dict mapping Odoo field names to SQL field names
            only_new: If True, only create new records, skip updates entirely

        Returns:
            tuple: (created_count, updated_count, skipped_count)
        """
        partner_obj = self.env['res.partner']

        # Get all IDs for batch lookup
        sql_ids = [r[sql_id_field] for r in sql_records if r.get(sql_id_field)]

        if not sql_ids:
            _logger.info(f'No {partner_type}s with {sql_id_field} found in MSSQL')
            return 0, 0, 0

        # Fetch existing partners
        rank_field = 'supplier_rank' if partner_type == 'supplier' else 'customer_rank'
        existing_partners = {
            getattr(p, odoo_id_field): p for p in partner_obj.search([
                (odoo_id_field, 'in', sql_ids),
                (rank_field, '>', 0)
            ])
        }

        # Separate into create and update batches
        to_create = []
        to_update = []
        skipped = 0

        for record in sql_records:
            record_id = record[sql_id_field]
            if not record_id:
                continue

            vals = {
                'name': record[field_mapping['name']] or f"{partner_type.title()} {record_id}",
                odoo_id_field: record_id,
                rank_field: 1,
            }

            # Map fields dynamically
            for odoo_field, sql_field in field_mapping.items():
                if odoo_field == 'name':  # Already handled
                    continue

                # Handle special field mapping syntax
                if isinstance(sql_field, dict):
                    # {'_concat': ['Field1', 'Field2']} - concatenate with " / "
                    if '_concat' in sql_field:
                        parts = [str(record.get(f, '') or '').strip() for f in sql_field['_concat']]
                        parts = [p for p in parts if p]  # Remove empty
                        if parts:
                            vals[odoo_field] = ' / '.join(parts)

                    # {'_combine': ['Field1', 'Field2']} - combine with ", "
                    elif '_combine' in sql_field:
                        parts = [str(record.get(f, '') or '').strip() for f in sql_field['_combine']]
                        parts = [p for p in parts if p]  # Remove empty
                        if parts:
                            vals[odoo_field] = ', '.join(parts)

                    # {'_note': [('Label', 'Field'), ...]} - build labeled note
                    elif '_note' in sql_field:
                        note_parts = []
                        for label, field_name in sql_field['_note']:
                            value = str(record.get(field_name, '') or '').strip()
                            if value:
                                note_parts.append(f"{label}: {value}")
                        if note_parts:
                            vals[odoo_field] = '\n'.join(note_parts)

                # Handle list of fields (fallback - first non-empty)
                elif isinstance(sql_field, list):
                    for sf in sql_field:
                        if record.get(sf):
                            vals[odoo_field] = record[sf]
                            break

                # Simple field mapping
                elif record.get(sql_field):
                    # Convert to string for ref field to handle integer IDs
                    value = record[sql_field]
                    if odoo_field == 'ref':
                        value = str(value)
                    vals[odoo_field] = value

            if record_id in existing_partners:
                # Skip existing records if only_new is True
                if only_new:
                    skipped += 1
                    continue
                to_update.append((existing_partners[record_id], vals))
            else:
                to_create.append(vals)

        # Batch create
        created = 0
        if to_create:
            _logger.info(f"Creating {len(to_create)} new {partner_type}s in batches...")
            batch_size = 1000
            for i in range(0, len(to_create), batch_size):
                batch = to_create[i:i + batch_size]
                partner_obj.create(batch)
                created += len(batch)
                _logger.info(f"{partner_type.title()} creation progress: {created}/{len(to_create)}")
                self.env.clear()

        # Batch update
        updated = 0
        if to_update:
            update_map = {p.id: vals for p, vals in to_update}
            batch_size = 1000
            partner_ids = list(update_map.keys())
            for i in range(0, len(partner_ids), batch_size):
                batch_ids = partner_ids[i:i + batch_size]
                batch_partners = partner_obj.browse(batch_ids)
                for partner in batch_partners:
                    partner.write(update_map[partner.id])
                updated += len(batch_ids)
                if i % (batch_size * 10) == 0:
                    self.env.clear()

        return created, updated, skipped

    # ── Product / Warehouse Lookup Helpers ─────────────────────────────

    def _get_product_map(self, item_ids=None):
        """Build a dict mapping x_sql_item_id -> product.product record.

        Args:
            item_ids: optional list of ItemIDs to filter. If None, fetches all MSSQL products.
        Returns:
            dict: {int(ItemID): product.product record}
        """
        domain = [('x_sql_item_id', '!=', False)]
        if item_ids:
            domain.append(('x_sql_item_id', 'in', list(item_ids)))
        products = self.env['product.product'].search(domain)
        return {p.x_sql_item_id: p for p in products}

    def _get_warehouse_map(self):
        """Build a dict mapping x_sql_branch_id -> stock.warehouse record.

        Returns:
            dict: {int(BranchID): stock.warehouse record}
        """
        warehouses = self.env['stock.warehouse'].search([
            ('x_sql_branch_id', '!=', False),
            ('company_id', '=', self.env.company.id),
        ])
        return {w.x_sql_branch_id: w for w in warehouses}
