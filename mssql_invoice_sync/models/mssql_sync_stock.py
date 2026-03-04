from odoo import models, fields, api
from odoo.exceptions import UserError
import json
import logging

_logger = logging.getLogger(__name__)


class MssqlSyncStock(models.Model):
    _inherit = 'mssql.sync'

    # ── Stock Sync Tracking Fields ────────────────────────────────────
    smart_connect_done = fields.Boolean(string='Smart Connect Done', default=False,
                                         help='Set to True after Smart Connect has run successfully')
    initial_sync_date = fields.Datetime(string='Initial Sync Date',
                                         help='Date when Smart Connect first ran and set initial stock')
    last_stock_sync_date = fields.Datetime(string='Last Stock Sync Date')
    last_stock_sync_trans_id = fields.Char(string='Last Stock Sync TransID',
                                            help='Watermark TransID from tblItemsTrans for incremental sync')
    stock_transfers_synced = fields.Integer(string='Stock Transfers Synced', default=0)
    stock_adjustments_synced = fields.Integer(string='Stock Adjustments Synced', default=0)
    last_reconciliation_date = fields.Datetime(string='Last Reconciliation Date')
    reconciliation_discrepancy_count = fields.Integer(string='Reconciliation Discrepancies', default=0)

    # ── Smart Connect ─────────────────────────────────────────────────

    def action_smart_connect(self):
        """One-click setup: test connection, sync all master data, set initial stock."""
        self.ensure_one()

        _logger.info("=== Smart Connect: Starting ===")

        # Step 1: Test connection
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            # Step 2: Sync branches -> warehouses
            _logger.info("Smart Connect: Syncing branches...")
            branches = self._query_all_branches(cursor)
            branch_ids = [b['BranchID'] for b in branches]
            conn.close()

            if branch_ids:
                self._get_or_create_warehouses(branch_ids)
            _logger.info(f"Smart Connect: {len(branch_ids)} branches synced")

            # Step 3: Sync products (storable)
            _logger.info("Smart Connect: Syncing products...")
            self.sync_products()

            # Step 4: Migrate any existing non-storable products
            _logger.info("Smart Connect: Migrating products to storable...")
            self.action_migrate_products_to_storable()

            # Step 5: Sync vendors
            _logger.info("Smart Connect: Syncing vendors...")
            self.sync_vendors()

            # Step 6: Sync customers
            _logger.info("Smart Connect: Syncing customers...")
            self.sync_customers()

            # Step 7: Skip initial stock — qty starts at 0, managed by PO/SO pickings
            _logger.info("Smart Connect: Skipping initial stock (all products start at 0)")

            # Step 8: Record watermarks
            conn2 = self._get_connection()
            cursor2 = conn2.cursor(as_dict=True)
            max_trans_id = self._query_max_trans_id(cursor2)
            conn2.close()

            now = fields.Datetime.now()
            self.write({
                'smart_connect_done': True,
                'initial_sync_date': now,
                'last_product_sync_date': now,
                'last_stock_sync_date': now,
                'last_stock_sync_trans_id': max_trans_id,
            })

            _logger.info(f"=== Smart Connect: Complete (watermark TransID={max_trans_id}) ===")
            return self._success_notification(
                'Smart Connect Complete',
                f'Branches: {len(branch_ids)}, Products synced, Initial stock set. '
                f'Watermark TransID: {max_trans_id}')

        except Exception as e:
            _logger.error(f"Smart Connect failed: {str(e)}", exc_info=True)
            try:
                conn.close()
            except Exception:
                pass
            raise UserError(f'Smart Connect failed: {str(e)}')

    def _set_initial_stock_levels(self):
        """Set initial stock quants from tblItemsTrans per branch/product.

        Optimized for 15k+ products using direct SQL batch insert.
        This is appropriate for initial stock setup (no prior state to adjust from).
        """
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            stock_data = self._query_current_stock_levels(cursor)
            conn.close()
        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            raise UserError(f'Failed to query stock levels: {str(e)}')

        if not stock_data:
            _logger.info("No stock levels found in MSSQL")
            return

        # Build lookup maps
        product_map = self._get_product_map()
        warehouse_map = self._get_warehouse_map()

        company_id = self.env.company.id
        uid = self.env.uid
        now = fields.Datetime.now()

        # Deduplicate: keep last entry per (product_id, location_id)
        # and resolve product/warehouse in one pass
        quant_entries = {}  # {(product_id, location_id): qty}
        skip_count = 0
        skipped_lines = []

        for row in stock_data:
            product = product_map.get(row['ItemID'])
            warehouse = warehouse_map.get(row['BranchID'])
            if not product or not warehouse or not warehouse.lot_stock_id:
                balance = float(row['CurrentBalance'] or 0)
                if balance != 0:
                    reasons = []
                    if not product:
                        reasons.append(f"Product not found for ItemID={row['ItemID']}")
                    if not warehouse or not warehouse.lot_stock_id:
                        reasons.append(f"Warehouse not found for BranchID={row['BranchID']}")
                    skipped_lines.append({
                        'name': f"Stock: Item {row['ItemID']} @ Branch {row['BranchID']}",
                        'mssql_id': f"{row['ItemID']}_{row['BranchID']}",
                        'mssql_table': 'tblItemsTrans',
                        'record_data': json.dumps({
                            'ItemID': row['ItemID'],
                            'BranchID': row['BranchID'],
                            'CurrentBalance': float(row['CurrentBalance'] or 0),
                        }),
                        'state': 'failed',
                        'error_message': '; '.join(reasons),
                    })
                skip_count += 1
                continue
            balance = float(row['CurrentBalance'] or 0)
            if balance == 0:
                skip_count += 1
                continue
            quant_entries[(product.id, warehouse.lot_stock_id.id)] = balance

        # Create queue for skipped records with non-zero balance
        if skipped_lines:
            queue = self.env['mssql.sync.queue'].create({
                'sync_config_id': self.id,
                'sync_type': 'initial_stock',
            })
            for line_vals in skipped_lines:
                line_vals['queue_id'] = queue.id
                self.env['mssql.sync.queue.line'].create(line_vals)
            _logger.info(f"Initial stock: {len(skipped_lines)} skipped records "
                         f"queued for retry in {queue.name}")

        if not quant_entries:
            _logger.info("No non-zero stock levels to set")
            return

        _logger.info(f"Setting initial stock for {len(quant_entries)} product/location pairs...")

        # Pre-fetch existing quants in ONE query to handle re-runs
        all_product_ids = list({pid for pid, _ in quant_entries})
        all_location_ids = list({lid for _, lid in quant_entries})

        self.env.cr.execute("""
            SELECT id, product_id, location_id
            FROM stock_quant
            WHERE product_id = ANY(%s)
              AND location_id = ANY(%s)
              AND company_id = %s
              AND lot_id IS NULL
              AND package_id IS NULL
              AND owner_id IS NULL
        """, (all_product_ids, all_location_ids, company_id))
        existing = {(r[1], r[2]): r[0] for r in self.env.cr.fetchall()}

        # Split into inserts and updates
        to_insert = []
        to_update = []
        for (product_id, location_id), qty in quant_entries.items():
            if (product_id, location_id) in existing:
                to_update.append((qty, existing[(product_id, location_id)]))
            else:
                to_insert.append((product_id, location_id, qty))

        # Batch INSERT new quants (1000 per batch)
        BATCH = 1000
        inserted = 0
        for i in range(0, len(to_insert), BATCH):
            batch = to_insert[i:i + BATCH]
            args = []
            placeholders = []
            for product_id, location_id, qty in batch:
                placeholders.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                args.extend([product_id, location_id, qty, 0, company_id, now, uid, now, uid, now])
            self.env.cr.execute(
                "INSERT INTO stock_quant "
                "(product_id, location_id, quantity, reserved_quantity, company_id, in_date, "
                "create_uid, create_date, write_uid, write_date) VALUES "
                + ", ".join(placeholders), args)
            inserted += len(batch)
            if inserted % 5000 == 0:
                _logger.info(f"Initial stock insert progress: {inserted}/{len(to_insert)}")

        # Batch UPDATE existing quants
        updated = 0
        if to_update:
            self.env.cr.executemany(
                "UPDATE stock_quant SET quantity = %s, write_date = %s, write_uid = %s WHERE id = %s",
                [(qty, now, uid, qid) for qty, qid in to_update])
            updated = len(to_update)

        # Invalidate ORM cache so Odoo sees the new quant data
        self.env['stock.quant'].invalidate_model()

        _logger.info(f"Initial stock: inserted {inserted}, updated {updated}, skipped {skip_count}")

    # ── Stock Transfers Sync ──────────────────────────────────────────

    def sync_stock_transfers(self, date_from=None, date_to=None):
        """Sync stock transfers from tblStockTransfer via queue.

        Args:
            date_from: Optional date filter (inclusive)
            date_to: Optional date filter (inclusive)
        """
        self.ensure_one()
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            synced_ids = self._get_synced_ids('transfer', 'tblStockTransfer')
            transfers = self._query_stock_transfers(cursor, exclude_ids=synced_ids or None)

            # Date filtering if provided
            if date_from or date_to:
                filtered = []
                for t in transfers:
                    td = t['TransferDate']
                    if td:
                        d = td.date() if hasattr(td, 'date') else td
                        if date_from and d < date_from:
                            continue
                        if date_to and d > date_to:
                            continue
                    filtered.append(t)
                transfers = filtered

            if not transfers:
                conn.close()
                _logger.info("sync_stock_transfers: No new transfers found")
                return

            transfer_ids = [t['TransferID'] for t in transfers]
            details = self._query_stock_transfer_details(cursor, transfer_ids)
            conn.close()

            # Group details by TransferID
            details_map = {}
            for d in details:
                details_map.setdefault(d['TransferID'], []).append(d)

            # Check existing queue lines to avoid duplicates
            existing_queued = set(
                self.env['mssql.sync.queue.line'].search([
                    ('queue_id.sync_config_id', '=', self.id),
                    ('queue_id.sync_type', '=', 'stock_transfer'),
                    ('mssql_table', '=', 'tblStockTransfer'),
                    ('state', 'in', ('draft', 'failed')),
                ]).mapped('mssql_id'))

            # Create queue
            queue = self.env['mssql.sync.queue'].create({
                'sync_config_id': self.id,
                'sync_type': 'stock_transfer',
            })

            for transfer in transfers:
                tid = str(transfer['TransferID'])
                if tid in existing_queued:
                    continue
                lines = details_map.get(transfer['TransferID'], [])
                data = {'transfer': transfer, 'details': lines}
                self.env['mssql.sync.queue.line'].create({
                    'queue_id': queue.id,
                    'name': f"Transfer {tid}",
                    'mssql_id': tid,
                    'mssql_table': 'tblStockTransfer',
                    'record_data': json.dumps(data, default=str),
                })

            if not queue.line_ids:
                queue.unlink()
                return self._success_notification(
                    'Stock Transfers', 'No new transfers to process.')

            queue.action_process_queue()

            self.write({
                'stock_transfers_synced': self.stock_transfers_synced + queue.done_count
            })

            return {
                'type': 'ir.actions.act_window',
                'res_model': 'mssql.sync.queue',
                'res_id': queue.id,
                'view_mode': 'form',
                'target': 'current',
            }

        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            _logger.error(f"sync_stock_transfers failed: {str(e)}", exc_info=True)
            raise UserError(f'Stock transfer sync failed: {str(e)}')

    def _create_internal_transfer(self, from_wh, to_wh, lines, product_map,
                                  origin='', date=None, notes=''):
        """Create and validate an internal stock.picking for a transfer.

        Args:
            from_wh: source stock.warehouse record
            to_wh: destination stock.warehouse record
            lines: list of dicts with ItemID, Quantity
            product_map: {ItemID: product.product}
            origin: picking origin string
            date: scheduled date
            notes: picking notes
        Returns:
            stock.picking record or False
        """
        move_vals = []
        for line in lines:
            product = product_map.get(line['ItemID'])
            if not product:
                _logger.warning(f"Transfer: product not found for ItemID={line['ItemID']}")
                continue
            qty = float(line.get('Quantity', 0))
            if qty <= 0:
                continue
            move_vals.append({
                'name': product.name,
                'product_id': product.id,
                'product_uom_qty': qty,
                'product_uom': product.uom_id.id,
                'location_id': from_wh.lot_stock_id.id,
                'location_dest_id': to_wh.lot_stock_id.id,
            })

        if not move_vals:
            return False

        # Use the internal transfer picking type from source warehouse
        picking_type = from_wh.int_type_id
        if not picking_type:
            _logger.error(f"No internal transfer type for warehouse {from_wh.name}")
            return False

        picking = self.env['stock.picking'].create({
            'picking_type_id': picking_type.id,
            'location_id': from_wh.lot_stock_id.id,
            'location_dest_id': to_wh.lot_stock_id.id,
            'origin': origin,
            'note': notes,
            'scheduled_date': date or fields.Datetime.now(),
            'move_ids': [(0, 0, mv) for mv in move_vals],
        })

        # Confirm -> set quantities -> validate
        picking.action_confirm()
        for move in picking.move_ids:
            move.quantity = move.product_uom_qty
        picking.button_validate()

        _logger.info(f"Created internal picking {picking.name} ({origin})")
        return picking

    # ── Stock Adjustments Sync ────────────────────────────────────────

    def sync_stock_adjustments(self, date_from=None, date_to=None):
        """Sync stock adjustments (StockType 5=Opening, 6=Manual) via queue."""
        self.ensure_one()
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            synced_ids = self._get_synced_ids('adjustment', 'tblItemsStockAdjustment')
            adjustments = self._query_stock_adjustments(cursor, [5, 6], exclude_ids=synced_ids or None)

            if date_from or date_to:
                filtered = []
                for a in adjustments:
                    ad = a['StockDate']
                    if ad:
                        d = ad.date() if hasattr(ad, 'date') else ad
                        if date_from and d < date_from:
                            continue
                        if date_to and d > date_to:
                            continue
                    filtered.append(a)
                adjustments = filtered

            if not adjustments:
                conn.close()
                _logger.info("sync_stock_adjustments: No new adjustments found")
                return

            stock_ids = [a['StockID'] for a in adjustments]
            details = self._query_adjustment_details(cursor, stock_ids)
            conn.close()

            details_map = {}
            for d in details:
                details_map.setdefault(d['StockID'], []).append(d)

            # Check existing queue lines to avoid duplicates
            existing_queued = set(
                self.env['mssql.sync.queue.line'].search([
                    ('queue_id.sync_config_id', '=', self.id),
                    ('queue_id.sync_type', '=', 'stock_adjustment'),
                    ('mssql_table', '=', 'tblItemsStockAdjustment'),
                    ('state', 'in', ('draft', 'failed')),
                ]).mapped('mssql_id'))

            # Create queue
            queue = self.env['mssql.sync.queue'].create({
                'sync_config_id': self.id,
                'sync_type': 'stock_adjustment',
            })

            for adj in adjustments:
                sid = str(adj['StockID'])
                if sid in existing_queued:
                    continue
                lines = details_map.get(adj['StockID'], [])
                data = {'adjustment': adj, 'details': lines}
                self.env['mssql.sync.queue.line'].create({
                    'queue_id': queue.id,
                    'name': f"Adjustment {sid}",
                    'mssql_id': sid,
                    'mssql_table': 'tblItemsStockAdjustment',
                    'record_data': json.dumps(data, default=str),
                })

            if not queue.line_ids:
                queue.unlink()
                return self._success_notification(
                    'Stock Adjustments', 'No new adjustments to process.')

            queue.action_process_queue()

            self.write({
                'stock_adjustments_synced': self.stock_adjustments_synced + queue.done_count
            })

            return {
                'type': 'ir.actions.act_window',
                'res_model': 'mssql.sync.queue',
                'res_id': queue.id,
                'view_mode': 'form',
                'target': 'current',
            }

        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            _logger.error(f"sync_stock_adjustments failed: {str(e)}", exc_info=True)
            raise UserError(f'Stock adjustment sync failed: {str(e)}')

    def _apply_quant_adjustments(self, lines, product_map, location):
        """Apply inventory quantity adjustments via stock.quant.

        Optimized: pre-fetches existing quants in one query, then batch-applies.

        Args:
            lines: list of dicts with ItemID, Quantity
            product_map: {ItemID: product.product}
            location: stock.location record
        Returns:
            int: number of quants adjusted
        """
        quant_obj = self.env['stock.quant']

        # Filter valid lines first
        valid_lines = []
        for line in lines:
            product = product_map.get(line['ItemID'])
            if not product:
                continue
            qty = float(line.get('Quantity', 0))
            if qty == 0:
                continue
            valid_lines.append((product, qty))

        if not valid_lines:
            return 0

        # Pre-fetch all existing quants for this location in ONE query
        product_ids = [p.id for p, _ in valid_lines]
        existing_quants = {
            q.product_id.id: q for q in quant_obj.search([
                ('product_id', 'in', product_ids),
                ('location_id', '=', location.id),
                ('company_id', '=', self.env.company.id),
            ])
        }

        # Set inventory_quantity on all quants, collect for batch apply
        all_quants = self.env['stock.quant']
        for product, qty in valid_lines:
            quant = existing_quants.get(product.id)
            if not quant:
                quant = quant_obj.with_context(inventory_mode=True).create({
                    'product_id': product.id,
                    'location_id': location.id,
                    'inventory_quantity': qty,
                })
            else:
                new_qty = quant.quantity + qty
                quant.with_context(inventory_mode=True).write({
                    'inventory_quantity': new_qty,
                })
            all_quants |= quant

        # Batch apply — creates all stock moves at once instead of one-by-one
        if all_quants:
            all_quants.with_context(inventory_mode=True).action_apply_inventory()

        return len(valid_lines)

    # ── Scrap / Write-off Sync ────────────────────────────────────────

    def sync_stock_scrap(self, date_from=None, date_to=None):
        """Sync scrap/write-off records (StockType=7) via queue."""
        self.ensure_one()
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            synced_ids = self._get_synced_ids('scrap', 'tblItemsStockAdjustment')
            scraps = self._query_stock_adjustments(cursor, [7], exclude_ids=synced_ids or None)

            if date_from or date_to:
                filtered = []
                for s in scraps:
                    sd = s['StockDate']
                    if sd:
                        d = sd.date() if hasattr(sd, 'date') else sd
                        if date_from and d < date_from:
                            continue
                        if date_to and d > date_to:
                            continue
                    filtered.append(s)
                scraps = filtered

            if not scraps:
                conn.close()
                _logger.info("sync_stock_scrap: No new scrap records found")
                return

            stock_ids = [s['StockID'] for s in scraps]
            details = self._query_adjustment_details(cursor, stock_ids)
            conn.close()

            details_map = {}
            for d in details:
                details_map.setdefault(d['StockID'], []).append(d)

            # Check existing queue lines to avoid duplicates
            existing_queued = set(
                self.env['mssql.sync.queue.line'].search([
                    ('queue_id.sync_config_id', '=', self.id),
                    ('queue_id.sync_type', '=', 'stock_scrap'),
                    ('mssql_table', '=', 'tblItemsStockAdjustment'),
                    ('state', 'in', ('draft', 'failed')),
                ]).mapped('mssql_id'))

            # Create queue
            queue = self.env['mssql.sync.queue'].create({
                'sync_config_id': self.id,
                'sync_type': 'stock_scrap',
            })

            for scrap in scraps:
                sid = str(scrap['StockID'])
                if sid in existing_queued:
                    continue
                lines = details_map.get(scrap['StockID'], [])
                data = {'scrap': scrap, 'details': lines}
                self.env['mssql.sync.queue.line'].create({
                    'queue_id': queue.id,
                    'name': f"Scrap {sid}",
                    'mssql_id': sid,
                    'mssql_table': 'tblItemsStockAdjustment',
                    'record_data': json.dumps(data, default=str),
                })

            if not queue.line_ids:
                queue.unlink()
                return self._success_notification(
                    'Stock Scrap', 'No new scrap records to process.')

            queue.action_process_queue()

            _logger.info(f"sync_stock_scrap: {queue.done_count} done, "
                         f"{queue.failed_count} failed")

            return {
                'type': 'ir.actions.act_window',
                'res_model': 'mssql.sync.queue',
                'res_id': queue.id,
                'view_mode': 'form',
                'target': 'current',
            }

        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            _logger.error(f"sync_stock_scrap failed: {str(e)}", exc_info=True)
            raise UserError(f'Stock scrap sync failed: {str(e)}')

    def _create_scrap_records(self, lines, product_map, location):
        """Create and validate stock.scrap records.

        Args:
            lines: list of dicts with ItemID, Quantity
            product_map: {ItemID: product.product}
            location: stock.location record
        Returns:
            list of stock.scrap records
        """
        scrap_obj = self.env['stock.scrap']
        created = []

        # Get a scrap location
        scrap_location = self.env['stock.location'].search([
            ('scrap_location', '=', True),
            ('company_id', 'in', [self.env.company.id, False]),
        ], limit=1)

        for line in lines:
            product = product_map.get(line['ItemID'])
            if not product:
                continue
            qty = abs(float(line.get('Quantity', 0)))
            if qty <= 0:
                continue

            scrap_vals = {
                'product_id': product.id,
                'scrap_qty': qty,
                'product_uom_id': product.uom_id.id,
                'location_id': location.id,
                'company_id': self.env.company.id,
            }
            if scrap_location:
                scrap_vals['scrap_location_id'] = scrap_location.id

            scrap = scrap_obj.create(scrap_vals)
            scrap.do_scrap()
            created.append(scrap)

        return created

    # ── Queue Processor Methods ─────────────────────────────────────────

    @staticmethod
    def _parse_datetime_str(val):
        """Parse a datetime string that may include microseconds.

        JSON serialization via default=str produces strings like
        '2025-06-11 14:30:45.717000' which Odoo cannot parse directly.
        This strips microseconds and returns a clean string, or the
        original value if it's already a datetime or None.
        """
        if not val:
            return val
        if isinstance(val, str) and '.' in val:
            return val.split('.')[0]
        return val

    def _process_queue_stock_transfer(self, data, queue_line):
        """Process a single stock transfer from queue."""
        transfer = data['transfer']
        lines = data['details']
        tid = transfer['TransferID']

        if not lines:
            raise ValueError(f'Transfer {tid}: No detail lines')

        # Coerce numeric fields from JSON string back to int/float
        transfer['FromStoreID'] = int(transfer['FromStoreID'])
        transfer['ToStoreID'] = int(transfer['ToStoreID'])
        for line in lines:
            if 'ItemID' in line and line['ItemID'] is not None:
                line['ItemID'] = int(line['ItemID'])
            if 'Quantity' in line and line['Quantity'] is not None:
                line['Quantity'] = float(line['Quantity'])

        warehouse_map = self._get_warehouse_map()
        from_wh = warehouse_map.get(transfer['FromStoreID'])
        to_wh = warehouse_map.get(transfer['ToStoreID'])

        if not from_wh or not to_wh:
            raise ValueError(
                f'Missing warehouse: from={transfer["FromStoreID"]}, '
                f'to={transfer["ToStoreID"]}')

        all_item_ids = list({d['ItemID'] for d in lines if d.get('ItemID')})
        product_map = self._get_product_map(all_item_ids)

        picking = self._create_internal_transfer(
            from_wh, to_wh, lines, product_map,
            origin=f'MSSQL Transfer {tid}',
            date=self._parse_datetime_str(transfer.get('TransferDate')),
            notes=transfer.get('TransferDescreption', ''))

        if not picking:
            raise ValueError(f'Transfer {tid}: No valid lines to transfer')

        self._log_sync('transfer', str(tid), 'tblStockTransfer',
                       odoo_model='stock.picking', odoo_record_id=picking.id)
        return {'model': 'stock.picking', 'id': picking.id}

    def _process_queue_stock_adjustment(self, data, queue_line):
        """Process a single stock adjustment from queue."""
        adjustment = data['adjustment']
        lines = data['details']
        sid = adjustment['StockID']

        if not lines:
            raise ValueError(f'Adjustment {sid}: No detail lines')

        # Coerce numeric fields from JSON string back to int/float
        adjustment['BranchID'] = int(adjustment['BranchID'])
        for line in lines:
            if 'ItemID' in line and line['ItemID'] is not None:
                line['ItemID'] = int(line['ItemID'])
            if 'Quantity' in line and line['Quantity'] is not None:
                line['Quantity'] = float(line['Quantity'])

        warehouse_map = self._get_warehouse_map()
        warehouse = warehouse_map.get(adjustment['BranchID'])
        if not warehouse:
            raise ValueError(
                f'Missing warehouse for BranchID={adjustment["BranchID"]}')

        location = warehouse.lot_stock_id
        all_item_ids = list({d['ItemID'] for d in lines if d.get('ItemID')})
        product_map = self._get_product_map(all_item_ids)

        applied = self._apply_quant_adjustments(lines, product_map, location)

        self._log_sync('adjustment', str(sid), 'tblItemsStockAdjustment',
                       odoo_model='stock.quant', status='success',
                       notes=f'Applied {applied} quant adjustments')
        return {'model': 'stock.quant'}

    def _process_queue_stock_scrap(self, data, queue_line):
        """Process a single scrap record from queue."""
        scrap = data['scrap']
        lines = data['details']
        sid = scrap['StockID']

        if not lines:
            raise ValueError(f'Scrap {sid}: No detail lines')

        # Coerce numeric fields from JSON string back to int/float
        scrap['BranchID'] = int(scrap['BranchID'])
        for line in lines:
            if 'ItemID' in line and line['ItemID'] is not None:
                line['ItemID'] = int(line['ItemID'])
            if 'Quantity' in line and line['Quantity'] is not None:
                line['Quantity'] = float(line['Quantity'])

        warehouse_map = self._get_warehouse_map()
        warehouse = warehouse_map.get(scrap['BranchID'])
        if not warehouse:
            raise ValueError(
                f'Missing warehouse for BranchID={scrap["BranchID"]}')

        location = warehouse.lot_stock_id
        all_item_ids = list({d['ItemID'] for d in lines if d.get('ItemID')})
        product_map = self._get_product_map(all_item_ids)

        created_scraps = self._create_scrap_records(lines, product_map, location)

        self._log_sync('scrap', str(sid), 'tblItemsStockAdjustment',
                       odoo_model='stock.scrap', status='success',
                       notes=f'Created {len(created_scraps)} scrap records')
        return {'model': 'stock.scrap'}

    def _process_queue_initial_stock(self, data, queue_line):
        """Process a single initial stock entry from queue.

        Used for retrying previously skipped records (missing product/warehouse).
        """
        item_id = int(data['ItemID'])
        branch_id = int(data['BranchID'])
        balance = float(data['CurrentBalance'])

        product_map = self._get_product_map([item_id])
        warehouse_map = self._get_warehouse_map()

        product = product_map.get(item_id)
        warehouse = warehouse_map.get(branch_id)

        if not product:
            raise ValueError(f'Product not found for ItemID={item_id}')
        if not warehouse or not warehouse.lot_stock_id:
            raise ValueError(f'Warehouse not found for BranchID={branch_id}')

        company_id = self.env.company.id
        uid = self.env.uid
        now = fields.Datetime.now()

        # Check for existing quant
        self.env.cr.execute("""
            SELECT id, quantity FROM stock_quant
            WHERE product_id = %s AND location_id = %s
              AND company_id = %s AND lot_id IS NULL
              AND package_id IS NULL AND owner_id IS NULL
        """, (product.id, warehouse.lot_stock_id.id, company_id))
        existing = self.env.cr.fetchone()

        if existing:
            self.env.cr.execute(
                "UPDATE stock_quant SET quantity = %s, write_date = %s, "
                "write_uid = %s WHERE id = %s",
                (balance, now, uid, existing[0]))
        else:
            self.env.cr.execute(
                "INSERT INTO stock_quant "
                "(product_id, location_id, quantity, reserved_quantity, "
                "company_id, in_date, create_uid, create_date, write_uid, "
                "write_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (product.id, warehouse.lot_stock_id.id, balance, 0,
                 company_id, now, uid, now, uid, now))

        self.env['stock.quant'].invalidate_model()
        return {'model': 'stock.quant'}

    # ── Update Quantities ────────────────────────────────────────────────

    def action_update_quantities(self):
        """Update all stock quant quantities from tblItemsTrans.

        Re-syncs current stock levels from MSSQL. Creates queue for
        any records that can't be matched (missing product/warehouse).
        """
        self.ensure_one()
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            stock_data = self._query_current_stock_levels(cursor)
            conn.close()
        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            raise UserError(f'Failed to query stock levels: {str(e)}')

        if not stock_data:
            return self._success_notification(
                'Update Quantities', 'No stock data found in MSSQL.')

        product_map = self._get_product_map()
        warehouse_map = self._get_warehouse_map()

        company_id = self.env.company.id
        uid = self.env.uid
        now = fields.Datetime.now()

        quant_entries = {}
        skipped_lines = []

        for row in stock_data:
            product = product_map.get(row['ItemID'])
            warehouse = warehouse_map.get(row['BranchID'])
            if not product or not warehouse or not warehouse.lot_stock_id:
                balance = float(row['CurrentBalance'] or 0)
                if balance != 0:
                    reasons = []
                    if not product:
                        reasons.append(
                            f"Product not found for ItemID={row['ItemID']}")
                    if not warehouse or not warehouse.lot_stock_id:
                        reasons.append(
                            f"Warehouse not found for BranchID={row['BranchID']}")
                    skipped_lines.append({
                        'name': f"Stock: Item {row['ItemID']} "
                                f"@ Branch {row['BranchID']}",
                        'mssql_id': f"{row['ItemID']}_{row['BranchID']}",
                        'mssql_table': 'tblItemsTrans',
                        'record_data': json.dumps({
                            'ItemID': row['ItemID'],
                            'BranchID': row['BranchID'],
                            'CurrentBalance': float(
                                row['CurrentBalance'] or 0),
                        }),
                        'state': 'failed',
                        'error_message': '; '.join(reasons),
                    })
                continue
            balance = float(row['CurrentBalance'] or 0)
            quant_entries[(product.id, warehouse.lot_stock_id.id)] = balance

        # Create queue for skipped records
        queue = None
        if skipped_lines:
            queue = self.env['mssql.sync.queue'].create({
                'sync_config_id': self.id,
                'sync_type': 'initial_stock',
            })
            for line_vals in skipped_lines:
                line_vals['queue_id'] = queue.id
                self.env['mssql.sync.queue.line'].create(line_vals)
            _logger.info(f"Update quantities: {len(skipped_lines)} skipped "
                         f"records queued in {queue.name}")

        if not quant_entries:
            _logger.info("No matchable stock entries to update")
            if queue:
                return {
                    'type': 'ir.actions.act_window',
                    'res_model': 'mssql.sync.queue',
                    'res_id': queue.id,
                    'view_mode': 'form',
                    'target': 'current',
                }
            return self._success_notification(
                'Update Quantities', 'No matchable stock entries found.')

        _logger.info(f"Updating quantities for {len(quant_entries)} "
                     f"product/location pairs...")

        # Pre-fetch existing quants in ONE query
        all_product_ids = list({pid for pid, _ in quant_entries})
        all_location_ids = list({lid for _, lid in quant_entries})

        self.env.cr.execute("""
            SELECT id, product_id, location_id
            FROM stock_quant
            WHERE product_id = ANY(%s)
              AND location_id = ANY(%s)
              AND company_id = %s
              AND lot_id IS NULL
              AND package_id IS NULL
              AND owner_id IS NULL
        """, (all_product_ids, all_location_ids, company_id))
        existing = {(r[1], r[2]): r[0] for r in self.env.cr.fetchall()}

        # Split into inserts and updates
        to_insert = []
        to_update = []
        for (product_id, location_id), qty in quant_entries.items():
            if (product_id, location_id) in existing:
                to_update.append((qty, existing[(product_id, location_id)]))
            else:
                if qty != 0:  # Don't insert zero-quantity quants
                    to_insert.append((product_id, location_id, qty))

        # Batch INSERT new quants (1000 per batch)
        BATCH = 1000
        inserted = 0
        for i in range(0, len(to_insert), BATCH):
            batch = to_insert[i:i + BATCH]
            args = []
            placeholders = []
            for product_id, location_id, qty in batch:
                placeholders.append(
                    "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                args.extend([
                    product_id, location_id, qty, 0, company_id,
                    now, uid, now, uid, now])
            self.env.cr.execute(
                "INSERT INTO stock_quant "
                "(product_id, location_id, quantity, reserved_quantity, "
                "company_id, in_date, create_uid, create_date, write_uid, "
                "write_date) VALUES "
                + ", ".join(placeholders), args)
            inserted += len(batch)

        # Batch UPDATE existing quants
        updated = 0
        if to_update:
            self.env.cr.executemany(
                "UPDATE stock_quant SET quantity = %s, write_date = %s, "
                "write_uid = %s WHERE id = %s",
                [(qty, now, uid, qid) for qty, qid in to_update])
            updated = len(to_update)

        # Invalidate ORM cache
        self.env['stock.quant'].invalidate_model()

        summary = f'Inserted {inserted}, updated {updated} stock quants'
        if skipped_lines:
            summary += f', {len(skipped_lines)} skipped (queued for retry)'

        _logger.info(f"Update quantities: {summary}")

        if queue:
            return {
                'type': 'ir.actions.act_window',
                'res_model': 'mssql.sync.queue',
                'res_id': queue.id,
                'view_mode': 'form',
                'target': 'current',
            }
        return self._success_notification('Update Quantities', summary)

    # ── 15-Minute Incremental Stock Move Sync ─────────────────────────

    def sync_recent_stock_moves(self):
        """Cron method: sync recent stock moves from tblItemsTrans (TransType 3-7).

        Uses last_stock_sync_trans_id as watermark for incremental polling.
        """
        self.ensure_one()
        if not self.smart_connect_done:
            _logger.info("sync_recent_stock_moves: Smart Connect not done, skipping")
            return
        if not self.last_stock_sync_trans_id:
            _logger.info("sync_recent_stock_moves: No watermark TransID, skipping")
            return

        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            moves = self._query_recent_stock_moves(cursor, self.last_stock_sync_trans_id)
            conn.close()
        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            _logger.error(f"sync_recent_stock_moves query failed: {str(e)}")
            return

        if not moves:
            _logger.info("sync_recent_stock_moves: No new moves since last sync")
            self.write({'last_stock_sync_date': fields.Datetime.now()})
            return

        # Build lookup maps
        all_item_ids = list({m['ItemID'] for m in moves if m['ItemID']})
        product_map = self._get_product_map(all_item_ids)
        warehouse_map = self._get_warehouse_map()

        # Group transfers by RefID (Type 3=OUT, 4=IN are paired)
        transfer_groups = {}
        adjustments = []
        scraps = []

        for move in moves:
            trans_id = str(move['TransID'])

            # Skip if already synced
            if self._is_already_synced('transfer', trans_id, 'tblItemsTrans') or \
               self._is_already_synced('adjustment', trans_id, 'tblItemsTrans') or \
               self._is_already_synced('scrap', trans_id, 'tblItemsTrans'):
                continue

            trans_type = move['TransType']
            if trans_type in (3, 4):
                ref_id = move.get('RefID')
                if ref_id:
                    transfer_groups.setdefault(ref_id, []).append(move)
                else:
                    transfer_groups.setdefault(f'single_{trans_id}', []).append(move)
            elif trans_type in (5, 6):
                adjustments.append(move)
            elif trans_type == 7:
                scraps.append(move)

        max_processed_id = self.last_stock_sync_trans_id
        processed = 0

        # Process transfers (grouped by RefID)
        for ref_id, group in transfer_groups.items():
            try:
                self._process_trans_transfer_group(group, product_map, warehouse_map)
                processed += len(group)
            except Exception as e:
                _logger.error(f"Failed to process transfer group {ref_id}: {str(e)}")
                for m in group:
                    self._log_sync('transfer', str(m['TransID']), 'tblItemsTrans',
                                   status='error', error_message=str(e))

        # Process adjustments
        for move in adjustments:
            trans_id = str(move['TransID'])
            try:
                warehouse = warehouse_map.get(move['BranchID'])
                product = product_map.get(move['ItemID'])
                if not warehouse or not product:
                    self._log_sync('adjustment', trans_id, 'tblItemsTrans',
                                   status='skipped',
                                   notes=f'Missing product={move["ItemID"]} or warehouse={move["BranchID"]}')
                    continue

                location = warehouse.lot_stock_id
                qty = float(move.get('TransQty', 0))
                if qty == 0:
                    self._log_sync('adjustment', trans_id, 'tblItemsTrans',
                                   status='skipped', notes='Zero quantity')
                    continue

                self._apply_quant_adjustments(
                    [{'ItemID': move['ItemID'], 'Quantity': qty}],
                    product_map, location)

                self._log_sync('adjustment', trans_id, 'tblItemsTrans',
                               odoo_model='stock.quant', status='success')
                processed += 1

            except Exception as e:
                _logger.error(f"Failed to process adjustment TransID={trans_id}: {str(e)}")
                self._log_sync('adjustment', trans_id, 'tblItemsTrans',
                               status='error', error_message=str(e))

        # Process scraps
        for move in scraps:
            trans_id = str(move['TransID'])
            try:
                warehouse = warehouse_map.get(move['BranchID'])
                product = product_map.get(move['ItemID'])
                if not warehouse or not product:
                    self._log_sync('scrap', trans_id, 'tblItemsTrans',
                                   status='skipped',
                                   notes=f'Missing product={move["ItemID"]} or warehouse={move["BranchID"]}')
                    continue

                location = warehouse.lot_stock_id
                qty = abs(float(move.get('TransQty', 0)))
                if qty <= 0:
                    self._log_sync('scrap', trans_id, 'tblItemsTrans',
                                   status='skipped', notes='Zero quantity')
                    continue

                self._create_scrap_records(
                    [{'ItemID': move['ItemID'], 'Quantity': qty}],
                    product_map, location)

                self._log_sync('scrap', trans_id, 'tblItemsTrans',
                               odoo_model='stock.scrap', status='success')
                processed += 1

            except Exception as e:
                _logger.error(f"Failed to process scrap TransID={trans_id}: {str(e)}")
                self._log_sync('scrap', trans_id, 'tblItemsTrans',
                               status='error', error_message=str(e))

        # Update watermark to max TransID
        if moves:
            max_processed_id = str(max(m['TransID'] for m in moves))

        self.write({
            'last_stock_sync_trans_id': max_processed_id,
            'last_stock_sync_date': fields.Datetime.now(),
        })
        _logger.info(f"sync_recent_stock_moves: Processed {processed} moves, watermark={max_processed_id}")

    def _process_trans_transfer_group(self, group, product_map, warehouse_map):
        """Process a group of tblItemsTrans records for an internal transfer.

        TransType 3 = Transfer OUT (from BranchID), TransType 4 = Transfer IN (to BranchID).
        """
        # Find source (type 3) and destination (type 4) branches
        from_branch = None
        to_branch = None
        lines = []

        for move in group:
            if move['TransType'] == 3:
                from_branch = move['BranchID']
                lines.append({
                    'ItemID': move['ItemID'],
                    'Quantity': abs(float(move.get('TransQty', 0))),
                })
            elif move['TransType'] == 4:
                to_branch = move['BranchID']

        if not from_branch or not to_branch:
            # Fallback: if we only have type 3 or type 4 records, handle individually
            for move in group:
                trans_id = str(move['TransID'])
                warehouse = warehouse_map.get(move['BranchID'])
                product = product_map.get(move['ItemID'])
                if not warehouse or not product:
                    self._log_sync('transfer', trans_id, 'tblItemsTrans',
                                   status='skipped', notes='Incomplete transfer pair')
                    continue
                # Treat as adjustment instead
                location = warehouse.lot_stock_id
                qty = float(move.get('TransQty', 0))
                self._apply_quant_adjustments(
                    [{'ItemID': move['ItemID'], 'Quantity': qty}],
                    product_map, location)
                self._log_sync('transfer', trans_id, 'tblItemsTrans',
                               odoo_model='stock.quant', status='success',
                               notes='Incomplete pair - applied as adjustment')
            return

        from_wh = warehouse_map.get(from_branch)
        to_wh = warehouse_map.get(to_branch)

        if not from_wh or not to_wh or not lines:
            for move in group:
                self._log_sync('transfer', str(move['TransID']), 'tblItemsTrans',
                               status='error',
                               error_message=f'Missing warehouse from={from_branch} to={to_branch}')
            return

        ref_id = group[0].get('RefID', '')
        picking = self._create_internal_transfer(
            from_wh, to_wh, lines, product_map,
            origin=f'MSSQL Trans RefID={ref_id}')

        for move in group:
            self._log_sync('transfer', str(move['TransID']), 'tblItemsTrans',
                           odoo_model='stock.picking',
                           odoo_record_id=picking.id if picking else 0,
                           status='success' if picking else 'error',
                           error_message='' if picking else 'Failed to create picking')

    # ── Smart Stock Reconciliation ────────────────────────────────────

    def action_reconcile_stock(self):
        """Smart reconciliation: find and process MISSING moves only.

        Phase A: Find missing stock operations (TransType 3-7) in tblItemsTrans
        Phase B: Find missing POS sessions
        Phase C: Find missing purchase invoices
        Phase D: Compare tblItemsCost vs stock.quant and report discrepancies
        """
        self.ensure_one()
        if not self.initial_sync_date:
            raise UserError('Cannot reconcile: Smart Connect has not been run yet.')

        _logger.info("=== Stock Reconciliation: Starting ===")
        since_date = self.initial_sync_date.strftime('%Y-%m-%d %H:%M:%S')

        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        report = []

        try:
            # Phase A: Find missing stock moves (TransType 3-7)
            _logger.info("Reconciliation Phase A: Checking tblItemsTrans...")
            all_trans = self._query_all_stock_trans_for_reconciliation(cursor, since_date)

            synced_trans_ids = set()
            for stype in ('transfer', 'adjustment', 'scrap'):
                synced_trans_ids |= self._get_synced_ids(stype, 'tblItemsTrans')

            missing_trans = [t for t in all_trans if str(t['TransID']) not in synced_trans_ids]
            report.append(f"Phase A: {len(missing_trans)} missing stock moves found (of {len(all_trans)} total)")

            if missing_trans:
                # Process missing moves
                all_item_ids = list({m['ItemID'] for m in missing_trans if m['ItemID']})
                product_map = self._get_product_map(all_item_ids)
                warehouse_map = self._get_warehouse_map()

                # Group transfers by RefID
                transfer_groups = {}
                adj_moves = []
                scrap_moves = []

                for move in missing_trans:
                    tt = move['TransType']
                    if tt in (3, 4):
                        ref_id = move.get('RefID', f'single_{move["TransID"]}')
                        transfer_groups.setdefault(ref_id, []).append(move)
                    elif tt in (5, 6):
                        adj_moves.append(move)
                    elif tt == 7:
                        scrap_moves.append(move)

                # Process each type
                for ref_id, group in transfer_groups.items():
                    try:
                        self._process_trans_transfer_group(group, product_map, warehouse_map)
                    except Exception as e:
                        _logger.error(f"Reconcile: transfer group {ref_id} failed: {e}")

                for move in adj_moves:
                    tid = str(move['TransID'])
                    try:
                        wh = warehouse_map.get(move['BranchID'])
                        if wh:
                            self._apply_quant_adjustments(
                                [{'ItemID': move['ItemID'], 'Quantity': float(move.get('TransQty', 0))}],
                                product_map, wh.lot_stock_id)
                            self._log_sync('adjustment', tid, 'tblItemsTrans',
                                           odoo_model='stock.quant', status='success',
                                           notes='Recovered via reconciliation')
                    except Exception as e:
                        _logger.error(f"Reconcile: adjustment {tid} failed: {e}")

                for move in scrap_moves:
                    tid = str(move['TransID'])
                    try:
                        wh = warehouse_map.get(move['BranchID'])
                        if wh:
                            self._create_scrap_records(
                                [{'ItemID': move['ItemID'], 'Quantity': abs(float(move.get('TransQty', 0)))}],
                                product_map, wh.lot_stock_id)
                            self._log_sync('scrap', tid, 'tblItemsTrans',
                                           odoo_model='stock.scrap', status='success',
                                           notes='Recovered via reconciliation')
                    except Exception as e:
                        _logger.error(f"Reconcile: scrap {tid} failed: {e}")

                report.append(f"  -> Processed: {len(transfer_groups)} transfer groups, "
                              f"{len(adj_moves)} adjustments, {len(scrap_moves)} scraps")

            # Phase B: Find missing POS sessions
            _logger.info("Reconciliation Phase B: Checking POS sessions...")
            sessions = self._query_sessions_since(cursor, since_date)
            missing_sessions = []
            if sessions:
                session_ids = [s['SessionID'] for s in sessions]
                # Check which sessions have matching SOs
                existing_sos = self.env['sale.order'].search([
                    ('client_order_ref', '!=', False),
                ]).mapped('client_order_ref')
                # Extract session IDs from client_order_ref
                synced_session_ids = set()
                for ref in existing_sos:
                    if ref:
                        for sid in session_ids:
                            if str(sid) in ref:
                                synced_session_ids.add(sid)

                missing_sessions = [s for s in sessions if s['SessionID'] not in synced_session_ids]

            report.append(f"Phase B: {len(missing_sessions)} missing POS sessions "
                          f"(of {len(sessions)} total)")
            if missing_sessions:
                session_list = ', '.join(str(s['SessionID']) for s in missing_sessions[:20])
                report.append(f"  -> Missing session IDs: {session_list}"
                              + (" ..." if len(missing_sessions) > 20 else ""))

            # Phase C: Find missing purchase invoices
            _logger.info("Reconciliation Phase C: Checking purchase invoices...")
            purchase_invs = self._query_purchase_invoices_since(cursor, since_date)
            missing_purchases = []
            if purchase_invs:
                for pi in purchase_invs:
                    inv_id = str(pi['InvoiceID'])
                    if not self._is_already_synced('adjustment', inv_id, 'tblPurchaseInvoice'):
                        po_exists = self.env['purchase.order'].search_count([
                            ('origin', 'like', f'MSSQL%{inv_id}'),
                        ], limit=1)
                        if not po_exists:
                            missing_purchases.append(pi)

            report.append(f"Phase C: {len(missing_purchases)} missing purchase invoices "
                          f"(of {len(purchase_invs)} total)")

            # Phase D: Compare final stock levels
            _logger.info("Reconciliation Phase D: Comparing stock levels...")
            stock_levels = self._query_current_stock_levels(cursor)
            conn.close()

            product_map = self._get_product_map()
            warehouse_map = self._get_warehouse_map()
            discrepancies = 0

            for row in stock_levels:
                product = product_map.get(row['ItemID'])
                warehouse = warehouse_map.get(row['BranchID'])
                if not product or not warehouse:
                    continue

                mssql_qty = float(row['CurrentBalance'] or 0)
                odoo_qty = sum(self.env['stock.quant'].search([
                    ('product_id', '=', product.id),
                    ('location_id', '=', warehouse.lot_stock_id.id),
                ]).mapped('quantity'))

                if abs(mssql_qty - odoo_qty) > 0.01:
                    discrepancies += 1

            report.append(f"Phase D: {discrepancies} stock level discrepancies found")

            now = fields.Datetime.now()
            self.write({
                'last_reconciliation_date': now,
                'reconciliation_discrepancy_count': discrepancies,
            })

            summary = '\n'.join(report)
            _logger.info(f"=== Reconciliation Complete ===\n{summary}")

            return self._success_notification('Stock Reconciliation Complete', summary)

        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            _logger.error(f"Stock reconciliation failed: {str(e)}", exc_info=True)
            raise UserError(f'Stock reconciliation failed: {str(e)}')

    # ── Cron Entry Points ─────────────────────────────────────────────

    @api.model
    def cron_stock_and_product_sync(self):
        """Cron job: 15-min auto-sync for new products and stock moves."""
        configs = self.search([('smart_connect_done', '=', True)])
        for config in configs:
            try:
                config.sync_new_products()
            except Exception as e:
                _logger.error(f"Cron: sync_new_products failed for {config.name}: {e}")

            try:
                config.sync_recent_stock_moves()
            except Exception as e:
                _logger.error(f"Cron: sync_recent_stock_moves failed for {config.name}: {e}")

    @api.model
    def cron_daily_reconciliation(self):
        """Cron job: daily stock reconciliation."""
        configs = self.search([('smart_connect_done', '=', True)])
        for config in configs:
            try:
                config.action_reconcile_stock()
            except Exception as e:
                _logger.error(f"Cron: reconciliation failed for {config.name}: {e}")

    # ── SQL Queries ───────────────────────────────────────────────────

    def _query_all_branches(self, cursor):
        """Fetch all branches from MSSQL."""
        cursor.execute("""
            SELECT BranchID, BranchName, BranchLocation
            FROM [dbo].[tblBranch]
        """)
        return cursor.fetchall()

    def _query_current_stock_levels(self, cursor):
        """Fetch current stock levels per product/branch from tblItemsTrans."""
        cursor.execute("""
            SELECT
                ItemID, BranchID,
                SUM(QuantityIn) - SUM(QuantityOut) AS CurrentBalance
            FROM [dbo].[tblItemsTrans]
            WHERE BranchID > 0
            GROUP BY ItemID, BranchID
            HAVING SUM(QuantityIn) - SUM(QuantityOut) != 0
        """)
        return cursor.fetchall()

    def _query_stock_transfers(self, cursor, exclude_ids=None):
        """Fetch posted stock transfers, excluding already-synced IDs."""
        base_sql = """
            SELECT
                st.TransferID, st.TransferDate, st.FromStoreID, st.ToStoreID,
                st.TransferDescreption, st.PostedDate
            FROM [dbo].[tblStockTransfer] st
            WHERE st.Posted = 1
        """
        if exclude_ids:
            placeholders = ','.join(['%s'] * len(exclude_ids))
            base_sql += f" AND st.TransferID NOT IN ({placeholders})"
            cursor.execute(base_sql, list(exclude_ids))
        else:
            cursor.execute(base_sql)
        return cursor.fetchall()

    def _query_stock_transfer_details(self, cursor, transfer_ids):
        """Fetch details for given transfer IDs."""
        if not transfer_ids:
            return []
        placeholders = ','.join(['%s'] * len(transfer_ids))
        cursor.execute(f"""
            SELECT
                std.TransferID, std.ItemID, std.Quantity, std.Descreption
            FROM [dbo].[tblStockTransferDetail] std
            WHERE std.TransferID IN ({placeholders})
        """, list(transfer_ids))
        return cursor.fetchall()

    def _query_stock_adjustments(self, cursor, stock_types, exclude_ids=None):
        """Fetch posted stock adjustments by type, excluding synced IDs.

        Args:
            stock_types: list of StockType ints (e.g. [5,6] for adjustments, [7] for scrap)
            exclude_ids: set of StockID strings to skip
        """
        type_placeholders = ','.join(['%s'] * len(stock_types))
        base_sql = f"""
            SELECT
                sa.StockID, sa.StockDate, sa.BranchID, sa.StockType,
                sa.StockNote, sa.ModifiedDate
            FROM [dbo].[tblItemsStockAdjustment] sa
            WHERE sa.Posted = 1
              AND sa.StockType IN ({type_placeholders})
        """
        params = list(stock_types)
        if exclude_ids:
            id_placeholders = ','.join(['%s'] * len(exclude_ids))
            base_sql += f" AND sa.StockID NOT IN ({id_placeholders})"
            params.extend(list(exclude_ids))
        cursor.execute(base_sql, params)
        return cursor.fetchall()

    def _query_adjustment_details(self, cursor, stock_ids):
        """Fetch detail lines for given stock adjustment IDs."""
        if not stock_ids:
            return []
        placeholders = ','.join(['%s'] * len(stock_ids))
        cursor.execute(f"""
            SELECT
                sad.StockID, sad.ItemID, sad.Quantity, sad.StockDetailNote
            FROM [dbo].[tblItemsStockAdjustmentDetail] sad
            WHERE sad.StockID IN ({placeholders})
        """, list(stock_ids))
        return cursor.fetchall()

    def _query_recent_stock_moves(self, cursor, after_trans_id):
        """Fetch stock-related tblItemsTrans records after a watermark TransID.

        Only TransType 3-7 (skip 1=sales, 2=purchase - handled by existing crons).
        """
        cursor.execute("""
            SELECT
                it.TransID, it.TransDate, it.TransType, it.ItemID,
                it.BranchID, it.TransQty, it.RefID, it.TransDescreption
            FROM [dbo].[tblItemsTrans] it
            WHERE it.TransID > %s
              AND it.TransType IN (3, 4, 5, 6, 7)
            ORDER BY it.TransID ASC
        """, (int(after_trans_id),))
        return cursor.fetchall()

    def _query_max_trans_id(self, cursor):
        """Get the current maximum TransID from tblItemsTrans."""
        cursor.execute("SELECT MAX(TransID) AS MaxID FROM [dbo].[tblItemsTrans]")
        row = cursor.fetchone()
        return str(row['MaxID']) if row and row['MaxID'] else '0'

    def _query_all_stock_trans_for_reconciliation(self, cursor, since_date):
        """Fetch all stock-related tblItemsTrans since a date for reconciliation."""
        cursor.execute("""
            SELECT
                it.TransID, it.TransDate, it.TransType, it.ItemID,
                it.BranchID, it.TransQty, it.RefID, it.TransDescreption
            FROM [dbo].[tblItemsTrans] it
            WHERE it.TransDate >= %s
              AND it.TransType IN (3, 4, 5, 6, 7)
            ORDER BY it.TransID ASC
        """, (since_date,))
        return cursor.fetchall()

    def _query_sessions_since(self, cursor, since_date):
        """Fetch CashierActivity sessions since a date for reconciliation."""
        cursor.execute("""
            SELECT
                ca.SessionID, ca.BranchID, ca.OpenDate, ca.CloseDate
            FROM [dbo].[tblCashierActivity] ca
            WHERE ca.OpenDate >= %s
            ORDER BY ca.SessionID
        """, (since_date,))
        return cursor.fetchall()

    def _query_purchase_invoices_since(self, cursor, since_date):
        """Fetch posted purchase invoices since a date for reconciliation."""
        cursor.execute("""
            SELECT
                pi.InvoiceID, pi.InvoiceDate, pi.SupplierID,
                pi.TotalAmount, pi.BranchID
            FROM [dbo].[tblPurchaseInvoice] pi
            WHERE pi.InvoiceDate >= %s
              AND pi.Posted = 1
            ORDER BY pi.InvoiceID
        """, (since_date,))
        return cursor.fetchall()
