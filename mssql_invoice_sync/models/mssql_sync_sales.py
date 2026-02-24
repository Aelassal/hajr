from odoo import models, fields
from odoo.exceptions import UserError
from datetime import timedelta
import json
import logging

_logger = logging.getLogger(__name__)


class MssqlSyncSales(models.Model):
    _inherit = 'mssql.sync'

    # Fields that belong to this domain
    invoice_date = fields.Date(string='Invoice Date', default=fields.Date.today)
    sales_warehouse_id = fields.Many2one('stock.warehouse', string='Sales Warehouse',
                                         help='Warehouse to use for sales orders. If not set, will use default warehouse.')
    batch_size = fields.Integer(string='Batch Size', default=200,
                                help='Number of records to process in each batch. Lower values use less memory but are slower.')
    enable_batch_processing = fields.Boolean(string='Enable Batch Processing', default=True,
                                            help='Process invoices in batches for better performance with large datasets.')
    payment_method_cash_journal_id = fields.Many2one('account.journal', string='Cash Journal',
                                                      domain=[('type', '=', 'cash')],
                                                      help='Journal for Cash payments (Payment Method 1). If not set, uses default cash journal.')
    payment_method_mada_journal_id = fields.Many2one('account.journal', string='Mada Journal',
                                                      domain=[('type', 'in', ['bank', 'cash'])],
                                                      help='Journal for Mada/Bank Card payments (Payment Method 2). If not set, uses default bank journal.')
    payment_method_visa_journal_id = fields.Many2one('account.journal', string='Visa Journal',
                                                      domain=[('type', 'in', ['bank', 'cash'])],
                                                      help='Journal for Visa payments (Payment Method 3). If not set, uses default bank journal.')
    payment_method_mastercard_journal_id = fields.Many2one('account.journal', string='MasterCard Journal',
                                                            domain=[('type', 'in', ['bank', 'cash'])],
                                                            help='Journal for MasterCard payments (Payment Method 4). If not set, uses default bank journal.')
    payment_method_coupon_journal_id = fields.Many2one('account.journal', string='Coupon Journal',
                                                        domain=[('type', 'in', ['bank', 'cash'])],
                                                        help='Journal for Coupon payments (Payment Method 20). If not set, uses default cash journal.')
    payment_method_stcpay_journal_id = fields.Many2one('account.journal', string='STC Pay Journal',
                                                        domain=[('type', 'in', ['bank', 'cash'])],
                                                        help='Journal for STC Pay payments (Payment Method 60). If not set, uses default bank journal.')
    payment_method_points_journal_id = fields.Many2one('account.journal', string='Points Journal',
                                                        domain=[('type', 'in', ['bank', 'cash'])],
                                                        help='Journal for Points payments (Payment Method 40). If not set, uses default cash journal.')

    def _get_sales_warehouse(self):
        """Get warehouse for sales orders"""
        if self.sales_warehouse_id:
            return self.sales_warehouse_id

        # Get default warehouse
        warehouse = self.env['stock.warehouse'].search([
            ('company_id', '=', self.env.company.id)
        ], limit=1)

        if not warehouse:
            raise UserError('No warehouse found. Please configure a warehouse or set Sales Warehouse in configuration.')

        return warehouse

    def _create_sales_order(self, partner, order_lines, invoice_date, warehouse, reference=None):
        """Create a sales order with given lines

        Args:
            partner: res.partner record
            order_lines: list of tuples (0, 0, {...}) with product_id, product_uom_qty, price_unit
            invoice_date: date for the order
            warehouse: stock.warehouse record
            reference: optional reference text

        Returns:
            sale.order record
        """
        so_vals = {
            'partner_id': partner.id,
            'date_order': invoice_date,
            'warehouse_id': warehouse.id,
            'order_line': order_lines,
        }

        if reference:
            so_vals['client_order_ref'] = reference

        sale_order = self.env['sale.order'].create(so_vals)
        _logger.info(f"Created Sales Order {sale_order.name} for partner {partner.name}")

        return sale_order

    def _validate_picking(self, picking):
        """Validate a stock picking (already created and confirmed by SO)

        Args:
            picking: stock.picking record (auto-created by SO.action_confirm())

        Returns:
            bool: True if successful
        """
        if not picking:
            return False

        # Picking is already created and confirmed by SO.action_confirm()
        # Use move_ids_without_package and set quantity = product_uom_qty
        for move in picking.move_ids_without_package:
            # Set the quantity to process
            move.quantity = move.product_uom_qty

        # Validate the picking
        picking.button_validate()
        _logger.info(f"Validated picking {picking.name} - final state: {picking.state}")

        return True

    def create_session_based_invoices(self, invoice_date):
        """Create invoices based on POS sessions for a specific date.

        Fetches all session data from MSSQL, creates a sync queue with one
        line per session, and processes the queue. Failed sessions can be
        retried from the queue view.

        Args:
            invoice_date: Date to process sessions for

        Returns:
            Action to display the sync queue
        """
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            date_str = invoice_date.strftime('%Y-%m-%d')
            next_date = (invoice_date + timedelta(days=1)).strftime('%Y-%m-%d')

            _logger.info("=" * 80)
            _logger.info(f"SESSION-BASED INVOICE SYNC FOR DATE: {date_str}")
            _logger.info("=" * 80)

            # ── Phase 1: Fetch all data from MSSQL ────────────────────────
            _logger.info("Phase 1: Fetching all session data in bulk...")

            sessions = self._query_sessions_for_date(cursor, date_str, next_date)
            if not sessions:
                conn.close()
                raise UserError(f'No POS sessions found for date {date_str}')

            session_ids = [s['SessionID'] for s in sessions]
            _logger.info(f"Found {len(sessions)} sessions for {date_str}")

            all_session_lines = self._query_all_session_lines(cursor, session_ids)
            all_return_details = self._query_all_session_return_details(cursor, session_ids)
            all_pt5_returns = self._query_all_session_pt5_returns(cursor, session_ids)
            all_credit_sales = self._query_all_session_credit_sales(cursor, session_ids)
            all_payments = self._query_all_session_payments(cursor, session_ids)
            all_invoice_ranges = self._query_all_session_invoice_ranges(cursor, session_ids)

            # Enrich CRA vouchers with OriginalSessionID for original invoice lookup
            all_original_invoice_ids = set()
            for vouchers in all_return_details.values():
                for v in vouchers:
                    if v.get('OriginalInvoiceID'):
                        all_original_invoice_ids.add(v['OriginalInvoiceID'])

            if all_original_invoice_ids:
                original_invoice_sessions = self._query_original_invoice_sessions(
                    cursor, list(all_original_invoice_ids))
                for vouchers in all_return_details.values():
                    for v in vouchers:
                        orig_inv_id = v.get('OriginalInvoiceID')
                        v['OriginalSessionID'] = original_invoice_sessions.get(orig_inv_id)

            conn.close()
            _logger.info("Phase 1 complete. MSSQL connection closed.")

            # ── Phase 2: Create queue with lines ──────────────────────────
            _logger.info("Phase 2: Creating sync queue...")

            # Check for already-synced sessions (idempotency via client_order_ref)
            existing_refs = set(
                self.env['sale.order'].search([
                    ('client_order_ref', '!=', False),
                ]).mapped('client_order_ref')
            )

            queue = self.env['mssql.sync.queue'].create({
                'sync_config_id': self.id,
                'sync_type': 'sales_session',
                'sync_date': invoice_date,
            })

            line_vals_list = []
            skipped_existing = 0

            for session in sessions:
                session_id = session['SessionID']

                # Skip already-synced sessions
                if any(str(session_id) in ref for ref in existing_refs):
                    skipped_existing += 1
                    continue

                session_lines = all_session_lines.get(session_id, [])
                if not session_lines:
                    continue

                # Serialize all session data for retry
                record_data = json.dumps({
                    'session': dict(session),
                    'lines': [dict(l) for l in session_lines],
                    'returns': {
                        'cra_vouchers': all_return_details.get(session_id, []),
                        'pt5_amount': all_pt5_returns.get(session_id),
                    },
                    'credit_sales': all_credit_sales.get(session_id, {}),
                    'payments': [dict(p) for p in all_payments.get(session_id, [])],
                    'invoice_range': all_invoice_ranges.get(session_id, {}),
                }, default=str)

                cashier_name = session['CashierName'] or f"Cashier {session['EmployeeID']}"
                line_vals_list.append({
                    'queue_id': queue.id,
                    'name': f"Session {session_id} - {cashier_name}",
                    'mssql_id': str(session_id),
                    'mssql_table': 'tblCashierActivity',
                    'record_data': record_data,
                })

            if skipped_existing:
                _logger.info(f"Skipped {skipped_existing} already-synced sessions")

            if not line_vals_list:
                queue.unlink()
                return self._success_notification(
                    'Sales Sync', 'No new sessions to process')

            self.env['mssql.sync.queue.line'].create(line_vals_list)
            _logger.info(f"Created queue {queue.name} with {len(line_vals_list)} lines")

            # ── Phase 3: Process queue ────────────────────────────────────
            queue.action_process_queue()

            return {
                'type': 'ir.actions.act_window',
                'name': f'Sales Queue - {invoice_date}',
                'res_model': 'mssql.sync.queue',
                'res_id': queue.id,
                'view_mode': 'form',
                'target': 'current',
            }

        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise UserError(f'Session-based invoice creation failed: {str(e)}')

    @staticmethod
    def _coerce_numeric(val):
        """Coerce a value to float if it's a numeric string.

        JSON round-trip via default=str turns Decimal('123.45') into '123.45'.
        This converts such strings back to float for arithmetic/comparisons.
        """
        if val is None:
            return None
        if isinstance(val, str):
            try:
                return float(val)
            except (ValueError, TypeError):
                return val
        return val

    def _process_queue_sales_session(self, data, queue_line):
        """Process a single sales session from queue line data.

        Called by the queue line dispatcher. Creates SO → Picking → Invoice → Payment
        for one POS session.

        Args:
            data: dict parsed from queue line's record_data JSON
            queue_line: mssql.sync.queue.line record (for context)

        Returns:
            dict with 'model' and 'id' of created invoice, or None
        """
        session = data['session']
        session_lines = data['lines']
        session_payments = data.get('payments', [])
        invoice_range = data.get('invoice_range', {})

        # Parse returns data — backward compatible with old numeric format
        returns_raw = data.get('returns')
        if isinstance(returns_raw, (int, float, str)) or returns_raw is None:
            # Old format: single negative number (all returns as SO line)
            returns_data = {
                'cra_vouchers': [],
                'pt5_amount': self._coerce_numeric(returns_raw),
            }
        else:
            # New format: structured dict with cra_vouchers and pt5_amount
            returns_data = returns_raw
            returns_data['pt5_amount'] = self._coerce_numeric(returns_data.get('pt5_amount'))
            for v in returns_data.get('cra_vouchers', []):
                v['ReturnAmount'] = self._coerce_numeric(v.get('ReturnAmount')) or 0
                for dl in v.get('detail_lines', []):
                    for k in ('Quantity', 'UnitPrice', 'SubTotal', 'TaxAmount', 'TaxPercent'):
                        dl[k] = self._coerce_numeric(dl.get(k)) or 0
                    if dl.get('ItemID') is not None:
                        try:
                            dl['ItemID'] = int(dl['ItemID'])
                        except (ValueError, TypeError):
                            pass

        # Parse credit sales data
        credit_sales = data.get('credit_sales', {})
        credit_amount = self._coerce_numeric(credit_sales.get('total')) or 0

        # Coerce numeric fields that survive JSON round-trip as strings
        session_id = session['SessionID']
        cashier_name = session.get('CashierName') or f"Cashier {session.get('EmployeeID', '?')}"
        net_total = self._coerce_numeric(session['NetTotal'])

        # Coerce numeric fields in session lines
        for line in session_lines:
            for key in ('ItemID', 'AvgPrice', 'TotalQuantity', 'TotalDiscount',
                        'SubTotal', 'Quantity', 'UnitPrice'):
                if key in line:
                    line[key] = self._coerce_numeric(line[key])
            # Ensure ItemID is int for product lookup
            if line.get('ItemID') is not None:
                try:
                    line['ItemID'] = int(line['ItemID'])
                except (ValueError, TypeError):
                    pass

        # Coerce numeric fields in payments
        for pay in session_payments:
            for key in ('PaymentAmount', 'Amount', 'PaymentType'):
                if key in pay:
                    pay[key] = self._coerce_numeric(pay[key])

        # Parse session date
        session_date_raw = session.get('SessionDate')
        if isinstance(session_date_raw, str):
            from datetime import date as date_type
            session_date = date_type.fromisoformat(session_date_raw[:10])
        elif hasattr(session_date_raw, 'date'):
            session_date = session_date_raw.date()
        else:
            session_date = session_date_raw

        _logger.info(f"Processing Session {session_id} - {cashier_name} "
                     f"(NetTotal: {net_total}, Date: {session_date})")

        # ── Prepare products ──────────────────────────────────────────
        all_item_ids = set()
        item_info = {}
        for line in session_lines:
            item_id = line['ItemID']
            if item_id:
                all_item_ids.add(int(item_id))
                if item_id not in item_info:
                    item_info[item_id] = {
                        'name': line.get('ItemName') or line.get('EnglishName') or f"Item {item_id}",
                    }

        product_obj = self.env['product.product']
        existing_records = product_obj.search([
            ('x_sql_item_id', 'in', list(all_item_ids))
        ])

        # Update invoice policy if needed
        to_update = existing_records.filtered(lambda p: p.invoice_policy != 'order')
        if to_update:
            to_update.write({'invoice_policy': 'order'})

        existing_products = {p.x_sql_item_id: p for p in existing_records}

        # Create missing products
        missing = all_item_ids - set(existing_products.keys())
        if missing:
            new_prods = product_obj.create([{
                'name': item_info[iid]['name'],
                'x_sql_item_id': iid,
                'type': 'consu',
                'is_storable': True,
                'invoice_policy': 'order',
            } for iid in missing])
            for p in new_prods:
                existing_products[p.x_sql_item_id] = p

        # ── Prepare tax and journals ──────────────────────────────────
        tax_15 = self.env['account.tax'].search([
            ('type_tax_use', '=', 'sale'),
            ('amount', '=', 15.0),
            ('company_id', '=', self.env.company.id)
        ], limit=1)

        if not tax_15:
            tax_15 = self.env['account.tax'].create({
                'name': 'VAT 15%',
                'amount': 15.0,
                'amount_type': 'percent',
                'type_tax_use': 'sale',
                'company_id': self.env.company.id,
            })

        cash_journal = self.env['account.journal'].search([
            ('type', '=', 'cash'),
            ('company_id', '=', self.env.company.id)
        ], limit=1)
        bank_journal = self.env['account.journal'].search([
            ('type', '=', 'bank'),
            ('company_id', '=', self.env.company.id)
        ], limit=1)

        if not cash_journal and not bank_journal:
            raise UserError('No cash or bank journal found.')

        payment_journal_map = {
            1: self.payment_method_cash_journal_id or cash_journal,
            2: self.payment_method_mada_journal_id or bank_journal,
            3: self.payment_method_visa_journal_id or bank_journal,
            4: self.payment_method_mastercard_journal_id or bank_journal,
            5: cash_journal,  # Return voucher used as payment
            10: cash_journal,
            20: self.payment_method_coupon_journal_id or cash_journal,
            30: cash_journal,
            40: self.payment_method_points_journal_id or cash_journal,
            60: self.payment_method_stcpay_journal_id or bank_journal,
        }

        # ── Get warehouse and partner ─────────────────────────────────
        warehouse = self._get_sales_warehouse()
        partner_obj = self.env['res.partner']
        customer_name = 'عميل نقدي'
        partner = partner_obj.search([('name', '=', customer_name)], limit=1)
        if not partner:
            partner = partner_obj.create({
                'name': customer_name,
                'customer_rank': 1,
            })

        return_product = self._get_or_create_return_product()
        decimal_product = self._get_or_create_decimal_product()

        # ── Classify PT5 return type ────────────────────────────────
        # PT5 positive = customer redeeming a return voucher as PAYMENT
        #   → CRA records the voucher source (not a new return)
        #   → No credit note, no negative SO line; PT5 is just a payment
        # PT5 negative = actual return happening (money going out)
        #   → CRA creates voucher if present; PT5 is a negative SO line
        pt5_amount = returns_data.get('pt5_amount')
        is_voucher_redemption = pt5_amount is not None and pt5_amount > 0

        if is_voucher_redemption:
            _logger.info(
                f"Session {session_id}: PT5 is positive ({pt5_amount}) "
                f"— voucher redemption, will register as payment"
            )

        # ── Prepare SO lines ──────────────────────────────────────────
        # Only pass negative PT5 for Return SO line; positive PT5 is a payment
        so_lines = self._prepare_session_so_lines_optimized(
            session_lines,
            pt5_amount if pt5_amount and pt5_amount < 0 else None,
            existing_products, tax_15, return_product
        )

        if not so_lines:
            raise UserError(f'No SO lines generated for session {session_id}')

        # ── Build reference ───────────────────────────────────────────
        min_inv_id = invoice_range.get('MinInvoiceID', '')
        max_inv_id = invoice_range.get('MaxInvoiceID', '')
        ref_text = f"Session {session_id} - {cashier_name} - invs {min_inv_id} to {max_inv_id}"

        # ── Create Sales Order ────────────────────────────────────────
        sale_order = self._create_sales_order(
            partner=partner,
            order_lines=so_lines,
            invoice_date=session_date,
            warehouse=warehouse,
            reference=ref_text,
        )

        # ── Decimal Adjustment ────────────────────────────────────────
        # Voucher redemption (positive PT5): target = NetTotal only
        #   (PT5 is a payment, CRA is metadata — no credit note needed)
        # Actual return (negative PT5 or no PT5): target = NetTotal + CRA
        #   (credit notes will reduce invoice residual by CRA amount)
        cra_vouchers = returns_data.get('cra_vouchers', [])
        total_cra = sum(v['ReturnAmount'] for v in cra_vouchers)

        if is_voucher_redemption:
            mssql_total = round(float(net_total) + credit_amount, 2)
        else:
            mssql_total = round(float(net_total) + total_cra + credit_amount, 2)

        for attempt in range(1, 6):
            sale_order = self.env['sale.order'].browse(sale_order.id)
            so_total = round(sale_order.amount_total, 2)
            difference = round(mssql_total - so_total, 2)

            if abs(difference) < 0.01:
                break

            if abs(difference) > 1.00:
                _logger.warning(f"Large difference: {difference}")

            pre_tax_adj = difference / 1.15
            decimal_line = sale_order.order_line.filtered(
                lambda l: l.product_id.id == decimal_product.id
            )

            if decimal_line:
                decimal_line.write({'price_unit': decimal_line.price_unit + pre_tax_adj})
            else:
                self.env['sale.order.line'].create({
                    'order_id': sale_order.id,
                    'product_id': decimal_product.id,
                    'product_uom_qty': 1,
                    'price_unit': pre_tax_adj,
                    'discount': 0,
                    'name': 'Decimal',
                    'tax_id': [(6, 0, [tax_15.id])],
                })

        # ── Validation checks (logging only) ─────────────────────────
        self._validate_so_vs_epos(sale_order, session_lines, session_id, mssql_total,
                                  return_product, decimal_product)

        # ── Confirm SO and validate picking ───────────────────────────
        sale_order.action_confirm()

        picking = sale_order.picking_ids[0] if sale_order.picking_ids else False
        if picking:
            self._validate_picking(picking)

        # ── Create invoice ────────────────────────────────────────────
        invoice_wizard = self.env['sale.advance.payment.inv'].with_context({
            'active_ids': [sale_order.id],
            'active_model': 'sale.order',
        }).create({'advance_payment_method': 'delivered'})
        invoice_wizard.create_invoices()

        sale_order = self.env['sale.order'].browse(sale_order.id)
        invoice = sale_order.invoice_ids[0] if sale_order.invoice_ids else False

        if not invoice:
            raise UserError(f'Failed to create invoice from SO {sale_order.name}')

        invoice.write({
            'ref': ref_text,
            'invoice_date': session_date,
            'date': session_date,
        })

        # ── Add narration for credit sales (unpaid invoices) ─────────
        if credit_amount > 0:
            credit_invoices = credit_sales.get('invoices', [])
            note_lines = [f"Credit sales (unpaid) — {credit_amount:.2f} SAR:"]
            for cinv in credit_invoices:
                inv_id = cinv.get('InvoiceID', '?')
                cust_name = cinv.get('CustomerName') or '?'
                phone = cinv.get('PhoneNo') or ''
                inv_total = self._coerce_numeric(cinv.get('NetTotal')) or 0
                phone_part = f" ({phone})" if phone else ""
                note_lines.append(
                    f"  Invoice {inv_id} | Customer: {cust_name}{phone_part} | {inv_total:.2f} SAR"
                )
                for prod in cinv.get('products', []):
                    p_name = prod.get('ItemName') or f"Item {prod.get('ItemID', '?')}"
                    p_qty = self._coerce_numeric(prod.get('Quantity')) or 0
                    p_price = self._coerce_numeric(prod.get('UnitPrice')) or 0
                    p_sub = self._coerce_numeric(prod.get('SubTotal')) or 0
                    note_lines.append(
                        f"    - {p_name} qty={p_qty:g} @ {p_price:.2f} = {p_sub:.2f}"
                    )
            invoice.write({'narration': '\n'.join(note_lines)})
            _logger.info(f"Session {session_id}: Credit sales {credit_amount:.2f} SAR "
                         f"({len(credit_invoices)} invoice(s)) noted in narration")

        # ── Post invoice ──────────────────────────────────────────────
        invoice.action_post()

        # ── Create credit notes for CRA return vouchers ───────────────
        # Skip credit notes for voucher redemptions (positive PT5) —
        # the CRA just records which voucher the customer is using as payment
        if cra_vouchers and not is_voucher_redemption:
            self._create_return_credit_notes(
                invoice, cra_vouchers, partner, tax_15, session_date
            )

        # ── Register payments ─────────────────────────────────────────
        # For voucher redemptions, add PT5 as a payment (return voucher used to pay)
        if is_voucher_redemption:
            session_payments = list(session_payments) if session_payments else []
            session_payments.append({
                'PaymentType': 5,
                'PaymentMethodName': 'Return Voucher',
                'Amount': round(pt5_amount, 2),
            })

        if session_payments:
            self._register_session_payments_optimized(
                invoice, session_payments, session_date,
                payment_journal_map, cash_journal, bank_journal
            )

        _logger.info(f"Session {session_id}: SO {sale_order.name}, "
                     f"Invoice {invoice.name} created successfully")

        return {'model': 'account.move', 'id': invoice.id}

    def action_create_invoice(self):
        """Create session-based invoices using the invoice_date field"""
        if not self.invoice_date:
            raise UserError('Please select an invoice date')
        return self.create_session_based_invoices(self.invoice_date)

    def _get_or_create_return_product(self):
        """Get or create a 'Return' product for handling return transactions

        # ============================================================================
        # RETURNS PRODUCT - This may be changed in the future
        # Currently creates a single "Return" product for all return transactions
        # Future options:
        # - Create separate return entries per product
        # - Use credit notes instead
        # - Handle returns in a separate flow
        # ============================================================================
        """
        product = self.env['product.product'].search([
            ('name', '=', 'Return'),
            ('type', '=', 'service'),
        ], limit=1)

        if not product:
            product = self.env['product.product'].create({
                'name': 'Return',
                'type': 'service',  # Service type - no stock impact
                'invoice_policy': 'order',
            })
            _logger.info(f"Created 'Return' product with ID {product.id}")

        return product

    def _prepare_session_so_lines(self, session_lines, return_amount, existing_products):
        """Prepare Sales Order lines from session data

        Args:
            session_lines: List of aggregated line items from _query_session_lines()
            return_amount: Return amount (negative) from _query_session_return_amount()
            existing_products: Dict mapping ItemID to product.product records

        Returns:
            List of SO line tuples [(0, 0, {...}), ...]
        """
        so_lines = []

        # Get or create 15% VAT tax
        tax_15 = self.env['account.tax'].search([
            ('type_tax_use', '=', 'sale'),
            ('amount', '=', 15.0),
            ('company_id', '=', self.env.company.id)
        ], limit=1)

        if not tax_15:
            tax_15 = self.env['account.tax'].create({
                'name': 'VAT 15%',
                'amount': 15.0,
                'amount_type': 'percent',
                'type_tax_use': 'sale',
                'company_id': self.env.company.id,
            })
            _logger.info(f"Created VAT 15% tax with ID {tax_15.id}")

        # Process regular lines
        for line in session_lines:
            item_id = line['ItemID']
            if not item_id:
                continue

            product = existing_products.get(item_id)
            if not product:
                _logger.warning(f"Product not found for ItemID {item_id}, skipping line")
                continue

            avg_price = float(line['AvgPrice'] or 0)
            quantity = float(line['TotalQuantity'] or 0)
            total_discount = float(line['TotalDiscount'] or 0)
            subtotal = float(line['SubTotal'] or 0)  # Final amount after discount and tax from MSSQL

            if quantity <= 0:
                continue

            # Calculate discount percentage for display
            # Formula: discount_pct = (TotalDiscount / (AvgPrice * TotalQuantity)) * 100
            original_amount = avg_price * quantity
            discount_pct = 0.0
            if original_amount > 0 and total_discount > 0:
                discount_pct = round((total_discount / original_amount) * 100, 2)

            # Back-calculate price_unit from SubTotal to ensure exact match with MSSQL
            # SubTotal = price_unit × qty × (1 - discount%) × 1.15
            discount_factor = (100 - discount_pct) / 100
            if quantity > 0 and discount_factor > 0 and subtotal > 0:
                price_unit = subtotal / (quantity * discount_factor * 1.15)
            else:
                price_unit = avg_price  # Fallback to AvgPrice

            so_lines.append((0, 0, {
                'product_id': product.id,
                'product_uom_qty': quantity,
                'price_unit': price_unit,
                'discount': discount_pct,
                'name': product.name,
                'tax_id': [(6, 0, [tax_15.id])],
            }))

        # ============================================================================
        # RETURNS HANDLING - Create a "Return" product with negative price
        # TODO: This section may be changed in the future
        # Currently: Single "Return" product with total return amount as negative price
        # ============================================================================
        if return_amount and return_amount < 0:
            return_product = self._get_or_create_return_product()
            so_lines.append((0, 0, {
                'product_id': return_product.id,
                'product_uom_qty': 1,
                'price_unit': return_amount,  # Already negative from SQL query
                'discount': 0,
                'name': 'Return',
                'tax_id': [(6, 0, [tax_15.id])],
            }))
            _logger.info(f"Added return line with amount {return_amount}")
        # ============================================================================
        # END RETURNS HANDLING
        # ============================================================================

        return so_lines

    def _register_session_payments(self, invoice, session_payments, invoice_date):
        """Register payments for a session-based invoice

        # ============================================================================
        # PAYMENT AMOUNT SOURCE - Using ActualAmount (terminal counted)
        # This may be changed in the future to use PCAmount (system calculated)
        #
        # ActualAmount = What the cashier counted/terminal reported
        # PCAmount = What the POS system calculated
        # ============================================================================

        Args:
            invoice: account.move record (posted invoice)
            session_payments: List of payment records from _query_session_payments()
            invoice_date: Date for the payments

        Returns:
            List of created payment IDs
        """
        if not session_payments:
            _logger.warning(f"No payments to register for invoice {invoice.name}")
            return []

        # Post invoice first if not already posted
        if invoice.state != 'posted':
            invoice.action_post()
            _logger.info(f"Posted invoice {invoice.name} before registering payments")

        # Get payment journals
        cash_journal = self.env['account.journal'].search([
            ('type', '=', 'cash'),
            ('company_id', '=', self.env.company.id)
        ], limit=1)

        bank_journal = self.env['account.journal'].search([
            ('type', '=', 'bank'),
            ('company_id', '=', self.env.company.id)
        ], limit=1)

        if not cash_journal and not bank_journal:
            raise UserError('No cash or bank journal found. Please create at least one payment journal.')

        # Payment method to journal mapping
        payment_method_journal_map = {
            1: self.payment_method_cash_journal_id or cash_journal,       # Cash
            2: self.payment_method_mada_journal_id or bank_journal,       # Mada
            3: self.payment_method_visa_journal_id or bank_journal,       # Visa
            4: self.payment_method_mastercard_journal_id or bank_journal,  # MasterCard
            10: cash_journal,                                              # Donation
            20: self.payment_method_coupon_journal_id or cash_journal,    # Coupon
            30: cash_journal,                                              # Ports
            40: self.payment_method_points_journal_id or cash_journal,    # Points
            60: self.payment_method_stcpay_journal_id or bank_journal,    # STC Pay
        }

        # Payment method names for logging
        payment_method_names = {
            1: 'Cash',
            2: 'Mada',
            3: 'Visa',
            4: 'MasterCard',
            10: 'Donation',
            20: 'Coupon',
            30: 'Ports',
            40: 'Points',
            60: 'STC Pay',
        }

        all_payment_ids = []

        for payment in session_payments:
            payment_type = payment['PaymentType']

            # ============================================================================
            # PAYMENT AMOUNT - Using ActualAmount
            # Change to PCAmount if needed in the future
            # ============================================================================
            amount = round(float(payment['Amount'] or 0), 2)
            if amount <= 0:
                continue

            journal = payment_method_journal_map.get(payment_type)
            if not journal:
                _logger.warning(f"No journal mapping for payment type {payment_type}, using default")
                journal = cash_journal or bank_journal

            # Check if invoice still has residual
            invoice = self.env['account.move'].browse(invoice.id)  # Refresh
            if invoice.amount_residual <= 0:
                _logger.info(f"Invoice {invoice.name} fully paid, stopping payment registration")
                break

            try:
                method_name = payment_method_names.get(payment_type, f'Method {payment_type}')
                payment_ref = f"{payment.get('PaymentMethodName') or method_name} - Session Payment"

                # Register payment using wizard
                payment_register = self.env['account.payment.register'].with_context(
                    active_model='account.move',
                    active_ids=invoice.ids,
                    dont_redirect_to_payments=True
                ).create({
                    'payment_date': invoice_date,
                    'journal_id': journal.id,
                    'amount': amount,
                    'communication': payment_ref,
                    'group_payment': False,
                })
                payment_register.action_create_payments()

                # Find the created payment
                recent_payment = self.env['account.payment'].search([
                    ('partner_id', '=', invoice.partner_id.id),
                    ('journal_id', '=', journal.id),
                    ('date', '=', invoice_date),
                    ('amount', '=', amount),
                ], order='id desc', limit=1)

                if recent_payment:
                    all_payment_ids.append(recent_payment.id)
                    _logger.info(f"Registered {method_name} payment: {amount} via {journal.name}")
                else:
                    _logger.warning(f"Payment created but not found for {method_name}: {amount}")

            except Exception as e:
                error_msg = str(e)
                if 'nothing left to pay' in error_msg.lower():
                    _logger.info(f"Invoice fully paid during {method_name} payment, stopping")
                    break
                else:
                    _logger.error(f"Failed to create {method_name} payment: {error_msg}")

        _logger.info(f"Registered {len(all_payment_ids)} payments for invoice {invoice.name}")
        return all_payment_ids

    # ============================================================================
    # OPTIMIZED HELPER METHODS - For bulk processing performance
    # These accept pre-fetched data to avoid repeated lookups
    # ============================================================================

    def _prepare_session_so_lines_optimized(self, session_lines, pt5_amount, existing_products, tax_15, return_product):
        """Prepare SO lines from session data - OPTIMIZED VERSION

        Performance: Accepts pre-fetched tax and return product to avoid
        repeated lookups during batch processing.

        Note: Decimal adjustment is handled separately after SO creation
        to ensure accurate matching with MSSQL NetTotal.

        Args:
            session_lines: List of aggregated line items
            pt5_amount: PaymentType=5 return amount (negative) or None.
                        CRA returns are handled as credit notes, not SO lines.
            existing_products: Dict mapping ItemID to product.product records
            tax_15: Pre-fetched 15% VAT tax record
            return_product: Pre-fetched return product record

        Returns:
            List of SO line tuples [(0, 0, {...}), ...]
        """
        so_lines = []
        tax_id_tuple = [(6, 0, [tax_15.id])]

        # Process regular lines
        for line in session_lines:
            item_id = line['ItemID']
            if not item_id:
                continue

            product = existing_products.get(item_id)
            if not product:
                _logger.warning(f"Product not found for ItemID {item_id}, skipping line")
                continue

            avg_price = float(line['AvgPrice'] or 0)
            quantity = float(line['TotalQuantity'] or 0)
            total_discount = float(line['TotalDiscount'] or 0)
            subtotal = float(line['SubTotal'] or 0)  # Final amount after discount and tax from MSSQL

            if quantity <= 0:
                continue

            # Calculate discount percentage for display
            # Formula: discount_pct = (TotalDiscount / (AvgPrice * TotalQuantity)) * 100
            original_amount = avg_price * quantity
            discount_pct = 0.0
            if original_amount > 0 and total_discount > 0:
                discount_pct = round((total_discount / original_amount) * 100, 2)

            # Back-calculate price_unit from SubTotal to ensure exact match with MSSQL
            # SubTotal = price_unit × qty × (1 - discount%) × 1.15
            discount_factor = (100 - discount_pct) / 100
            if quantity > 0 and discount_factor > 0 and subtotal > 0:
                price_unit = subtotal / (quantity * discount_factor * 1.15)
            else:
                price_unit = avg_price  # Fallback to AvgPrice

            so_lines.append((0, 0, {
                'product_id': product.id,
                'product_uom_qty': quantity,
                'price_unit': price_unit,
                'discount': discount_pct,
                'name': product.name,
                'tax_id': tax_id_tuple,
            }))

        # ============================================================================
        # PT5 RETURNS HANDLING (Mechanism B) - Lump sum return slips
        # CRA returns (Mechanism A) are handled as credit notes, not SO lines.
        # PT5 amount from MSSQL is TAX-INCLUSIVE (like SubTotal)
        # So we back-calculate the pre-tax price: price_unit = pt5_amount / 1.15
        # ============================================================================
        if pt5_amount and pt5_amount < 0:
            pt5_amount_float = float(pt5_amount)
            return_price_unit = pt5_amount_float / 1.15
            so_lines.append((0, 0, {
                'product_id': return_product.id,
                'product_uom_qty': 1,
                'price_unit': return_price_unit,  # Pre-tax price (negative)
                'discount': 0,
                'name': 'Return',
                'tax_id': tax_id_tuple,
            }))
        # ============================================================================
        # END PT5 RETURNS HANDLING
        # ============================================================================

        return so_lines

    def _register_session_payments_optimized(self, invoice, session_payments, invoice_date, payment_journal_map, cash_journal, bank_journal):
        """Register payments for a session invoice - OPTIMIZED VERSION

        Performance: Accepts pre-fetched journal map to avoid repeated lookups.

        # ============================================================================
        # PAYMENT AMOUNT SOURCE - Using ActualAmount (terminal counted)
        # This may be changed in the future to use PCAmount (system calculated)
        # ============================================================================

        Args:
            invoice: account.move record
            session_payments: List of payment records
            invoice_date: Date for the payments
            payment_journal_map: Pre-built dict of payment_type -> journal
            cash_journal: Pre-fetched cash journal
            bank_journal: Pre-fetched bank journal

        Returns:
            List of created payment IDs
        """
        if not session_payments:
            return []

        # Post invoice first if not already posted
        if invoice.state != 'posted':
            invoice.action_post()

        # Payment method names for logging
        payment_method_names = {
            1: 'Cash', 2: 'Mada', 3: 'Visa', 4: 'MasterCard', 5: 'Return Voucher',
            10: 'Donation', 20: 'Coupon', 30: 'Ports', 40: 'Points', 60: 'STC Pay',
        }

        all_payment_ids = []

        for payment in session_payments:
            payment_type = payment['PaymentType']

            # ============================================================================
            # PAYMENT AMOUNT - Using ActualAmount
            # Change to PCAmount if needed in the future
            # ============================================================================
            amount = round(float(payment['Amount'] or 0), 2)
            if amount <= 0:
                continue

            journal = payment_journal_map.get(payment_type)
            if not journal:
                journal = cash_journal or bank_journal

            # Check if invoice still has residual
            invoice = self.env['account.move'].browse(invoice.id)
            if invoice.amount_residual <= 0:
                break

            try:
                method_name = payment_method_names.get(payment_type, f'Method {payment_type}')
                payment_ref = f"{payment.get('PaymentMethodName') or method_name} - Session Payment"

                # Register payment using wizard
                payment_register = self.env['account.payment.register'].with_context(
                    active_model='account.move',
                    active_ids=invoice.ids,
                    dont_redirect_to_payments=True
                ).create({
                    'payment_date': invoice_date,
                    'journal_id': journal.id,
                    'amount': amount,
                    'communication': payment_ref,
                    'group_payment': False,
                })
                payment_register.action_create_payments()

                # Track payment (simplified - don't search for it, just count)
                all_payment_ids.append(True)

            except Exception as e:
                error_msg = str(e)
                if 'nothing left to pay' in error_msg.lower():
                    break
                else:
                    _logger.error(f"Failed to create payment: {error_msg}")

        return all_payment_ids

    # ============================================================================
    # VALIDATION - SO vs EPOS checks (logging only, never blocks)
    # ============================================================================

    def _validate_so_vs_epos(self, sale_order, session_lines, session_id, mssql_total,
                              return_product, decimal_product):
        """Compare SO data against EPOS session data and log differences.

        This is a diagnostic tool — it logs warnings but never blocks the sync.

        Args:
            sale_order: sale.order record
            session_lines: list of MSSQL aggregated line dicts
            session_id: MSSQL session ID
            mssql_total: expected total (NetTotal + CRA returns)
            return_product: Return service product (excluded from counts)
            decimal_product: Decimal adjustment product (excluded from counts)
        """
        try:
            exclude_ids = {return_product.id, decimal_product.id}

            # SO product lines (excluding Return and Decimal)
            product_lines = sale_order.order_line.filtered(
                lambda l: l.product_id.id not in exclude_ids
            )
            so_line_count = len(product_lines)
            so_total_qty = sum(l.product_uom_qty for l in product_lines)
            so_amount_total = round(sale_order.amount_total, 2)

            # EPOS counts
            epos_line_count = len([l for l in session_lines if l.get('ItemID')])
            epos_total_qty = sum(float(l.get('TotalQuantity') or 0) for l in session_lines if l.get('ItemID'))

            # Compare
            if so_line_count != epos_line_count:
                _logger.warning(
                    f"Session {session_id} VALIDATION: Line count mismatch — "
                    f"SO has {so_line_count} product lines, EPOS has {epos_line_count}"
                )

            qty_diff = abs(so_total_qty - epos_total_qty)
            if qty_diff > 0.01:
                _logger.warning(
                    f"Session {session_id} VALIDATION: Qty mismatch — "
                    f"SO total qty={so_total_qty:.2f}, EPOS total qty={epos_total_qty:.2f} "
                    f"(diff={qty_diff:.2f})"
                )

            total_diff = abs(so_amount_total - mssql_total)
            if total_diff > 0.05:
                _logger.warning(
                    f"Session {session_id} VALIDATION: Total mismatch — "
                    f"SO amount_total={so_amount_total:.2f}, MSSQL target={mssql_total:.2f} "
                    f"(diff={total_diff:.2f})"
                )
            else:
                _logger.info(
                    f"Session {session_id} VALIDATION: OK — "
                    f"{so_line_count} lines, qty={so_total_qty:.2f}, total={so_amount_total:.2f}"
                )

        except Exception as e:
            _logger.warning(f"Session {session_id} VALIDATION: Check failed — {e}")

    # ============================================================================
    # CREDIT NOTE CREATION FOR CRA RETURNS (via action_reverse + refund_moves)
    # ============================================================================

    def _create_return_credit_notes(self, session_invoice, cra_vouchers, partner, tax_15, session_date):
        """Create credit notes for CRA return vouchers using action_reverse/refund_moves.

        For each CRA voucher:
        - Finds the original Odoo invoice (via OriginalSessionID → sale.order)
        - Uses account.move.reversal wizard with reason='mssql return'
        - Calls refund_moves() to create a draft credit note linked to original
        - Replaces auto-generated lines with actual return details from ZATCA
        - Posts the credit note
        - Reconciles receivable lines with the session invoice to reduce residual
        - Falls back to standalone credit note if original invoice not found

        Args:
            session_invoice: account.move record (posted session invoice)
            cra_vouchers: list of voucher dicts with ReturnCode, ReturnAmount,
                          OriginalInvoiceID, OriginalSessionID, detail_lines, etc.
            partner: res.partner record
            tax_15: account.tax record for 15% VAT
            session_date: date for the credit notes
        """
        if not cra_vouchers:
            return

        product_obj = self.env['product.product']
        return_product = self._get_or_create_return_product()
        sale_journal = session_invoice.journal_id

        # Collect all ItemIDs from voucher detail lines for bulk product lookup
        all_item_ids = set()
        item_info = {}
        for voucher in cra_vouchers:
            for dl in voucher.get('detail_lines', []):
                item_id = dl.get('ItemID')
                if item_id:
                    all_item_ids.add(int(item_id))
                    if item_id not in item_info:
                        item_info[item_id] = {'name': dl.get('ItemName') or f"Item {item_id}"}

        # Find/create products for credit note lines
        cn_products = {}
        if all_item_ids:
            existing = product_obj.search([('x_sql_item_id', 'in', list(all_item_ids))])
            cn_products = {p.x_sql_item_id: p for p in existing}

            missing = all_item_ids - set(cn_products.keys())
            if missing:
                new_prods = product_obj.create([{
                    'name': item_info[iid]['name'],
                    'x_sql_item_id': iid,
                    'type': 'consu',
                    'is_storable': True,
                    'invoice_policy': 'order',
                } for iid in missing])
                for p in new_prods:
                    cn_products[p.x_sql_item_id] = p

        for voucher in cra_vouchers:
            return_code = voucher.get('ReturnCode')
            return_amount = voucher.get('ReturnAmount', 0)
            original_invoice_id = voucher.get('OriginalInvoiceID')
            original_session_id = voucher.get('OriginalSessionID')
            return_date_str = voucher.get('ReturnDate')
            detail_lines = voucher.get('detail_lines', [])

            if not return_amount:
                continue

            # Parse return date
            cn_date = session_date
            if isinstance(return_date_str, str):
                from datetime import date as date_type
                try:
                    cn_date = date_type.fromisoformat(return_date_str[:10])
                except (ValueError, TypeError):
                    pass

            # Find original Odoo invoice (best effort via sale.order client_order_ref)
            original_odoo_invoice = False
            if original_session_id:
                original_so = self.env['sale.order'].search([
                    ('client_order_ref', 'like', f'Session {original_session_id} -'),
                ], limit=1)
                if original_so and original_so.invoice_ids:
                    posted_invoices = original_so.invoice_ids.filtered(
                        lambda m: m.state == 'posted' and m.move_type == 'out_invoice'
                    )
                    if posted_invoices:
                        original_odoo_invoice = posted_invoices[0]

            # Build the return detail lines (used for both paths)
            cn_line_vals = self._build_return_line_vals(
                detail_lines, cn_products, return_product, return_amount, tax_15
            )

            credit_note = False

            # ── Path A: action_reverse on original invoice ──────────────
            if original_odoo_invoice:
                try:
                    credit_note = self._create_credit_note_via_reversal(
                        original_odoo_invoice, cn_date, return_code,
                        original_invoice_id, cn_line_vals, tax_15
                    )
                except Exception as e:
                    _logger.warning(
                        f"action_reverse failed for return {return_code} on "
                        f"invoice {original_odoo_invoice.name}: {e}. "
                        f"Falling back to standalone credit note."
                    )
                    credit_note = False

            # ── Path B: Fallback — standalone credit note ───────────────
            if not credit_note:
                credit_note = self._create_standalone_credit_note(
                    partner, cn_date, return_code, original_invoice_id,
                    sale_journal, cn_line_vals, original_odoo_invoice
                )

            if not credit_note:
                _logger.error(f"Failed to create credit note for return {return_code}")
                continue

            _logger.info(
                f"Created credit note {credit_note.name} for return code {return_code} "
                f"(amount: {credit_note.amount_total}, "
                f"method: {'reversal' if credit_note.reversed_entry_id else 'standalone'})"
            )

            # Reconcile credit note with session invoice
            self._reconcile_credit_note_with_session(credit_note, session_invoice)

    def _build_return_line_vals(self, detail_lines, cn_products, return_product, return_amount, tax_15):
        """Build invoice line vals for a return credit note.

        Uses ZATCA detail lines when available, otherwise creates a single
        Return service line with back-calculated pre-tax amount.

        Args:
            detail_lines: list of ZATCA detail dicts (may be empty)
            cn_products: dict mapping ItemID to product.product records
            return_product: fallback Return service product
            return_amount: total return amount (tax-inclusive, positive)
            tax_15: account.tax record for 15% VAT

        Returns:
            list of (0, 0, {...}) tuples for invoice_line_ids
        """
        cn_line_vals = []
        if detail_lines:
            for dl in detail_lines:
                item_id = dl.get('ItemID')
                product = cn_products.get(int(item_id)) if item_id else None
                if not product:
                    product = return_product

                cn_line_vals.append((0, 0, {
                    'product_id': product.id,
                    'quantity': abs(dl.get('Quantity', 1)),
                    'price_unit': dl.get('UnitPrice', 0),
                    'tax_ids': [(6, 0, [tax_15.id])],
                }))
        else:
            # No ZATCA detail — single Return line
            # ReturnAmount is tax-inclusive, back-calculate pre-tax
            pre_tax = return_amount / 1.15
            cn_line_vals.append((0, 0, {
                'product_id': return_product.id,
                'quantity': 1,
                'price_unit': pre_tax,
                'tax_ids': [(6, 0, [tax_15.id])],
            }))
        return cn_line_vals

    def _create_credit_note_via_reversal(self, original_invoice, cn_date, return_code,
                                          original_invoice_id, cn_line_vals, tax_15):
        """Create a credit note via action_reverse + refund_moves on the original invoice.

        1. Creates account.move.reversal wizard with reason='mssql return'
        2. Calls refund_moves() to generate a draft credit note
        3. Replaces auto-generated lines with actual MSSQL return details
        4. Posts the credit note

        Args:
            original_invoice: account.move record (posted original invoice)
            cn_date: date for the credit note
            return_code: MSSQL ReturnCode for reference
            original_invoice_id: MSSQL OriginalInvoiceID for reference
            cn_line_vals: list of (0, 0, {...}) line tuples from _build_return_line_vals
            tax_15: account.tax record for 15% VAT

        Returns:
            account.move record (posted credit note) or False
        """
        # Step 1: Create reversal wizard
        reversal_wizard = self.env['account.move.reversal'].with_context(
            active_model='account.move',
            active_ids=original_invoice.ids,
        ).create({
            'reason': 'mssql return',
            'date': cn_date,
            'journal_id': original_invoice.journal_id.id,
        })

        # Step 2: Call refund_moves to create draft credit note
        reversal_wizard.refund_moves()

        # Step 3: Find the newly created draft credit note
        credit_note = original_invoice.reversal_move_ids.filtered(
            lambda m: m.state == 'draft'
        )
        if not credit_note:
            _logger.warning(f"No draft credit note found after refund_moves for {original_invoice.name}")
            return False
        credit_note = credit_note[-1]  # Take the most recent one

        # Step 4: Replace auto-generated lines with MSSQL return details
        # Remove all product lines from the auto-generated credit note
        product_lines = credit_note.invoice_line_ids.filtered(
            lambda l: l.display_type == 'product'
        )
        if product_lines:
            credit_note.write({'invoice_line_ids': [(2, line.id) for line in product_lines]})

        # Add the actual return lines from MSSQL/ZATCA
        credit_note.write({'invoice_line_ids': cn_line_vals})

        # Update reference
        credit_note.write({
            'ref': f'Return {return_code} - Original MSSQL Invoice {original_invoice_id}',
        })

        # Step 5: Post the credit note
        credit_note.action_post()

        return credit_note

    def _create_standalone_credit_note(self, partner, cn_date, return_code,
                                        original_invoice_id, journal, cn_line_vals,
                                        original_odoo_invoice=False):
        """Create a standalone credit note (fallback when original invoice not found).

        Args:
            partner: res.partner record
            cn_date: date for the credit note
            return_code: MSSQL ReturnCode for reference
            original_invoice_id: MSSQL OriginalInvoiceID for reference
            journal: account.journal to use
            cn_line_vals: list of (0, 0, {...}) line tuples
            original_odoo_invoice: optional original invoice to link via reversed_entry_id

        Returns:
            account.move record (posted credit note) or False
        """
        cn_vals = {
            'move_type': 'out_refund',
            'partner_id': partner.id,
            'invoice_date': cn_date,
            'date': cn_date,
            'ref': f'Return {return_code} - Original MSSQL Invoice {original_invoice_id}',
            'journal_id': journal.id,
            'invoice_line_ids': cn_line_vals,
        }

        if original_odoo_invoice:
            cn_vals['reversed_entry_id'] = original_odoo_invoice.id

        try:
            credit_note = self.env['account.move'].create(cn_vals)
            credit_note.action_post()
            return credit_note
        except Exception as e:
            _logger.error(f"Failed to create standalone credit note for return {return_code}: {e}")
            return False

    def _reconcile_credit_note_with_session(self, credit_note, session_invoice):
        """Reconcile a credit note's receivable lines with the session invoice.

        Args:
            credit_note: account.move record (posted credit note)
            session_invoice: account.move record (posted session invoice)
        """
        try:
            lines_to_reconcile = (credit_note + session_invoice).line_ids.filtered(
                lambda l: l.account_id.account_type == 'asset_receivable' and not l.reconciled
            )
            if lines_to_reconcile:
                lines_to_reconcile.reconcile()
                _logger.info(f"Reconciled credit note {credit_note.name} with "
                             f"invoice {session_invoice.name}")
        except Exception as e:
            _logger.warning(f"Failed to reconcile credit note {credit_note.name}: {e}")

    # ============================================================================
    # SESSION SQL QUERY METHODS
    # ============================================================================

    def _query_sessions_for_date(self, cursor, date_str, next_date):
        """Fetch all POS sessions for a given date from tblCashierActivity

        Args:
            cursor: Database cursor
            date_str: Start date string (YYYY-MM-DD)
            next_date: End date string (YYYY-MM-DD)

        Returns:
            List of session records with cashier info and totals
        """
        cursor.execute("""
            SELECT
                ca.SessionID,
                ca.SessionDate,
                ca.EmployeeID,
                e.EmployeeName AS CashierName,
                ca.InvoiceCount,
                ca.SalesInvoiceCount,
                ca.ReturnInvoiceCount,
                ca.LineCount,
                ca.ItemsCount,
                ROUND(ca.NetTotal, 2) AS NetTotal,
                ROUND(ca.ActualAmount, 2) AS ActualAmount,
                ROUND(ca.NetTotalDiff, 2) AS NetTotalDiff,
                ca.CashierClosed,
                ca.SessionClosed
            FROM [dbo].[tblCashierActivity] ca
            LEFT JOIN [dbo].[tblEmployees] e ON ca.EmployeeID = e.EmployeeID
            WHERE ca.SessionDate >= %s AND ca.SessionDate < %s
            ORDER BY ca.SessionID
        """, (date_str, next_date))
        return cursor.fetchall()

    # ============================================================================
    # BULK QUERY METHODS - Optimized for performance
    # These methods fetch data for ALL sessions at once to minimize DB round trips
    # ============================================================================

    def _query_all_session_lines(self, cursor, session_ids):
        """Fetch aggregated invoice lines for ALL sessions in one query

        Performance optimization: Instead of N queries (one per session),
        we execute 1 query and group results in Python.

        Args:
            cursor: Database cursor
            session_ids: List of session IDs to fetch lines for

        Returns:
            Dict mapping SessionID to list of aggregated line items
        """
        if not session_ids:
            return {}

        placeholders = ','.join(['%s'] * len(session_ids))
        cursor.execute(f"""
            SELECT
                i.SessionID,
                id.ItemID,
                id.ItemName,
                id.EnglishName,
                id.BarCode,
                id.UnitName,
                ROUND(SUM(id.Quantity), 2) AS TotalQuantity,
                COUNT(*) AS TimesSold,
                AVG(id.UnitPrice) AS AvgPrice,
                ROUND(SUM(id.SubTotal), 2) AS SubTotal,
                ROUND(SUM(id.LineDiscount), 2) AS TotalDiscount
            FROM [dbo].[tblInvoice] i
            INNER JOIN [dbo].[tblInvoiceDetail] id ON i.InvoiceID = id.InvoiceID
            WHERE i.SessionID IN ({placeholders})
              AND i.IsReturned = 0
            GROUP BY
                i.SessionID,
                id.ItemID,
                id.ItemName,
                id.EnglishName,
                id.BarCode,
                id.UnitName
            ORDER BY i.SessionID, id.ItemID
        """, session_ids)

        # Group results by SessionID
        results = {}
        for row in cursor.fetchall():
            session_id = row['SessionID']
            if session_id not in results:
                results[session_id] = []
            results[session_id].append(row)

        return results

    def _query_all_session_returns(self, cursor, session_ids):
        """Fetch return amounts for ALL sessions in one query

        Performance optimization: Instead of N queries, we execute 1 query.

        Args:
            cursor: Database cursor
            session_ids: List of session IDs

        Returns:
            Dict mapping SessionID to return amount (negative value)
        """
        if not session_ids:
            return {}

        placeholders = ','.join(['%s'] * len(session_ids))
        cursor.execute(f"""
            SELECT
                r.SessionID,
                -ROUND(SUM(r.ReturnAmount), 2) AS ReturnAmount
            FROM [dbo].[tblCashierActivityReturnAmount] r
            WHERE r.SessionID IN ({placeholders})
            GROUP BY r.SessionID
        """, session_ids)

        return {row['SessionID']: row['ReturnAmount'] for row in cursor.fetchall()}

    def _query_all_session_invoice_ranges(self, cursor, session_ids):
        """Fetch invoice ID range (min/max) for ALL sessions in one query

        Args:
            cursor: Database cursor
            session_ids: List of session IDs

        Returns:
            Dict mapping SessionID to dict with MinInvoiceID and MaxInvoiceID
        """
        if not session_ids:
            return {}

        placeholders = ','.join(['%s'] * len(session_ids))
        cursor.execute(f"""
            SELECT
                i.SessionID,
                MIN(i.InvoiceID) AS MinInvoiceID,
                MAX(i.InvoiceID) AS MaxInvoiceID,
                COUNT(i.InvoiceID) AS InvoiceCount
            FROM [dbo].[tblInvoice] i
            WHERE i.SessionID IN ({placeholders})
            GROUP BY i.SessionID
        """, session_ids)

        return {row['SessionID']: {
            'MinInvoiceID': row['MinInvoiceID'],
            'MaxInvoiceID': row['MaxInvoiceID'],
            'InvoiceCount': row['InvoiceCount']
        } for row in cursor.fetchall()}

    def _query_all_session_payments(self, cursor, session_ids):
        """Fetch payment details for ALL sessions in one query

        Performance optimization: Instead of N queries, we execute 1 query.

        # ============================================================================
        # PAYMENT AMOUNT SOURCE - Using ActualAmount (terminal counted)
        # This may be changed in the future to use PCAmount (system calculated)
        # ============================================================================

        Args:
            cursor: Database cursor
            session_ids: List of session IDs

        Returns:
            Dict mapping SessionID to list of payment records
        """
        if not session_ids:
            return {}

        placeholders = ','.join(['%s'] * len(session_ids))
        cursor.execute(f"""
            SELECT
                ca.SessionID,
                cad.PaymentType,
                pt.PaymentType AS PaymentMethodName,
                ROUND(cad.ActualAmount, 2) AS Amount
            FROM [dbo].[tblCashierActivityDetail] cad
            INNER JOIN [dbo].[tblCashierActivity] ca ON cad.SessionID = ca.SessionID
            LEFT JOIN [dbo].[tblPaymentType] pt ON cad.PaymentType = pt.PaymentTypeID
            WHERE ca.SessionID IN ({placeholders})
              AND cad.ActualAmount > 0
              AND cad.PaymentType != 5
            ORDER BY ca.SessionID, cad.PaymentType
        """, session_ids)

        # Group results by SessionID
        results = {}
        for row in cursor.fetchall():
            session_id = row['SessionID']
            if session_id not in results:
                results[session_id] = []
            results[session_id].append(row)

        return results

    def _query_all_session_return_details(self, cursor, session_ids):
        """Fetch detailed CRA return vouchers with ZATCA product lines for all sessions.

        Joins tblCashierActivityReturnAmount → tblInvoiceReturnCode →
        tblZatcaCreditNote → tblZatcaCreditNoteDetail to get per-voucher
        product details when available.

        Args:
            cursor: Database cursor
            session_ids: List of session IDs

        Returns:
            Dict mapping SessionID to list of voucher dicts, each with:
            - ReturnCode, ReturnAmount (positive), OriginalInvoiceID,
              ReturnDate, ReturnReceiptID
            - detail_lines: list of {ItemID, ItemName, Quantity, UnitPrice,
              SubTotal, TaxAmount, TaxPercent} (may be empty)
        """
        if not session_ids:
            return {}

        placeholders = ','.join(['%s'] * len(session_ids))
        cursor.execute(f"""
            SELECT
                cra.ReturnCode, cra.SessionID,
                ROUND(cra.ReturnAmount, 2) AS ReturnAmount,
                cra.ReturnDate,
                rc.ReturnInvoiceID AS OriginalInvoiceID,
                rc.InvoiceID AS ReturnReceiptID,
                zd.ItemID, zd.ItemName, zd.Quantity, zd.UnitPrice,
                zd.SubTotal, zd.TaxAmount, zd.TaxPercent
            FROM [dbo].[tblCashierActivityReturnAmount] cra
            JOIN [dbo].[tblInvoiceReturnCode] rc ON cra.ReturnCode = rc.ReturnCode
            LEFT JOIN [dbo].[tblZatcaCreditNote] zcn ON rc.InvoiceID = zcn.InvoiceID
            LEFT JOIN [dbo].[tblZatcaCreditNoteDetail] zd ON zcn.InvoiceID = zd.InvoiceID
            WHERE cra.SessionID IN ({placeholders})
            ORDER BY cra.SessionID, cra.ReturnCode, zd.LineNumber
        """, session_ids)

        raw_vouchers = {}
        for row in cursor.fetchall():
            session_id = row['SessionID']
            return_code = row['ReturnCode']

            session_data = raw_vouchers.setdefault(session_id, {})
            if return_code not in session_data:
                session_data[return_code] = {
                    'ReturnCode': return_code,
                    'ReturnAmount': float(row['ReturnAmount']),
                    'OriginalInvoiceID': row['OriginalInvoiceID'],
                    'ReturnReceiptID': row['ReturnReceiptID'],
                    'ReturnDate': str(row['ReturnDate'])[:10] if row['ReturnDate'] else None,
                    'detail_lines': [],
                }

            if row.get('ItemID'):
                session_data[return_code]['detail_lines'].append({
                    'ItemID': row['ItemID'],
                    'ItemName': row['ItemName'],
                    'Quantity': float(row['Quantity']) if row['Quantity'] else 0,
                    'UnitPrice': float(row['UnitPrice']) if row['UnitPrice'] else 0,
                    'SubTotal': float(row['SubTotal']) if row['SubTotal'] else 0,
                    'TaxAmount': float(row['TaxAmount']) if row['TaxAmount'] else 0,
                    'TaxPercent': float(row['TaxPercent']) if row['TaxPercent'] else 0,
                })

        return {sid: list(vouchers.values()) for sid, vouchers in raw_vouchers.items()}

    def _query_all_session_pt5_returns(self, cursor, session_ids):
        """Fetch PaymentType=5 (return slip) amounts for all sessions.

        PT5 can be:
        - Negative: actual return happening (money going out to customer)
        - Positive: customer redeeming a return voucher as payment

        Args:
            cursor: Database cursor
            session_ids: List of session IDs

        Returns:
            Dict mapping SessionID to PT5 amount (negative=return, positive=voucher redemption)
        """
        if not session_ids:
            return {}

        placeholders = ','.join(['%s'] * len(session_ids))
        cursor.execute(f"""
            SELECT
                cad.SessionID,
                ROUND(SUM(cad.PCAmount), 2) AS PT5Amount
            FROM [dbo].[tblCashierActivityDetail] cad
            WHERE cad.SessionID IN ({placeholders})
              AND cad.PaymentType = 5
              AND cad.PCAmount != 0
            GROUP BY cad.SessionID
        """, session_ids)

        return {row['SessionID']: float(row['PT5Amount']) for row in cursor.fetchall()}

    def _query_all_session_credit_sales(self, cursor, session_ids):
        """Fetch credit sale invoices (unpaid) for all sessions in one query.

        A credit sale is an invoice where IsReturned=0, NetTotal > 0, but all
        payment method columns (CashAmount, SpanAmount, CreditCardAmount,
        VisaAmount, MasterCard, CheckAmount, ReturnAmount, ReturnSlip) are
        zero — the customer took products without paying.

        Args:
            cursor: Database cursor
            session_ids: List of session IDs

        Returns:
            Dict mapping SessionID to dict with:
            - 'total': sum of unpaid invoice NetTotals
            - 'invoices': list of {InvoiceID, CustomerName, PhoneNo, NetTotal,
              products: [{ItemID, ItemName, Quantity, UnitPrice, SubTotal}]}
        """
        if not session_ids:
            return {}

        placeholders = ','.join(['%s'] * len(session_ids))
        cursor.execute(f"""
            SELECT
                i.SessionID,
                i.InvoiceID,
                i.CustomerName,
                i.PhoneNo,
                ROUND(i.NetTotal, 2) AS NetTotal,
                id.ItemID,
                id.ItemName,
                id.Quantity,
                id.UnitPrice,
                ROUND(id.SubTotal, 2) AS SubTotal
            FROM [dbo].[tblInvoice] i
            INNER JOIN [dbo].[tblInvoiceDetail] id ON i.InvoiceID = id.InvoiceID
            WHERE i.SessionID IN ({placeholders})
              AND i.IsReturned = 0
              AND i.NetTotal > 0
              AND ISNULL(i.CashAmount, 0) = 0
              AND ISNULL(i.SpanAmount, 0) = 0
              AND ISNULL(i.CreditCardAmount, 0) = 0
              AND ISNULL(i.VisaAmount, 0) = 0
              AND ISNULL(i.MasterCard, 0) = 0
              AND ISNULL(i.CheckAmount, 0) = 0
              AND ISNULL(i.ReturnAmount, 0) = 0
              AND ISNULL(i.ReturnSlip, 0) = 0
            ORDER BY i.SessionID, i.InvoiceID
        """, session_ids)

        # Group by SessionID → InvoiceID → detail lines
        raw = {}
        for row in cursor.fetchall():
            session_id = row['SessionID']
            invoice_id = row['InvoiceID']

            session_data = raw.setdefault(session_id, {})
            if invoice_id not in session_data:
                session_data[invoice_id] = {
                    'InvoiceID': invoice_id,
                    'CustomerName': row['CustomerName'],
                    'PhoneNo': row['PhoneNo'],
                    'NetTotal': float(row['NetTotal']),
                    'products': [],
                }

            if row.get('ItemID'):
                session_data[invoice_id]['products'].append({
                    'ItemID': row['ItemID'],
                    'ItemName': row['ItemName'],
                    'Quantity': float(row['Quantity']) if row['Quantity'] else 0,
                    'UnitPrice': float(row['UnitPrice']) if row['UnitPrice'] else 0,
                    'SubTotal': float(row['SubTotal']) if row['SubTotal'] else 0,
                })

        # Build final structure with total per session
        results = {}
        for session_id, invoices_dict in raw.items():
            invoices_list = list(invoices_dict.values())
            results[session_id] = {
                'total': round(sum(inv['NetTotal'] for inv in invoices_list), 2),
                'invoices': invoices_list,
            }

        return results

    def _query_original_invoice_sessions(self, cursor, original_invoice_ids):
        """Map MSSQL InvoiceIDs to their SessionIDs.

        Used to find the Odoo invoice for the original sale so we can
        set reversed_entry_id on the credit note.

        Args:
            cursor: Database cursor
            original_invoice_ids: List of MSSQL InvoiceIDs

        Returns:
            Dict mapping InvoiceID to SessionID
        """
        if not original_invoice_ids:
            return {}

        placeholders = ','.join(['%s'] * len(original_invoice_ids))
        cursor.execute(f"""
            SELECT InvoiceID, SessionID
            FROM [dbo].[tblInvoice]
            WHERE InvoiceID IN ({placeholders})
        """, original_invoice_ids)

        return {row['InvoiceID']: row['SessionID'] for row in cursor.fetchall()}

    def _query_session_lines(self, cursor, session_id):
        """Fetch aggregated invoice lines for a specific session

        Groups all invoice lines by ItemID within the session, calculating
        totals for quantity, price average, subtotal, and discounts.

        Args:
            cursor: Database cursor
            session_id: The POS session ID

        Returns:
            List of aggregated line items for the session
        """
        cursor.execute("""
            SELECT
                id.ItemID,
                id.ItemName,
                id.EnglishName,
                id.BarCode,
                id.UnitName,
                ROUND(SUM(id.Quantity), 2) AS TotalQuantity,
                COUNT(*) AS TimesSold,
                AVG(id.UnitPrice) AS AvgPrice,
                ROUND(SUM(id.SubTotal), 2) AS SubTotal,
                ROUND(SUM(id.LineDiscount), 2) AS TotalDiscount
            FROM [dbo].[tblInvoice] i
            INNER JOIN [dbo].[tblInvoiceDetail] id ON i.InvoiceID = id.InvoiceID
            WHERE i.SessionID = %s
              AND i.IsReturned = 0
            GROUP BY
                id.ItemID,
                id.ItemName,
                id.EnglishName,
                id.BarCode,
                id.UnitName
        """, (session_id,))
        return cursor.fetchall()

    def _query_session_return_amount(self, cursor, session_id):
        """Fetch total return amount for a session

        Queries tblCashierActivityReturnAmount to get the sum of all returns
        for a session. Returns a negative amount for use as a line item.

        Args:
            cursor: Database cursor
            session_id: The POS session ID

        Returns:
            Single record with ReturnAmount (negative) or None if no returns
        """
        cursor.execute("""
            SELECT
                -ROUND(SUM(r.ReturnAmount), 2) AS ReturnAmount
            FROM [dbo].[tblCashierActivityReturnAmount] r
            WHERE r.SessionID = %s
        """, (session_id,))
        result = cursor.fetchone()
        return result

    def _query_session_payments(self, cursor, session_id):
        """Fetch payment details for a session

        Queries tblCashierActivityDetail for all payment methods used in
        the session with their amounts.

        # ============================================================================
        # PAYMENT AMOUNT SOURCE - Using ActualAmount (terminal counted)
        # This may be changed in the future to use PCAmount (system calculated)
        #
        # ActualAmount = What the cashier counted/terminal reported
        # PCAmount = What the POS system calculated
        # ============================================================================

        Args:
            cursor: Database cursor
            session_id: The POS session ID

        Returns:
            List of payment records with PaymentType, PaymentMethodName, and Amount
        """
        cursor.execute("""
            SELECT
                cad.PaymentType,
                pt.PaymentType AS PaymentMethodName,
                ROUND(cad.ActualAmount, 2) AS Amount
            FROM [dbo].[tblCashierActivityDetail] cad
            INNER JOIN [dbo].[tblCashierActivity] ca ON cad.SessionID = ca.SessionID
            LEFT JOIN [dbo].[tblPaymentType] pt ON cad.PaymentType = pt.PaymentTypeID
            WHERE ca.SessionID = %s
              AND cad.ActualAmount > 0
        """, (session_id,))
        return cursor.fetchall()
