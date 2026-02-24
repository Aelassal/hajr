from odoo import models, fields
from odoo.exceptions import UserError
from datetime import timedelta, datetime
import json
import logging

_logger = logging.getLogger(__name__)


class MssqlSyncPurchase(models.Model):
    _inherit = 'mssql.sync'

    # ── Purchase Fields ───────────────────────────────────────────────
    purchase_invoice_date = fields.Date(string='Purchase Invoice Date', default=fields.Date.today)

    # ── Purchase Invoice Sync ─────────────────────────────────────────

    def sync_purchase_invoices(self):
        """Sync purchase invoices from SQL Server via queue.

        Fetches all purchase invoices for the selected date, creates a sync
        queue with one line per invoice, and processes the queue. Failed
        invoices can be retried from the queue view.
        """
        if not self.purchase_invoice_date:
            raise UserError('Please select a purchase invoice date')

        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            date_str = self.purchase_invoice_date.strftime('%Y-%m-%d')
            next_date = (self.purchase_invoice_date + timedelta(days=1)).strftime('%Y-%m-%d')

            # Fetch purchase invoices for the selected date
            purchase_invoices = self._query_purchase_invoices(cursor, date_str, next_date)

            if not purchase_invoices:
                conn.close()
                raise UserError(f'No purchase invoices found for date {date_str}')

            # Fetch all invoice details in one query
            invoice_ids = [pi['PurchaseInvoiceID'] for pi in purchase_invoices]
            invoice_details = self._query_purchase_invoice_details(cursor, invoice_ids)
            conn.close()

            # Group details by invoice
            details_by_invoice = {}
            for detail in invoice_details:
                inv_id = detail['PurchaseInvoiceID']
                if inv_id not in details_by_invoice:
                    details_by_invoice[inv_id] = []
                details_by_invoice[inv_id].append(detail)

            # Pre-sync vendors if needed
            supplier_ids = list(set([pi['SupplierID'] for pi in purchase_invoices if pi['SupplierID']]))
            vendors = {
                p.x_sql_vendor_id: p for p in self.env['res.partner'].search([
                    ('x_sql_vendor_id', 'in', supplier_ids),
                    ('supplier_rank', '>', 0)
                ])
            }
            missing_supplier_ids = [sid for sid in supplier_ids if sid not in vendors]
            if missing_supplier_ids:
                _logger.info(f"Missing vendors detected. Auto-syncing...")
                try:
                    self.sync_vendors()
                except Exception as e:
                    _logger.error(f"Failed to auto-sync vendors: {str(e)}")

            # Pre-create warehouses
            branch_ids = list(set([pi['BranchID'] for pi in purchase_invoices if pi['BranchID']]))
            if branch_ids:
                self._get_or_create_warehouses(branch_ids)

            # Create queue
            queue = self.env['mssql.sync.queue'].create({
                'sync_config_id': self.id,
                'sync_type': 'purchase_invoice',
                'sync_date': self.purchase_invoice_date,
            })

            # Check for existing POs (idempotency)
            existing_origins = set(
                self.env['purchase.order'].search([
                    ('origin', '!=', False),
                ]).mapped('origin')
            )

            line_vals_list = []
            for pi in purchase_invoices:
                inv_id = pi['PurchaseInvoiceID']

                # Skip already-synced
                if any(str(inv_id) in origin for origin in existing_origins):
                    continue

                details = details_by_invoice.get(inv_id, [])
                if not details:
                    continue

                record_data = json.dumps({
                    'invoice': dict(pi),
                    'details': [dict(d) for d in details],
                }, default=str)

                supplier_name = pi.get('SupplierName') or f"Supplier {pi['SupplierID']}"
                line_vals_list.append({
                    'queue_id': queue.id,
                    'name': f"PI {inv_id} - {supplier_name}",
                    'mssql_id': str(inv_id),
                    'mssql_table': 'tblPurchaseInvoice',
                    'record_data': record_data,
                })

            if not line_vals_list:
                queue.unlink()
                return self._success_notification(
                    'Purchase Sync', 'No new purchase invoices to process')

            self.env['mssql.sync.queue.line'].create(line_vals_list)
            _logger.info(f"Created queue {queue.name} with {len(line_vals_list)} lines")

            # Process queue
            queue.action_process_queue()

            return {
                'type': 'ir.actions.act_window',
                'name': f'Purchase Queue - {self.purchase_invoice_date}',
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
            raise UserError(f'Purchase invoice sync failed: {str(e)}')

    def _process_queue_purchase_invoice(self, data, queue_line):
        """Process a single purchase invoice from queue line data.

        Called by the queue line dispatcher. Creates PO -> Picking -> Bill -> Payment.

        Args:
            data: dict parsed from queue line's record_data JSON
            queue_line: mssql.sync.queue.line record

        Returns:
            dict with 'model' and 'id' of created bill
        """
        purchase_invoice = data['invoice']
        invoice_lines = data['details']

        # Coerce numeric fields that survive JSON round-trip as strings
        invoice_id = purchase_invoice['PurchaseInvoiceID']
        supplier_id = int(purchase_invoice['SupplierID'])
        branch_id = int(purchase_invoice['BranchID'])

        for line in invoice_lines:
            for key in ('ItemID',):
                if key in line and line[key] is not None:
                    try:
                        line[key] = int(line[key])
                    except (ValueError, TypeError):
                        pass
            for key in ('Quantity', 'UnitPrice', 'LineDiscount', 'SubTotal',
                        'SubNetTotal', 'CostPrice', 'LineTax',
                        'RecivedQuantity'):
                if key in line and line[key] is not None:
                    try:
                        line[key] = float(line[key])
                    except (ValueError, TypeError):
                        pass

        # Get vendor
        vendor = self.env['res.partner'].search([
            ('x_sql_vendor_id', '=', supplier_id),
            ('supplier_rank', '>', 0)
        ], limit=1)
        if not vendor:
            raise UserError(
                f'Vendor (SupplierID: {supplier_id}) not found. '
                f'Please run "Sync Vendors" first.')

        # Get warehouse
        warehouse = self.env['stock.warehouse'].search([
            ('x_sql_branch_id', '=', branch_id),
            ('company_id', '=', self.env.company.id),
        ], limit=1)
        if not warehouse:
            raise UserError(
                f'Warehouse for BranchID={branch_id} not found.')

        # Get products
        item_ids = list(set([d['ItemID'] for d in invoice_lines if d['ItemID']]))
        products = {
            p.x_sql_item_id: p for p in self.env['product.product'].search([
                ('x_sql_item_id', 'in', item_ids)
            ])
        }

        # Create PO lines
        po_lines = []
        for line in invoice_lines:
            item_id = line['ItemID']
            product = products.get(item_id)

            if not product:
                product = self.env['product.product'].create({
                    'name': line['ItemName'] or line['EnglishName'] or f"Item {item_id}",
                    'x_sql_item_id': item_id,
                    'type': 'consu',
                    'is_storable': True,
                })
                products[item_id] = product

            quantity = float(line['Quantity'] or 0.0)
            original_price = float(line['UnitPrice'] or 0.0)
            line_discount = float(line['LineDiscount'] or 0.0)
            subtotal = float(line['SubTotal'] or 0.0)

            if quantity == 0:
                continue

            if quantity != 0 and subtotal != 0:
                price_unit = subtotal / quantity
            else:
                price_unit = original_price

            discount_pct = 0.0
            if abs(subtotal) > 0 and abs(line_discount) > 0:
                discount_pct = round((abs(line_discount) / abs(subtotal)) * 100, 2)

            # Parse invoice date
            inv_date = purchase_invoice['InvoiceDate']
            if isinstance(inv_date, str):
                inv_date = inv_date[:10]

            po_lines.append((0, 0, {
                'product_id': product.id,
                'product_qty': quantity,
                'price_unit': price_unit,
                'discount': discount_pct,
                'name': line['ItemName'] or line['EnglishName'] or product.name,
                'date_planned': inv_date,
            }))

        if not po_lines:
            raise UserError(f'No valid lines for Purchase Invoice {invoice_id}')

        inv_date = purchase_invoice['InvoiceDate']
        if isinstance(inv_date, str):
            inv_date = inv_date[:10]

        po_vals = {
            'partner_id': vendor.id,
            'date_order': inv_date,
            'order_line': po_lines,
            'notes': purchase_invoice.get('InvoiceNote', ''),
        }

        if warehouse and warehouse.in_type_id:
            po_vals['picking_type_id'] = warehouse.in_type_id.id

        po = self.env['purchase.order'].create(po_vals)

        # Decimal adjustment
        mssql_net_total = round(float(purchase_invoice['NetTotal'] or 0), 2)
        decimal_product = self._get_or_create_decimal_product()
        tax_15 = self.env['account.tax'].search([
            ('amount', '=', 15),
            ('type_tax_use', '=', 'purchase'),
            ('company_id', '=', self.env.company.id)
        ], limit=1)

        for attempt in range(1, 6):
            po = self.env['purchase.order'].browse(po.id)
            po_total = round(po.amount_total, 2)
            difference = round(mssql_net_total - po_total, 2)

            if abs(difference) < 0.01:
                break

            pre_tax_adj = difference / 1.15
            decimal_line = po.order_line.filtered(
                lambda l: l.product_id.id == decimal_product.id)

            if decimal_line:
                decimal_line.write({'price_unit': decimal_line.price_unit + pre_tax_adj})
            else:
                self.env['purchase.order.line'].create({
                    'order_id': po.id,
                    'product_id': decimal_product.id,
                    'product_qty': 1,
                    'price_unit': pre_tax_adj,
                    'name': 'Decimal Adjustment',
                    'date_planned': inv_date,
                    'taxes_id': [(6, 0, [tax_15.id])] if tax_15 else [],
                })

        # Confirm PO
        po.button_confirm()

        # Validate all pickings (IN for positive lines, OUT for return lines)
        for picking in po.picking_ids:
            for move in picking.move_ids_without_package:
                if move.quantity != move.product_uom_qty:
                    move.quantity = move.product_uom_qty
            picking.button_validate()

        # Create bill
        bill_result = po.action_create_invoice()
        bill_record = None

        if bill_result and 'res_id' in bill_result:
            bill_record = self.env['account.move'].browse(bill_result['res_id'])
        elif isinstance(bill_result, dict) and 'domain' in bill_result:
            bills = self.env['account.move'].search(bill_result['domain'])
            bill_record = bills[0] if bills else None

        if bill_record:
            inv_due_date = purchase_invoice.get('InvoiceDueDate') or inv_date
            if isinstance(inv_due_date, str):
                inv_due_date = inv_due_date[:10]

            bill_record.write({
                'invoice_date': inv_date,
                'invoice_date_due': inv_due_date,
                'ref': purchase_invoice.get('SupplierInvoiceID') or f"PI-{invoice_id}",
                'narration': purchase_invoice.get('InvoiceNote', ''),
            })

            # Update bill lines with discounts
            for bill_line in bill_record.invoice_line_ids:
                po_line_ref = bill_line.purchase_line_id
                if po_line_ref:
                    for line in invoice_lines:
                        if line['ItemID'] == po_line_ref.product_id.x_sql_item_id:
                            ld = float(line['LineDiscount'] or 0.0)
                            lq = float(line['Quantity'] or 0.0)
                            lp = float(line['UnitPrice'] or 0.0)
                            dpct = 0.0
                            if lq > 0 and lp > 0:
                                dpct = (ld / (lq * lp)) * 100
                            bill_line.write({'discount': dpct})
                            break

            # Register vendor payments
            self._register_vendor_payments(bill_record, invoice_id, purchase_invoice)

            _logger.info(f"Purchase Invoice {invoice_id}: PO {po.name}, "
                         f"Bill {bill_record.name} created")
            return {'model': 'account.move', 'id': bill_record.id}

        _logger.warning(f"Purchase Invoice {invoice_id}: PO {po.name} created but no bill")
        return {'model': 'purchase.order', 'id': po.id}

    def _get_or_create_warehouses(self, branch_ids):
        """Get or create warehouses from branch IDs.

        Creates warehouses with MSSQL branch names and renames the
        auto-generated view location to match the branch name
        (Odoo defaults the view location name to the warehouse code).
        """
        if not branch_ids:
            return {}

        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            branches = self._query_branches(cursor, branch_ids)
            conn.close()

            warehouse_obj = self.env['stock.warehouse']
            branch_warehouse_map = {}
            company = self.env.company

            for branch in branches:
                branch_id = branch['BranchID']
                branch_name = branch['BranchName'] or f"Branch {branch_id}"

                # Search for existing warehouse by SQL BranchID
                warehouse = warehouse_obj.search([
                    ('x_sql_branch_id', '=', branch_id),
                    ('company_id', '=', company.id)
                ], limit=1)

                if not warehouse:
                    # Create warehouse — Odoo auto-creates view location named after code
                    warehouse = warehouse_obj.create({
                        'name': branch_name,
                        'code': f"BR{branch_id}",
                        'company_id': company.id,
                        'x_sql_branch_id': branch_id,
                    })
                    # Rename view location from code (e.g. "BR1") to branch name
                    if warehouse.view_location_id:
                        warehouse.view_location_id.name = branch_name
                    # Link picking types (Odoo creates them but may not link all)
                    self._link_warehouse_picking_types(warehouse)
                    _logger.info(f"Created warehouse '{branch_name}' (BR{branch_id})")
                else:
                    # Update warehouse name if changed in MSSQL
                    if warehouse.name != branch_name:
                        warehouse.name = branch_name
                    # Ensure view location also has the branch name
                    if warehouse.view_location_id and warehouse.view_location_id.name != branch_name:
                        warehouse.view_location_id.name = branch_name

                branch_warehouse_map[branch_id] = warehouse

            return branch_warehouse_map

        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise UserError(f'Failed to get/create warehouses: {str(e)}')

    def _link_warehouse_picking_types(self, warehouse):
        """Ensure all picking type fields are linked on a warehouse.

        Odoo creates the picking types when a warehouse is created, but
        sometimes fails to link them back to the warehouse record.
        """
        picking_type_obj = self.env['stock.picking.type']
        wh_types = picking_type_obj.search([('warehouse_id', '=', warehouse.id)])

        type_map = {}
        for pt in wh_types:
            # Use raw name text to identify type
            name_str = str(pt.name)
            if 'Internal Transfers' in name_str and pt.code == 'internal':
                type_map['int_type_id'] = pt.id
            elif 'Receipts' in name_str and pt.code == 'incoming':
                type_map['in_type_id'] = pt.id
            elif 'Delivery Orders' in name_str and pt.code == 'outgoing':
                type_map['out_type_id'] = pt.id
            elif 'Pick' in name_str and pt.code == 'internal':
                type_map['pick_type_id'] = pt.id
            elif 'Pack' in name_str and pt.code == 'internal':
                type_map['pack_type_id'] = pt.id
            elif 'Quality Control' in name_str and pt.code == 'internal':
                type_map['qc_type_id'] = pt.id

        vals = {k: v for k, v in type_map.items() if not getattr(warehouse, k, None)}
        if vals:
            warehouse.write(vals)
            _logger.info(f"Linked picking types for warehouse {warehouse.name}: {list(vals.keys())}")

    # ── Vendor Payment Registration ───────────────────────────────────

    def _register_vendor_payments(self, bill, purchase_invoice_id, purchase_invoice_data):
        """Register vendor payments using OPTIMIZED wizard batch approach

        Uses Odoo's native wizard but processes in smart batches:
        - Groups payments by journal + date for batch processing
        - Uses wizard for reliability (no field errors, proper state management)
        - Much faster than one-by-one processing
        - IDENTICAL to customer payment registration logic
        """
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            # Query supplier payments linked to this purchase invoice
            vendor_payments = self._query_vendor_payments(cursor, purchase_invoice_id)
            conn.close()

            _logger.info(f"Fetched {len(vendor_payments)} vendor payment records for Purchase Invoice {purchase_invoice_id} (Bill {bill.id})")

            # Check if invoice is posted in MSSQL
            mssql_posted = purchase_invoice_data.get('Posted', False)
            mssql_closed = purchase_invoice_data.get('Closed', False)
            mssql_paid = purchase_invoice_data.get('Paid', False)

            _logger.info(f"MSSQL Invoice {purchase_invoice_id} status - Posted: {mssql_posted}, Closed: {mssql_closed}, Paid: {mssql_paid}")

            # If invoice is posted in MSSQL, post the bill in Odoo even if no payments found
            if mssql_posted and bill.state == 'draft':
                bill.action_post()
                _logger.info(f"Posted bill {bill.id} because MSSQL invoice {purchase_invoice_id} has Posted=1")

            if not vendor_payments:
                if mssql_posted:
                    _logger.info(f"No vendor payments found for Purchase Invoice {purchase_invoice_id} - Bill {bill.id} posted based on MSSQL Posted status")
                else:
                    _logger.info(f"No vendor payments found for Purchase Invoice {purchase_invoice_id} - Bill {bill.id} remains in '{bill.state}' state (unpaid)")
                return []

            # Ensure bill is posted before registering payments
            if bill.state != 'posted':
                bill.action_post()
                _logger.info(f"Posted bill {bill.id} before registering payments")

            _logger.info(f"Processing {len(vendor_payments)} vendor payment records for bill {bill.id} using OPTIMIZED BATCH wizard")

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

            _logger.info(f"Payment journals - Cash: {cash_journal.name if cash_journal else 'N/A'}, Bank: {bank_journal.name if bank_journal else 'N/A'}")

            # Map payment methods to journals
            # Vendor payment methods: 1=Cash, 2=Check, 3=Bank Transfer, etc.
            payment_method_journal_map = {
                1: cash_journal,      # Cash
                2: bank_journal,      # Check
                3: bank_journal,      # Bank Transfer
            }

            # STEP 1: Prepare and group payments by journal + date for batch processing
            payment_batches = {}  # Key: (journal_id, date_str), Value: list of payment data
            skipped_count = 0

            for payment_data in vendor_payments:
                payment_method = payment_data.get('PaymentMethod') or 1
                net_amount = float(payment_data.get('NetAmount') or 0.0)

                if net_amount <= 0:
                    skipped_count += 1
                    continue

                journal = payment_method_journal_map.get(payment_method, bank_journal or cash_journal)
                if not journal:
                    skipped_count += 1
                    continue

                payment_date = payment_data.get('PaymentDate') or purchase_invoice_data['InvoiceDate']

                # Build payment reference
                payment_ref = f"Vendor Payment - PI#{purchase_invoice_data.get('SupplierInvoiceID', purchase_invoice_id)}"

                check_no = payment_data.get('CheckNo')
                if check_no:
                    payment_ref += f" - Check: {check_no}"

                payment_note = payment_data.get('PaymentNote') or payment_data.get('InvoicePaymentNote')
                if payment_note:
                    payment_ref += f" - {payment_note}"

                # Group by journal and DATE ONLY (not timestamp!) for efficient batching
                # Convert datetime to date string for grouping
                if isinstance(payment_date, datetime):
                    date_key = payment_date.date()
                else:
                    date_key = payment_date

                batch_key = (journal.id, str(date_key))
                if batch_key not in payment_batches:
                    payment_batches[batch_key] = []

                payment_batches[batch_key].append({
                        'amount': net_amount,
                        'date': payment_date,
                        'journal_id': journal.id,
                    'communication': payment_ref,
                    'payment_id': payment_data.get('PaymentID'),
                })

            if not payment_batches:
                _logger.warning(f"No valid vendor payments to create for bill {bill.id} (skipped: {skipped_count})")
                return []

            total_payments = sum(len(batch) for batch in payment_batches.values())
            _logger.info(f"Grouped {total_payments} vendor payments into {len(payment_batches)} batches by journal+date (skipped: {skipped_count})")

            # STEP 2: Process each batch using wizard (MUCH faster than individual)
            all_payment_ids = []
            batch_num = 0

            for batch_key, batch_payments in payment_batches.items():
                batch_num += 1
                journal_id, date_str = batch_key

                _logger.info(f"Processing vendor payment batch {batch_num}/{len(payment_batches)}: {len(batch_payments)} payments for journal {journal_id} on {date_str}")

                # Check if bill still has amount to pay
                bill = self.env['account.move'].browse(bill.id)  # Refresh
                if bill.amount_residual <= 0:
                    _logger.info(f"Bill {bill.id} fully paid (residual: {bill.amount_residual}), stopping batch processing")
                    break

                # Process each payment in batch (still using wizard for reliability)
                batch_created = 0
                for payment_vals in batch_payments:
                    try:
                        # Check again before each payment
                        bill = self.env['account.move'].browse(bill.id)
                        if bill.amount_residual <= 0:
                            _logger.info(f"Bill {bill.id} fully paid, stopping at {batch_created}/{len(batch_payments)} in batch {batch_num}")
                            break

                        # Use wizard (reliable, handles all edge cases)
                        payment_register = self.env['account.payment.register'].with_context(
                            active_model='account.move',
                            active_ids=bill.ids,
                            dont_redirect_to_payments=True
                        ).create({
                            'payment_date': payment_vals['date'],
                            'journal_id': payment_vals['journal_id'],
                            'amount': payment_vals['amount'],
                            'communication': payment_vals['communication'],
                            'group_payment': False,
                        })

                        # Create payment
                        payment_register.action_create_payments()

                        # Find created payment
                        recent_payment = self.env['account.payment'].search([
                            ('partner_id', '=', bill.partner_id.id),
                            ('amount', '=', payment_vals['amount']),
                            ('journal_id', '=', payment_vals['journal_id']),
                            ('date', '=', payment_vals['date']),
                            ('payment_type', '=', 'outbound'),
                        ], order='id desc', limit=1)

                        if recent_payment:
                            all_payment_ids.append(recent_payment.id)
                            batch_created += 1

                            # Log every 10 payments in batch
                            if batch_created % 10 == 0:
                                _logger.info(f"Vendor batch {batch_num} progress: {batch_created}/{len(batch_payments)} payments created")

                    except Exception as e:
                        error_msg = str(e)
                        if 'nothing left to pay' in error_msg.lower():
                            _logger.info(f"Bill {bill.id} fully paid during vendor batch {batch_num}, stopping")
                            break
                        else:
                            _logger.warning(f"Failed to create vendor payment in batch {batch_num}: {error_msg}")
                            continue

                _logger.info(f"Vendor batch {batch_num} complete: {batch_created}/{len(batch_payments)} payments created")

            # Refresh bill for final status
            bill = self.env['account.move'].browse(bill.id)
            _logger.info(f"Vendor payment registration complete: {len(all_payment_ids)} payments created, Bill residual: {bill.amount_residual}")

            return all_payment_ids

        except Exception as e:
            try:
                conn.close()
            except:
                pass
            _logger.error(f"Vendor payment registration failed for bill {bill.id}: {str(e)}")
            # Don't raise error, just skip payment registration
            return []

    # ── SQL Queries ───────────────────────────────────────────────────

    def _query_purchase_invoices(self, cursor, date_str, next_date):
        """Fetch purchase invoices from MSSQL for date range"""
        cursor.execute("""
            SELECT
                pi.PurchaseInvoiceID,
                pi.SupplierInvoiceID,
                pi.SupplierID,
                pi.BranchID,
                pi.InvoiceDate,
                pi.InvoiceDueDate,
                pi.InvoiceTotal,
                pi.NetTotal,
                pi.TaxAmount,
                pi.Discount,
                pi.PaidAmount,
                pi.DueAmount,
                pi.InvoiceNote,
                pi.IsReturn,
                pi.Posted,
                pi.PostedDate,
                pi.Closed,
                pi.Paid,
                s.SupplierName
            FROM [dbo].[tblPurchaseInvoice] pi
            LEFT JOIN [dbo].[tblSuppliers] s ON pi.SupplierID = s.SupplierID
            WHERE pi.InvoiceDate >= %s
               AND pi.InvoiceDate < %s
               AND pi.IsReturn = 0
            ORDER BY pi.InvoiceDate DESC, pi.PurchaseInvoiceID
        """, (date_str, next_date))
        return cursor.fetchall()

    def _query_purchase_invoice_details(self, cursor, invoice_ids):
        """Fetch purchase invoice details for given invoice IDs"""
        placeholders = ','.join(['%s'] * len(invoice_ids))
        cursor.execute(f"""
            SELECT
                pid.PurchaseInvoiceID,
                pid.PurchaseInvoiceDetailID,
                pid.ItemID,
                pid.ItemName,
                pid.EnglishName,
                pid.Quantity,
                pid.RecivedQuantity,
                pid.UnitPrice,
                pid.SubTotal,
                pid.TaxAmount as LineTax,
                pid.LineDiscount,
                pid.SubNetTotal,
                pid.CostPrice,
                pid.LineStatus
            FROM [dbo].[tblPurchaseInvoiceDetail] pid
            WHERE pid.PurchaseInvoiceID IN ({placeholders})
            ORDER BY pid.PurchaseInvoiceID, pid.PurchaseInvoiceDetailID
        """, invoice_ids)
        return cursor.fetchall()

    def _query_branches(self, cursor, branch_ids):
        """Fetch branches from MSSQL for given branch IDs"""
        placeholders = ','.join(['%s'] * len(branch_ids))
        cursor.execute(f"""
            SELECT
                BranchID,
                BranchName,
                BranchLocation
            FROM [dbo].[tblBranch]
            WHERE BranchID IN ({placeholders})
        """, branch_ids)
        return cursor.fetchall()

    def _query_vendor_payments(self, cursor, purchase_invoice_id):
        """Fetch vendor payments for a purchase invoice"""
        cursor.execute("""
            SELECT
                sp.PaymentID,
                sp.PaymentDate,
                sp.PaymentAmount,
                sp.DebitAmount,
                sp.CreditAmount,
                sp.PaymentMethod,
                sp.CheckNo,
                sp.CheckDate,
                sp.PaymentNote,
                sp.Posted,
                sp.PayDiscount,
                spi.NetAmount,
                spi.SupplierInvoiceID,
                spi.InvoicePaymentNote
            FROM [dbo].[tblSuppliersPayment] sp
            INNER JOIN [dbo].[tblSuppliersPaymentInvoice] spi
                ON sp.PaymentID = spi.PaymentID
            WHERE spi.PurchaseInvoiceID = %s
            ORDER BY sp.PaymentDate, sp.PaymentID
        """, (purchase_invoice_id,))
        return cursor.fetchall()
