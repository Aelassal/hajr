from odoo import models, fields
from odoo.exceptions import UserError
import logging

_logger = logging.getLogger(__name__)


class MssqlSyncProduct(models.Model):
    _inherit = 'mssql.sync'

    # ── Product Tracking Fields ─────────────────────────────────────────

    products_migrated_to_storable = fields.Boolean(
        string='Products Migrated to Storable', default=False)
    last_product_sync_date = fields.Datetime(
        string='Last Product Sync Date',
        help='Watermark for new product detection')

    # ── Product Sync ────────────────────────────────────────────────────

    def sync_products(self):
        """Fetch products from SQL Server and create/update in Odoo - Optimized with change detection"""
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            # Fetch products with current prices
            sql_products = self._query_products_with_prices(cursor)
            conn.close()

            if not sql_products:
                _logger.info('No products found in SQL Server')
                return self._success_notification('Product Sync Complete', 'No products found')

            # Deduplicate by ItemID to ensure one record per product
            seen_item_ids = set()
            unique_products = []
            for item in sql_products:
                item_id = item['ItemID']
                if item_id and item_id not in seen_item_ids:
                    seen_item_ids.add(item_id)
                    unique_products.append(item)

            sql_products = unique_products

            product_obj = self.env['product.product']

            # Get all ItemIDs for batch lookup
            item_ids = [item['ItemID'] for item in sql_products if item['ItemID']]

            # Fetch all existing products in one query - O(1) lookup
            existing_products = {
                p.x_sql_item_id: p for p in product_obj.search([('x_sql_item_id', 'in', item_ids)])
            }

            # Separate into create and skip batches (no updates)
            to_create = []
            to_update = []
            skipped = 0

            for item in sql_products:
                item_id = item['ItemID']
                if not item_id:
                    continue

                name = item['ItemName'] or item['EnglishName'] or f"Item {item_id}"
                vals = {
                    'name': name,
                    'x_sql_item_id': item_id,
                    'x_english_name': item['EnglishName'],
                    'type': 'consu',
                    'is_storable': True,
                }

                # Add price information if available
                if item.get('PurchasePrice') is not None:
                    vals['standard_price'] = float(item['PurchasePrice'])
                if item.get('SellPrice') is not None:
                    vals['list_price'] = float(item['SellPrice'])

                if item_id in existing_products:
                    # Skip existing products - only add new ones
                    skipped += 1
                    continue
                else:
                    to_create.append(vals)

            # Batch create new products only
            created = 0
            if to_create:
                _logger.info(f"Creating {len(to_create)} new products in batches...")
                batch_size = 1000
                for i in range(0, len(to_create), batch_size):
                    batch = to_create[i:i + batch_size]
                    product_obj.create(batch)
                    created += len(batch)
                    _logger.info(f"Product creation progress: {created}/{len(to_create)}")
                    # Clear cache periodically
                    self.env.clear()

            # Only update flag if new products were created
            if created > 0:
                self.write({'products_fetched': True})

            # Return appropriate message
            if created == 0:
                return self._success_notification('Product Sync Complete', f'No new products found (checked: {len(sql_products)} products, {skipped} already exist)')
            else:
                return self._success_notification('Product Sync Complete', f'Created: {created} new products ({skipped} already existed)')
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise UserError(f'Product sync failed: {str(e)}')

    def update_prices(self):
        """Update product prices from SQL Server - Optimized for large datasets"""
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            # Fetch current prices for all products
            price_data = self._query_current_prices(cursor)
            conn.close()

            if not price_data:
                raise UserError('No price data found in SQL Server')

            product_obj = self.env['product.product']

            # Get all ItemIDs for batch lookup
            item_ids = [item['ItemID'] for item in price_data if item['ItemID']]

            # Fetch all existing products in one query - O(1) lookup
            existing_products = {
                p.x_sql_item_id: p for p in product_obj.search([('x_sql_item_id', 'in', item_ids)])
            }

            # Prepare price updates
            price_updates = {}
            for price_item in price_data:
                item_id = price_item['ItemID']
                if not item_id or item_id not in existing_products:
                    continue

                product = existing_products[item_id]
                if product.id not in price_updates:
                    price_updates[product.id] = {}

                # Update prices if available
                if price_item.get('PurchasePrice') is not None:
                    price_updates[product.id]['standard_price'] = float(price_item['PurchasePrice'])
                if price_item.get('SellPrice') is not None:
                    price_updates[product.id]['list_price'] = float(price_item['SellPrice'])

            # Batch update prices
            updated = 0
            if price_updates:
                batch_size = 1000
                product_ids = list(price_updates.keys())
                for i in range(0, len(product_ids), batch_size):
                    batch_ids = product_ids[i:i + batch_size]
                    batch_products = product_obj.browse(batch_ids)
                    for product in batch_products:
                        if product.id in price_updates:
                            product.write(price_updates[product.id])
                    updated += len(batch_ids)
                    # Clear cache periodically to free memory
                    if i % (batch_size * 10) == 0:
                        self.env.clear()

            return self._success_notification('Price Update Complete', f'Updated prices for {updated} products')
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise UserError(f'Price update failed: {str(e)}')

    # ── New Product Auto-Detection ──────────────────────────────────────

    def sync_new_products(self):
        """Detect and sync products created or modified in MSSQL since last sync."""
        self.ensure_one()
        if not self.last_product_sync_date:
            _logger.info("sync_new_products: No watermark set, skipping")
            return

        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            since = self.last_product_sync_date.strftime('%Y-%m-%d %H:%M:%S')
            new_products = self._query_new_products(cursor, since)
            conn.close()
        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            _logger.error(f"sync_new_products failed: {str(e)}")
            return

        if not new_products:
            self.write({'last_product_sync_date': fields.Datetime.now()})
            return

        # Deduplicate by ItemID
        seen = set()
        unique = []
        for item in new_products:
            iid = item['ItemID']
            if iid and iid not in seen:
                seen.add(iid)
                unique.append(item)

        product_obj = self.env['product.product']
        item_ids = [item['ItemID'] for item in unique if item['ItemID']]
        existing = {
            p.x_sql_item_id: p for p in product_obj.search([
                ('x_sql_item_id', 'in', item_ids)
            ])
        }

        to_create = []
        for item in unique:
            item_id = item['ItemID']
            if not item_id or item_id in existing:
                continue
            name = item['ItemName'] or item['EnglishName'] or f"Item {item_id}"
            vals = {
                'name': name,
                'x_sql_item_id': item_id,
                'x_english_name': item.get('EnglishName'),
                'type': 'consu',
                'is_storable': True,
            }
            if item.get('PurchasePrice') is not None:
                vals['standard_price'] = float(item['PurchasePrice'])
            if item.get('SellPrice') is not None:
                vals['list_price'] = float(item['SellPrice'])
            to_create.append(vals)

        created = 0
        if to_create:
            for i in range(0, len(to_create), 1000):
                batch = to_create[i:i + 1000]
                product_obj.create(batch)
                created += len(batch)
                self.env.clear()

        self.write({'last_product_sync_date': fields.Datetime.now()})
        _logger.info(f"sync_new_products: Created {created} new products")

    # ── Product Migration ───────────────────────────────────────────────

    def action_migrate_products_to_storable(self):
        """Migrate existing consumable products (with x_sql_item_id) to storable.

        Products WITHOUT done stock.move.lines -> ORM write (safe).
        Products WITH done stock.move.lines -> direct SQL (bypasses Odoo constraint).
        """
        product_obj = self.env['product.product']
        # Find all MSSQL-synced products that are NOT storable yet
        non_storable = product_obj.search([
            ('x_sql_item_id', '!=', False),
            ('is_storable', '=', False),
        ])

        if not non_storable:
            self.write({'products_migrated_to_storable': True})
            return self._success_notification(
                'Migration Complete', 'All MSSQL products are already storable.')

        # Split: products with done stock moves vs without
        products_with_moves = self.env['product.product']
        products_without_moves = self.env['product.product']

        if non_storable:
            # Check which products have done stock moves
            self.env.cr.execute("""
                SELECT DISTINCT pp.id
                FROM product_product pp
                JOIN stock_move_line sml ON sml.product_id = pp.id
                WHERE pp.id IN %s AND sml.state = 'done'
            """, (tuple(non_storable.ids),))
            ids_with_moves = {r[0] for r in self.env.cr.fetchall()}

            for p in non_storable:
                if p.id in ids_with_moves:
                    products_with_moves |= p
                else:
                    products_without_moves |= p

        # Safe ORM update for products without done moves
        orm_count = 0
        if products_without_moves:
            products_without_moves.write({'is_storable': True})
            orm_count = len(products_without_moves)

        # Direct SQL for products with done moves (bypasses Odoo constraint)
        sql_count = 0
        if products_with_moves:
            template_ids = products_with_moves.mapped('product_tmpl_id').ids
            self.env.cr.execute("""
                UPDATE product_template SET is_storable = TRUE
                WHERE id IN %s
            """, (tuple(template_ids),))
            sql_count = len(products_with_moves)
            # Invalidate cache for affected records
            self.env['product.template'].invalidate_model(['is_storable'])
            self.env['product.product'].invalidate_model(['is_storable'])

        self.write({'products_migrated_to_storable': True})

        msg = f'Migrated {orm_count + sql_count} products to storable '
        msg += f'({orm_count} via ORM, {sql_count} via SQL bypass)'
        _logger.info(msg)
        return self._success_notification('Migration Complete', msg)

    # ── SQL Queries ─────────────────────────────────────────────────────

    def _query_products_with_prices(self, cursor):
        """Fetch products with current prices from MSSQL"""
        cursor.execute("""
            SELECT
                i.ItemID,
                i.ItemName,
                i.EnglishName,
                i.Tax,
                i.SupplierID,
                i.CatID,
                i.DepartmentID,
                i.VatCat,
                ip.PurchasePrice,
                ip.SellPrice,
                ip.UnitName
            FROM [dbo].[tblItems] i
            LEFT JOIN (
                SELECT
                    ItemID,
                    PurchasePrice,
                    SellPrice,
                    UnitName,
                    ROW_NUMBER() OVER (PARTITION BY ItemID ORDER BY PriceDate DESC, PriceID DESC) as rn
                FROM [dbo].[tblItemsPriceChange]
                WHERE CurrentPrice = 1
            ) ip ON i.ItemID = ip.ItemID AND ip.rn = 1
        """)
        return cursor.fetchall()

    def _query_current_prices(self, cursor):
        """Fetch current prices for all products from MSSQL"""
        cursor.execute("""
            SELECT
                ip.ItemID,
                ip.PurchasePrice,
                ip.SellPrice,
                ip.UnitName
            FROM [dbo].[tblItemsPriceChange] ip
            WHERE ip.CurrentPrice = 1
        """)
        return cursor.fetchall()

    def _query_new_products(self, cursor, since_date):
        """Fetch products created or modified after a given date, with current prices."""
        cursor.execute("""
            SELECT
                i.ItemID, i.ItemName, i.EnglishName,
                i.Tax, i.SupplierID, i.CatID, i.DepartmentID, i.VatCat,
                ip.PurchasePrice, ip.SellPrice, ip.UnitName
            FROM [dbo].[tblItems] i
            LEFT JOIN (
                SELECT
                    ItemID, PurchasePrice, SellPrice, UnitName,
                    ROW_NUMBER() OVER (PARTITION BY ItemID ORDER BY PriceDate DESC, PriceID DESC) as rn
                FROM [dbo].[tblItemsPriceChange]
                WHERE CurrentPrice = 1
            ) ip ON i.ItemID = ip.ItemID AND ip.rn = 1
            WHERE i.DateCreated > %s OR i.ModifiedDate > %s
        """, (since_date, since_date))
        return cursor.fetchall()
