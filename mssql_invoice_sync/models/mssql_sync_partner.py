from odoo import models
from odoo.exceptions import UserError
import logging

_logger = logging.getLogger(__name__)


class MssqlSyncPartner(models.Model):
    _inherit = 'mssql.sync'

    # ── Vendor / Customer Sync ──────────────────────────────────────────

    def sync_vendors(self):
        """Fetch vendors from SQL Server and create/update in Odoo - Optimized with change detection"""
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            sql_vendors = self._query_vendors(cursor)
            conn.close()

            if not sql_vendors:
                _logger.info('No vendors found in SQL Server')
                return self._success_notification('Vendor Sync Complete', 'No vendors found')

            # Define field mapping
            # Special syntax:
            #   {'_concat': [...]} - concatenate with " / " separator
            #   {'_combine': [...]} - combine with ", " separator (for address parts)
            #   {'_note': [...]} - combine into comment with labels
            field_mapping = {
                'name': 'SupplierName',
                'ref': 'SupplierID',
                'street': 'SupplierAddress',
                'street2': {'_combine': ['StreetName', 'BuildingNo', 'Area', 'POBox']},
                'phone': {'_concat': ['Phone1', 'Phone2']},
                'mobile': 'Mobile',
                'email': 'EMailAdress',
                'website': 'WebSite',
                'vat': 'SuppliervatNumber',
                'city': 'City',
                'zip': 'PostalZone',
                'comment': {'_note': [('Note', 'SupplierNote'), ('Representative', 'RepresentativeName')]},
                'company_registry': 'CRNO',
            }

            # Use generic sync logic - only add new records
            created, updated, skipped = self._generic_partner_sync(
                sql_records=sql_vendors,
                sql_id_field='SupplierID',
                odoo_id_field='x_sql_vendor_id',
                partner_type='supplier',
                field_mapping=field_mapping,
                only_new=True
            )

            # Only update flag if new vendors were created
            if created > 0:
                self.write({'vendors_fetched': True})

            # Return appropriate message
            if created == 0:
                return self._success_notification('Vendor Sync Complete', f'No new vendors found (checked: {len(sql_vendors)} vendors, {skipped} already exist)')
            else:
                return self._success_notification('Vendor Sync Complete', f'Created: {created} new vendors ({skipped} already existed)')
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise UserError(f'Vendor sync failed: {str(e)}')

    def sync_customers(self):
        """Fetch customers from SQL Server and create/update in Odoo - Optimized with change detection"""
        conn = self._get_connection()
        cursor = conn.cursor(as_dict=True)

        try:
            sql_customers = self._query_customers(cursor)
            conn.close()

            if not sql_customers:
                _logger.info('No customers found in SQL Server')
                return self._success_notification('Customer Sync Complete', 'No customers found')

            # Define field mapping
            field_mapping = {
                'name': 'CustomerName',
                'street': 'CustomerAddress',
                'phone': ['Phone1', 'Phone2'],
                'mobile': 'Mobile',
                'email': 'EMail',
                'website': 'WebSite',
                'vat': 'CustVatNumber',
                'city': 'City',
                'zip': 'PostalZone',
                'comment': 'CustomerNote',
                'company_registry': 'CRNo',
                'credit_limit': 'CreditLimit',
            }

            # Use generic sync logic - only add new records
            created, updated, skipped = self._generic_partner_sync(
                sql_records=sql_customers,
                sql_id_field='CustomerID',
                odoo_id_field='x_sql_customer_id',
                partner_type='customer',
                field_mapping=field_mapping,
                only_new=True
            )

            # Only update flag if new customers were created
            if created > 0:
                self.write({'customers_fetched': True})

            # Return appropriate message
            if created == 0:
                return self._success_notification('Customer Sync Complete', f'No new customers found (checked: {len(sql_customers)} customers, {skipped} already exist)')
            else:
                return self._success_notification('Customer Sync Complete', f'Created: {created} new customers ({skipped} already existed)')
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise UserError(f'Customer sync failed: {str(e)}')

    # ── SQL Queries ─────────────────────────────────────────────────────

    def _query_vendors(self, cursor):
        """Fetch vendors from MSSQL"""
        cursor.execute("""
            SELECT
                SupplierID,
                SupplierName,
                SupplierAddress,
                Phone1,
                Phone2,
                Mobile,
                Fax,
                EMailAdress,
                WebSite,
                SuppliervatNumber,
                CRNO,
                City,
                StreetName,
                BuildingNo,
                PostalZone,
                POBox,
                Area,
                SupplierAccountNumber,
                SupplierIBAN,
                SupplierBankName,
                SupplierNote,
                RepresentativeName
            FROM [dbo].[tblSuppliers]
        """)
        return cursor.fetchall()

    def _query_customers(self, cursor):
        """Fetch customers from MSSQL"""
        cursor.execute("""
            SELECT
                CustomerID,
                CardNumber,
                CustomerName,
                CustomerAddress,
                Phone1,
                Phone2,
                Mobile,
                Fax,
                EMail,
                WebSite,
                CustVatNumber,
                CRNo,
                City,
                StreetName,
                BuildingNo,
                PostalZone,
                POBox,
                Area,
                ContactPerson,
                CustomerNote,
                CreditLimit,
                CustomerBalance
            FROM [dbo].[tblCustomers]
        """)
        return cursor.fetchall()
