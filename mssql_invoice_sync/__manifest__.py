{
    "name": "SQL Server Invoice & Stock Sync",
    "version": "18.0.2.0.0",
    "category": "Accounting",
    "summary": "Sync products, invoices, and stock/inventory from SQL Server with Smart Connect",
    "depends": ["product", "account", "purchase", "stock", "sale_management", "mail"],
    "data": [
        "security/ir.model.access.csv",
        "data/ir_sequence_data.xml",
        "views/mssql_sync_views.xml",
        "views/mssql_sync_queue_views.xml",
        "views/so_inv_views.xml",
        "views/mssql_operations_views.xml",
        "data/ir_cron_data.xml",
    ],
    "installable": True,
    "application": False,
    "license": "LGPL-3",
}



