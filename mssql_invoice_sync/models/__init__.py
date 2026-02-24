from . import mssql_sync           # Base: _name = 'mssql.sync'
from . import mssql_sync_partner   # _inherit = 'mssql.sync'
from . import mssql_sync_product   # _inherit = 'mssql.sync'
from . import mssql_sync_sales     # _inherit = 'mssql.sync'
from . import mssql_sync_purchase  # _inherit = 'mssql.sync'
from . import mssql_sync_stock     # _inherit = 'mssql.sync'
from . import mssql_sync_log
from . import mssql_sync_queue
from . import mssql_sync_queue_line
from . import product_product
from . import purchase_order
from . import res_partner
from . import stock_warehouse
from . import precision_override
