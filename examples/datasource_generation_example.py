# -*- coding: utf-8 -*-
from tableau_tools import *
from tableau_tools.tableau_documents import *

logger = Logger('datasource_generation_example.log')
file_dir = 'C:\\Users\\{}\\Documents\\My Tableau Repository\\Datasources\\'

# Simple data source connection to a single table on a Postgres DB
ds = TableauDatasourceGenerator('postgres', 'My DS', 'localhost', 'demo', logger)
ds.add_first_table('agency_sales', 'Super Store')
ds.save_file("one_table", file_dir)

# Adding in a table JOIN
ds2 = TableauDatasourceGenerator('postgres', 'My DS', 'localhost', 'demo', logger)
ds2.add_first_table('agency_sales', 'Super Store')
# Define each JOIN ON clause first, then JOIN the table. On clauses should be put into a list, to handle multiple keys
join_on = ds2.define_join_on_clause('Super Store', 'region', '=', 'Entitled People', 'region')
ds2.join_table('Inner', 'superstore_entitlements', 'Entitled People', [join_on, ])
ds2.save_file("two_tables", file_dir)

# Single table, but several data source filters
ds3 = TableauDatasourceGenerator('postgres', 'My DS', 'localhost', 'demo', logger)
ds3.add_first_table('agency_sales', 'Super Store')
# Single inclusive filter
ds3.add_dimension_datasource_filter('category', ['Furniture', ])
# Multiple inclusive filter
ds3.add_dimension_datasource_filter('region', ['East', 'West'])
# Single exclusive filter
ds3.add_dimension_datasource_filter('city', ['San Francisco', ], include_or_exclude='exclude')
# Multiple exclusive filter
ds3.add_dimension_datasource_filter('state', ['Arkansas', 'Texas'], include_or_exclude='exclude')
# Numeric inclusive filter
ds3.add_dimension_datasource_filter('row_id', [2, 5, 10, 22])
# Numeric continuous greater than filter
ds3.add_continuous_datasource_filter('profit', min_value=20)
# Relative date filter
ds3.add_relative_date_datasource_filter('order_date', 'year', previous_next_current='previous', number_of_periods=4)

# Add a calculation (this one does row level security
calc_id = ds3.add_calculation('IIF([salesperson_user_id]=USERNAME(),1,0) ', 'Row Level Security',
                              'dimension', 'discrete', 'integer')
# Create a data source filter that references the calculation
ds3.add_dimension_datasource_filter(calc_id, [1, ], custom_value_list=True)
ds3.save_file("one_table_many_ds_filters", file_dir)

# Create a data source with an extract
ds4 = TableauDatasourceGenerator('postgres', 'My DS', 'localhost', 'demo', logger)
ds4.add_first_table('agency_sales', 'Super Store')
# This is the name you want the TDE to have within the TDSX file. Not an external existing filename
ds4.add_extract('Datasource.tde')
ds4.add_dimension_extract_filter('region', ['East', 'West'])
ds4.add_dimension_extract_filter('sub-category', ['Paper', ], include_or_exclude='exclude')
ds4.add_continuous_extract_filter('order_date', '2013-04-01', '2014-04-23', date=True)
ds4.add_column_alias('region', 'Regional Descriptor', dimension_or_measure='dimension', discrete_or_continuous='discrete',
                     datatype='string')
ds4.save_file('extract_datasource', file_dir)


# ds.add_column_alias('profit', 'Profit', 'measure', u'continuous', 'real')
#

# SQL Server source with Custom SQL and Initial SQL with Parameters
initial_sql = '''
IF @@rowcount = 0
BEGIN
SELECT 1 FROM [Orders]
END
'''

ds5 = TableauDatasourceGenerator('sqlserver', 'My DS', 'demo-dbs', 'Superstore Star Schema', logger,
                                 initial_sql=initial_sql)
custom_sql = '''
SELECT [OrderFact].[OrderID] AS [OrderID],
  [OrderFact].[IDProduct] AS [IDProduct],
  [OrderFact].[IDShipMode] AS [IDShipMode],
  [OrderFact].[IDCustomer] AS [IDCustomer],
  [OrderFact].[IDOrderPriority] AS [IDOrderPriority],
  [OrderFact].[OrderDate] AS [OrderDate],
  [OrderFact].[ShipDate] AS [ShipDate],
  [OrderFact].[OrderQuantity] AS [OrderQuantity],
  [OrderFact].[Sales] AS [Sales],
  [OrderFact].[Discount] AS [Discount],
  [OrderFact].[Profit] AS [Profit],
  [OrderFact].[UnitPrice] AS [UnitPrice],
  [OrderFact].[ShippingCost] AS [ShippingCost],
  [OrderFact].[ProductBaseMargin] AS [ProductBaseMargin],
  [DimCustomer].[IDCustomer] AS [IDCustomer (DimCustomer)],
  [DimCustomer].[CustomerName] AS [CustomerName],
  [DimCustomer].[State] AS [State],
  [DimCustomer].[ZipCode] AS [ZipCode],
  [DimCustomer].[Region] AS [Region],
  [DimCustomer].[CustomerSegment] AS [CustomerSegment]
FROM [dbo].[OrderFact] [OrderFact]
  INNER JOIN [dbo].[DimCustomer] [DimCustomer] ON ([OrderFact].[IDCustomer] = [DimCustomer].[IDCustomer])
'''
ds5.add_first_custom_sql(custom_sql, 'Custom SQL One')

ds5.save_file("custom_sql", file_dir)
