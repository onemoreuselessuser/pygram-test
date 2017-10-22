# psycopg2 is a database driver allowing CPython to access Postgresql
import psycopg2

# Pygrametl's __init__ file provides a set of helper functions and more
# importantly the class ConnectionWrapper for wrapping PEP 249 connections
import pygrametl

# Pygrametl provides simple reading of data through datasources.
from pygrametl.datasources import SQLSource, CSVSource

# Interacting with the dimensions and the fact table is done through a set
# of classes. A suitable object must be created for each.
from pygrametl.tables import Dimension, FactTable

# Creation of a database connection to the sales database with a simple
# connection string, specifying the necessary host, username and passowrd
sales_string = "host='localhost' dbname='source' user='etl' password='ETL' port=54320"
sales_pgconn = psycopg2.connect(sales_string)

# A connection is also created for the data warehouse. The connection is
# then given to a ConnectionWrapper for it to implicitly shared between
# all the pygrametl abstractions that needs it with being passed around
dw_string = "host='localhost' dbname='etl' user='etl' password='ETL' port=54320"
dw_pgconn = psycopg2.connect(dw_string)

# Although the ConnectionWrapper is shared automatically between pygrametl
# abstractions, we still save in in a variable to allow for it to be closed
dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=dw_pgconn)

# As the location dimension stores the name of a location in the attribute
# "city" instead of in the attribute "store" as in the input data from the
# sales relation, a sequence of names matching the number of attributes in
# the relation is created, allowing the SQLSource to do the mapping for us
name_mapping= 'book', 'genre', 'city', 'timestamp', 'sale'

# Extraction of rows from a database using a PEP 249 connection and SQL
sales_source = SQLSource(connection=sales_pgconn, \
                         query="SELECT * FROM sales", names=name_mapping)

# Extraction of rows from a CSV file does not require SQL, just an open file
# handle to the file, as pygrametl uses Pythons DictReader for CSV files,
# and the header of the CSV file contains information about each column.
region_file_handle = open('c:\\work\\python\\region.csv', 'r', 16384)
region_source = CSVSource(f=region_file_handle, delimiter=',')

# An instance of Dimension is created for each dimension in the data
# warehouse. For each table, the name of the table, the primary key of
# the table, and a list of non key attributes in the table, are added.
# In addition, for the location dimension we specify which attributes
# should be used for a lookup of the primary key, as only the city is
# present in the sales database and is enough to perform a lookup of
# a unique primary key. As mentioned in the beginning of the guide, using
# named parameters is strongly encouraged.

book_dimension = Dimension(
    name='book',
    key='bookid',
    attributes=['book', 'genre'])

time_dimension = Dimension(
    name='time',
    key='timeid',
    attributes=['day', 'month', 'year'])

location_dimension = Dimension(
    name='location',
    key='locationid',
    attributes=['city', 'region'],
    lookupatts=['city'])

# A single instance of FactTable is created for the data warehouse's
# fact table, with the name of the table, a list of attributes constituting
# the primary key of the fact table, and lastly, the list of measures.
fact_table = FactTable(
    name='facttable',
    keyrefs=['bookid', 'locationid', 'timeid'],
    measures=['sale'])

# A normal Python function is used to split the timestamp into its parts
def split_timestamp(row):
    """Splits a timestamp containing a date into its three parts"""

    # First the timestamp is extracted from the row dictionary
    timestamp = row['timestamp']

    # Then the string is split on the / in the time stamp
    timestamp_split = timestamp.split('/')

    # Finally each part is reassigned to the row dictionary. It can then be
    # accessed by the caller as the row is a reference to the dict object
    row['year'] = timestamp_split[0]
    row['month'] = timestamp_split[1]
    row['day'] = timestamp_split[2]


# The Location dimension is filled with data from the CSV file, as the file
# contains information for both columns in the table. If the dimension was
# filled using the sales database, it would be necessary to update the
# region attribute with data from the CSV file later irregardless.
# To perform the insertion, the method Dimension.insert() is used which
# inserts a row into the table, and the connection wrapper is asked to
# commit to ensure that the data is present in the database to allow for
# lookups of keys for the fact table
[location_dimension.insert(row) for row in region_source]

# The file handle for the CSV file can then be closed
region_file_handle.close()

# As all the information needed for the other dimensions are stored in the
# sales database, we can loop through all the rows in it, split the timestamp
# into its three parts, and lookup the three dimension keys needed for
# the fact table while letting pygrametl update each dimension with the
# necessary data using Dimension.ensure(). Using this method instead of
# insert combines a lookup with a insertion so a new row only is inserted
# into the dimension or fact table, if it does not yet exist.
for row in sales_source:

    # The timestamp is split into its three parts
    split_timestamp(row)

    # We update the row with the primary keys of each dimension while at
    # the same time inserting new data into each dimension
    row['bookid'] = book_dimension.ensure(row)
    row['timeid'] = time_dimension.ensure(row)

    # We do not use ensure() for the location dimension, as city
    # and region information of all stores has already been loaded into
    # the table, and having a sale from a store we do not know about is
    # probably either an error in the sales or region data. We use lookup
    # instead which does not insert data and returns None, if no row with
    # the requested data is available, allowing for simple implementation
    # of error handling in ETL flow, which is shown here as an exception
    row['locationid'] = location_dimension.lookup(row)

    # A simple example of how to check if a lookup was successful, so
    # errors can be handled in some way. Here we just give up, and throw
    # an error.
    if not row['locationid']:
       raise ValueError("City was not present in the location dimension")

    # As the number of sales was already conveniently aggregated in the
    # sales table, the row can now be inserted into the data warehouse as
    # we have all the IDs we need. If aggregations, or other more advanced
    # manipulation is required, the full power Python is available as shown
    # with the call to the split_timestamp(row) function.
    fact_table.insert(row)

# After all the data is inserted, we close the connection in order to
# ensure that all data is committed to the database and that the
# connection is correctly released
dw_conn_wrapper.commit()
dw_conn_wrapper.close()

# Finally, the connection to the sales database is closed
sales_pgconn.close()