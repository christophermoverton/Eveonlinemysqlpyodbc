import pyodbc
import logging
#logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s')
logging.basicConfig(filename='C:\\Users\\chris\\Evepythonimport\\myapp.log', level=logging.INFO)
#logger = logging.getLogger('example_logger')
# Specifying the ODBC driver, server name, database, etc. directly
cnxn = pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};SERVER=localhost;DATABASE=testdb;UID=root;PWD=admin')
print(pyodbc.drivers())
# Using a DSN, but providing a password as well
##cnxn = pyodbc.connect('DSN=test;PWD=password')

# Create a cursor from the connection
cursor = cnxn.cursor()
cursor.execute('Show tables;')
logging.info(f'{cursor.fetchall()}')