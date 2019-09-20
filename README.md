# Eveonlinemysqlpyodbc

Requires:
Python 3.x, Pyodbc and MySQL ODBC 3.51 Driver installation

Multi line strings are not yet included at the moment, but the importer serves basic purposes.
Parser has import exceptions on .yaml files containing multi level (non flat) data structures
and parsing certain unicode character sets.
Evedbimporter2.py allows for directory fsd imports...I've configured this for the typeID table.  Multi level data trees are flattened is configured for 'utf-8' encoding with reading of 'en' (english) language.   

Includes a rudimentary .Yaml parser to mysql importer. 

Instructions:
To setup database, you will want to set up your MySQL database, and adjust Evedbimporter.py and Evedbimporter.py files according to login credentials, set locally configuration path to the your Eve developers kit download directory (assumed unzipped).  (For instance, my path directory is ""C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)")
First run Evedbimporter.py script (sets up bsd tables)
Then run Evedbimporter2.py script (sets up typeIDs table)...I still have to work on parsing other fsd directory tables.  typeIDs import has an exclusion on 'masteries'...
Fsd table imports now include typeIDs table and blueprints, certificates, graphicIDs, groupIDs table.  

Single line text strings for a given row are only imported for given row/column values at the moment. 
