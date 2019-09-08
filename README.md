# Eveonlinemysqlpyodbc

Requires:
Python 3.x, Pyodbc and MySQL ODBC 3.51 Driver installation

Multi line strings are not yet included at the moment, but the importer serves basic purposes.
Parser has import exceptions on .yaml files containing multi level (non flat) data structures
and parsing certain unicode character sets.
Evedbimporter2.py allows for directory fsd imports...I've configured this for the typeID table.  Multi level data trees are flattened is configured for 'utf-8' encoding with reading of 'en' (english) language.   

Includes a rudimentary .Yaml parser to mysql importer.  
