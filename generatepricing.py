import pyodbc
import datetime
import os

from FormatQuery import formatQuery
class Generatepricinglist:
    def clearLog(self, log):
        log['state'] = 'normal'
        log.delete(1.0, 'end')
        
    def writeToLog(self, msg, log, maxlines):
        numlines = log.index('end - 1 line').split('.')[0]
        log['state'] = 'normal'
        if numlines==maxlines:
            log.delete(1.0, 2.0)
        if log.index('end-1c')!='1.0':
            log.insert('end', '\n')
        log.insert('end', msg)
        log['state'] = 'disabled'

    def queryverification(self, rows):
        rowlines = str(len(rows))
        fq = formatQuery(rows)
        writeline = fq.createquerylog()
        self.clearLog(self.qlog)
        self.writeToLog(writeline, self.qlog, 2000)
        self.writeToLog(rowlines + ' query lines retrieved.', self.log,
                            2000)
        
    def writeprices(self, prices):
        self.path = os.getcwd()
        file = '\\pricing list.txt'
        pathfile = self.path + file
        file = open(pathfile, 'w')
        wlines = []
        for price in prices:
            writeline = ''
            writeline += str(price[0]) + ' ' + str(price[1]) + '\n'
            wlines.append(writeline)
        file.writelines(wlines)
        file.close()

    def settypenamesfilter(self):
        sqlquery = ''
        sqlquery += ' and c.typeName in ('
        maxlen = len(self.typeNamesdict)-1
        index = 0
        for typeID in self.typeNamesdict:
            typeName = self.typeNamesdict[typeID]
            if not index == maxlen:
                sqlquery += "'" + typeName + "',"
            else:
                sqlquery += "'" + typeName + "') "
            index += 1
        return sqlquery

    def setregionIDs(self):
        sqlquery = ''
        try:
            index = 0
            maxlen = len(self.regionIDs)-1
            for regionID in self.regionIDs:
                if not index == maxlen:
                    sqlquery += str(regionID) + ','
                else:
                    sqlquery += str(regionID)
                index += 1
            
        except:
            logmessage = 'Unable to process query since no region has'
            logmessage += ' been selected.'
            self.writeToLog(logmessage, self.log,
                            2000)
        return sqlquery
            
    
    def priceItems(self):

        sqlquery = ''        
        sqlquery += 'SELECT c.typeName,  SUM(a.quantity*b.averageprice) as price, '
        sqlquery += 'd.regionName ' 
        sqlquery += 'FROM [EVE].[dbo].invTypeMaterials a, '
        sqlquery += '(SELECT averageprice, typeID, date, regionID' 
        sqlquery += ' FROM [Eve].[dbo].reactionmaterialsavgindex WHERE '
        sqlquery += 'regionID in (' + self.setregionIDs() + ') and date IN ' 
        sqlquery += '(SELECT MIN(date) '
        sqlquery += 'FROM [Eve].[dbo].reactionmaterialsavgindex)) b, '
        sqlquery += '[Eve].[dbo].invTypes c, Eve.dbo.mapRegions d'
        sqlquery += ' WHERE a.materialTypeID = b.typeID and a.typeID = c.typeID '
        sqlquery += 'and d.regionID = b.regionID '
##        sqlquery += 'and (CURRENT_TIMESTAMP - b.date) in ' 
##        sqlquery += '(SELECT MIN(CURRENT_TIMESTAMP - date) '
##        sqlquery += 'FROM Eve.dbo.reactionmaterialsavgindex)'
        if not len(self.typeNamesdict) == 0:
            sqlquery += self.settypenamesfilter()
        
        sqlquery += 'GROUP BY a.typeID, c.typeName, d.regionName ' 
        sqlquery += 'ORDER BY a.typeID ASC;'
        print(sqlquery)
        self.cursor.execute(sqlquery)
        rows = self.cursor.fetchall()
        self.queryverification(rows)
        prices = []
        for row in rows:
            prices.append([row.typeName, row.price])
        return prices
        
    def __init__(self, regionIDs, cnxn, cursor, log, qlog,
                 typeNamesdict = {}, storeflg = False):
        self.regionIDs = regionIDs
##        self.cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=CHRISTOPHER-PC\SQLEXPRESS;DATABASE=Eve;UID=shcooper;PWD=abcdeabcdeabcde')
##        self.cursor = self.cnxn.cursor()
        self.cnxn = cnxn
        self.cursor = cursor
        self.log = log
        self.qlog = qlog
        self.typeNamesdict = typeNamesdict
        self.storeflg = storeflg
        prices = self.priceItems()
        self.writeprices(prices)
        

##a = Generatepricinglist(regionID = 10000002)