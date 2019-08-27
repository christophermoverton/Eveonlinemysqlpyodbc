import pyodbc
import time
import datetime
class Avgindices:
    def testcreatetable(self):
        try:
            sqlqueryline = ''
            sqlqueryline += 'SELECT * FROM reactionmaterialsavgindex;'

            self.cursor.execute(sqlqueryline)

        except:
            print('Unable to find table.  Will drop a new table.')
            sqlqueryline = ''
            sqlqueryline += 'CREATE TABLE reactionmaterialsavgindex'
            sqlqueryline += ' (averageprice float, typeID bigint, '
            sqlqueryline += 'date datetime);'

            self.cursor.execute(sqlqueryline)
            self.cnxn.commit()

    def settablevals(self, avgontypeIDdict):
        
        
        for typeID in avgontypeIDdict:
            sqlqueryline = ''
            sqlqueryline += "INSERT INTO reactionmaterialsavgindex"
            sqlqueryline += " VALUES('" + str(avgontypeIDdict[typeID])
            sqlqueryline += "', '" + str(typeID) + "', '"
            sqlqueryline += datetime.datetime.now().isoformat(' ') + "');"
            self.cursor.execute(sqlqueryline)
            self.cnxn.commit()
        
       # self.cursor.execute(sqlqueryline)
       # self.cnxn.commit()
    
    def converttodatetimeobj(self, datetimestring):
        return datetime.datetime.strptime(datetimestring,
                                          '%Y-%m-%d %H:%M:%S.%f')
    def getdistincttypeIDs(self):
        sqlqueryline = ''
        sqlqueryline = 'SELECT DISTINCT typeID'
        sqlqueryline += ' FROM reactionmaterialsindex'
        self.cursor.execute(sqlqueryline)
        rows = self.cursor.fetchall()
        typeIDs = []
        for row in rows:
            typeIDs.append(row.typeID)
        return typeIDs

    def getcloseprices(self):
        def convertSQLDateTimeToTimestamp(value):
            return time.mktime(time.strptime(value, '%Y-%m-%d %H:%M:%S'))
        aprices = {}
        typeIDs = self.getdistincttypeIDs()
        typeIDdict = {}
        for typeID in typeIDs:
            biddict = {}
            sqlqueryline = ''
            sqlqueryline = 'SELECT price, '
            sqlqueryline += 'typeID, unix_timestamp(batchDate) as bd, orderID, volRemaining FROM reactionmaterialsindex'
            sqlqueryline += ' WHERE (volEntered - volRemaining) > 0' ## and bid & 1 = 1'
            sqlqueryline += ' and batchDate BETWEEN CURRENT_TIMESTAMP'
            sqlqueryline += '- INTERVAL 90 DAY and issueDate - CURRENT_TIMESTAMP'
            sqlqueryline += ' < duration '
            sqlqueryline += 'and typeID = "' +str(typeID)+'" ORDER BY price DESC;'

            self.cursor.execute(sqlqueryline)
            rows = self.cursor.fetchall()
            prices = {}
            for row in rows:
                if (row.orderID not in prices):
                    batches = {}
                    batches[str(row.bd)] = {'price': row.price, 'batchDate': row.bd, 'volRemaining': row.volRemaining}
                    prices[row.orderID] = batches
                else:
                    prices[row.orderID][str(row.bd)] = {'price': row.price, 'batchDate': row.bd, 'volRemaining': row.volRemaining}
            nbatches = {}                                                   
            for orderID in prices:
                batches = prices[orderID]
                i = 0
                batches_list=list(batches.values())
                batcheskeys_list=list(batches.keys())
                for val in batches:
                    tsum = 0
                    tvol = 0  
                    if i == 0:
                        i+=1
                        continue
                    vol1 = batches[val]['volRemaining']
                    vol2 = batches_list[i-1]['volRemaining']
                    if (vol1-vol2 > 0):
                        volDiff = vol1-vol2
                        tvol += volDiff
                        tsum = batches[val]['price']*volDiff
                        bkey = batcheskeys_list[i]
                        if (bkey not in nbatches):
                            nprices = {}
                            nprices[orderID] = {'tvol': tvol, 'tsum': tsum}
                            nbatches[bkey] = nprices
                        else:
                            nprices = nbatches[bkey]
                            nprices[orderID] = {'tvol': tvol, 'tsum': tsum}
                    i+=1;    
           ## tsum = tsum / tvol
            nbatchclose = {}
            for bkey in nbatches:
                ntsum = 0
                ntvol = 0
                nprices = nbatches[bkey]
                for orderID in nprices:
                    ntsum += nprices[orderID]['tsum']
                    ntvol += nprices[orderID]['tvol']
                nbatchclose[bkey] = {'close': ntsum/ntvol}
            ##print(nbatchclose)
            aprices[typeID] = nbatchclose
            biddict['buy'] = prices
            
        #     sqlqueryline = ''
        #     sqlqueryline = 'SELECT price, '
        #     sqlqueryline += 'typeID, unix_timestamp(batchDate) as bd, orderID, volRemaining FROM reactionmaterialsindex'
        #     sqlqueryline += ' WHERE (volEntered - volRemaining) > 0 and bid & 0 = 0'
        #     sqlqueryline += ' and batchDate BETWEEN CURRENT_TIMESTAMP'
        #     sqlqueryline += '- INTERVAL 90 DAY and issueDate - CURRENT_TIMESTAMP'
        #     sqlqueryline += ' < duration '
        #     sqlqueryline += 'and typeID = "' +str(typeID)+'" ORDER BY price ASC;'

        #     self.cursor.execute(sqlqueryline)
        #     rows = self.cursor.fetchall()
        #     prices = {}
        #     for row in rows:
        #         if (row.orderID not in prices):
        #             batches = {}
        #             batches[str(row.bd)] = {'price': row.price, 'batchDate': row.bd, 'volRemaining': row.volRemaining}
        #             prices[row.orderID] = batches
        #         else:
        #             prices[row.orderID][str(row.bd)] = {'price': row.price, 'batchDate': row.bd, 'volRemaining': row.volRemaining}
        #     nbatches2 = {}                                                   
        #     for orderID in prices:
        #         batches = prices[orderID]
        #         i = 0
        #         batches_list=list(batches.values())
        #         batcheskeys_list=list(batches.keys())
        #         for val in batches:
        #             tsum = 0
        #             tvol = 0  
        #             if i == 0:
        #                 i+=1
        #                 continue
        #             vol1 = batches[val]['volRemaining']
        #             vol2 = batches_list[i-1]['volRemaining']
        #             if (vol1-vol2 > 0):
        #                 volDiff = vol1-vol2
        #                 tvol += volDiff
        #                 tsum = batches[val]['price']*volDiff
        #                 bkey = batcheskeys_list[i]
        #                 if (bkey not in nbatches2):
        #                     nprices = {}
        #                     nprices[orderID] = {'tvol': tvol, 'tsum': tsum}
        #                     nbatches2[bkey] = nprices
        #                 else:
        #                     nprices = nbatches2[bkey]
        #                     nprices[orderID] = {'tvol': tvol, 'tsum': tsum}
        #             i+=1;    
        #    ## tsum = tsum / tvol
        #     nbatchclose2 = {}
        #     for bkey in nbatches2:
        #         ntsum = 0
        #         ntvol = 0
        #         nprices = nbatches2[bkey]
        #         for orderID in nprices:
        #             ntsum += nprices[orderID]['tsum']
        #             ntvol += nprices[orderID]['tvol']
        #         nbatchclose2[bkey] = {'close': ntsum/ntvol}
        #     print(nbatchclose2)
            # prices = []
            # for row in rows:
            #     prices.append(row.price)
            ##biddict['sell'] = prices
            ##typeIDdict[typeID] = biddict       
        print(aprices)

    def gettopprices(self):
        typeIDs = self.getdistincttypeIDs()
        typeIDdict = {}
        for typeID in typeIDs:
            biddict = {}
            sqlqueryline = ''
            sqlqueryline = 'SELECT price, '
            sqlqueryline += 'typeID FROM reactionmaterialsindex'
            sqlqueryline += ' WHERE (volEntered - volRemaining) > 0 and bid & 1 = 1'
            sqlqueryline += ' and batchDate BETWEEN CURRENT_TIMESTAMP'
            sqlqueryline += '- INTERVAL 90 DAY and issueDate - CURRENT_TIMESTAMP'
            sqlqueryline += ' < duration '
            sqlqueryline += 'and typeID = "' +str(typeID)+'" ORDER BY price DESC LIMIT 10;'

            self.cursor.execute(sqlqueryline)
            rows = self.cursor.fetchall()
            prices = []
            for row in rows:
                prices.append(row.price)
            biddict['buy'] = prices
            
            sqlqueryline = ''
            sqlqueryline = 'SELECT price, '
            sqlqueryline += 'typeID FROM reactionmaterialsindex'
            sqlqueryline += ' WHERE (volEntered - volRemaining) > 0 and bid & 0 = 0'
            sqlqueryline += ' and batchDate BETWEEN CURRENT_TIMESTAMP'
            sqlqueryline += '- INTERVAL 90 DAY and issueDate - CURRENT_TIMESTAMP'
            sqlqueryline += ' < duration '
            sqlqueryline += 'and typeID = "' +str(typeID)+'" ORDER BY price ASC LIMIT 10;'

            self.cursor.execute(sqlqueryline)
            rows = self.cursor.fetchall()
            prices = []
            for row in rows:
                prices.append(row.price)
            biddict['sell'] = prices
            typeIDdict[typeID] = biddict
            #print (typeIDdict)
        return typeIDdict

    def avgindices(self):
        typeIDdict = self.gettopprices()
        avgontypeIDdict = {}
        for typeID in typeIDdict:
            orderssum = 0
            norders = 0
            for bidcase in typeIDdict[typeID]:
                for price in typeIDdict[typeID][bidcase]:
                    orderssum += price
                    norders += 1
            avg = orderssum / norders
            avgontypeIDdict[typeID] = avg
        return avgontypeIDdict

    def getcloseindices(self):
        self.getcloseprices()
                
        
    def getsetindices(self):
##        sqlqueryline = ''
##        sqlqueryline += 'SELECT price, volRemaining, typeID, '
##        sqlqueryline += 'orderID, volEntered, issueDate, '
##        sqlqueryline += 'batchDate FROM [Eve].[dbo].[reactionmaterialsindex]'
##        sqlqueryline += 'WHERE batchDate BETWEEN CURRENT_TIMESTAMP'
##        sqlqueryline += '- INTERVAL 90 DAYS AND issueDate - CURRENT_TIMESTAMP'
##        sqlqueryline += ' < duration;'
##
##        self.cursor.execute(sqlqueryline)
##        rows = self.cursor.fetchall()
##        orders = {}
##        for row in rows:
##            batches = {}
##            #using dicts first key by orderID, second by batchdate
##            batchdate = converttodatetimeobj(row.batchDate)
##            issuedate = converttodatetimeobj(row.issueDate)
##            batches[batchdate] = (row.typeID, row.price, row.volRemaining,
##                                  row.volEntered, issuedate)
##            orders[row.orderID] = batches
        avgontypeIDdict = self.avgindices()
        print (avgontypeIDdict)
        self.testcreatetable()
        self.settablevals(avgontypeIDdict)
            
    def __init__(self, cursor = '', cnxn = ''):
        if cursor == '':
            self.cnxn = pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};SERVER=localhost;DATABASE=testdb;UID=root;PWD=admin')
            #pyodbc.connect('DRIVER={SQL Server};SERVER=CHRISTOPHER-PC\SQLEXPRESS;DATABASE=Eve;UID=shcooper;PWD=password')
            self.cursor = self.cnxn.cursor()
        else:
            self.cursor = cursor
            self.cnxn = cnxn

avgindices = Avgindices()
avgindices.getcloseindices()
##avgindices.getsetindices()
