
import os
import datetime
import pyodbc
import logging

reactionmaterials = ['Amber Cytoserocin', 'Amber Mykoserocin',
                     'Atmospheric Gases', 'Azure Cytoserocin',
                     'Azure Mykoserocin', 'Bacteria', 'Biofuels', 'Biomass','Black Morphite',
                     'C3-FTM Acid', 'Cadmium', 'Caesarium Cadmide',
                     'Caesium', 'Carbon Polymers', 'Carbon-86 Epoxy Resin',
                     'Celadon Cytoserocin', 'Celadon Mykoserocin',
                     'Ceramic Powder', 'Chiral Sturctures', 'Chromodynamic Tricarboxyls',
                     'Chromium', 'Cobalt',
                     'Crystalline Carbonide', 'Crystallite Alloy',
                     'Dysporite', 'Dysprosium', 'Electrolytes','Evaporite Deposits',
                     'Fermionic Condensates', 'Fernite Alloy',
                     'Fernite Carbide', 'Ferrofluid', 'Ferrogel',
                     'Fluxed Condensates', 'Freedom Fighters',
                     'Fullerene Intercalated Graphite', 'Fullerides',
                     'Fullerite-C28', 'Fullerite-C32', 'Fullerite-C320',
                     'Fullerite-C50', 'Fullerite-C540', 'Fullerite-C60',
                     'Fullerite-C70', 'Fullerite-C72', 'Fullerite-C84',
                     'Fulleroferrocene', 'Fullero-Ferrocene', 'Garbage',
                     'generic item 4', 'generic item 6', 'Golden Cytoserocin',
                     'Golden Mykoserocin', 'Graphene Nanoribbons', 'Hafnium',
                     'Heavy Water', 'Helium Isotopes', 'Hydrogen Isotopes',
                     'Hexite', 'Hydrocarbons', 'Hydrochloric Acid',
                     'Hyperflurite', 'Hypersynaptic Fibers', 'Isogen',
                     'Lanthanum Metallofullerene', 'Lime Cytoserocin',
                     'Lime Mykoserocin', 'Liquid Ozone','Malachite Cytoserocin',
                     'Malachite Mykoserocin', 'Megacyte', 'Mercury',
                     'Methanofullerene', 'Mexallon', 'Morphite',
                     'Nanobud Polymers', 'Nanotori Polymers','Nitrogen Isotopes',
                     'Nanotransistors', 'Neo Mercurite', 'Neodymium',
                     'Nocxium', 'Oxidizing Compound', 'Oxygen', 'Oxygen Isotopes', 
                     'Phenolic Composites', 'Plasmoids',
                     'Platinum', 'Proteins',
                     'Platinum Technite', 'Plutonium Metallofullerene',
                     'Polyfullerene Condensate', 'PPD Fullerene Fibers',
                     'Precious Metals', 
                     'Promethium', 'Prometium',
                     'Pure Improved Blue Pill Booster',
                     'Pure Improved Crash Booster',
                     'Pure Improved Drop Booster',
                     'Pure Improved Exile Booster',
                     'Pure Improved Frentix Booster',
                     'Pure Improved Mindflood Booster',
                     'Pure Improved Sooth Sayer Booster',
                     'Pure Improved X-Instinct Booster',
                     'Pure Standard Blue Pill Booster',
                     'Pure Standard Crash Booster',
                     'Pure Standard Drop Booster',
                     'Pure Standard Exile Booster',
                     'Pure Standard Frentix Booster',
                     'Pure Standard Mindflood Booster',
                     'Pure Standard Sooth Sayer Booster',
                     'Pure Standard X-Instinct Booster',
                     'Pure Strong Blue Pill Booster',
                     'Pure Strong Crash Booster',
                     'Pure Strong Drop Booster',
                     'Pure Strong Exile Booster',
                     'Pure Strong Frentix Booster',
                     'Pure Strong Mindflood Booster',
                     'Pure Strong Sooth Sayer Booster',
                     'Pure Strong X-Instinct Booster',
                     'Pure Synth Blue Pill Booster',
                     'Pure Synth Crash Booster',
                     'Pure Synth Drop Booster',
                     'Pure Synth Exile Booster',
                     'Pure Synth Frentix Booster',
                     'Pure Synth Mindflood Booster',
                     'Pure Synth Sooth Sayer Booster',
                     'Pure Synth X-Instinct Booster',
                     'Pyerite', 'Reactive Metals', 'Rolled Tungsten Alloy',
                     'Scandium', 'Scandium Metallofullerene',
                     'Silicates', 'Silicon','Silicon Diborite', 'Slaves', 'Solerium',
                     'Spirits', 'Strontium Clathrates','Sulfuric Acid', 'Sylramic Fibers',
                     'Technetium', 'Thulium', 'Titanium', 'Titanium Carbide',
                     'Titanium Chromide', 'Toxic Metals','Tritanium', 'Tungsten',
                     'Tungsten Carbide', 'Unrefined Dysporite',
                     'Unrefined Ferrofluid', 'Unrefined Fluxed Condensates',
                     'Unrefined Hyperflurite', 'Unrefined Neo Mercurite',
                     'Unrefined Prometium', 'Vanadium', 'Vanadium Hafnite',
                     'Vermillion Cytoserocin', 'Vermillion Mykoserocin',
                     'Viridian Cytoserocin', 'Viridian Mykoserocin',
                     'Water', 'Zydrine']
#dirpath = ''
#backuppath = ''
dirpath = 'C:\\Users\\chris\\Documents\\EVE\\logs\\Marketlogs'
backuppath = 'C:\\Users\\chris\\Documents\\EVE\\logs\\Marketlogs\\Backup'
INISETTINGS = {'DIRPATH':dirpath, 'BACKUPPATH': backuppath,
               'SERVER': '', 'DATABASE': '', 'USERID': '', 'PASSWORD':''}
##dirpath = 'C:\\Users\\christopher\\Documents\\EVE\\logs\\Marketlogs'
##backuppath = 'C:\\Users\\christopher\\Documents\\EVE\\logs\\Marketlogs\\Backup'

sqlimportfilename = 'EVEsqlimport'

class Evesqlimporter:

    def retrieveReactionMaterials(self):
        query = "Select DISTINCT a.name FROM  typeIDs a,"
        query += " invTypeReactions b WHERE b.typeID = a.typeID "
        query += "ORDER BY a.name ASC;"
        reactionmaterials = []
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            for row in rows:
                reactionmaterials.append(row.name)
        except:
            print('Unable to get reaction materials used for tablename construct.')

        query = "Select DISTINCT a.name FROM  typeIDs a,"
        query += " planetSchematicsTypeMap b WHERE b.typeID = a.typeID "
        query += "ORDER BY a.name ASC;"

        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            for row in rows:
                reactionmaterials.append(row.name)
        except:
            print('Unable to get reaction materials used for tablename construct.')
            
        return reactionmaterials

    def getfiles(self):
        filenames = []
        global INISETTINGS
        dirpath = INISETTINGS['DIRPATH']
        for filename in os.listdir(dirpath):
            if filename.endswith('.txt'):
                filenames.append(dirpath + '/' + filename)
        return filenames

    def checkitemname(self, itemname):
        tablename = ''
        reactionmaterials = self.retrieveReactionMaterials()
        if itemname in reactionmaterials:
            tablename = 'reactionmaterialsindex'
        else:
            tablename = 'itemsindex'
        return tablename

    def removespaces(self, name):
        returnname = ''
        if ' ' in name:
            nameparts = name.split(' ')
            for namepart in nameparts:
                returnname += namepart
        else:
            returnname = name
        return returnname

    def removeextensions(self, name):
        returnname = ''
        if '.' in name:
            nameparts = name.split('.txt')
            for namepart in nameparts:
                returnname += namepart
        else:
            returnname = name
        return returnname

    def getbatchdate(self, filename):
        print(filename)
        statinfo = os.stat(filename)
        ctime = statinfo.st_ctime
        print(int(ctime))
        dt = datetime.datetime.fromtimestamp(int(ctime))
        print(dt)
        batchdate = dt.isoformat(' ')
        batchdate += '.000'
        return batchdate



    def filenameparser(self, filename):
        def extendparse(filenamec):
            filenamecsplit = filenamec.split('-')
            regionName = filenamecsplit[0]
            batchdate = filenamecsplit[len(filenamecsplit)-1]
            filenamesplitcpy = filenamecsplit[1:len(filenamecsplit)-1]
            tablename = ''
            index = 0
            maxfileindex = len(filenamesplitcpy)-1
            for filenamepart in filenamesplitcpy:
                if not index == maxfileindex:
                    tablename += filenamepart + '-'
                else:
                    tablename += filenamepart
            tablename = self.checkitemname(tablename)
            return regionName, tablename, batchdate
        print(dirpath)
        print(filename.split(dirpath + '/'))            
        null, filenamec = filename.split(dirpath + '/')
        
        if len(filenamec.split('-')) == 2:
            name, batchdate = filenamec.split('-')
            tablename = name
        elif len(filenamec.split('-')) == 3:
            regionName, itemname, batchdate = filenamec.split('-')
            tablename = self.checkitemname(itemname)
        else:
            regionName, tablename, batchdate = extendparse(filenamec)
            
        tablename = self.removespaces(tablename)
        batchdate = self.getbatchdate(filename)
        print(batchdate)
        return tablename, batchdate    
        

    def renamefilestobackup(self):
    ##    for filename in os.listdir(dirpath):
    ##        if filename.endswith('.txt'):
                #truncfilename = filename.split('.txt')
                filepath = dirpath + '\\*.*' 
                filebackuppath = backuppath 
                commandline = 'move '
                commandline += filepath + ' ' + filebackuppath
                print(commandline)
                results = os.popen(commandline).read()
                print(results)
                commandline = 'dir '
                commandline += dirpath
                results = os.popen(commandline).read()
                print(results)
                

    def readfilesetimport(self, filepath, tablename, batchdate):
    ##    tablename = ''
    ##    if 'My Orders' in filepath:
    ##        tablename = 'myorders'
        file = open(filepath, 'rU')
        count = 0
        sqllines = []
        for line in file:
            if not count == 0:
                csvlinedata = line.split(',')
                linedatalen = len(csvlinedata) - 1
                colcount = 0
                writeline = 'INSERT INTO ' + tablename + ' VALUES('
                for col in csvlinedata:
                    if not colcount == linedatalen:
                        if not col.find("'") == -1:
                            colstring = ''     
                            for part in col.split("'"):
                                colstring += part
                                col = colstring
                        if colcount == linedatalen - 1:
                            
                            writeline += "'" + col +"'"
                        else:
                            if col == 'True':
                                writeline += "b'1',"
                            elif col == 'False':
                                writeline += "b'0',"
                            else: 
                                writeline += "'" + col +"',"
                    else:
                        writeline += ",'"
                        writeline += batchdate
                        writeline +=  "');\n"
                    colcount += 1
                             
            if not count == 0:
                sqllines.append(writeline)
            count += 1
        file.close()
        return sqllines, tablename

    def writesqlfile(self, sqllinelist):
        curdir = os.getcwd()
        sqlfilepath = curdir + '/'+ sqlimportfilename
        print('sqlfilepath: ', sqlfilepath)
        newsqllines = []
        for sqllines in sqllinelist:
            for sqlline in sqllines:
                newsqllines.append(sqlline)
        file = open(sqlfilepath, 'w')
        file.writelines(newsqllines)
        file.close()

    def returncolids(self, filepath):
    ##    if not len(file) == 0:
        file = open(filepath, 'rU')
        index = 0
        for line in file:
            if index == 0:
                idline = line
                csvlinedata = idline.split(',')
                break
        file.close()
        return csvlinedata

    def booltest(self, typeobj):
        boolt = False
        if 'True' in typeobj:
            typeobj = True
            boolt = True
        elif 'False' in typeobj:
            typeobj = False
            boolt = True
        return typeobj,boolt

    def converttype(self, typeobj):
        test = True
        returntype = typeobj

    ##    if 'True' or 'False' in typeobj:
    ##        returntype = bool(typeobj)
        rtypeobj, boolt = self.booltest(typeobj)
        if not boolt:
            try:
                returntype = int(typeobj)
            except:
                test = False
            if not test:
                try:
                    returntype = float(typeobj)
                except:
                    returntype = typeobj
        else:
            returntype = rtypeobj


        return returntype
            

    def returntypescsvlinedata(self, filepath):
        file = open(filepath, 'rU')
        typecont = []
    ##    if not len(file) < 2:
        index = 0
        for line in file:
            if index == 1:
                csvlinedata = line.split(',')
                for col in csvlinedata:
                    colobj = self.converttype(col)
                    typecont.append(type(colobj))
                break
            index += 1
        file.close()
        colids = self.returncolids(filepath)
        typecont = typecont[0:len(typecont)-1]
        colids = colids[0:len(colids)-1]
        typecont.append(str)
        colids.append('batchDate')
        return typecont, colids

    def setsqltypecont(self, typecont):
        sqltypecont = []
        for typei in typecont:
            if typei == int:
                sqltypecont.append('bigint')
            elif typei == str:
                sqltypecont.append('nvarchar(500)')
            elif typei == bool:
                sqltypecont.append('bit')
            elif typei == float:
                sqltypecont.append('float')
        print(sqltypecont)
        return sqltypecont

    def settabledrop(self, tablename, sqltypecont, colids, cursor):
        tablexist = True
        try:
            execline = "Select * from "
            execline += tablename
            cursor.execute(execline)
        except:
            tablexist = False

        if not tablexist:    
            writeline = "CREATE TABLE "
            writeline += tablename
            writeline += "( "
            index = 0
            maxid = len(colids)-1
            for colid in colids:
                if not index == maxid:
                    if 'issueDate' in colid:
                        writeline += colid
                        writeline += " "
                        writeline += "datetime"
                        writeline += ", "
                    elif 'range' in colid:
                        writeline += "ranger"
                        writeline += " "
                        writeline += "bigint"
                        writeline += ", "             
                    else:
                        writeline += colid
                        writeline += " "
        ##                print(index)
                        writeline += sqltypecont[index]
                        writeline += ", "


                else:
                    writeline += colid
                    writeline += " "
    ##                print(index)
                    writeline += "datetime"
                    writeline += ");"                
                index += 1
    ##        writeline += "CONSTRAINT "
    ##        writeline += tablename
    ##        writeline += "_PK_PRIMARY_KEY_CLUSTERED ("
    ##        writeline += colids[0]
    ##        writeline += "))"
            #writeline += ' ENGINE=testdb;'
            print(writeline)    
            cursor.execute(writeline)
            self.cnxn.commit()
            print('Added table successfully!')
        else:
            print('Table already exists.')

    def tableexist(self, cursor, tablename):
        tablexist = True
        try:
            execline = "Select * from "
            execline += tablename
            cursor.execute(execline)
        except:
            tablexist = False
        return tablexist

    def executesqllist(self, sqllinelist, cursor):
    ##    print(sqllinelist)
        for sqllines in sqllinelist:
            for sqlline in sqllines:
                cursor.execute(sqlline)
                self.cnxn.commit()

    def setINISETTINGS(self, settings):
        for setting in settings:
            global INISETTINGS
            INISETTINGS[setting] = settings[setting]
        global dirpath
        dirpath = INISETTINGS['DIRPATH']

    def getINISETTINGS(self):
        global INISETTINGS
        return INISETTINGS
    
    def writeToLog(self, msg):
        #numlines = log.index('end - 1 line').split('.')[0]
        #log['state'] = 'normal'
        # if numlines==24:
        #     log.delete(1.0, 2.0)
        # if log.index('end-1c')!='1.0':
        #     log.insert('end', '\n')
        # log.insert('end', msg)
        #log['state'] = 'disabled'
        logging.info(f'{msg}')

    def initializeLogging(self):
        logging.basicConfig(filename='C:\\Users\\chris\\Evepythonimport\\myapp.log', level=logging.INFO)
        
    def __init__(self, settings, cursor, cnxn):
        print('checking sql database.')
        print('settings: ', settings)
        self.setINISETTINGS(settings)
        self.cnxn = cnxn
        self.cursor = cursor
##        self.win = win
##        self.p = p
        

    def run(self):
##        cnxnstring = ''
##        cnxnstring += 'DRIVER={SQL Server};'
##        cnxnstring += 'SERVER=' + INISETTINGS['SERVER'] + ';'
##        cnxnstring += 'DATABASE=' + INISETTINGS['DATABASE'] + ';'
##        cnxnstring += 'UID=' + INISETTINGS['USERID'] + ';'
##        cnxnstring += 'PWD=' + INISETTINGS['PASSWORD'] + ';'
####        self.cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=CHRISTOPHER-PC\SQLEXPRESS;DATABASE=Eve;UID=shcooper;PWD=abcdeabcdeabcde')
##        self.cnxn = pyodbc.connect(cnxnstring)
##        self.cursor = cnxn.cursor()
        self.initializeLogging()
        msg = 'grabbing file data at ' + dirpath
        print('grabbing file data at ', dirpath)
        self.writeToLog(msg)
        filenames = self.getfiles()
        sqllinelist = []
##        div = 3*len(filenames)
##        inc = int(self.p['maximum']/div)
##        track = 0
##        inc2 = int(self.p['maximum']/3)
        
        for filename in filenames:
            tablename, batchdate = self.filenameparser(filename)
            sqllines, tablename = self.readfilesetimport(filename,
                                                         tablename, batchdate)
            print(sqllines)  
            print(tablename)                                           
            if not self.tableexist(self.cursor, tablename):
                typecont, colids = self.returntypescsvlinedata(filename)
                print(len(typecont),len(colids))
                print(typecont, colids)
                sqltypecont = self.setsqltypecont(typecont)
                self.settabledrop(tablename, sqltypecont, colids, self.cursor)        
            sqllinelist.append(sqllines)
##            track += inc
##            self.p['value'] = track
            msg = 'Importing: ' + filename
            self.writeToLog(msg)
            print('Importing: ' , filename)
        #self.writesqlfile(sqllinelist)
##        self.p['value'] += inc2
        msg = 'Writing sql file.'
        self.writeToLog(msg)
##        self.p['value'] += inc2
        msg = 'Completing importation of Eve Market data.'
        self.writeToLog(msg)
        self.executesqllist(sqllinelist, self.cursor)
##        self.win.destroy()
##renamefilestobackup()
cnxn = pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};SERVER=localhost;DATABASE=testdb;UID=root;PWD=****')
cursor = cnxn.cursor()
eimporter = Evesqlimporter(INISETTINGS,cursor,cnxn)
eimporter.run()
##if not len(filenames) == 0:
##    filepath = filenames[0]
##    typecont, colids = returntypescsvlinedata(filepath)
##    print(len(typecont),len(colids))
##    print(typecont, colids)
##    sqltypecont = setsqltypecont(typecont)
##    settabledrop(tablename, sqltypecont, colids, cursor)