import os
import datetime
import pyodbc
import logging

importdir = "C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)"

INISETTINGS = {'DIRPATH':importdir, 'BACKUPPATH': '',
               'SERVER': '', 'DATABASE': '', 'USERID': '', 'PASSWORD':''}

class Evedbimporter:

    def getfiles(self):
        filenames = []
        global INISETTINGS
        dirpath = INISETTINGS['DIRPATH']
        # for filename in os.listdir(dirpath):
        #     if filename.endswith('.txt'):
        #         filenames.append(dirpath + '/' + filename)
        for root, dirs, files in os.walk(importdir):
            for file in files:
                if file.endswith(".yaml"):
                    ##print(os.path.join(root, file))
                    filenames.append(os.path.join(root, file))
        return filenames

    def setsqltypecont(self, typecont):
        sqltypecont = []
        for typei in typecont:
            if type(typei) is int:
                sqltypecont.append('bigint')
            elif type(typei) is str:
                sqltypecont.append('nvarchar(500)')
            elif type(typei) is bool:
                sqltypecont.append('bit')
            elif type(typei) is float:
                sqltypecont.append('float')
       # print(sqltypecont)
        return sqltypecont

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

    def testInt(self, sobj):
        val = self.converttype(sobj)
        if type(val) is int:
            return True
        return False

    def teststr(self , sobj):
        val = self.converttype(sobj)
        if type(val) is str:
            return True
        return False

    def testhlead(self, line):
        if '-' in line:
            lined = line.split('-')
            if lined[0] == '':
                return True
        return False

    def settabledrop(self, tablename, sqltypecont, colids, dropTrue = False):
        tablexist = True
        try:
            execline = "Select * from "
            execline += tablename
            self.cursor.execute(execline)
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
                    elif 'range' == colid:
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
                    writeline += sqltypecont[index]
                    writeline += ");"                
                index += 1
    ##        writeline += "CONSTRAINT "
    ##        writeline += tablename
    ##        writeline += "_PK_PRIMARY_KEY_CLUSTERED ("
    ##        writeline += colids[0]
    ##        writeline += "))"
            #writeline += ' ENGINE=testdb;'
            print(writeline)    
            self.cursor.execute(writeline)
            self.cnxn.commit()
            print('Added table successfully!')
        else:
            print('table exists.')
            if dropTrue:
                print('Dropping table.')
                self.cursor.execute('DROP TABLE ' + tablename + ';')
                self.cnxn.commit()
                print('Dropped table.')

    def insertDatatoTable(self, tablename, colitems):
        def escapeSingleQuote(val):
            if "'" in val:
                return val.replace("'", "''")
            return val
        
        for colitem in colitems:
            i = 0
            writeline = "INSERT INTO " + tablename + "("
            for key in colitem:
                writeline += str(key)
                if i < len(colitem) - 1:
                    writeline += ","
                else:
                    writeline += ")"
                i+=1
            i = 0
            writeline += " VALUES('"
            for key in colitem:
                
                if colitem[key] == 'True':
                    writeline += "b'1',"
                elif colitem[key] == 'False':
                    writeline += "b'0',"
                else:
                    if str(colitem[key]) != "''":
                        rval = escapeSingleQuote(str(colitem[key]))
                        writeline += rval 
                if i < len(colitem)-1:
                    writeline += "','"
                else:
                    writeline += "');"
                i+=1
            print(writeline)    
            self.cursor.execute(writeline)
            self.cnxn.commit()
            

    def readYamlfile(self, filename):
        def checkID(val):
            if " " in val.lstrip().strip():
                return True
            return False
        def checkIndentLevel(val, targetlevel, addcount = 0):
            ilvl = len(val)+addcount - len(val.lstrip())
            if ilvl - targetlevel > 0:
                return True
            return False
        def getTargetilevel(val, addcount = 0):
            return len(val)+addcount - len(val.lstrip())

        print (filename)
        file = open(filename, 'rU')
        colids = []
        colitems = []
        colidsDone = False
        i = 0
        noReadlist = ["C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)\\sde\\bsd\\trnTranslations.yaml",
                     "C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)\\sde\\fsd\\categoryIDs.yaml",
                     "C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)\\sde\\fsd\\groupIDs.yaml",
                     "C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)\\sde\\fsd\\typeIDs.yaml",
                     "C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)\\sde\\bsd\\chrAncestries.yaml",
                     "C:\\Users\\chris\\Downloads\sde-20190625-TRANQUILITY (1)\\sde\\bsd\\chrAttributes.yaml",
                     "C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)\\sde\\bsd\\chrBloodlines.yaml",
                     "C:\\Users\\chris\\Downloads\sde-20190625-TRANQUILITY (1)\\sde\\bsd\\chrFactions.yaml",
                     "C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)\\sde\\fsd\\tournamentRuleSets.yaml"]
        if filename in noReadlist:
            return [[],[],0]
        targetlevel = 0
        for line in file:
            
            if self.testhlead(line) and i == 0:
                linedat = line.split('-')
                targetlevel = getTargetilevel(linedat[1], 1)
                rID = linedat[1].split(':')[0].lstrip()
                if self.testInt(rID):
                    return [[],[], 0]
                ritem = linedat[1].split(':')[1].lstrip().strip()
                print(ritem)
                print(targetlevel)
                colids.append(rID)
                colitems.append({rID: ritem}) 
            elif self.testhlead(line) and not colidsDone:
                linedat = line.split('-')
               # print(linedat)
                rID = linedat[1].split(':')[0].lstrip()
                ritem = linedat[1].split(':')[1].lstrip().strip()
                #colids.append(rID)
                colitems.append({rID: ritem})
                colidsDone = True
            elif self.testhlead(line) and colidsDone:
                linedat = line.split('-')
               # print(linedat)
                rID = linedat[1].split(':')[0].lstrip()
                ritem = linedat[1].split(':')[1].lstrip().strip()
                #colids.append(rID)
                colitems.append({rID: ritem})                 
            elif not self.testhlead(line) and not colidsDone:
                if checkIndentLevel(line, targetlevel):
                    continue
                if ':' not in line:
                    continue

                rID = line.split(':')[0].lstrip()
                if checkID(rID):
                    continue
                ritem = line.split(':')[1].lstrip().strip()
                if not self.teststr(rID):
                    print(rID)
                    break
                    #return [[],[]]
                colids.append(rID)
                colitems[len(colitems)-1][rID] = ritem 
            else:
                if checkIndentLevel(line, targetlevel):
                    continue
                if ':' not in line:
                    continue
                rID = line.split(':')[0].lstrip()
                if checkID(rID):
                    continue
                #colids.append(rID)
                ritem = line.split(':')[1].lstrip().strip()
                if not self.teststr(rID):
                    print(rID)
                    break
                    #return [[],[]]
                #colids.append(rID)
                colitems[len(colitems)-1][rID] = ritem                                
            i+=1
        maxcol = 0
        maxindx = []
        if len(colitems) > 0:
            maxinds = colitems[0]
        i = 0 
        for colitem in colitems:
            for key in colitem:
                if key not in colids:
                    colids.append(key)
                    maxinds[key] = colitem[key]
            # if len(colitem) > maxcol:
            #     maxcol = len(colitem)
            #     maxind = i
            #     ncolids = []
            #     for key in colitem:
            #         ncolids.append(key)
            #     colids = ncolids
            i += 1
        print(colids)
        #print(colitems)
        return (colids,colitems, maxinds)

    def setINISETTINGS(self, settings):
        global INISETTINGS
        for setting in settings:            
            INISETTINGS[setting] = settings[setting]
        global dirpath
        
        dirpath = INISETTINGS['DIRPATH']

    def setup(self, dropTrue = False, skipInsert = False):
        filenames = self.getfiles()
        colids = []
        for filename in filenames:
            tbn = filename.split('\\')
            tablename = tbn[len(tbn)-1].split('.yaml')[0]
            colids, colitems, colindex = self.readYamlfile(filename)
            if len(colids) == 0:
                continue
            typecont = []
            tcolids = colindex
            for colid in tcolids:
                colobj = self.converttype(tcolids[colid])
                typecont.append(colobj)
            print(typecont)
            sqltypecont = self.setsqltypecont(typecont)
            print(sqltypecont)
            self.settabledrop(tablename, sqltypecont, colids, dropTrue)
            print(colids)
            if skipInsert:
                continue
            self.insertDatatoTable(tablename, colitems)
        #print(colids)

    def __init__(self, settings, cursor, cnxn):
        print('checking sql database.')
        print('settings: ', settings)
        self.setINISETTINGS(settings)
        self.cnxn = cnxn
        self.cursor = cursor
        print (self.getfiles())

cnxn = pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};SERVER=localhost;DATABASE=testdb;UID=root;PWD=****')
cursor = cnxn.cursor()
eimporter = Evedbimporter(INISETTINGS,cursor,cnxn)
eimporter.setup()

