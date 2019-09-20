import os
import datetime
import pyodbc
import logging

importdir = "C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)\\sde\\fsd\\"
filestoadd = ['groupIDs.yaml']#'graphicIDs.yaml']#'certificates.yaml']#'blueprints.yaml']##"typeIDs.yaml"]
languagetoimport = "en"

INISETTINGS = {'DIRPATH':importdir, 'BACKUPPATH': '',
               'SERVER': '', 'DATABASE': '', 'USERID': '', 'PASSWORD':''}

class Evedbimporter2:

    def getfiles(self):
        filenames = []
        global INISETTINGS
        global filestoadd
        dirpath = INISETTINGS['DIRPATH']
        for filename in filestoadd:
            filenames.append(dirpath + filename)
        # for filename in os.listdir(dirpath):
        #     if filename.endswith('.txt'):
        #         filenames.append(dirpath + '/' + filename)
        # for root, dirs, files in os.walk(importdir):
        #     for file in files:
        #         if file.endswith(".yaml"):
        #             ##print(os.path.join(root, file))
        #             filenames.append(os.path.join(root, file))

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

    def testhlead(self, line, itera = 0):
        # if '-' in line:
        #     lined = line.split('-')
        #     if lined[0] == '':
        #         return True
        splitf = line.split('\n')
        splitl = splitf[0].split(' ')
        ##litera = list(range(0,70))


        if splitl[0] != '' and splitl[0] != "'":
            return True
        # if itera in litera:
        #     print(splitl)
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
            

    def readYamlfile(self, filename, readflag = 0):
        def strInttest(val):
            try:
                nval = int(val)
                return True
            except:
                return False

        def hyphentest(val):
            if '- ' in val.lstrip():
                ls = val.split('- ')[0]
                if ls.lstrip().strip() == '':
                    return True
            return False

        def getleadID(val):
            lsplit = val.split(' ')
            for lsp in lsplit:
                if lsp == '':
                    continue
                return lsp.split(':')[0].strip()

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
        
        def getLevelDict(keyIDs, colitems):
            colitemd = colitems[len(colitems)-1]
            for keyid in keyIDs:
                colitemd = colitemd[keyid]
            return colitemd

        def sethyphendict(wdict, rID, ritem, newEntry = False):
            
            if newEntry:
                wdict[len(wdict)] = {rID:ritem}
            else:
                wdict[len(wdict)-1][rID] = ritem

        def sethyphendict2(wdict, rID, ritem, newEntry = False):
            
            if newEntry:
                wdict[len(wdict)] = {'typeID': rID}
            else:
                wdict[len(wdict)-1][rID] = ritem

        def sethyphendict3(wdict, rID, ritem, newEntry = False):
                
            if newEntry:
                wdict[len(wdict)] = rID
            else:
                wdict[len(wdict)-1][rID] = ritem

        def setkeyIDs(keyIDs, linev, targetlevel, hlevel):
            linelvl = getTargetilevel(linev)
            if targetlevel - linelvl > 0:
                ntlvl = targetlevel
                ntlvl -= 4
                nkeyIDs = keyIDs[0: len(keyIDs)-1]
                return [ntlvl, nkeyIDs, False]
            return [targetlevel, keyIDs, hlevel]

        def setkeyIDs2(keyIDs, linev, targetlevel, hlevel):
            linelvl = getTargetilevel(linev)
            if targetlevel - linelvl > 0:
                ntlvl = targetlevel
                ntlvl -= (targetlevel - linelvl)
                nkeyIDs = keyIDs[0: len(keyIDs)-1]
                return [ntlvl, nkeyIDs, False]
            return [targetlevel, keyIDs, hlevel]
        
        def buildSingleLevelEntries(colitemd, writed = [{}], ikey = '', lastlevel = 0):
            for key in colitemd:
                if type(colitemd[key]) == dict:
                    if type(key) == str:
                        newikey = ikey
                        if ikey == '':
                            newikey = key
                        else:
                            newikey += '_'+key
                        buildSingleLevelEntries(colitemd[key], writed,newikey, 1)
                    elif type(key) == int:
                        ndict = colitemd[key]
                        i = 0
                        for nkey in ndict:
                            nikey = ikey + '_'+ nkey
                            n2dict = {}
                            if i == 0:
                                n2dict = writed[0].copy()
                            else:
                                n2dict = writed[len(writed)-1].copy()
                            n2dict[nikey] = ndict[nkey]
                            if i == len(ndict)-1:
                                writed[len(writed)-1] = n2dict
                            else:
                                writed.append(n2dict)
                            i +=1
                else:
                    wkey = ikey 
                    if lastlevel == 0:
                        wkey = key
                    else:
                        #ikey += '_'+key
                        wkey = ikey + '_' + key
                    writed[0][wkey] = colitemd[key]

        def returnIncludes(wdict, excludes):
            rwdict = {}
            for key in wdict:
                if not key in excludes:
                    rwdict[key] = wdict[key]
            return rwdict

        def buildSingleLevelEntries2(colitemd, writed = [{}], ikey = '', lastlevel = 0, excludes = ['description', 'name', 'groupID','recommendedFor_typeID']):
            for key in colitemd:
                if type(colitemd[key]) == dict:
                    if type(key) == str:
                        t1 = strInttest(key)
                        if t1:
                            #print('hit')
                            newikey = ikey
                            newikey += '_'+'typeID'
                            newdict = writed[0].copy()
                            newdict = returnIncludes(newdict, excludes)
                            newdict[newikey] = key
                            writed.append(newdict)
                            buildSingleLevelEntries2(colitemd[key], writed,newikey, 2)
                        else:
                            newikey = ikey
                            if ikey == '':
                                newikey = key
                            else:
                                newikey += '_'+key
                            buildSingleLevelEntries2(colitemd[key], writed,newikey, 1)
                    elif type(key) == int:
                        ndict = colitemd[key]
                        i = 0
                        for nkey in ndict:
                            nikey = ikey + '_'+ nkey
                            n2dict = {}
                            if i == 0:
                                n2dict = writed[0].copy()
                            else:
                                n2dict = writed[len(writed)-1].copy()
                            n2dict[nikey] = ndict[nkey]
                            if i == len(ndict)-1:
                                writed[len(writed)-1] = n2dict
                            else:
                                writed.append(n2dict)
                            i +=1
                else:
                    wkey = str(ikey) 
                    if lastlevel == 0:
                        wkey = key
                    else:
                        #ikey += '_'+key
                        wkey = ikey + '_' + str(key)
                    if lastlevel == 2:
                        writed[len(writed)-1][wkey] = colitemd[key]
                    else:
                        writed[0][wkey] = colitemd[key]

        print (filename)
        file = open(filename, encoding='utf-8', mode='rU')
        colids = []
        colitems = []
        colidsDone = False
        i = 0
        noReadlist = ["C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)\\sde\\bsd\\trnTranslations.yaml",
                     "C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)\\sde\\fsd\\categoryIDs.yaml",
                     "C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)\\sde\\bsd\\chrAncestries.yaml",
                     "C:\\Users\\chris\\Downloads\sde-20190625-TRANQUILITY (1)\\sde\\bsd\\chrAttributes.yaml",
                     "C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)\\sde\\bsd\\chrBloodlines.yaml",
                     "C:\\Users\\chris\\Downloads\sde-20190625-TRANQUILITY (1)\\sde\\bsd\\chrFactions.yaml",
                     "C:\\Users\\chris\\Downloads\\sde-20190625-TRANQUILITY (1)\\sde\\fsd\\tournamentRuleSets.yaml"]
        if filename in noReadlist:
            return [[],[],0]
        targetlevel = 0
        hlevel = False
        lastID = ''
        lastIDs = []
        for line in file:
            
            if self.testhlead(line) and i == 0:

                rID = getleadID(line)
                # if self.testInt(rID):
                #     return [[],[], 0]
                if not readflag == 3:
                    targetlevel = 4
                else:
                    targetlevel = 2
                ##print(ritem)
                print(targetlevel)
                colids.append("typeID")
                colitems.append({"typeID": rID})
                #lastIDs.append(rID) 
            elif self.testhlead(line) and not colidsDone:
                rID = getleadID(line)
                if not readflag == 3:
                    targetlevel = 4
                else:
                    targetlevel = 2
               # print(linedat)
                hlevel = False
                #colids.append(rID)
                colitems.append({"typeID": rID})
                colidsDone = True
                lastIDs = []
            elif self.testhlead(line) and colidsDone:
                rID = getleadID(line)
                if not readflag == 3:
                    targetlevel = 4
                else:
                    targetlevel = 2
               # print(linedat)
                #rID = linedat[1].split(':')[0].lstrip()
                #ritem = linedat[1].split(':')[1].lstrip().strip()
                #colids.append(rID)
                hlevel = False
                colitems.append({"typeID": rID})
                lastIDs = []                 
            elif not self.testhlead(line) and not colidsDone:
                rflist = [2,3,4]
                if not readflag in rflist:
                    if ':' not in line:
                        continue
                lr = line.replace(' - ', '   ')
                rID = lr.split('\n')[0].split(':')[0].lstrip()
                if not readflag in rflist:
                    if checkID(rID):
                        continue
                ritem = ''
                if len(line.split(':')) > 1:
                    ritem = line.split('\n')[0].split(':')[1].lstrip().strip()                
                if (readflag == 1):  ## reading blueprints
                    ## set target level and keyIDs
                    
                    targetlevel, lastIDs, hlevel = setkeyIDs(lastIDs, lr, targetlevel, hlevel)
                    dwrite = getLevelDict(lastIDs, colitems)
                    
                    if ritem == '':
                        lastIDs.append(rID)
                        dwrite[rID] = {}
                        targetlevel += 4
                    else:
                        if hyphentest(line):
                            sethyphendict(dwrite, rID, ritem, True)
                            hlevel = True
                        elif hlevel and not hyphentest(line):
                            sethyphendict(dwrite, rID, ritem)
                        elif not hlevel and not hyphentest(line):
                            dwrite[rID] = ritem
                elif (readflag == 2):  ## reading blueprints
                    ## set target level and keyIDs

                    targetlevel, lastIDs, hlevel = setkeyIDs2(lastIDs, lr, targetlevel, hlevel)
                    dwrite = getLevelDict(lastIDs, colitems)
                    tvar = ''
                    if len(lastIDs) > 0:
                        tvar = lastIDs[len(lastIDs)-1]
                    print(targetlevel)
                    if ritem == '' and not rID == 'description' and not tvar == 'description' and not tvar == 'recommendedFor':
                        lastIDs.append(rID)
                        dwrite[rID] = {}
                        if rID == 'recommendedFor':
                            targetlevel += 2
                        else:
                            targetlevel += 4
                    elif ritem != '' and rID  == 'description':
                        print('hit')
                        lastIDs.append(rID)
                        dwrite[rID] = ''
                        targetlevel += 4
                        print(targetlevel)
                        print(dwrite)
                    else:
                        if hyphentest(line):
                            sethyphendict2(dwrite, rID, ritem, True)
                            hlevel = True
                        elif hlevel and not hyphentest(line):
                            sethyphendict2(dwrite, rID, ritem)
                        elif not hlevel and not hyphentest(line):
                            print(lastIDs)
                            if not tvar == 'description':
                                dwrite[rID] = ritem
                            else:
                                dwrite += rID
                                getLevelDict(lastIDs[0:len(lastIDs)-1],colitems)['description'] = dwrite

                elif (readflag == 3):  ## reading blueprints
                    ## set target level and keyIDs

                    targetlevel, lastIDs, hlevel = setkeyIDs2(lastIDs, lr, targetlevel, hlevel)
                    dwrite = getLevelDict(lastIDs, colitems)
                    tvar = ''
                    tvarlist = ['description']
                    if len(lastIDs) > 0:
                        tvar = lastIDs[len(lastIDs)-1]                    
                    if ritem == '' and not rID == 'description' and not tvar in tvarlist and not hyphentest(line):
                        lastIDs.append(rID)
                        dwrite[rID] = dict()
                        targetlevel += 2
                       
                    else:
                        if hyphentest(line):
                            sethyphendict3(dwrite, rID, ritem, True)
                            hlevel = True
                            # print(rID)
                            # print(ritem)
                            # print(targetlevel)
                        elif hlevel and not hyphentest(line):
                            sethyphendict3(dwrite, rID, ritem)
                        elif not hlevel and not hyphentest(line):
                            if not tvar == 'description':
                                tid = ['graphicFile', 'folder']
                                if rID in tid:
                                    ritem = line.split('\n')[0].split(':')[1].lstrip().strip()+':'+line.split('\n')[0].split(':')[2].lstrip().strip()
                                dwrite[rID] = ritem
                                if rID == 'description':
                                    lastIDs.append(rID)
                                    targetlevel += 2                                
                            else:
                                dwrite += rID
                                getLevelDict(lastIDs[0:len(lastIDs)-1],colitems)['description'] = dwrite                

                else:
                    if checkIndentLevel(line, targetlevel):
                        continue
                    if ritem == '' and rID != 'masteries':
                        lastID = rID
                        targetlevel = 8
                    if rID == 'masteries':
                        continue
                
                    if targetlevel == 8:
                        if rID == 'en':
                            rID = lastID
                            targetlevel = 4
                        else:
                            continue
                    if not self.teststr(rID):
                        print(rID)
                        break
                        #return [[],[]]
                    colids.append(rID)
                    colitems[len(colitems)-1][rID] = ritem 
            else:
                rflist = [2,3,4]
                if not readflag in rflist:
                    if ':' not in line:
                        continue
                lr = line.replace(' - ', '   ') ## 
                rID = lr.split('\n')[0].split(':')[0].lstrip()
                if not readflag in rflist:
                    if checkID(rID):
                        continue
                #colids.append(rID)
                ritem = ''
                if len(line.split(':')) > 1:
                    ritem = line.split('\n')[0].split(':')[1].lstrip().strip()
                if (readflag == 1):  ## reading blueprints
                    ## set target level and keyIDs

                    targetlevel, lastIDs, hlevel = setkeyIDs(lastIDs, lr, targetlevel, hlevel)
                    dwrite = getLevelDict(lastIDs, colitems)
                    
                    if ritem == '':
                        lastIDs.append(rID)
                        dwrite[rID] = {}
                        targetlevel += 4
                    else:
                        if hyphentest(line):
                            sethyphendict(dwrite, rID, ritem, True)
                            hlevel = True
                        elif hlevel and not hyphentest(line):
                            sethyphendict(dwrite, rID, ritem)
                        elif not hlevel and not hyphentest(line):
                            dwrite[rID] = ritem
                elif (readflag == 2):  ## reading blueprints
                    ## set target level and keyIDs

                    targetlevel, lastIDs, hlevel = setkeyIDs2(lastIDs, lr, targetlevel, hlevel)
                    dwrite = getLevelDict(lastIDs, colitems)
                    tvar = ''
                    if len(lastIDs) > 0:
                        tvar = lastIDs[len(lastIDs)-1]                    
                    if ritem == '' and not rID == 'description' and not tvar == 'description' and not tvar == 'recommendedFor':
                        lastIDs.append(rID)
                        dwrite[rID] = {}
                        if rID == 'recommendedFor':
                            targetlevel += 2
                        else:
                            targetlevel += 4
                    elif ritem != '' and rID  == 'description':
                        lastIDs.append(rID)
                        dwrite[rID] = ''
                        targetlevel += 4
                    else:
                        if hyphentest(line):
                            sethyphendict2(dwrite, rID, ritem, True)
                            hlevel = True
                        elif hlevel and not hyphentest(line):
                            sethyphendict2(dwrite, rID, ritem)
                        elif not hlevel and not hyphentest(line):
                            if not tvar == 'description':
                                dwrite[rID] = ritem
                            else:
                                dwrite += rID
                                getLevelDict(lastIDs[0:len(lastIDs)-1],colitems)['description'] = dwrite
                elif (readflag == 3):  ## reading blueprints
                    ## set target level and keyIDs

                    targetlevel, lastIDs, hlevel = setkeyIDs2(lastIDs, lr, targetlevel, hlevel)
                    dwrite = getLevelDict(lastIDs, colitems)
                    tvar = ''
                    tvarlist = ['description']
                    if len(lastIDs) > 0:
                        tvar = lastIDs[len(lastIDs)-1]                    
                    if ritem == '' and not rID == 'description' and not tvar in tvarlist and not hyphentest(line):
                        lastIDs.append(rID)
                        dwrite[rID] = dict()
                        targetlevel += 2
                        #print(rID)
                    else:
                        if hyphentest(line):
                            sethyphendict3(dwrite, rID, ritem, True)
                            hlevel = True
                            # print(rID)
                            # print(targetlevel)
                        elif hlevel and not hyphentest(line):
                            # print(rID)
                            # print(line)
                            sethyphendict3(dwrite, rID, ritem)
                        elif not hlevel and not hyphentest(line):
                            if not tvar == 'description':
                                tid = ['graphicFile', 'folder']
                                if rID in tid:
                                
                                    ritem = line.split('\n')[0].split(':')[1].lstrip().strip()+':'+line.split('\n')[0].split(':')[2].lstrip().strip()
                                if rID == 'description':
                                    lastIDs.append(rID)
                                    targetlevel += 2
                                dwrite[rID] = ritem
                            else:
                                dwrite += rID
                                getLevelDict(lastIDs[0:len(lastIDs)-1],colitems)['description'] = dwrite
                else:
                    if checkIndentLevel(line, targetlevel):
                        continue
                    if ritem == '' and rID != 'masteries':
                        lastID = rID
                        targetlevel = 8
                    if rID == 'masteries':
                        continue
                    if not self.teststr(rID):
                        print(rID)
                        break
                    if targetlevel == 8:
                        if rID == 'en':
                            rID = lastID
                            targetlevel = 4
                        else:
                            continue
                        #return [[],[]]
                    #colids.append(rID)
                    colitems[len(colitems)-1][rID] = ritem                                
            i+=1
        maxcol = 0
        maxindx = []
        ncolitems = []
        rflist2 = [2,3]
        if readflag == 1:
            ## need to flatten dictionary values
            for colitem in colitems:
                writed = [{}]
                buildSingleLevelEntries(colitem,writed)
                ncolitems = ncolitems + writed
            colitems = ncolitems[0:len(ncolitems)]
        elif readflag in rflist2:
            for colitem in colitems:
                writed = [{}]
                buildSingleLevelEntries2(colitem,writed)
                ncolitems = ncolitems + writed
            colitems = ncolitems[0:len(ncolitems)]

        maxinds = []
        if len(colitems) > 0:
            maxinds = colitems[0].copy()
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
        readlist = ['blueprints']
        readlist2 = ['certificates']
        readlist3 = ['graphicIDs']
        readlist4 = ['groupIDs']
        for filename in filenames:
            tbn = filename.split('\\')
            tablename = tbn[len(tbn)-1].split('.yaml')[0]
            readFlag = 0
            if tablename in readlist:
                readFlag = 1
            elif tablename in readlist2:
                readFlag = 2
            elif tablename in readlist3:
                readFlag = 3
            elif tablename in readlist4:
                readFlag = 2
            colids, colitems, colindex = self.readYamlfile(filename,readFlag)
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
            print(len(colitems))
            print(colitems[0:10])#[len(colitems)-1])
            #print(colitems[0:110])
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

cnxn = pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};SERVER=localhost;DATABASE=testdb;UID=root;PWD=youpassword')
cursor = cnxn.cursor()
eimporter = Evedbimporter2(INISETTINGS,cursor,cnxn)
eimporter.setup()