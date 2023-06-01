###############################################################################
# Script by rawad.kharma@wdc.com
# Reliability team tefen,il
###############################################################################
# test duration 
# Ver Beta 0.0.1

import os,sys
from datetime import *
import pytz
import pypyodbc
import csv
import json
import xlrd



def reLog(relMessage, mode="a+"):
    fLOG = open("run_log.txt", mode)
    fLOG.write("\n%s" % datetime.now().strftime("%d/%m/%Y %H:%M:%S") )
    if (type(relMessage) != str) & (type(relMessage) != unicode) & (len(relMessage) > 1):
        print( relMessage[0] )
        if("Error" in relMessage[0]): print("[REL Info] Check run_log.txt")
        fLOG.write("%s " % relMessage[0] )
        relMessage.pop(0)
        for m in relMessage:
            fLOG.write("\n\t%s"%( str(m) ) )
    else:
        print(str(relMessage))
        fLOG.write("\t%s" % relMessage )
    fLOG.close()
def DBConnect(uid, pwd, server, db):
    '''
    # config and INIT DB connection
    return: OBJ database connection cursor
    '''
    try:
        strConnection = "Driver={ODBC Driver 18 for SQL Server};Server=%s;uid=%s;pwd=%s; database=%s; Encrypt=No" % (str(server),uid,pwd,db)
        reLog("[INFO] connection string: %s" % strConnection)
        connection = pypyodbc.connect(strConnection)
        connection.unicode_results = True
        return connection.cursor()
    except Exception as SQLServerException:
        reLog(["[REL Error] DB connection failed", SQLServerException, strConnection])

def getEstimatedTime():
    JsonEstimatedTime = json.loads('{}')#"TestEstimatedTime":[]
    try:
        locExcelFile = ("REL_Estimated_Time.xls")
    except Exception as Ex:
        reLog(["[REL Error] can not find the REL_Estimated excel file!"])
        return False
    finally:
        WorkBook = xlrd.open_workbook(locExcelFile)
        sheet = WorkBook.sheet_by_index(0)
        for i in range(sheet.nrows):
            if i > 0: #skip first row "header"
                JsonEstimatedTime.update({str(sheet.cell_value(i,4)):str(sheet.cell_value(i,5))})
    return JsonEstimatedTime
    


def executeQuery(Cycle=None, Adapter=None, DUTID=None):
    testEstimatedTime = getEstimatedTime()

    SQL = """
    SELECT 
        [CycleDefinition].[CycleID] as [CycleID],
        [Executions].[ID] as [ExecutionID],
        [Cycles].[FirmwareVersion],
        [Executions].[DUTSerial],
        [Executions].[PlatformProperties],
        [TestPlans].[Description] as [TestDescription],
        [Cycles].[Description] as [CycleDescription],
        [results].[Name] as [results],
        [StartDateTime],
        [EndDateTime],
        [Executions].[Capacity],
        [Executions].[DeviceType]
        FROM [dbo].[Executions] [Executions]
        join [dbo].[CycleDefinition] [CycleDefinition] on [Executions].[cycledefinitionid] = [CycleDefinition].[id]
        join [dbo].[Cycles] [Cycles] on [Cycles].[id] = [CycleDefinition].[CycleID]
        join [dbo].[EnumResults] [results] on [Executions].[Result] = [results].[id]
        join [dbo].[TestPlans] [TestPlans]  on [CycleDefinition].[TestID] = [TestPlans].[ID]
    """
    if (Cycle):
        SQL += "\nWHERE [CycleDefinition].[CycleID] = '%s' " % Cycle if type(Cycle) is str  else "\nWHERE [CycleDefinition].[CycleID] in (%s) " % (','.join(Cycle))
        reLog("[INFO] props ^Cycles: %s" % str(Cycle))
    if (Adapter):
        op = "AND" if Cycle else "WHERE" 
        SQL += "\n" + op + " [Executions].[PlatformProperties] = '%s' " % Adapter if type(Adapter) is str else "\n" + op + " [Executions].[PlatformProperties] in (%s) " % (','.join(Adapter))
        reLog("[INFO] props ^Adapters: %s" % str(Adapter))
    if (DUTID):
        op = "AND" if Cycle else "WHERE"
        if (op == "WHERE"):
            op = "AND" if Adapter else "WHERE"
        SQL += "\n" + op + " [Executions].[DUTSerial] = '%s' " % DUTID if type(DUTID) is str else "\n" + op + " [Executions].[DUTSerial] in ('%s') " % ("','".join(DUTID))
        reLog("[INFO] props ^DUTIDs: %s" % str(DUTID))

    reLog([("[INFO] running Query params:\n\t %s %s %s " % (Cycle,Adapter,DUTID))])
    reLog([("[REL Info] running query:"),(SQL)])

    print("\n\n\t\t PLEASE WAIT!!!!!")

    try:
        odbcCourser.execute(SQL)
    except Exception as SQLQueryException:
        reLog("[REL Error] failed running sql Query !! \n %s" % SQLQueryException)
        sys.exit(1)
    else:
        resExecutions = odbcCourser.fetchall()

    results = {}
    for row in resExecutions:
        try:
            # if the DUTID in results object
            if row[3] in results.keys():
                if row[9]:
                    duration = (row[9] + timedelta(hours=2)) - \
                        (row[8] + timedelta(hours=2)) + \
                        results[row[3]][11]
                else:
                    duration = datetime.now() - \
                    (row[8] + timedelta(hours=2)) + \
                    results[row[3]][11]
            # if DUTID not in results object "new execution!!"
            else:
                if row[9] != None:
                    duration = (row[9] + timedelta(hours=2)) - (row[8] + timedelta(hours=2))
                else:
                    duration = datetime.now() - (row[8] + timedelta(hours=2))
            if str(row[5]) not in testEstimatedTime:
                reLog([("[REL Exception] \n\t%s not in REL_Estimated_Time.xls" % str(row[5]) ),("\t\t row %s " % str(row))])
                EstimatedDelta = 0
            else:
                EstimatedDelta = timedelta(hours = float(testEstimatedTime[str(row[5])])) - duration 
            results[row[3]] = [ row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                row[7], row[10], row[11], row[8] + timedelta(hours=2),
                duration,EstimatedDelta ]

        except Exception as e:
            reLog([("[REL Exception] \n\t%s" % e),("\t\t row %s " % str(row))])

    with open("TestDurationReport.csv", 'wb') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(["CycleID","ExecutionID","FirmwareVersion","DUTSerial","PlatformProperties","TestDescription","CycleDescription","results","Capacity","DeviceType","Started","Duration","EstimatedDelta"])
        try:
            for res in results:
                writer.writerow(results[res])
        except Exception as CSVWriteException:
            reLog([
                ("[REL CSVWriteException] \n\t%s" % CSVWriteException),
                ("\t\t row %s " % str(row))
            ])
    reLog("[INFO] number of rows returned from DataBase: %s" % str(len(resExecutions)))
    reLog("[INFO] DONE")

#######################################################################################################################################
#######################################################################################################################################
#######################################################################################################################################

if __name__ == "__main__":
    ####################################################################
    # INIT script 
    ####################################################################
    # reset the execution log file
    reLog("[REL Info] new execution started", mode="w")
    # getEstimatedTime()
    # sys.exit(0)
    # check for env vars
    try:
        os.environ['ReliabilityGrid']
    except Exception as RelException:
        reLog(["[REL Error] Reliability Grid not set !! ", RelException])
        sys.exit(1)
    # init data base connection 
    # WDIL ip: 10.24.8.188
    # WDIN ip: 10.206.65.131
    try:
        odbcCourser = DBConnect(uid='Ace', pwd='Ace2018!', server='10.24.8.188', db='%s' % os.environ['ReliabilityGrid'] )
    except Exception as RelException:
        reLog(["[REL Error] DataBase connection INIT Failed!! ", RelException])
        sys.exit(1)
    

    try:
        if not os.environ.get('Cycles') and not os.environ.get('Adapter') and not os.environ.get('DUTID'):
            reLog(["[REL Error] need to set minimum one param ","set Cycle or Adapter or DUTID"])
            sys.exit(1)
    except Exception as e:
        reLog(["[REL Error] set Cycle or Adapter or DUTID !!",e])
        sys.exit(1)
    else:
        ### WDIN_REL_Side_A_1005 / REL_Side_A_39 / REL_Side_B_49
        ### CycleID  single string [123] or list [123, 456]
        CycleIDs = None
        if os.environ.get('Cycles'):
            CycleIDs = [x.strip() for x in os.environ.get('Cycles').split(',')]
            CycleIDs = CycleIDs if len(CycleIDs) > 1 else str(CycleIDs[0])
        ### Adapter  single string [FLX] or list [FLX12345, FLX12346]
        Adapters = None
        if os.environ.get('Adapter'):
            Adapters = os.environ.get('Adapter').split(',')
            Adapters = Adapters if len(Adapters) > 1 else str(Adapters[0])
        ### DUT_ID   single string [0X0] or list [0X001234, 0X005678]
        DUTIDs = None
        if os.environ.get('DUTID'):
            DUTIDs = [x.strip() for x in os.environ.get('DUTID').split(',')]
            DUTIDs = DUTIDs if len(DUTIDs) > 1 else str(DUTIDs[0])

        executeQuery(CycleIDs,Adapters,DUTIDs )
    ####################################################################
