###############################################################################
# Script by rawad.kharma@wdc.com
# Reliability team tefen,il
###############################################################################
# Get test devices and the LOT  
# Ver Beta 0.0.1

import os,sys
from datetime import datetime
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
        if sys.platform == "win32":
            strConnection = "Driver={SQL Server};Server=%s;uid=%s;pwd=%s; database=%s; Encrypt=No" % (str(server),uid,pwd,db)
            reLog("[INFO] connection string: %s" % strConnection)
            connection = pypyodbc.connect(strConnection)
        else:
            strConnection = "Driver={ODBC Driver 18 for SQL Server};Server=%s;uid=%s;pwd=%s; database=%s; Encrypt=No " % (str(server),uid,pwd,db)
            reLog("[INFO] connection string: %s" % strConnection)
            connection = pypyodbc.connect(strConnection)
            connection.unicode_results = True
        return connection.cursor()
    except Exception as SQLServerException:
        reLog(["[REL Error] DB connection failed", SQLServerException])
        sys.exit(1)
        

def executeQuery(Cycle=None, Adapter=None, DUTID=None):
    SQL = """SELECT MAX([dbo].[Cycles].[ID]) AS [CyclesID]
      ,[dbo].[TestPlans].[Description] AS [TestName]
      ,MAX([dbo].[Executions].[ID]) AS [ExecutionsID]
      ,[dbo].[Executions].[DUTSerial]
      ,[dbo].[Executions].[PlatformProperties]
      ,null as [FirmwareVersion]
      ,null as [stickerid]
      ,null as [siteID]
      ,null as [LOT]
      ,null as [Rel_project]
      ,null as [UROM_data]
      ,null as [IR]
      ,null as [Device_Capacity]

      FROM [dbo].[Cycles]
      LEFT JOIN [dbo].[CycleDefinition] ON [dbo].[CycleDefinition].[CycleID] = [dbo].[Cycles].[ID]
      LEFT JOIN [dbo].[Executions] ON [dbo].[Executions].[CycleDefinitionID] = [dbo].[CycleDefinition].ID
      LEFT JOIN [dbo].[TestPlans] ON [dbo].[CycleDefinition].[TestID] = [dbo].[TestPlans].[ID] """
    if (Cycle):
        SQL += "\nWHERE [dbo].[Cycles].[ID] = '%s' " % Cycle if type(Cycle) is str  else "\nWHERE [dbo].[Cycles].[ID] in (%s) " % (','.join(Cycle))
        reLog("[INFO] props ^Cycles: %s" % str(Cycle))
    if (Adapter):
        op = "AND" if Cycle else "WHERE" 
        SQL += "\n" + op + " [dbo].[Executions].[PlatformProperties] = '%s' " % Adapter if type(Adapter) is str else "\n" + op + " [dbo].[Executions].[PlatformProperties] in (%s) " % (','.join(Adapter))
        reLog("[INFO] props ^Adapters: %s" % str(Adapter))
    if (DUTID):
        op = "AND" if Cycle else "WHERE"
        op = "AND" if Adapter else "WHERE"
        SQL += "\n" + op + " [dbo].[Executions].[DUTSerial] = '%s' " % DUTID if type(DUTID) is str else "\n" + op + " [dbo].[Executions].[DUTSerial] in (%s) " % (','.join(DUTID))
        reLog("[INFO] props ^DUTIDs: %s" % str(DUTID))
    SQL += "GROUP BY [dbo].[Executions].[DUTSerial], [dbo].[Executions].[PlatformProperties], [TestPlans].[Description]"
    
    reLog([("[INFO] running Query params:\n\t %s %s %s " % (Cycle,Adapter,DUTID))])
    reLog([("[REL Info] running query:"),(SQL)])
    # odbcCourser.execute(SQL)
    print("\n\n\t\t PLEASE WAIT!!!!!")

    try:
        odbcCourser.execute(SQL)
    except Exception as SQLQueryException:
        reLog("[REL Error] failed running sql Query !! \n %s" % SQLQueryException)
        sys.exit(1)
    else:
        resExecutions = odbcCourser.fetchall()

    try:
        with open("GetLotLotReport.csv", 'wb') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow([column[0] for column in odbcCourser.description])
            for i in resExecutions:
                if i[3]:
                    odbcLOTCourser.execute("SELECT * FROM [dwh].[uidsticker] WHERE [dwh].[uidsticker].[uid] = '%s'" % i[3])
                    row = odbcLOTCourser.fetchone()
                    if row:
                        odbcCourser.execute("""
    SELECT [dbo].[Cycles].[FirmwareVersion] 
      FROM [dbo].[Cycles]
      LEFT JOIN [dbo].[CycleDefinition] ON [dbo].[CycleDefinition].[CycleID] = [dbo].[Cycles].[ID]
      LEFT JOIN [dbo].[Executions] ON [dbo].[Executions].[CycleDefinitionID] = [dbo].[CycleDefinition].ID
      LEFT JOIN [dbo].[TestPlans] ON [dbo].[CycleDefinition].[TestID] = [dbo].[TestPlans].[ID] 
       WHERE [dbo].[Executions].[ID] = '%s'
                                            """ % i[0])
                        fw_ver = odbcCourser.fetchone()[0]
                    else:
                        fw_ver = 'None'
                    try:
                        writer.writerow([i[0],i[1],i[2],i[3],i[4],fw_ver,row[2],row[3],row[4],row[5],row[6],row[7],row[8]])
                    except Exception as CSVWriteException:
                        reLog([("[REL CSVWriteException] \n\t%s" % CSVWriteException),("\t\t row %s " % str(row))])

    except Exception as SQLQueryException:
        reLog([("[REL Error] failed running sql Query !!"),(SQLQueryException)])
        sys.exit(1)

    reLog("[INFO] number of rows returned from DataBase: %s" % str(len(resExecutions)))
    reLog("[INFO] DONE")

#######################################################################################################################################
#######################################################################################################################################
#######################################################################################################################################

if __name__ == "__main__":
    ####################################################################
    # INIT script 
    ####################################################################
    # # reset the execution log file
    reLog("[REL Info] new execution started", mode="w")
    # getEstimatedTime()
    # sys.exit(0)
    # check for env vars
    try:
        os.environ['ReliabilityGrid']
    except Exception as RelException:
        reLog(["[REL Error] Reliability Grid not set !! ", RelException])
        sys.exit(1)
        
    # # init data base connection 
    # WDIL ip: 10.24.8.188
    # WDIN ip: 10.206.65.131
    try:
        odbcCourser = DBConnect(uid='Ace', pwd='Ace2018!', server='10.24.8.188', db='%s' % os.environ['ReliabilityGrid'] )
    except Exception as RelException:
        reLog(["[REL Error] DataBase connection INIT Failed!! ", RelException])
        sys.exit(1)
    try:
        odbcLOTCourser = DBConnect(uid='ETL', pwd='etl123!', server='10.24.8.170', db='CardBio_DWH' )
    except Exception as RelException:
        reLog(["[REL Error] DataBase connection INIT Failed!! ", RelException])
        sys.exit(1)
    
    ####################################################################
    ### startDate,  endDate   /  format '2020-12-21 00:00:00'
    try:
        if not os.environ.get('Cycles') and not os.environ.get('Adapter') and not os.environ.get('DUTID'):
            reLog(["[REL Error] need to set minimum one param ","set Cycle or Adapter or DUTID"])
            sys.exit(1)
    except Exception as e:
        reLog(["[REL Error] set Cycles or Adapter or DUTID !!",e])
        sys.exit(1)
    else:
        ### WDIN_REL_Side_A_1005 / REL_Side_A_39 / REL_Side_B_49
        ### CycleID  single string [123] or list [123, 456]
        CycleIDs = None
        if os.environ.get('Cycles'):
            CycleIDs = os.environ.get('Cycles').split(',')
            CycleIDs = CycleIDs if len(CycleIDs) > 1 else str(CycleIDs[0])
        ### Adapter  single string [FLX] or list [FLX12345, FLX12346]
        Adapters = None
        if os.environ.get('Adapter'):
            Adapters = os.environ.get('Adapter').split(',')
            Adapters = Adapters if len(Adapters) > 1 else str(Adapters[0])
        ### DUT_ID   single string [0X0] or list [0X001234, 0X005678]
        DUTIDs = None
        if os.environ.get('DUTID'):
            DUTIDs = os.environ.get('DUTID').split(',')
            DUTIDs = DUTIDs if len(DUTIDs) > 1 else str(DUTIDs[0])

        executeQuery(CycleIDs,Adapters,os.environ.get('DUTID',None) )
    ####################################################################
