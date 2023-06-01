###############################################################################
# Script by rawad.kharma@wdc.com
# Reliability team tefen,il
###############################################################################
# Cycles Dinamic TorboWrite report 
# Ver Beta 0.1
import os
from datetime import datetime
import pypyodbc
import csv

CSV_HEADER = ["CycleID","Test","ExecutionID","DUTSerial","DeviceType","Capacity","bufferSize","ExecStatus"]

CYCLES_SQL = """
      SELECT 
        [Executions].[ID]
        ,[CycleDefinition].[CycleID]  
        ,[Executions].DUTSerial
        ,[TestPlans].[Description]
        ,[Executions].[DeviceType]
        ,[Executions].[Capacity]
        ,[EnumExecutionStatuses].[Name] as [ExecStatus]
        FROM {automationDB}.[dbo].[Executions] [Executions]
        JOIN {automationDB}.[dbo].[CycleDefinition] [CycleDefinition] 
            ON [Executions].[cycledefinitionid] = [CycleDefinition].[id]
        JOIN {automationDB}.[dbo].[Cycles] [Cycles] 
            ON [Cycles].[id] = [CycleDefinition].[CycleID]
        left JOIN {automationDB}.[dbo].[TestPlans] [TestPlans]
            ON [CycleDefinition].TestID = [TestPlans].[ID]
        JOIN [dbo].[EnumExecutionStatuses] ON [dbo].[EnumExecutionStatuses].[Id] = [Executions].[Status]
        
        WHERE [CycleDefinition].[CycleID] in ({Cycles})
    """

BUF_SIZE_SQL = """
        SELECT
        [REL_TW_details].[device_id]
        ,[REL_TW_details].[buffer_size]
        ,[REL_TW_details].[execution_id]
        ,[REL_TW_details].[creation_date]
        FROM [REL].[dwh].[REL_TW_details]
        WHERE [device_id] = '{id}'
        ORDER By test_id DESC
    """

EXE_ID = 0
CYCLE_ID = 1
DUT_SERIAL = 2, 
TEST_DESCRIPTION = 3
DEVICE_TYPE = 4
CAPACITY = 5
EXE_STATUS = 6


def reLog(relMessage, mode="a+"):
    '''
    Reiability Logger
     . print and log single line messages 
     . OR
     . log long messages [] or {} and print short info
     to reset the log file init with mode="w"
    return: run_log.txt file in the same folder as the script
    '''
    fLOG = open("run_log.txt", mode)
    if (type(relMessage) != str) & (type(relMessage) != "<type 'unicode'>"):
        if(relMessage[0] == "[REL Info]"):
            print(str(relMessage[1]).strip())
        else:
            print("[REL Info] Check run_log.txt")
            for messageLine in relMessage:
                fLOG.write("\n%s" % datetime.now().strftime("%d/%m/%Y %H:%M:%S") )
                fLOG.write("\t%s"%( str(messageLine) ) )
    else:
        print(80*">")
        print(relMessage)
        print(80*"<")
        # fLOG.write("\t%s" % relMessage )
    fLOG.close()
    

def DBConnect(strConnectionArgs):
    '''
    # config and INIT DB connection

    return: OBJ database connection cursor

    '''
    try:
        connection = pypyodbc.connect(strConnectionArgs)
        return connection.cursor()
    except Exception as SQLServerException:
        reLog(("[REL Error] DB connection failed \n", SQLServerException))
        reLog(("[Connection String]",strConnectionArgs))
        print(SQLServerException)
        exit(1)

def NewDUTID(dutid, listCsv):
   for line in listCsv:
       if dutid == line[3]:
           return False
   return True

def filterNotConfigured(listCsv, listNotConfigured):
    listFiltered = []
    for line in listNotConfigured:
        boolFound = False
        for line2 in listCsv:
            if line[3] == line2[3]:
              boolFound = True
        if not boolFound:
            listFiltered.append(line)
    return listFiltered


def main(strCycles,strAutomation):
    listExe = []
    cyclesSQLExecutions = None
    # get cycles query
    cyclesSQL = CYCLES_SQL.format(Cycles=strCycles, automationDB=strAutomation)
    listMsg = [
        ("[INFO Query] running query on:\n\tCycles: %s \n\tAutomation DB: %s" % (strCycles, strAutomation)),
        ("[INFO SQL] %s" % cyclesSQL)
    ] 
    reLog(listMsg)

    print("\n\n\t\t PLEASE WAIT!!!!!")

    
    try:
        cyclesSQLCourser.execute(cyclesSQL)
    except Exception as SQLQueryException:
        reLog([("[REL Error]"),("failed running sql Query !!"),("\n %s" % SQLQueryException)])
    else:
        cyclesSQLExecutions = cyclesSQLCourser.fetchall()
        reLog([("[REL Debug]"),("count of executions found:"), ("%s" % str(len(cyclesSQLExecutions))) ])

    if cyclesSQLExecutions == None:
        reLog([
            ("[REL Error]"),(" failed running sql Query !! \n No Executions in Cycle"),
            ("Cycles String: %s" % str(strCycles)),
            ("Automation: %s" % str(strAutomation))
        ])
    # result format [ExecutionID, CycleID, DUTSerial, Test Description, DeviceType, Capacity, ExecStatus]
    #                   0           1       2               3               4           5        6

    csvDistenation = os.path.join(reportDistenation,"TW_report.csv")
    reLog([("[REL Info]"),("saving report to:> %s" % csvDistenation)])

    EXE_ID = 0
    CYCLE_ID = 1
    DUT_SERIAL = 2 
    TEST_DESCRIPTION = 3
    DEVICE_TYPE = 4
    CAPACITY = 5
    EXE_STATUS = 6

    listExe.append(CSV_HEADER)
    listNotConfigured = []

    for exe in cyclesSQLExecutions:
        # if exec failed at init test Automation issue
        if not exe[DUT_SERIAL]: 
            pass


        try:
            bufferSizeSQL = BUF_SIZE_SQL.format(id=str(exe[DUT_SERIAL]))
            bufferSizeSQLCourser.execute(bufferSizeSQL)
            bufferSizeSQLExecution = bufferSizeSQLCourser.fetchone()
            if not bufferSizeSQLExecution:
                raise Exception("no REL_TW_details for execution")

            reLog([ ("[REL Info]"), ("Checking ExecutionID:> %s" % str(exe[EXE_ID])) ])
            # if bufferSizeSQLExecution[1]:
            if NewDUTID(exe[DUT_SERIAL], listExe):
                listExe.append([exe[CYCLE_ID],exe[TEST_DESCRIPTION],exe[EXE_ID],exe[DUT_SERIAL],exe[DEVICE_TYPE],exe[CAPACITY],bufferSizeSQLExecution[1],exe[EXE_STATUS]])

        except Exception as bufferSizeSQLException:
            reLog([
                ("[REL Error]"),("No REL_TW_details for execution: %s" % str(exe[EXE_ID]))
                ,("CycleID: %d" % exe[CYCLE_ID])
                ,("DUT-ID '%s' not found in REL_TW_details" % str(exe[DUT_SERIAL]) )
                ,("Exception: %s" % bufferSizeSQLException)
                ])

            if NewDUTID(exe[DUT_SERIAL], listExe):
                listNotConfigured.append([exe[CYCLE_ID],exe[TEST_DESCRIPTION],exe[EXE_ID],exe[DUT_SERIAL],exe[DEVICE_TYPE],exe[CAPACITY],'Not Configured',exe[EXE_STATUS]])

    listExe += filterNotConfigured(listExe, listNotConfigured)
    
    listCsv = []
    for line in listExe:
        listLine = []
        for item in line:
          listLine.append(str(item))
        listCsv.append(",".join(listLine))
    
    f = open(csvDistenation, "wb")
    f.writelines("\n".join(listCsv))
    f.close()

if __name__ == "__main__":
    ####################################################################
    # INIT script 
    ####################################################################
    # reset the execution log file
    reLog("[REL Info] new execution started", mode="w")
    # set distenation folder for report CSV file [Empty = same folder as the script]
    reportDistenation = ""
    # init data base connection install ODBC driver from MS @https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15
    cyclesSQLCourser = DBConnect('Driver={ODBC Driver 18 for SQL Server};Server=10.24.8.188;Database='+os.environ['ReliabilityGrid']+';uid=Ace;pwd=Ace2018!;Encrypt=No')
    #bufferSizeSQLCourser =  DBConnect('Driver={ODBC Driver 18 for SQL Server};Server=10.0.158.146\\DWH;Database=REL;uid=rel_user;pwd=rel_user!123;Encrypt=No')
    bufferSizeSQLCourser =  DBConnect('Driver={ODBC Driver 18 for SQL Server};Server=10.24.8.170;Database=REL;uid=rel_user;pwd=reluser!123;Encrypt=No')
    
    main(os.environ['Cycles'],os.environ['ReliabilityGrid'])
