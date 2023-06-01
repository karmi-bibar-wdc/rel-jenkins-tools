
###############################################################################
# Script by rawad.kharma@wdc.com
# Reliability team tefen,il
###############################################################################
# new Test package regression check list [FULL Report]
# Ver Beta 0.9


###############################################################################
# @TODO
###############################################################################


import os
from datetime import datetime
import pypyodbc
import csv

from utils import utils as utils


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
            print(relMessage[1])
        else:
            print("[REL Info] Check run_log.txt\n\t %s" % relMessage[0])
            fLOG.write("\n%s" % datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
            fLOG.write("\t%s\n" % relMessage[0])
            # relMessage.pop(0) if type(relMessage) != tuple else 0
            for messageLine in relMessage:
                fLOG.write("\t%s\n" % (str(messageLine)))
    else:
        print(80*">")
        print(relMessage)
        print(80*"<")
        fLOG.write("\n%s" % datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        fLOG.write("\t%s" % relMessage)
    fLOG.close()


def DBConnect(strConnectionArgs):
    '''
    # config and INIT DB connection

    return: OBJ database connection cursor

    '''
    reLog(("Connection STR: %s" % strConnectionArgs))
    try:
        connection = pypyodbc.connect(strConnectionArgs)
        return connection.cursor()
    except Exception as SQLServerException:
        reLog(("[REL Error] DB connection failed ", SQLServerException,"[Connection String]", strConnectionArgs))
        print(SQLServerException)
        exit(1)


def executeQuery(Cycle):
    # SETUP
    global CSVheader, reportDistention, relExecutionDumps, odbcCourser,\
        bufferSizeDefragDetailsSQLCourser

    dictTestStatus = {"All": 0, "Running": "1", "Completed": "2",
                      "Terminated": "3", "PausedOnError": "4",
                      "TimeOut": "5", "Replaced": "6", "TestComplete": "7"}
    Status = dictTestStatus[os.environ['Status']] if (os.environ['Status'] in dictTestStatus) else None
    dictTestResults = {"None": 0, "Pass": "1", "Fail": "2", "Error": "3", "Unknown": "4",
                       "PassWithException": "5", "FailWithException": "6", "Stuck": "7"}
    Result = dictTestResults[os.environ['Results']] if (os.environ['Results'] in dictTestResults) else None

    '''
    NewPack Regression Checklist
    Parameters:
        Cycles (str) :coma separated cycles id
        Status (int) :[Default "ALL"]
                        0   Pending
                        1   Running
                        2   Completed
                        3   Terminated
                        4   PausedOnError
                        5   TimeOut
                        6   Replaced
                        7   TestComplete
        Results (int) :[Default "ALL"]
                        0   None
                        1   Pass
                        2   Fail
                        3   Error
                        4   Unknown
                        5   PassWithException
                        6   FailWithException
                        7   Stuck
    Returns:
        csv file in folder and run_log.txt
    '''
    # >> build the SQL query >>
    sql = """SELECT
       [dbo].[Cycles].[ID] as [CycID]
      ,[dbo].[Executions].[ID] as [EXEID]
      ,[dbo].[Executions].[StationName]
      ,[dbo].[SetupMappings].[AdditionalHW]
      ,[dbo].[SetupMappings].[PlatformProperties]
      ,[dbo].[Cycles].[Description]
      ,[dbo].[EnumExecutionStatuses].[Name] as [ExecStatus]
      ,[dbo].[Cycles].[Project]
      ,[dbo].[Cycles].[FirmwareVersion]
      ,[dbo].[Cycles].[ResultInfo]
      ,[dbo].[CycleDefinition].[ID] as [DEFID]
      ,[dbo].[SetupMappings].[Capacity]
      ,[dbo].[Executions].[DUTSerial]
      ,[dbo].[Executions].[AdditionalInfo]
      ,[dbo].[EnumResults].[Name] as [ResultName]
      ,[dbo].[Executions].[ResultInfo]
      ,[dbo].[TestPlans].[Description] as [TestDescription]
      ,[dbo].[Xplorer].[URL]
      ,[dbo].[TestPlans].[TestRelativePath] as [FullScriptName]
      ,[dbo].[Executions].[Status]
      ,[dbo].[Executions].[Result]
      FROM [dbo].[Cycles]
      LEFT JOIN [dbo].[CycleDefinition] ON [dbo].[CycleDefinition].[CycleID] = [dbo].[Cycles].[ID]
      LEFT JOIN [dbo].[Executions] ON [dbo].[CycleDefinition].ID = [dbo].[Executions].[CycleDefinitionID]
      LEFT JOIN [dbo].SetupMappings ON [dbo].[SetupMappings].[ID] = [dbo].[Executions].[SetupMappingID]
      LEFT JOIN [dbo].[EnumExecutionStatuses] ON [dbo].[EnumExecutionStatuses].Id = [dbo].[Executions].[Status]
      LEFT JOIN [dbo].[EnumResults] ON [dbo].[EnumResults].[Id] = [dbo].[Executions].[Result]
      LEFT JOIN [dbo].[TestPlans] ON [dbo].[CycleDefinition].[TestID] = [dbo].[TestPlans].[ID]
      LEFT JOIN [dbo].[Xplorer] ON [dbo].[Executions].[ID] = [dbo].[Xplorer].[ExecutionId]
      LEFT JOIN [dbo].[TestPlanParamValues] on [dbo].[TestPlanParamValues].[TestPlanID] = [dbo].[TestPlans].[ID]
      WHERE [dbo].[Cycles].[ID] in ( %s )
       """ % Cycle
    if Status:
        sql += "AND [dbo].[Executions].[Status] = %s " % str(Status)
    if Result:
        sql += "AND [dbo].[Executions].[Result] = %s " % str(Result)
    # << build the SQL query <<
    reLog([
        ("[INFO query] running query on:\n\tCycles: %s\n\tStatus: %s\n\tResult: %s" % (Cycle, Status, Result)),
        ("[INFO SQL] %s" % sql)
    ])

    print("\n\n\t\t PLEASE WAIT!!!!!")

    resExecutions = None
    try:
        odbcCourser.execute(sql)
    except Exception as SQLQueryException:
        reLog("[REL Error] failed running sql Query !! \n %s" % SQLQueryException)
    else:
        resExecutions = odbcCourser.fetchall()
        reLog("[REL Info] count of executions found: %s" % str(len(resExecutions)))
    odbcCourser.close()

    # create CSV file in folder [reportDistention]
    csvDistention = os.path.join(reportDistention, "LAB_report.csv")
    if not os.path.exists(csvDistention):
        with open(csvDistention, 'w'):
            pass

    # END setup ###############################################################
    resultsData = []
    for execRow in resExecutions:
        dUID = 0
        reLog([
            ("[REL Info]"),
            ("parsing Cycle ID:%s | Execution ID:%s" % (str(execRow[0]),
                                                        str(execRow[1])))
            ])
        #######################################################################
        # @START cycleDumpFolder
        # find Log Path in NET drive
        try:
            FWver = os.environ['FWver']
        except KeyError:
            INT_NUM_LENTGH = 2
            FWVerNum = execRow[8].split('.')[1]
            FWver = ""
            for i in range(0, len(FWVerNum), INT_NUM_LENTGH):
                FWver += str(int(FWVerNum[i: i+INT_NUM_LENTGH]))
        project = execRow[7].upper() if execRow[7] else "None"
        cycleDumpFolder1 = os.path.join("/", "mnt", "tfnlab", "REL",
                                        "ExceutionDumps",
                                        "UFS", project, FWver,
                                        str(execRow[18])
                                        .split("\\")[-1]
                                        .split(".")[0])
        cycleDumpFolder2 = os.path.join("/", "mnt", "tfnrel",
                                        "ExecutionDumps_tfnlab",
                                        "UFS", project, FWver,
                                        str(execRow[18])
                                        .split("\\")[-1]
                                        .split(".")[0])
            
        # @@@@@ iterate on folder content finde the UID
        logPath = False
        try:  # catch exception no log path !!
            for d in os.listdir(os.path.join(cycleDumpFolder1, str(execRow[0]))):
                if execRow[12] and d.endswith(execRow[12]):
                    dUID = d
                    for d2 in os.listdir(os.path.join(cycleDumpFolder1, str(execRow[0]), d)):
                        for d3 in os.listdir(os.path.join(cycleDumpFolder1, str(execRow[0]), d, d2)):
                            logPath = os.path.join(cycleDumpFolder1, str(execRow[0]), d, d2, d3)  # full path to DC
                            break
            if not logPath:
                for d in os.listdir(os.path.join(cycleDumpFolder2, str(execRow[0]))):
                    if execRow[12] and d.endswith(execRow[12]):
                        dUID = d
                        for d2 in os.listdir(os.path.join(cycleDumpFolder2, str(execRow[0]), d)):
                            for d3 in os.listdir(os.path.join(cycleDumpFolder2, str(execRow[0]), d, d2)):
                                logPath = os.path.join(cycleDumpFolder2, str(execRow[0]), d, d2, d3)  # full path to DC
                                break
        except Exception as logPathException:
            reLog([
                ("[Exception logPathException] no DUT_ID in dump folder #1 !"),
                ("[Exception logPathException] cant find {%s} in [%s]" % (str(execRow[12]), os.path.join(cycleDumpFolder1,str(execRow[0])))),
                ("[Exception logPathException] cant find {%s} in [%s]" % (str(execRow[12]), os.path.join(cycleDumpFolder2,str(execRow[0])))),
                ("[INFO] Will check path without 4Rerun folder !!"),
                ("[Exception logPathException] Exception %s " % logPathException)])

        if (logPath is False) or (logPath == 'False'):
            try:
                for d in os.listdir(os.path.join(cycleDumpFolder.rstrip("_4Rerun"), str(execRow[0]))):
                    if execRow[12] and d.endswith(execRow[12]):
                        dUID = d
                        for d2 in os.listdir(os.path.join(cycleDumpFolder, str(execRow[0]), d)):
                            for d3 in os.listdir(os.path.join(cycleDumpFolder, str(execRow[0]), d, d2)):
                                logPath = os.path.join(cycleDumpFolder, str(execRow[0]), d, d2, d3)  # full path to DC
                                break
                    if not logPath:
                        if execRow[12] and d.endswith(execRow[12]):
                            dUID = d
                            for d2 in os.listdir(os.path.join(cycleDumpFolder,str(execRow[0]),d)):
                                for d3 in os.listdir(os.path.join(cycleDumpFolder,str(execRow[0]),d,d2)):
                                    logPath = os.path.join(cycleDumpFolder,str(execRow[0]),d,d2,d3)  # full path to DC
                                    break
            except Exception as logPathException:
                reLog([
                    ("[Exception logPathException] no DUT_ID in dump folder #2 !!"),
                    ("[Exception logPathException] cant find {%s} in [%s]" % ( str(execRow[12]), os.path.join(cycleDumpFolder1.rstrip("_4Rerun"),str(execRow[0]) )) ),
                    ("[Exception logPathException] cant find {%s} in [%s]" % ( str(execRow[12]), os.path.join(cycleDumpFolder2.rstrip("_4Rerun"),str(execRow[0]) )) ),
                    ("[Exception logPathException] Exception %s " % logPathException)])

        # @END cycleDumpFolder
        #######################################################################

        #######################################################################
        # @START TW details
        bufferSize = "Not Configured"
        try:
            bufferSizeSQL = """
                SELECT
                [REL_TW_details].[device_id]
                ,[REL_TW_details].[buffer_size]
                ,[REL_TW_details].[execution_id]
                ,[REL_TW_details].[creation_date]
                FROM [REL].[dwh].[REL_TW_details]
                WHERE [execution_id] = '%s'
                ORDER By test_id DESC
                """ % str(execRow[1])
            bufferSizeDefragDetailsSQLCourser.execute(bufferSizeSQL)
            bufferSizeSQLExecution = bufferSizeDefragDetailsSQLCourser.fetchone()
            if not bufferSizeSQLExecution:
                raise Exception("no REL_TW_details for execution")
            bufferSize = bufferSizeSQLExecution[1]
        except Exception as bufferSizeSQLException:
            reLog([
                ("[REL Error] No REL_TW_details for execution: %s" % str(execRow[0]))
                ,("DUT-ID '%s' not found in REL_TW_details" % str(execRow[12]) )
                ,("Exception: %s" % bufferSizeSQLException)
                ,("Query: %s"% bufferSizeSQL) 
                ])

        #######################################################################
        # @START DEFRAG details 
        defragDetails = "Not Configured"
        try:
            defragDetailsSQL = """
                SELECT TOP (1000) [test_id]
                    ,[test_name]
                    ,[device_id]
                    ,[file_size_level]
                    ,[creation_date]
                    ,[execution_id]
                    ,[Automation_id]
                FROM [REL].[dwh].[REL_defrag_details]
                WHERE [execution_id] = '%s'
                ORDER By test_id DESC
                """ % str(execRow[1])
            bufferSizeDefragDetailsSQLCourser.execute(defragDetailsSQL)
            defragDetailsSQLExecution = bufferSizeDefragDetailsSQLCourser.fetchone()
            if not defragDetailsSQLExecution:
                raise Exception("no REL_defrag_details for execution")
            defragDetails = defragDetailsSQLExecution[3]
        except Exception as defragDetailsSQLException:
            reLog([
                ("[REL Error] No REL_defrag_details for execution: %s" % str(execRow[1]))
                ,("DUT-ID '%s' not found in REL_defrag_details" % str(execRow[12]) )
                ,("Exception: %s" % defragDetailsSQLException)
                # ,("Query: %s"% defragDetailsSQL) 
                ])

        #######################################################################
        # @START HID_details details 
        HID_details = "None"
        try:
            HID_detailsSQL = """
                SELECT TOP (1000) [test_id]
                    ,[test_name]
                    ,[device_id]
                    ,[HID_VCP_level]
                    ,[creation_date]
                    ,[execution_id]
                    ,[Automation_id]
                FROM [REL].[dbo].[REL_HID_details]
                WHERE [execution_id] = '%s'
                ORDER By test_id DESC
                """ % str(execRow[1])
            bufferSizeDefragDetailsSQLCourser.execute(HID_detailsSQL)
            HID_detailsSQLExecution = bufferSizeDefragDetailsSQLCourser.fetchone()
            if not HID_detailsSQLExecution:
                raise Exception("no REL_HID_details for execution")
            HID_details = HID_detailsSQLExecution[3]
        except Exception as HID_detailsSQLException:
            reLog([
                ("[REL Error] No REL_HID_details for execution: %s" % str(execRow[1]))
                ,("DUT-ID '%s' not found in REL_HID_details" % str(execRow[12]) )
                ,("Exception: %s" % HID_detailsSQLException)
                # ,("Query: %s"% defragDetailsSQL) 
                ])
        #######################################################################
        # @START Log path Link Format
        if logPath:
            netLogPath = utils.fixpath(logPath)
            # @TODO
            # netLogPath = '"=HYPERLINK({}^{})"'.format(winlogPath, str(execRow[1]))
        else:
            netLogPath = "NoLogsInNET"
        #######################################################################
        # @START get smartReport data
        all_subdirs = {}
        latest_subdir = 0
        AverageEraseCountSLC = AverageEraseCountMlc = AverageEraseCountEnhanced = ratio = 0
        if logPath:
            for dc in os.listdir(logPath):
                if os.path.isdir(os.path.join(logPath,dc)): 
                    all_subdirs[dc] = os.path.getctime(os.path.join(logPath,dc))
            
            latest_subdir = max(all_subdirs, key=all_subdirs.get)
            # ROTWSmartReport.csv SMSGSmartReport.csv
            print "latest_subdir: "+str(latest_subdir)
        if latest_subdir:
            # print (os.path.exists(os.path.join(logPath,dc,"SMSGSmartReport.csv")))
            smartReport = "SMSGSmartReport.csv" if (os.path.exists(os.path.join(logPath,latest_subdir,"SMSGSmartReport.csv"))) else "ROTWSmartReport.csv"
            if os.path.isfile(os.path.join(logPath,latest_subdir,smartReport)):
                with open(os.path.join(logPath,latest_subdir,smartReport), 'r') as csv_file:
                    reader = csv.reader(csv_file)
                    for row in reader:
                        # print(row)
                        if "AverageEraseCountSLC" == row[0].strip() or "avgEraseCountSlc" == row[0].strip():
                            AverageEraseCountSLC = row[1]
                        if "AverageEraseCountMlc" == row[0].strip() or "avgEraseCountMlc"  == row[0].strip() :
                            AverageEraseCountMlc = row[1]
                        if "AverageEraseCountEnhanced" == row[0].strip() or "avgEraseCountEnhanced" == row[0].strip():
                            AverageEraseCountEnhanced = row[1]
                    if int(AverageEraseCountSLC) > 0 and int(AverageEraseCountMlc) > 0:
                        ratio = int(AverageEraseCountSLC) / int(AverageEraseCountMlc)
        #######################################################################
        # @TODO check if DUTID in results
        # print resultsData
        resultsData.append(r",".join(( str(execRow[0]),str(execRow[1])\
            ,str(execRow[4]),str(execRow[16]),str(execRow[2]), str(execRow[6])\
            ,netLogPath,bufferSize,defragDetails,HID_details,\
            str(dUID),str(AverageEraseCountSLC),str(AverageEraseCountMlc),\
            str(AverageEraseCountEnhanced),str(ratio) )))

    with open(csvDistention, 'w') as csvFile:
        writer = csv.writer(csvFile)
        # CSV report header
        writer.writerow(CSVheader)

        # for row in resultsData:
        writer.writerows([i.strip().split(',') for i in resultsData])
        csvFile.close()
        reLog("[INFO report] saving report to $ %s" % csvDistention)


#######################################################################################################################################
#######################################################################################################################################
#######################################################################################################################################

if __name__ == "__main__":

    ####################################################################
    # INIT script 
    ####################################################################
    # reset the execution log file
    reLog("[REL Info] new execution started", mode="w")
    # CSV file header
    CSVheader = ["CycleID", "ExecID", "Adapter", "TestName", "station", 
                 "status", "logPath", "BufferSize", "defragDetails","HID_details",
                 "DUTID", "SLC", "MLC", "enhanced", "ratio"]
    # set Distention folder for report CSV file [Empty = same folder as the script]
    reportDistention = ""
    # ### @TODO get FW str function from REL_TP ####
    relExecutionDumps = os.environ['relExecutionDumps']
    # init data base connection install ODBC driver from MS @https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15
    odbcCourser = DBConnect('Driver={ODBC Driver 18 for SQL Server};Server=10.24.8.188;Database='+os.environ['ReliabilityGrid']+';uid=Ace;pwd=Ace2018!;Encrypt=No')
    bufferSizeDefragDetailsSQLCourser =  DBConnect(r"Driver={ODBC Driver 18 for SQL Server};Server=10.0.158.146\DWH;Database=REL;uid=rel_user;pwd=rel_user!123;Encrypt=No")

    executeQuery(os.environ['Cycles'])