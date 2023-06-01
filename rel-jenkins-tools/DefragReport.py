###############################################################################
# Script by rawad.kharma@wdc.com
# Reliability team tefen,il
###############################################################################
# Cycles Dinamic defrag details report 
# Ver Beta 0.1
import os
from datetime import datetime
import pypyodbc
import csv


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
    fLOG.write("\n%s" % datetime.now().strftime("%d/%m/%Y %H:%M:%S") )
    if (type(relMessage) != str) & (type(relMessage) != "<type 'unicode'>"):
        print("[REL Info] Check run_log.txt")
        fLOG.write("%s\n" % relMessage[0] )
        for messageLine in relMessage:
            fLOG.write("\t%s\n"%( str(messageLine) ) )
    else:
        print(80*">")
        print(relMessage)
        print(80*"<")
        fLOG.write("\t%s" % relMessage )
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
        exit(1)

def main(strCycles):
    CSVheader = ["CycleID","Test","ExecutionID","DUTSerial","DeviceType","Capacity","file_size_level","ExecStatus","FirmwareVersion"]
    cyclesSQLExecutions = None
    # get cycles query
    cyclesSQL = """
      SELECT 
        [Executions].[ID]
        ,[CycleDefinition].[CycleID]  
        ,[Executions].DUTSerial
        ,[TestPlans].[Description]
        ,[Executions].[DeviceType]
        ,[Executions].[Capacity]
        ,[EnumExecutionStatuses].[Name] as [ExecStatus]
        ,[Cycles].[FirmwareVersion]
        FROM {automationDB}.[dbo].[Executions] [Executions]
        JOIN {automationDB}.[dbo].[CycleDefinition] [CycleDefinition] 
            ON [Executions].[cycledefinitionid] = [CycleDefinition].[id]
        JOIN {automationDB}.[dbo].[Cycles] [Cycles] 
            ON [Cycles].[id] = [CycleDefinition].[CycleID]
        left JOIN {automationDB}.[dbo].[TestPlans] [TestPlans]
            ON [CycleDefinition].TestID = [TestPlans].[ID]
        JOIN [dbo].[EnumExecutionStatuses] ON [dbo].[EnumExecutionStatuses].[Id] = [Executions].[Status]
        
        WHERE [CycleDefinition].[CycleID] in ({Cycles})
    """.format(Cycles=strCycles, automationDB=os.environ['ReliabilityGrid'])
    reLog([
        ("[INFO Query] running query on:\n\tCycles: %s \n\tAutomation DB: %s" % (strCycles, os.environ['ReliabilityGrid'])),
        ("[INFO SQL] %s" % cyclesSQL)
    ])

    print("\n\n\t\t PLEASE WAIT!!!!!")

    try:
        cyclesSQLCourser.execute(cyclesSQL)
    except Exception as SQLQueryException:
        reLog("[REL Error] failed running sql Query !! \n %s" % SQLQueryException)
    else:
        cyclesSQLExecutions = cyclesSQLCourser.fetchall()

    if cyclesSQLExecutions == None:
        reLog([
            ("[REL Error] failed running sql Query !! \n No Executions in Cycle"),
            ("Cycles String: " % str(strCycles)),
            ("Automation: " % str(os.environ['ReliabilityGrid']))
        ])
    # result format [ExecutionID, CycleID, DUTSerial, Test Description, DeviceType, Capacity]
    #                   0           1       2               3               4           5

    csvDistenation = os.path.join(reportDistenation,"defrag_report.csv")
    reLog("[INFO report] saving report to \n%s" % csvDistenation)

    with open(csvDistenation, 'wb') as csvFile:
        csvWriter = csv.writer(csvFile)
        # CSV report header
        csvWriter.writerow(CSVheader)
        defragDetailsSQL = None
        for execution in cyclesSQLExecutions:
            # print execution
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
                    WHERE [device_id] = '%s'
                    ORDER By test_id DESC
                    """ % str(execution[2])
                defragDetailsSQLCourser.execute(defragDetailsSQL)
                defragDetailsSQLExecution = defragDetailsSQLCourser.fetchone()
                if not defragDetailsSQLExecution:
                    raise Exception("no REL_defrag_details for execution")
            except Exception as defragDetailsSQLException:
                reLog([
                    ("[REL Error] No REL_defrag_details for execution: %s" % str(execution[0]))
                    ,("DUT-ID '%s' not found in REL_defrag_details" % str(execution[2]) )
                    ,("Exception: %s" % defragDetailsSQLException)
                    ,("Query: %s"% defragDetailsSQL) 
                    ])
                csvWriter.writerow([execution[1],execution[3],execution[0],execution[2],execution[4],execution[5],'Not Configured',execution[6],execution[7]])
                print [execution[1],execution[3],execution[0],execution[2],execution[4],execution[5],'Not Configured',execution[6],execution[7]]
            else:
                print [execution[1],execution[3],execution[0],execution[2],execution[4],execution[5],defragDetailsSQLExecution[3],execution[6],execution[7]]
                csvWriter.writerow([execution[1],execution[3],execution[0],execution[2],execution[4],execution[5],defragDetailsSQLExecution[3],execution[6],execution[7]])

if __name__ == "__main__":
    ####################################################################
    # INIT script 
    ####################################################################
    # reset the execution log file
    reLog("[REL Info] new execution started", mode="w")
    # set distenation folder for report CSV file [Empty = same folder as the script]
    reportDistenation = ""
    # INIT db connection
    cyclesSQLCourser = DBConnect('Driver={ODBC Driver 18 for SQL Server};Server=10.24.8.188;Database='+os.environ['ReliabilityGrid']+';uid=Ace;pwd=Ace2018!;Encrypt=No')
    defragDetailsSQLCourser =  DBConnect('Driver={ODBC Driver 18 for SQL Server};Server=10.0.158.146\\DWH;Database=REL;uid=rel_user;pwd=rel_user!123;Encrypt=No')
    main(os.environ['Cycles'])