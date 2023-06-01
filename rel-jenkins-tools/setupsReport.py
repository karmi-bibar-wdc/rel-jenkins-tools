###############################################################################
# Script by rawad.kharma@wdc.com
# Reliability team tefen,il
###############################################################################
# Setups Report
# Ver Beta 0.0.1

import os
import sys
from datetime import datetime
import pypyodbc
import xlsxwriter

def reLog(relMessage, mode="a+"):
    '''
    Reliability Logger
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
            fLOG.write("\n%s" % datetime.now().strftime("%d/%m/%Y %H:%M:%S") )
            fLOG.write("\t%s\n" % relMessage[0] )
            try:
                relMessage.pop(0) if type(relMessage) != tuple else 0
                for messageLine in relMessage:
                    fLOG.write("\t%s\n"%( str(messageLine) ) )
            except Exception:
                fLOG.write("\t%s\n"%( str(relMessage) ) )
            
    else:
        print(80*">")
        print(relMessage)
        print(80*"<")
        fLOG.write("\n%s" % datetime.now().strftime("%d/%m/%Y %H:%M:%S") )
        fLOG.write("\t%s" % relMessage )
    fLOG.close()
def DBConnect(uid, pwd, server, db):
    """ config and INIT DB connection

    Args:
        uid (_type_): user name
        pwd (_type_): password
        server (_type_): server address
        db (_type_): select database 

    Returns:
        _type_: OBJ database connection cursor
    """
    try:
        strConnection = "Driver={ODBC Driver 18 for SQL Server};Server=%s;uid=%s;pwd=%s; database=%s;Encrypt=No" % (str(server),uid,pwd,db)
        reLog("[INFO] connection string: %s" % strConnection)
        connection = pypyodbc.connect(strConnection)
        connection.unicode_results = True
        return connection.cursor()
    except Exception as SQLServerException:
        reLog(["[REL Error] DB connection failed", SQLServerException, strConnection])

def setupsReport():

    try:
        odbcCourser.execute("""
            IF OBJECT_ID('[dbo].[REL_SetupReport]', 'V') IS NOT NULL
            DROP VIEW [dbo].[REL_SetupReport];
        """)
        reLog([("[INFO query] running query DROP old View")])
    except Exception as SQLQueryException:
        reLog(["[REL Error] failed running sql Query !! \n DROP OLD view\n",SQLQueryException])
    
    createViewSQL = """
        CREATE VIEW [dbo].[REL_SetupReport] 
        AS
        SELECT TOP (1000) 
            [dbo].[Executions].[PlatformProperties]
            ,[dbo].[Executions].[SetupMappingID]
            ,[dbo].[SetupMappings].[InspectorSerial]
            ,[dbo].[Executions].[StationName]
            ,[dbo].[Executions].[Platform]
            ,[dbo].[Executions].[StartDateTime]
            ,[dbo].[Executions].[EndDateTime]
            ,[dbo].[EnumExecutionStatuses].[Name] as [ExecutionStatus]
            ,[dbo].[EnumResults].[Name] as [ResultsName]
            ,[dbo].[Executions].[ResultInfo]
            ,[dbo].[Cycles].[ID] as [CycleID]
        FROM [REL_Side_B_49].[dbo].[Executions]
        JOIN [REL_Side_B_49].[dbo].[SetupMappings]
            ON [dbo].[SetupMappings].[ID] = [dbo].[Executions].[SetupMappingID]
        LEFT JOIN [dbo].[EnumExecutionStatuses] 
            ON [dbo].[EnumExecutionStatuses].Id = [dbo].[Executions].[Status]
        LEFT JOIN [dbo].[EnumResults] 
            ON [dbo].[EnumResults].[Id] = [dbo].[Executions].[Result]
        JOIN [dbo].[CycleDefinition] 
            ON [dbo].[CycleDefinition].ID = [dbo].[Executions].[CycleDefinitionID]
        JOIN [dbo].[Cycles] 
            ON [dbo].[Cycles].[ID] = [dbo].[CycleDefinition].[CycleID]
        WHERE [dbo].[Executions].[Result] > 1
        AND StartDateTime > '{}'
        AND StartDateTime < '{}'
    """.format(
        '{} 00:00:00.000'.format(os.environ['startDate']),
        '{} 00:00:00.000'.format(os.environ['endDate'])
        )

    readViewSQL = """
        SELECT 
            1 as [count]
            ,[dbo].[REL_SetupReport].[PlatformProperties]
            ,[dbo].[REL_SetupReport].[SetupMappingID]
            ,[dbo].[REL_SetupReport].[InspectorSerial]
            ,[dbo].[REL_SetupReport].[StationName]
            ,[dbo].[REL_SetupReport].[Platform]
            ,[dbo].[REL_SetupReport].[StartDateTime]
            ,[dbo].[REL_SetupReport].[EndDateTime]
            ,[dbo].[REL_SetupReport].[ExecutionStatus]
            ,[dbo].[REL_SetupReport].[ResultsName]
            ,[dbo].[REL_SetupReport].[ResultInfo]
            ,[dbo].[REL_SetupReport].[CycleID]
            ,'' as [issue]
        FROM [dbo].[REL_SetupReport]
        WHERE [dbo].[REL_SetupReport].[ResultInfo] LIKE '{}'
        OR [dbo].[REL_SetupReport].[ResultInfo] LIKE '{}'
        OR [dbo].[REL_SetupReport].[ResultInfo] LIKE '{}'
        OR [dbo].[REL_SetupReport].[ResultInfo] LIKE '{}'
        OR [dbo].[REL_SetupReport].[ResultInfo] LIKE '{}'
        OR [dbo].[REL_SetupReport].[ResultInfo] LIKE '{}'
    """.format(
        '%{}%'.format("ATB device response timeout"), 
        '%{}%'.format("SetPowerMode"),
        '%{}%'.format("DmeLinkStartUpCmd"),
        '%{}%'.format("Null Pointer error"),
        '%{}%'.format("USB connection failure"),
        '%{}%'.format("No adapters found"))
    # << build the SQL query <<

    reLog([
        ("[INFO query] running query Date range:\n\tStart: %s\n\tEnd: %s" % (os.environ['startDate'], os.environ['endDate']) ),
        ("[INFO SQL1] %s" % createViewSQL),
        ("[INFO SQL2] %s" % readViewSQL)
    ])

    print "\n\n\t\t PLEASE WAIT!!!!!"
    res_executions = None
    try:
        createViewRes = odbcCourser.execute(createViewSQL)
        reLog([("[REL Info]"),("running query create new View")])
    except Exception as SQLQueryException:
        reLog("[REL Error] failed running createViewSQL Query !! \n %s" % SQLQueryException)

    try:
        odbcCourser.execute(readViewSQL)
        reLog([("[REL Info]"),("running query read View")])
    except Exception as SQLQueryException:
        reLog("[REL Error] failed running readViewSQL Query !! \n %s" % SQLQueryException)
    else:
        res_executions = odbcCourser.fetchall()
        reLog("[REL Info] count of failed executions found: %s" % str(len(res_executions)) )
    odbcCourser.close()

    ### create Excel File
    # Create a workbook and add a worksheets.
    workbook = xlsxwriter.Workbook('setupReport.xlsx')
    worksheet = workbook.add_worksheet()
    worksheetRES = workbook.add_worksheet()
    # Set tab colors
    worksheetRES.set_tab_color('green')
    # Adjust the column width.
    worksheetRES.set_column(0, 0, 30)
    worksheetRES.set_column(0, 1, 15)
    # Add a bold format to use to highlight cells.
    bold = workbook.add_format({'bold': 1})
    # Add an Excel date format.
    date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm:ss'})

    # Start from the first cell below the headers.
    row = 1
    col = 0
    obj_results = {}
    lst_error = [
        "ATB device response timeout",
        "SetPowerMode",
        "DmeLinkStartUpCmd",
        "Null Pointer error",
        "USB connection failure",
        "No adapters found"
    ]
    error_count = {}
    for rowData in (res_executions): 
        err = 0
        for e in lst_error:
            if e in rowData[10]:
                err = e
                # error_count[e] = error_count[e] + 1
                if e in error_count.keys():
                    error_count[e] = error_count[e] + 1 
                else:
                    error_count[e] = 1

        if err:
            if rowData[1] in obj_results.keys():
                obj_results[rowData[1]].append(rowData[6])
                obj_results[rowData[1]].append(rowData[7])
                obj_results[rowData[1]].append(rowData[8])
                obj_results[rowData[1]].append(rowData[9])
                obj_results[rowData[1]].append(rowData[11])
                obj_results[rowData[1]].append(err)
                obj_results[rowData[1]][0] = obj_results[rowData[1]][0] + 1
            else:
                obj_results[rowData[1]] = [
                    rowData[0],rowData[1],rowData[2],rowData[3],rowData[4],
                    rowData[5],rowData[6],rowData[7],rowData[8],rowData[9],rowData[11],err
                    ]
    reLog("[REL Info] count of Errors from list found: %s" % str(len(obj_results)) )
    addVal = 0
    for i in obj_results: #each row
        for j in obj_results[i]: #each val
            ### set header 
            rowLen = len(obj_results[i])
            if j is None: j = 0 # avoid null cel issues
            if rowLen > 12:
                rowLen = rowLen - 12 #remove base len
                if addVal < rowLen / 5:
                    addVal = rowLen / 5
            if isinstance(j, datetime):
                time = j.strftime("%d/%m/%Y %H:%M:%S")
                worksheet.write_string(row, col, time, date_format)
            elif type(j) is str:
                worksheet.write_string(row, col, j)
            elif type(j) is int or j.isnumeric():
                worksheet.write_number(row, col, int(j))
            else:
                worksheet.write_string(row, col, j)
            col += 1
        col = 0
        row += 1

    headerNames = ['count','platformProperties','setupMappingid','inspectorSerial',\
    'stationName','platform','startDatetime','endDatetime','executionStatus','resultsname','cycle','error']
    for i in range(0,addVal):
        headerNames.append('startDatetime_'+str(i+1))
        headerNames.append('endDatetime_'+str(i+1))
        headerNames.append('executionStatus_'+str(i+1))
        headerNames.append('resultsname_'+str(i+1))
        headerNames.append('cycle_'+str(i+1))
        headerNames.append('error_'+str(i+1))
    k=0
    for j in headerNames:
        worksheet.write(0,k, j, bold)
        k = k +1
    
    worksheetRES.write_string(0, 0, 'startDate')
    worksheetRES.write_string(0, 1, os.environ['startDate'], date_format)
    worksheetRES.write_string(1, 0, 'endDate')
    worksheetRES.write_string(1, 1, os.environ['endDate'], date_format)
    worksheetRES.write_string(2, 0, "count of failed executions found")
    worksheetRES.write_number(2, 1, len(res_executions))
    worksheetRES.write_string(3, 0, "count of Errors from list found")
    worksheetRES.write_number(3, 1, len(obj_results))
    
    worksheetRES.write_string(4, 0, "")
    worksheetRES.write_string(5, 0, "count of failures",bold)
    row=6
    for r in error_count:
        worksheetRES.write_string(row, 0, r)
        worksheetRES.write_number(row, 1, error_count[r])
        row = row+1
    
    workbook.close()

if __name__ == "__main__":
    ####################################################################
    # INIT script 
    ####################################################################
    ### reset the execution log file
    reLog("[REL Info] new execution started", mode="w")

    ### check for env vars
    try:
        os.environ['ReliabilityGrid']
        # startDate,  endDate   /  format '2020-12-21 00:00:00'
        os.environ['startDate']
        os.environ['endDate']
    except Exception as RelException:
        reLog(["[REL Error] please set query variables !! ", RelException])
        sys.exit(1)

    ### init data base connection     # WDIL ip: 10.24.8.188    # WDIN ip: 10.206.65.131
    try:
        odbcCourser = DBConnect(uid='Ace', pwd='Ace2018!', server='10.24.8.188', db='%s' % os.environ['ReliabilityGrid'] )
    except Exception as RelException:
        reLog(["[REL Error] DataBase connection INIT Failed!! ", RelException])
        sys.exit(1)
    setupsReport()
