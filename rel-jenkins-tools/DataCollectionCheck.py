
###############################################################################
# Script by rawad.kharma@wdc.com
# Reliability team tefen,il
###############################################################################
# new Test package regresion check list
# Ver Beta 0.9


###############################################################################
# @TODO remove "relExceutionDumps" and build it from execution data [# INIT script __main__]
# @TODO build the CSV in separete function  [csvBuilder()]
###############################################################################


import os
import getpass
from datetime import datetime
import pyodbc
import shutil
import csv
import re
import pip
import glob
import requests
import json


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
            fLOG.write("\n%s" % datetime.now().strftime("%d/%m/%Y %H:%M:%S") )
            fLOG.write("\t%s\n" % relMessage[0] )
            relMessage.pop(0) if type(relMessage) != tuple else 0
            for messageLine in relMessage:
                fLOG.write("\t%s\n"%( str(messageLine) ) )
    else:
        print(80*">")
        print(relMessage)
        print(80*"<")
        fLOG.write("\n%s" % datetime.now().strftime("%d/%m/%Y %H:%M:%S") )
        fLOG.write("\t%s" % relMessage )
    fLOG.close()


def DBConnect(strConnectionArgs):
    '''
    # config and INIT DB connection

    return: OBJ database connection cursor

    '''
    try:
        connection = pyodbc.connect(strConnectionArgs)
        return connection.cursor()
    except Exception as SQLServerException:
        reLog(("[REL Error] DB connection failed \n", SQLServerException))
        print(SQLServerException)
        exit(1)

def fixpath(path):
    return path.replace("/mnt","R:").replace("/","\\")

def executeQuery(Cycle):
    global CSVheader, reportDistenation, relExceutionDumps, xPlorerServer, odbcCourser

    dictTestTatus = {"All":0,"Running":"1","Completed":"2","Terminated":"3",
            "PausedOnError":"4","TimeOut":"5","Replaced":"6","TestComplete":"7"}
    Status =  dictTestTatus[os.environ['Status']] if (os.environ['Status'] in dictTestTatus) else None
    dictTestResults = {"None":0,"Pass":"1","Fail":"2","Error":"3","Unknown":"4",
            "PassWithException":"5","FailWithException":"6","Stuck":"7"}
    Result = dictTestResults[os.environ['Results']] if (os.environ['Results'] in dictTestResults) else None

    '''
    NewPack Regression Checklist 
    Parameters:
        Cycles (str) :coma seperated cycles id
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
        sql += "AND [dbo].[Executions].[Status] = %s " %  str(Status)
    if Result:
        sql += "AND [dbo].[Executions].[Result] = %s " % str(Result)
    # << build the SQL query <<
    reLog([
        ("[INFO query] running query on:\n\tCycles: %s\n\tStatus: %s\n\tResult: %s" % (Cycle, Status, Result) ),
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
        reLog("[REL Info] count of executions found: %s" % str(len(resExecutions)) )
    odbcCourser.close()
    csvDistenation = os.path.join(reportDistenation,"DC_report.csv")
    if not os.path.exists(csvDistenation):
        with open(csvDistenation, 'w'): pass
    reLog("[INFO report] saving report to $ %s" % csvDistenation)


    with open(csvDistenation, 'w') as csvFile:
        writer = csv.writer(csvFile)
        # CSV report header
        writer.writerow(CSVheader)

        for execRow in resExecutions:
            reLog([
                ("[REL Info]"),
                ("parsing Cycle ID:%s | Execution ID:%s" % ( str(execRow[0]),str(execRow[1]) ) ) 
                ])
            #####################################################################################################################################
            ## @START cycleDumpFolder
            # if not FWver global var use str((int(execRow[8].split('.')[1])/10)).replace('.','')
            # FWver = os.environ['FWver'] if (os.environ['FWver'] != None) else str((int(execRow[8].split('.')[1])/10)).replace('.','')

            try:
                FWver = os.environ['FWver']
            except KeyError:
                INT_NUM_LENTGH = 2
                FWVerNum = execRow[8].split('.')[1]
                FWver = ""
                for i in range(0, len(FWVerNum), INT_NUM_LENTGH):
                    FWver += str(int(FWVerNum[i: i+INT_NUM_LENTGH]))

            cycleDumpFolder = os.path.join(relExceutionDumps,execRow[7].upper(),FWver,str(execRow[18]).split("\\")[-1].split(".")[0],str(execRow[0]) )
            # @@@@@ iterate on folder content finde the UID
            logPath = sessionLOG = False
            try:# catch exception no log path !!
                for d in os.listdir(cycleDumpFolder):
                    if d.endswith(execRow[12]):
                        for d2 in os.listdir(os.path.join(cycleDumpFolder,d)):
                            for d3 in os.listdir(os.path.join(cycleDumpFolder,d,d2)):
                                logPath = os.path.join(cycleDumpFolder,d,d2,d3)  # full path to DC
                                break
            except Exception as logPathException:
                reLog("[Exception logPathException] no DUT_ID in dump folder #1 !!")
                reLog("[Exception logPathException] cant find {%s} in [%s]" % (str(execRow[12]), str(logPath).replace("/mnt","r:")))
                reLog("[INFO] Will check path without 4Rerun folder !!")
                reLog("[Exception logPathException] Exception %s " % logPathException)


            if (logPath == False) or (logPath == 'False') :
                try:
                    dcCheck = os.path.join(relExceutionDumps,execRow[7].upper(),FWver,str(execRow[18]).split("\\")[-1].split(".")[0].rstrip("_4Rerun"),str(execRow[0]) )
                    for d in os.listdir(dcCheck):
                        if d.endswith(execRow[12]):
                            for d2 in os.listdir(os.path.join(dcCheck,d)):
                                for d3 in os.listdir(os.path.join(dcCheck,d,d2)):
                                    logPath = os.path.join(dcCheck,d,d2,d3)  # full path to DC
                                    break
                except Exception as logPathException:
                    reLog("[Exception logPathException] no DUT_ID in dump folder #2 !!")
                    reLog("[Exception logPathException] cant find {%s} in [%s]" % (str(execRow[12]), str(dcCheck).replace("/mnt","r:")) )
                    reLog("[Exception logPathException] Exception %s " % logPathException)


            # @@@@@ check for 'sessionlog.log' xPlorer sessions file
            UID_bad_blocks = UID_metablocks = UID_spare_blocks = UID_wear_leveling = "Nune"
            if logPath != False:
                sessionLOG = "OK" if "sessionlog.log" in os.listdir(logPath) else "none"
                # @@@@@ check dataCollection last flder or Final
                all_subdirs = []
                latest_subdir = 0
                for dc in os.listdir(logPath):
                    if dc == "Final":
                        latest_subdir = os.path.join(logPath,dc)
                    else:
                        if os.path.isdir(os.path.join(logPath,dc)): 
                            all_subdirs.append(os.path.join(logPath,dc))
                            latest_subdir = latest_subdir if latest_subdir else max(all_subdirs, key=os.path.getmtime)
                # netLogPath = fixpath(os.path.join(logPath,latest_subdir)) if os.path.isdir(os.path.join(logPath,latest_subdir)) else "NO_LOGS" # log folder path in net drive
                if latest_subdir :
                    netLogPath = '=HYPERLINK("'+os.path.join(logPath,latest_subdir)+'","'+latest_subdir.rsplit('/',1)[1]+'")'
            
                    ######################################################################################################
                    # CHeck for the files
                    ######################################################################################################

                    UID         = "OK" if str(str(execRow[4]) + "_UID.bin") in os.listdir(os.path.join(logPath,latest_subdir)) else "none"
                    UID_MblksRecords = "OK" if "MblksRecords.bin" in os.listdir(os.path.join(logPath,latest_subdir)) else "none"
                    UID_And_Header   = "OK" if "UID_And_Header.bin" in os.listdir(os.path.join(logPath,latest_subdir)) else "none"
                    
                    # UID_Parsed_Files = ["bad_blocks", "metablocks", "spare_blocks", "wear_leveling"]
                    for name in glob.glob(os.path.join(logPath,latest_subdir)+'/*bad_blocks*'): # pylint: disable=unused-variable
                        UID_bad_blocks = "OK"
                        break
                    for name in glob.glob(os.path.join(logPath,latest_subdir)+'/*metablocks*'):
                        UID_metablocks = "OK"
                        break
                    for name in glob.glob(os.path.join(logPath,latest_subdir)+'/*spare_blocks*'):
                        UID_spare_blocks = "OK"
                        break
                    for name in glob.glob(os.path.join(logPath,latest_subdir)+'/*wear_leveling*'):
                        UID_wear_leveling = "OK"
                        break

                    # smart_report = ["bin", "csv", "json"]
                    SmartReportBIN  = "OK" if "SMSG_SmartReport.bin" in os.listdir(os.path.join(logPath,latest_subdir)) else "none" 
                    SmartReportBIN  = "OK" if "ROTW_SmartReport.bin" in os.listdir(os.path.join(logPath,latest_subdir)) else SmartReportBIN
                    smartReportJson = "OK" if "SmartReport.json" in os.listdir(os.path.join(logPath,latest_subdir)) else "none"
                    smartReportCSV  = "OK" if "ROTWSmartReport.csv" in os.listdir(os.path.join(logPath,latest_subdir)) else "none"
                    smartReportCSV  = "OK" if "SMSGSmartReport.csv" in os.listdir(os.path.join(logPath,latest_subdir)) else smartReportCSV 

                    faDumpFile  = "OK" if "faDumpFile.fad" in os.listdir(os.path.join(logPath,latest_subdir)) else "none"
                    faScheme    = "OK" if "faScheme.sdb" in os.listdir(os.path.join(logPath,latest_subdir)) else "none"

                    PDL         = "OK" if "PdlCounters.pdl" in os.listdir(os.path.join(logPath,latest_subdir)) else "none"
                    PDL_Jsonl   = "OK" if "PdlCounters_.jsonl" in os.listdir(os.path.join(logPath,latest_subdir)) else "none"
                    PDL_analysis= "OK" if "pdl_analysis.xlsx" in os.listdir(os.path.join(logPath)) else "none"

                    MST_log     = "OK" if "SwProMstLog.mst" in os.listdir(os.path.join(logPath,latest_subdir)) else "none"
                    MST_last    = "OK" if "SwPro_lastMstPart.mst" in os.listdir(os.path.join(logPath,latest_subdir)) else "none"

                    UseRomMetaDataCSV  = "OK" if "UromTrimMetadata.csv" in os.listdir(os.path.join(logPath,latest_subdir)) else "none"
                    UseRomMetaDataBIN  = "OK" if "URomTrimMetadata.bin" in os.listdir(os.path.join(logPath,latest_subdir)) else "none"
                else: netLogPath = "NO_LOGS" # log folder path in net drive
            ## @END cycleDumpFolder
            #####################################################################################################################################

                ######################################################################################################
                # check xPlorer for the files
                ######################################################################################################

                xPlorerLastSession = execRow[17]  # default to first xPlorer session from automation test INIT

                if (sessionLOG == "OK") & (execRow[4] != None): # Check for session and PlatformProperties "not terminated"
                    # @START read session log last xPlorer session
                    FileHandler = open(os.path.join(logPath, "sessionlog.log"),"r")
                    try:
                        lastSession = FileHandler.readlines()[-1].split(':')[1] or "None"
                        if not lastSession:
                            reLog([("[REL sessionLog]"),
                                ("can't read last xPlorer session from LOG!! "),
                                ("\t\t in: %s" % str(os.path.join(logPath, "sessionlog.log")) )
                            ])
                    except Exception as sessionLogException:
                        reLog([("[REL sessionLogException]"),
                            ("can't read last xPlorer session from LOG!! "),
                            ("\t\t in: %s" % str(os.path.join(logPath, "sessionlog.log")) ),
                            ("\t\t Exception: %s " % sessionLogException)
                        ])
                        lastSession = "None"
                    ######################### @END read last xPlorer session

                xPlorerLastSession = "http://{}/xplorer/console/ui/{}".format(xPlorerServer, lastSession).rstrip()
                URL = response = None
                if lastSession == "None":
                    xplorerFiles = {'fad':'none','SmartReport':'none','mst1':'none','mst2':'none','pdl':'none','coverage':'none', 'UromTrimMetadata':'none', 'UIDMetablocks':'none'}
                else:
                    #
                    try:
                        URL = "http://{}/xplorer/api/session/getBaseInfo?sessionId={}".format(str(xPlorerServer), str(lastSession).replace("\r\n","") )
                        response = requests.get(URL) 
                        reLog([("[REL Info]"),("URL:: %s" % URL),("Status:: %s" % str(response.status_code))])
                        # print(80*'>')print("request from::::", URL)print("response.status_code:::::", response.status_code)print(80*'<')
                    except Exception as requestException:
                        reLog("[Error] error getting response from server!! %s" % requestException)

                try:
                    responseJSON = json.loads(response.text)
                    xplorerFiles = {}
                    for key in responseJSON:
                        value = responseJSON[key]
                        # print("The {} - state  = ({})".format(key, value['state']))
                        xplorerFiles[key] = value['state']
                except Exception as Exp:
                    reLog([("[Error XPlorer]"),
                        ("Error parcing the xPlorer responce \n\t %s" % Exp),
                        ("URL: %s" % URL),
                        ("\tresponse code: %s" % (str(response.status_code) if response else "NoResponse") )
                    ])

                xplorerFad              = "OK" if 'fad' in xplorerFiles else "OK" if 'FAD' in xplorerFiles else "NONE"
                xplorerSmartReport      = "OK" if 'SmartReport' in xplorerFiles else "NONE"
                xplorercoverage         = "OK" if 'coverage' in xplorerFiles else "NONE"
                xplorerUromTrimMetadata = "OK" if 'UromTrimMetadata' in xplorerFiles else "NONE"
                xplorerUIDMetablocks    = "OK" if 'UIDMetablocks' in xplorerFiles else "NONE"
                xplorerMST1             = "OK" if 'mst1' in xplorerFiles else "OK" if 'MST1' in xplorerFiles else "NONE"
                xplorerMST2             = "OK" if 'mst2' in xplorerFiles else "OK" if 'MST2' in xplorerFiles else "NONE"
                xplorerPDL              = "OK" if 'pdl'  in xplorerFiles else "OK" if 'PDL' in xplorerFiles else "NONE"

                # write row to CSV
                xplorerLast = '=HYPERLINK("'+ xPlorerLastSession +'","'+str(lastSession).replace("\r\n","")+'")'
                netLastPath = netLogPath.replace("/mnt","r:")
                try:
                    writer.writerow([ execRow[0], execRow[1], execRow[4], execRow[16], execRow[6], netLastPath, xplorerLast, UID, UID_MblksRecords, UID_And_Header,
                        UID_bad_blocks, UID_metablocks, UID_spare_blocks, UID_wear_leveling, faDumpFile, faScheme, sessionLOG, SmartReportBIN, smartReportJson, smartReportCSV, 
                        faDumpFile, faScheme, PDL, PDL_Jsonl, PDL_analysis, MST_log, MST_last, UseRomMetaDataCSV, UseRomMetaDataBIN, xplorerFad, xplorerSmartReport, xplorercoverage,
                        xplorerUromTrimMetadata, xplorerUIDMetablocks, xplorerMST1, xplorerMST2, xplorerPDL
                    ]) # ignore MST temp
                except Exception as CSVWriteException:
                    reLog([
                        ("[REL CSVWriteException] \n\t%s" % CSVWriteException),
                        ("\t\t row %s " % str(execRow))
                    ])
            else:
                reLog([
                    ("[REL Info] no xPlorer session or execution terminated 'NO PlatformProperties' in: "),
                    ("\t\t Cycle> %s ExecutionID> %s Test> %s" % (execRow[0], execRow[1], execRow[16]))
                ])
                    

    csvFile.close()
    
    


#######################################################################################################################################
#######################################################################################################################################
#######################################################################################################################################

if __name__ == "__main__":

    ####################################################################
    # INIT script 
    ####################################################################
    # reset the execution log file
    reLog("[REL Info] new execution started",mode="w")
    # CSV file header
    CSVheader = ["CycleID", "ExecID", "Adapter", "TestName", "status", "logPath", "xPlorerLastSession","UID", "UID_MblksRecords", "UID_And_Header", 
                "UID_bad_blocks", "UID_metablocks", "UID_spare_blocks", "UID_wear_leveling", "faDumpFile", "faScheme", "sessionLOG", "SmartReportBIN", "smartReportJson", "smartReportCSV", 
                "faDumpFile", "faScheme", "PDL", "PDL_Jsonl", "PDL_Analysis", "MST_log", "MST_last" , "UseRomMetaDataCSV", "UseRomMetaDataBIN", "xplorerFAD", "xplorerSmartReport", "xplorercoverage",
                "xplorerUromTrimMetadata", "xplorerUIDMetablocks", "xplorerMST1", "xplorerMST2", "xplorerPDL"]
   
    # set distenation folder for report CSV file [Empty = same folder as the script]
    reportDistenation = ""

    # check if running from Jenkins
    # if getpass.getuser() != 'jenkins':
    #     reLog("Not running from Jenkins Exit")
    #     exit(1)
    xPlorerServer = os.environ['xPlorer_server']
    # ### @TODO get FW str function from REL_TP ####
    relExceutionDumps = os.environ['relExceutionDumps']
    # init data base connection install ODBC driver from MS @https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15
    odbcCourser = DBConnect('Driver={ODBC Driver 18 for SQL Server};Server=10.24.8.188;Database='+os.environ['ReliabilityGrid']+';uid=Ace;pwd=Ace2018!;Encrypt=No')
    reportDistenation = '' #os.path.join("/","srv","jenkins","reports")
    executeQuery(os.environ['Cycles'])
    
########################################################################################################################
    ### Cycles , Status[Default "ALL"], Results [Default "ALL"] 
    # executeQuery("2096,2097,2098,2099,2100,2101,2102,2103,2104,2105")
    ########################################################################################################################


