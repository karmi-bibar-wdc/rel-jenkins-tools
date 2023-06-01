####################################################################
# Script by rawad.kharma@wdc.com
# Reliability team tefen,il
####################################################################

# Ver Beta 0.8

import os
import pypyodbc
# import shutil
import csv
# import re
# import pip
import win32wnet
# import time

# set distention folder for report CSV file [Empty = same folder as the script]
dest = ""
# dest = "\\\\10.24.8.26\\tfnlab\\REL\\EagleM_LOGS\\"

connection = pypyodbc.connect('Driver={SQL Server};'
                            'Server=10.24.8.188;'
                            'Database=REL_Side_A_39;'
                            'uid=Ace;pwd=Ace2018!;Encrypt=No')
cursor = connection.cursor()

# @TODO add Linux support
# wmiexec.py './Lab_Admin:Qwerty123!'@10.24.197.37 'findstr /C:"SPO = " "D:\Results\FaFolder\faFolder_54757\Test_Power_On_Off_4Rerun_20220322_172802_54757\VTFLog.log" '


def wnet_connect(host, username, password):
    unc = ''.join(['\\\\', host])
    try:
        win32wnet.WNetAddConnection2(0, None, unc, None, username, password)
    except Exception, err:
        if isinstance(err, win32wnet.error):
            # Disconnect previous connections if detected, and reconnect.
            if err[0] == 1219:
                win32wnet.WNetCancelConnection2(unc, 0, 0)
                return wnet_connect(host, username, password)
        print("[InfoNET] cant connect to " + host + "\n")
        print(err)


def executeQuery(Cycle, Status=False, Result=False):
    global dest

    sql = """SELECT [dbo].[Cycles].[ID] as [CycID]
      ,[dbo].[Executions].[ID] as [EXEID]
      ,[dbo].[Executions].[SetupMappingID]
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
      ,[dbo].[EnumResults].[Name]
      ,[dbo].[Executions].[StartDateTime]
      ,[dbo].[Executions].[EndDateTime]
      ,[dbo].[Executions].[DeviceType]
      
      FROM [dbo].[Cycles]
      JOIN [dbo].[CycleDefinition] ON [dbo].[CycleDefinition].[CycleID] = [dbo].[Cycles].[ID]
      JOIN [dbo].[Executions] ON [dbo].[CycleDefinition].ID = [dbo].[Executions].[CycleDefinitionID]
      JOIN [dbo].SetupMappings ON [dbo].[SetupMappings].[ID] = [dbo].[Executions].[SetupMappingID]
      JOIN [dbo].[EnumExecutionStatuses] ON [dbo].[EnumExecutionStatuses].Id = [dbo].[Executions].[Status]
      JOIN [dbo].[EnumResults] ON [dbo].[EnumResults].[Id] = [dbo].[Executions].[Result]
      WHERE [dbo].[Cycles].[ID] in ( %s )""" % Cycle
    if Status:
        sql = sql + "AND [dbo].[Executions].[Status] = %s" % str(Status)
    if Result:
        sql = sql + "AND [dbo].[Executions].[Result] = %s" %  str(Result)

    print "running query on:\n\tCycles: %s\n\tStatus: %s\n\tResult: %s" % (Cycle, Status, Result)
    print "\n\n\t\t PLEASE WAIT!!!!!"
    cursor.execute(sql)
    eee = cursor.fetchall()

    print sql
    csvdest = os.path.join(dest,"IsAvgPEC_report_"+Cycle+".csv")
    with open(csvdest, 'wb') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(["CycleID", "Start", "End", "ADD_HW", "ExecID", "FLX", "logPath", "status", "Capacity","DUTSerial", "DeviceType", "Results", "IS_SLC", "Enhanced_SLC", "MLC"])

        for execution in eee:
            # print execution
            wnet_connect(str(execution[3]), r".\Lab_Admin", "Qwerty123!")
            baseDir = '\\\\'+str(execution[3])+'\\D$\\Results\\FaFolder\\faFolder_'+str(execution[1])
            print "base:"+baseDir
            # endTime = execution[19] if execution.length() > 18 else 0
            if not os.path.exists(baseDir):
                print(" [Info] '" + baseDir + "' folder not accessible!!" + str(execution))
                writer.writerow([execution[0], execution[16], execution[17], execution[4], execution[1], execution[5], baseDir, execution[7], execution[12], execution[13], execution[18], execution[15], "" ])
            else:
                for logDir in os.listdir(baseDir):
                    if os.path.isdir(os.path.join(baseDir, logDir)):
                        for file in os.listdir(os.path.join(baseDir, logDir)):
                            if file == "VTFLog.log":
                                t1 = os.path.join(baseDir, logDir, file)
                                FileHandler = open(t1,"r")
                                FileLines = FileHandler.readlines()
                                # IS_SLC = Enhanced_SLC = MLC = 0
                                for line in reversed(FileLines):
                                    if "<seq_D_DltHost> Pec values are :" in line:
                                        lineSegments = line.split("<seq_D_DltHost> Pec values are :")
                                        rowData = lineSegments[1].strip("</seq_D_DltHost>\n")
                                        data = rowData.split(",")
                                        print data
                                        break
                        # if file == "CTFLog.txt":
                        #     t2 = time.ctime(os.path.getmtime(os.path.join(os.path.join(baseDir, logDir), file)))

                        # if file == "Coverage_log.log":
                        #     t3 = time.ctime(os.path.getmtime(os.path.join(os.path.join(baseDir, logDir), file)))

                        writer.writerow([ execution[0],  execution[16], execution[17], execution[4], execution[1], execution[5], os.path.join(baseDir, logDir), execution[7], execution[12], execution[13], execution[18], execution[15], data[0], data[1], data[2], ])


            #             # mydest = os.path.join(dest,str(execution[4])+"_"+str(execution[0]))
            #             FileHandler = open(myfile,"r")
            #             FileLines = FileHandler.readlines()

###############################################################################
###############################################################################
###############################################################################

# usage
# AND [dbo].[Executions].[Status] = 1
#         0	Pending
#         1	Running
#         2	Completed
#         3	Terminated
#         4	PausedOnError
#         5	TimeOut
#         6	Replaced
#         7	TestComplete
#         */
#
# AND [dbo].[Executions].[Result] = 0
#         /*
#         0	None
#         1	Pass
#         2	Fail
#         3	Error
#         4	Unknown
#         5	PassWithException
#         6	FailWithException
#         7	Stuck
#         */
#         """

# Cycles , Status[Default "Running"], Results [Default "None"] 


###############################################################################
###############################################################################
###############################################################################

# findstr /N /C:"SPO = " c:\VTFLog.log


executeQuery("4765")
