####################################################################
# Script by rawad.kharma@wdc.com
# Reliability team tefen,il
####################################################################

# Ver Beta 0.8

import os
import pypyodbc
import shutil
import csv
import re
import pip
import win32wnet
import time

# set distenation folder for report CSV file [Empty = same folder as the script]
dest = ""
# dest = "\\\\10.24.8.26\\tfnlab\\REL\\Ali_Morad\\EagleM_LOGS\\"

connection = pypyodbc.connect('Driver={SQL Server};'
                            'Server=10.24.8.188;' 
                            'Database=REL_Side_A_39;'
                            'uid=Ace;pwd=Ace2018!;Encrypt=No')
cursor = connection.cursor()
TW_Conn = pypyodbc.connect('Driver={SQL Server};Server=10.0.158.146\\DWH;Database=REL;uid=rel_user;pwd=rel_user!123;Encrypt=No')

bufferSizeSQLCourser =  TW_Conn.cursor()
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
      ,[dbo].[Executions].[Result]
      
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
        SQL = sql + "AND [dbo].[Executions].[Result] = %s" %  str(Result)

    print "running query on:\n\tCycles: %s\n\tStatus: %s\n\tResult: %s" % (Cycle, Status, Result)
    print "\n\n\t\t PLEASE WAIT!!!!!"
    cursor.execute(sql)
    eee = cursor.fetchall()
    # print eee
    print sql
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
    csvdest = os.path.join(dest,"IsAvgPEC_report_"+Cycle+".csv")
    with open(csvdest, 'wb') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(["CycleID", "Start", "End", "ADD_HW", "ExecID", "FLX", "logPath", "status", "Capacity","DUTSerial", "DeviceType", "Results", "IS_SLC", "Enhanced_SLC", "MLC", "Ratio", "Buffer"])

        for execc in eee:
            # print execc
            bufferSizeSQL = BUF_SIZE_SQL.format(id=str(execc[13]))
            bufferSizeSQLCourser.execute(bufferSizeSQL)
            bufferSizeSQLExecution = bufferSizeSQLCourser.fetchone()
            if not bufferSizeSQLExecution:
                raise Exception("no REL_TW_details for execution")
            
            wnet_connect(str(execc[3]), r".\Lab_Admin", "Qwerty123!")
            baseDir = '\\\\'+str(execc[3])+'\\D$\\Results\\FaFolder\\faFolder_'+str(execc[1])
            print "base:"+baseDir
            # endTime = execc[19] if execc.length() > 18 else 0
            if not os.path.exists(baseDir):
                print(" [Info] '" + baseDir + "' folder not accessible!!\n" + str(execc))
                writer.writerow([ execc[0], execc[16], execc[17], execc[4], execc[1], execc[5], baseDir, execc[7], execc[12], execc[13], execc[18], execc[15], "","","","","" ])
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
                                        data = []
                                        lineSegments = line.split("<seq_D_DltHost> Pec values are :")
                                        rowData = lineSegments[1].strip("</seq_D_DltHost>\n")
                                        dataLine = rowData.split(",")
                                        for e in dataLine:
                                            data.append(e.split("=")[1].strip())
                                        print data
                                        break
                        # if file == "CTFLog.txt":
                        #     t2 = time.ctime(os.path.getmtime(os.path.join(os.path.join(baseDir, logDir), file)))

                        # if file == "Coverage_log.log":
                        #     t3 = time.ctime(os.path.getmtime(os.path.join(os.path.join(baseDir, logDir), file)))

                        writer.writerow([ execc[0],  execc[16], execc[17], execc[4], execc[1], execc[5], os.path.join(baseDir, logDir), execc[7], execc[12], execc[13], execc[18], execc[15], data[0], data[1], data[2], data[3], bufferSizeSQLExecution[1]])


            #             # mydest = os.path.join(dest,str(execc[4])+"_"+str(execc[0]))
            #             FileHandler = open(myfile,"r")
            #             FileLines = FileHandler.readlines()

#######################################################################################################################################
#######################################################################################################################################
#######################################################################################################################################

### usage 
### AND [dbo].[Executions].[Status] = 1
###         0	Pending
###         1	Running
###         2	Completed
###         3	Terminated
###         4	PausedOnError
###         5	TimeOut
###         6	Replaced
###         7	TestComplete
###         */
### 
### AND [dbo].[Executions].[Result] = 0
###         /*
###         0	None
###         1	Pass
###         2	Fail
###         3	Error
###         4	Unknown
###         5	PassWithException
###         6	FailWithException
###         7	Stuck
###         */
###         """

### Cycles , Status[Default "Running"], Results [Default "None"] 

executeQuery("5959,5958,5957")

#######################################################################################################################################
#######################################################################################################################################
#######################################################################################################################################

# findstr /N /C:"SPO = " c:\VTFLog.log

