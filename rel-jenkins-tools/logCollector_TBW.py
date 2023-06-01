####################################################################
# Script by rawad.kharma@wdc.com
# Reliability team tefen,il
####################################################################

# Ver Beta 0.9

import os
import pypyodbc
import shutil
import csv
import re
import pip
from utils import utils as utils
import sys
import traceback


def executeQuery(Cycle):
    TbwDict = {128: {8: 20, 4: 40, 2: 80,  1: 160},
                   256: {8: 40, 4: 80, 2: 160, 1: 320},
                   512: {8: 80, 4: 160,2: 320, 1: 640}
                    #       64     128     256     512
        }

    sql = """SELECT [dbo].[Cycles].[ID] as [CycID]
      ,[dbo].[Executions].[ID] as [EXEID]
      ,[dbo].[Executions].[SetupMappingID]
      ,[dbo].[Executions].[StationName]
      ,[dbo].[Cycles].[AdditionalHW]
      ,[dbo].[Executions].[PlatformProperties]
      ,[dbo].[Cycles].[Description]
      ,[dbo].[EnumExecutionStatuses].[Name] as [ExecStatus]
      ,[dbo].[Cycles].[Project]
      ,[dbo].[Cycles].[FirmwareVersion]
      ,[dbo].[Cycles].[ResultInfo]
      ,[dbo].[CycleDefinition].[ID] as [DEFID]
      ,[dbo].[Executions].[Capacity]
      ,[dbo].[Executions].[DeviceType]
      ,[dbo].[Executions].[DUTSerial]
      ,[dbo].[Executions].[AdditionalInfo]
      ,[dbo].[EnumResults].[Name]
      FROM [dbo].[Cycles]
      JOIN [dbo].[CycleDefinition] ON [dbo].[CycleDefinition].[CycleID] = [dbo].[Cycles].[ID]
      JOIN [dbo].[Executions] ON [dbo].[CycleDefinition].ID = [dbo].[Executions].[CycleDefinitionID]
      JOIN [dbo].[EnumExecutionStatuses] ON [dbo].[EnumExecutionStatuses].Id = [dbo].[Executions].[Status]
      JOIN [dbo].[EnumResults] ON [dbo].[EnumResults].[Id] = [dbo].[Executions].[Result]
      WHERE [dbo].[Cycles].[ID] in ( %s )""" % (Cycle)

    print "running query on:\n\tCycles: %s" % (Cycle)
    print "\n\n\t\t PLEASE WAIT!!!!!"
    utils.reLog([("[INFO] running Query params:\n\t %s  " % (Cycle))])
    utils.reLog([("[REL Info] running query:"),(sql)])
    
    print "running query on:\n\tCycles: %s" % (Cycle)
    print "\n\n\t\t PLEASE WAIT!!!!!"
    cursor.execute(sql)
    executions = cursor.fetchall()
    results = {}

    for execution in executions:
        print 80*">"
        DeviceType = int(execution[13][:3]) 
        try:
            TbwTarget = TbwDict[DeviceType][DeviceType / execution[12]]
        except Exception:
            utils.reLog([" [Warn] 'Can't get device Capacity or Type !!\n",str(execution)])
            continue
            
        # Set logs folder path
        mydir = '/mnt/tfnlab/REL/ExceutionDumps/TBW_Logs/'
        if os.name == 'nt':
            mydir = '\\\\10.24.8.10\\tfnlab\\REL\\ExceutionDumps\\TBW_Logs'
        # check if log folder exest on station
        if not os.path.exists(mydir):
            print "log file not available %s" % mydir
            utils.reLog([" [Warn] '" + mydir + "' folder not accessible!!\n",str(execution)])
            continue
        else:
            myfile = os.path.join(mydir, execution[14]+".txt")
            utils.reLog(["reading log file %s" % myfile])
            FileHandler = open(myfile,"r")
            FileLines = FileHandler.readlines()
            for line in reversed(FileLines):
                print "LINE: %s" % line
                if "Data written to file:" in line:
                    col = line.split("file:")[1].split(' ')
                    if execution[14] in results.keys():
                        newexecutionount = results[execution[14]][14] + 1
                        results[execution[14]] = [ execution[0], execution[4], execution[1], execution[5], utils.fixpath(myfile), execution[7], execution[14], execution[13], execution[12], execution[16], float(col[1]), TbwTarget, TbwTarget - float(col[1]),col[7], newexecutionount ]
                    else:
                        results[execution[14]] = [ execution[0], execution[4], execution[1], execution[5], utils.fixpath(myfile), execution[7], execution[14], execution[13], execution[12], execution[16], float(col[1]), TbwTarget, TbwTarget - float(col[1]),col[7], 1 ]
                    break
        print 80*"<"
    
    with open("TBW_report.csv", 'wb') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(["CycleID", "ADD_HW", "ExecID", "FLX", "logPath", "status", "DUTSerial", "Capacity", "DeviceType", "Result", "TBW", "Target", "Delta","Estimated_end_date", "executionount" ])
        for res in results:
            writer.writerow(results[res])
            



#######################################################################################################################################
#######################################################################################################################################
#######################################################################################################################################

if __name__ == "__main__":
    ####################################################################
    # INIT script 
    ####################################################################
    # # reset the execution log file
    utils.reLog("[REL Info] new execution started", mode="w")
    # check for env vars
    try:
        grid = os.environ['ReliabilityGrid']
    except Exception as RelException:
        utils.reLog(["[REL Error] Reliability Grid not set !! ", RelException])
        sys.exit(1)
    
    if os.environ.get('Cycles'):
        CycleIDs = [x.strip() for x in os.environ.get('Cycles').split(',')]
        CycleIDs = CycleIDs if type(CycleIDs) == str() else ','.join(CycleIDs)
    
    if os.name == 'nt':
        connection = pypyodbc.connect('Driver={SQL Server};Server=10.24.8.188;Database=%s;uid=Ace;pwd=Ace2018!;Encrypt=No' % grid)
    else:
        connection = pypyodbc.connect('Driver={ODBC Driver 18 for SQL Server};Server=10.24.8.188;Database=%s;uid=Ace;pwd=Ace2018!;Encrypt=No' % grid)
    
    try:
        cursor = connection.cursor()
        executeQuery(CycleIDs)
    except Exception as RelException:
        # traceback.print_exc()
        utils.reLog(["[REL Error] Failed!! ", RelException,traceback.print_exc()])
        sys.exit(1)

#######################################################################################################################################
#######################################################################################################################################
#######################################################################################################################################


