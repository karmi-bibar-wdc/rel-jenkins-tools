###############################################################################
# Script by rawad.kharma@wdc.com
# Reliability team tefen,il
###############################################################################
# Get test devices and the LOT
# Ver Beta 0.0.1

import os
import sys
from datetime import datetime
import csv
import pypyodbc

def reLog(relMessage, mode="a+"):
    """ Rel log helper

    Args:
        relMessage (_type_): str / List
        mode (str, optional): log file write mode. Defaults to "a+".
    """
    file_log = open("run_log.txt", mode)
    file_log.write("\n%s" % datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    if isinstance(relMessage, str) & isinstance(relMessage, unicode) & (len(relMessage) > 1):
        print relMessage[0]
        if "Error" in relMessage[0]: print "[REL Info] Check run_log.txt"
        file_log.write("%s " % relMessage[0])
        relMessage.pop(0)
        for str_message in relMessage:
            file_log.write("\n\t%s"%(str(str_message)))
    else:
        print str(relMessage)
        file_log.write("\t%s" % relMessage)
    file_log.close()

def DBConnect(uid, pwd, server, db):
    '''
    # config and INIT DB connection
    return: OBJ database connection cursor
    '''
    try:
        if sys.platform == "win32":
            str_conn = "Driver={SQL Server};Server=%s;uid=%s;pwd=%s; database=%s; Encrypt=No"\
                % (str(server), uid, pwd, db)
            reLog("[INFO] connection string: %s" % str_conn)
            connection = pypyodbc.connect(str_conn)
        else:
            str_conn = "Driver={ODBC Driver 18 for SQL Server};Server=%s;uid=%s;pwd=%s;database=%s;Encrypt=No"\
                % (str(server), uid, pwd, db)
            reLog("[INFO] connection string: %s" % str_conn)
            connection = pypyodbc.connect(str_conn)
            connection.unicode_results = True
        return connection.cursor()
    except pypyodbc.Error as exception:
        reLog(["[REL Error] DB connection failed", exception])
        sys.exit(1)


def executeQuery(DUTID=None):
    SQL = """SELECT TOP (1000)
      [PlatformProperties]
      ,[AdditionalHW]
      ,[DUTSerial]
      ,[Capacity]
      ,[FirmwareVersion]
      ,[DeviceType]
      ,[InspectorSerial]
      ,[UserComments]
      ,null as [stickerid]
      ,null as [siteID]
      ,null as [LOT]
      ,null as [Rel_project]
      ,null as [UROM_data]
      ,null as [IR]
      ,null as [Device_Capacity]
  FROM [REL_Halo2_110].[dbo].[SetupMappings] """

    if (DUTID):
        SQL += "\nWHERE[dbo].[SetupMappings].[DUTSerial] in (%s) " % (DUTID)
        reLog("[INFO] props ^DUTIDs: %s" % str(DUTID))

    reLog([("[INFO] running Query params:\n\t %s " % (DUTID))])
    reLog([("[REL Info] running query:"),(SQL)])
    odbcCourser.execute(SQL)
    print("\n\n\t\t PLEASE WAIT!!!!!")

    try:
        odbcCourser.execute(SQL)
    except Exception as SQLQueryException:
        reLog("[REL Error] failed running sql Query !! \n %s" % SQLQueryException)
        sys.exit(1)
    else:
        resExecutions = odbcCourser.fetchall()

    try:
        with open("LotReport.csv", 'wb') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow([column[0] for column in odbcCourser.description])
            for i in resExecutions:
                odbcLOTCourser.execute("SELECT * FROM [dwh].[uidsticker] WHERE [dwh].[uidsticker].[uid] = '%s'" % i[2])
                row = odbcLOTCourser.fetchone()
                try:
                    writer.writerow([i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],row[2],row[3],row[4],row[5],row[6],row[7],row[8]])
                except Exception as CSVWriteException:
                    reLog([("[REL CSVWriteException] \n\t%s" % CSVWriteException),("\t\t row %s " % str(row))])
                        
    except Exception as SQLQueryException:
        reLog("[REL Error] failed running sql Query !!")
        sys.exit(1)

    reLog("[INFO] number of rows returned from DataBase: %s" % str(len(resExecutions)))
    reLog("[INFO] DONE")

###################################################################################################
###################################################################################################
###################################################################################################

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
        if not os.environ.get('Cycle') and not os.environ.get('Adapter') and not os.environ.get('DUTID'):
            reLog(["[REL Error] need to set minimum one param ","set Cycle or Adapter or DUTID"])
            sys.exit(1)
    except Exception as e:
        reLog(["[REL Error] set Cycle or Adapter or DUTID !!",e])
        sys.exit(1)
    else:
        ### WDIN_REL_Side_A_1005 / REL_Side_A_39 / REL_Side_B_49
        DUTIDs = None
        if os.environ.get('DUTID'):
            DUTIDs = os.environ.get('DUTID').split(',')
            DUTIDs = DUTIDs if len(DUTIDs) > 1 else str(DUTIDs[0])

        executeQuery(os.environ.get('DUTID',None) )
    ####################################################################
