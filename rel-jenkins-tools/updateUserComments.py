'''
Script for updating setupmappings usercommint

Author Rawad.Kharma@wdc.com
'''
import sys
from datetime import datetime
import pypyodbc
import os

def relog(rel_message, mode="a+"):
    '''
    log to filr and stdout
    '''
    log_file = open("run_log.txt", mode)
    log_file.write("\n%s" % datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    if not (isinstance(rel_message, str)) \
        and not (isinstance(rel_message, unicode)) \
        and (len(rel_message) > 1):
        print rel_message[0]
        if "Error" in rel_message[0]:
            print "[REL Info] Check run_log.txt"
        log_file.write("%s " % rel_message[0])
        rel_message.pop(0)
        for message in rel_message:
            log_file.write("\n\t%s"%(str(message)))
    else:
        print str(rel_message)
        log_file.write("\t%s" % rel_message)
    log_file.close()

def db_connect(uid, pwd, server, dbn):
    '''
    # config and INIT DB connection
    return: OBJ database connection cursor
    '''
    try:
        if sys.platform == "win32":
            str_connection = "Driver={SQL Server};Server=%s;uid=%s;pwd=%s; database=%s; Encrypt=No" %\
                (str(server), uid, pwd, dbn)
            relog("[INFO] connection string: %s" % str_connection)
            connection = pypyodbc.connect(str_connection)
        else:
            str_connection = "Driver={ODBC Driver 18 for SQL Server};\
                Server=%s;uid=%s;pwd=%s; database=%s; Encrypt=No" %\
                (str(server), uid, pwd, dbn)
            relog("[INFO] connection string: %s" % str_connection)
            connection = pypyodbc.connect(str_connection)
            connection.unicode_results = True
        return connection.cursor()
    except Exception as sql_server_exception:
        relog(["[REL Error] DB connection failed", sql_server_exception])
        sys.exit(1)

def execute_query(add_hw,comm=None):
    '''
    main method
    str add_hw
    '''
    sql_select = """
        SELECT [dbo].[SetupMappings].[ID]
        ,[dbo].[SetupMappings].[PlatformProperties]
        ,[dbo].[SetupMappings].[InspectorSerial]
        ,[dbo].[SetupMappings].[AdditionalHW]
        ,[dbo].[SetupMappings].[UserComments]
        ,[dbo].[SetupMappings].[DUTSerial]

        FROM [dbo].[SetupMappings]
        WHERE [AdditionalHW] = '{add_hw}' 
    """
    sql_update = """
        UPDATE [dbo].[SetupMappings]
        SET [dbo].[SetupMappings].[UserComments] = CONCAT(UserComments,' {comm}')
        WHERE [dbo].[SetupMappings].[DUTSerial] = '{uid}' 
    """
    sql_cardbio = """
        SELECT [Id]
        ,[uid]
        ,[stickerid]
        ,[siteID]
        ,[LOT]
        ,[Rel_project]
        ,[UROM_data]
        ,[IR]
        ,[Device_Capacity]
        FROM [CardBio_DWH].[dwh].[uidsticker]
        WHERE [uid] = '{uid}'
    """

    relog([("[INFO] running Query params:\n\t ADD HW = %s " % (add_hw))])
    quiry = sql_select.format(add_hw=str(add_hw))
    relog([("[REL Info] running query"), (quiry)])
    print "\n\n\t\t PLEASE WAIT!!!!!"

    try:
        ODBC_COURSER.execute(quiry)
    except Exception as sql_query_exception:
        relog("[REL Error] failed running sql Query !! \n %s" % sql_query_exception)
        sys.exit(1)
    else:
        res_rows = ODBC_COURSER.fetchall()
    for row in res_rows:
        if comm:
            update_quiry = sql_update.format(comm=str(comm), uid=str(row[5]))
            print update_quiry
            ODBC_COURSER.execute(update_quiry)
            ODBC_COURSER.commit()
        else:
            ODBC_CARDBIO_COURSER.execute(sql_cardbio.format(uid=str(row[5])))
            cardbio = ODBC_CARDBIO_COURSER.fetchone()
            device_size_dict = {
                1: "11x13",
                2: "11.5x13"
            }
            if cardbio[8].isdigit() and int(cardbio[8]) in device_size_dict:
                print 80 * "*"
                comm = device_size_dict[int(cardbio[8])]
            else:
                comm = str(cardbio[8])
            update_quiry = sql_update.format(comm=str(comm), uid=str(cardbio[1]))
            print update_quiry
            ODBC_COURSER.execute(update_quiry)
            ODBC_COURSER.commit()
            comm=None
###############################################################################
###############################################################################
###############################################################################

if __name__ == "__main__":
    ####################################################################
    # INIT script
    ####################################################################
    # # reset the execution log file
    relog("[REL Info] new execution started", mode="w")

    # # init data base connection
    # WDIL ip: 10.24.8.188
    # WDIN ip: 10.206.65.131
    try:
        ODBC_COURSER = db_connect(uid='Ace', pwd='Ace2018!', \
            server='10.24.8.188', dbn='REL_Halo2_110')
    except Exception as rel_exception:
        relog(["[REL Error] DataBase connection INIT Failed!! ", rel_exception])
        sys.exit(1)
    try:
        ODBC_CARDBIO_COURSER = db_connect(uid='ETL', pwd='etl123!', \
            server='10.24.8.170', dbn='CardBio_DWH')
    except Exception as rel_exception:
        relog(["[REL Error] DataBase connection INIT Failed!! ", rel_exception])
        sys.exit(1)

    ### AdditionalHW
    AdditionalHW = os.environ.get('AdditionalHW')
    if not os.environ.get('AdditionalHW'):
        print("[ERR] No AdditionalHW!!!!")
        sys.exit(1)
    comm=None
    if os.environ.get('comment'):
        comm = os.environ.get('comment')
    execute_query(AdditionalHW, comm)

