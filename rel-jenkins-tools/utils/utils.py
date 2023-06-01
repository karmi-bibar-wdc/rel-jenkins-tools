import os,sys
from datetime import datetime
import pyodbc

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
