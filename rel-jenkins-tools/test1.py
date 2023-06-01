# DILUTED = 100
# k=0
# for i in range(2555):
#     if i % DILUTED == 1:
#         print "Line: " + str(i) + " DILUTED: " + str(i%DILUTED) + " K:" +str(k)
#         k = k + 1

############################################################################################################
# for mac
# sudo ln -sfn /usr/local/Cellar/openssl@1.1/1.1.1o /usr/local/opt/openssl
# import pyodbc
# try:
#     CONNECTION = pyodbc.connect(r"Driver={ODBC Driver 18 for SQL Server};Server=10.0.158.146\DWH;Database=REL;uid=rel_user;pwd=rel_user!123;Encrypt=No")
#     CONNECTION.cursor()
# except Exception as SQLServerException:
#     print("[REL Error] DB connection failed \n", SQLServerException)
#     print(SQLServerException)
#     exit(1)
############################################################################################################
# import xml.etree.ElementTree as ET
# import glob

# def mergexmlcontent(myList =[], *args):
#     global insertion_point, data, xml_files
#     root = ET.Element("document")
#     for x in myList:
#         filepath = 'Files'+'/'+x
#         xml_files = glob.glob(filepath)
#         xml_element_tree = None

#         for xml_file in xml_files:
#             xml_element_root = ET.parse(xml_file).getroot()
#             print(ET.tostring(xml_element_root, encoding='utf8', method="xml").decode())

# list = ['File1.xml', 'File2.xml']
# mergexmlcontent(list)