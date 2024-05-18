import mysql.connector
from datetime import datetime, timedelta
import schedule
import time
import requests
import json
import time
import asyncio
import aiohttp

def count():
    #epion admission
    conn1 = mysql.connector.connect(host="localhost", user="root", password="rational", database="epion")
    cursor1 = conn1.cursor()
    sql_query = "SELECT COUNT(DISTINCT AdmissionID) FROM trnadmission"
    cursor1.execute(sql_query)
    rows1 = cursor1.fetchall()
    print(rows1[0][0])
    cursor1.close()
    conn1.close()


    conn2=mysql.connector.connect(host="localhost", user="root", password="rational", database="cashlessai")
    cursor2=conn2.cursor()

     #salus admission
    sql_query2 = "SELECT COUNT(DISTINCT AdmissionID) FROM trnadmission" 
    cursor2.execute(sql_query2)
    rows2 = cursor2.fetchall()
    print(rows2[0][0])



#preauth saved count
    sql_query3 = "SELECT COUNT(DISTINCT AdmissionID) AS total_entries FROM trnpreauth" 
    cursor2.execute(sql_query3)
    rows3 = cursor2.fetchall()
    print(rows3[0][0])



    #preauth verified sent
    sql_query4 = "SELECT count(DISTINCT AdmissionID) FROM auditemailsent WHERE Attachment REGEXP 'PreAuthForm\.pdf|PreAuthForm.pdf';" 
    cursor2.execute(sql_query4)
    rows4 = cursor2.fetchall()
    print(rows4[0][0])

    #preauth email sent
    sql_query5 = "SELECT count(DISTINCT AdmissionID) FROM auditemailsent WHERE Attachment REGEXP 'PreAuthForm\.pdf|PreAuthForm.pdf';" 
    cursor2.execute(sql_query5)
    rows5 = cursor2.fetchall()
    print(rows5[0][0])

#preauth Approved
    sql_query6 = """
        SELECT * 
        FROM cashlessai.trnadmission 
        left outer JOIN auditpreauthresponses ON trnadmission.AdmissionID = auditpreauthresponses.AdmissionID
        WHERE EStatus='Pre-Auth Approved' and Action = "new";

                """
    cursor2.execute(sql_query6)
    rows6 = cursor2.fetchall()
    print(rows6[0][0])

#preauth query received
    sql_query7 = """
select count(*)
from cashlessai.trnadmission adm
    inner join auditpreauthqueries res on res.AdmissionID=adm.AdmissionID  AND res.ReasonType = 'Query'
    inner join auditemailsent sen on sen.AdmissionID=adm.AdmissionID
    where (sen.Type = 'PreAuth' or sen.Type = 'PreAuth Query Reply' or sen.Type='PreAuth Rejection Reply');

                """
    cursor2.execute(sql_query7)
    rows7 = cursor2.fetchall()
    print(rows7[0][0])

#preauth query reply
    sql_query8 = """
		SELECT count(*)
        FROM auditlogadmission 
        INNER JOIN trnadmission ON trnadmission.ThirdPartyAdmissionID = auditlogadmission.ThirdPartyAdmissionID 
        INNER JOIN auditemailsent ON trnadmission.AdmissionID = auditemailsent.AdmissionID
        WHERE auditemailsent.Type='PreAuth Query Reply';
                """
    cursor2.execute(sql_query8)
    rows8 = cursor2.fetchall()
    print(rows8[0][0])

#preauth rejection

    sql_query9 = """
select count(*) 
from cashlessai.trnadmission adm
    left outer join auditpreauthqueries res on res.AdmissionID=adm.AdmissionID  AND res.ReasonType = 'Reject'
    left outer join auditemailsent sen on sen.AdmissionID=adm.AdmissionID
    WHERE 
    sen.Type IN ('PreAuth', 'PreAuth Query Reply', 'PreAuth Rejection Reply');
                """
    cursor2.execute(sql_query9)
    rows9 = cursor2.fetchall()
    print(rows9[0][0])

#preauth rejection reply count
    sql_query10 = """
		SELECT COUNT(*)
        FROM trnadmission 
        left outer JOIN auditemailsent ON trnadmission.AdmissionID = auditemailsent.AdmissionID
        WHERE auditemailsent.Type='PreAuth Rejection Reply';

                """
    cursor2.execute(sql_query10)
    rows10 = cursor2.fetchall()
    print(rows10[0][0])


#enhancement request sent

    sql_query10 = """
        SELECT COUNT(*) AS RecordCount
        FROM cashlessai.trnadmission 
        INNER JOIN auditpreauthresponses ON trnadmission.AdmissionID = auditpreauthresponses.AdmissionID
        WHERE Action = "update";

                """
    cursor2.execute(sql_query10)
    rows10 = cursor2.fetchall()
    print(rows10[0][0])

#enhancement request sent
    sql_query11 = """
SELECT 
count(*)
FROM 
    trnadmission
Inner JOIN 
    trnpreauthresponses ON trnadmission.AdmissionID = trnpreauthresponses.AdmissionID
INNER JOIN 
    auditemailsent ON auditemailsent.AdmissionID = trnadmission.AdmissionID
WHERE 
    auditemailsent.Type IN ('Enhancement', 'Enhancement Rejection' ,'Enhancement Rejection Reply');

                """
    cursor2.execute(sql_query11)
    rows11 = cursor2.fetchall()
    print(rows11[0][0])


#enhancement approved 
    sql_query12 = """
SELECT 
count(*)
FROM 
    trnadmission
Inner JOIN 
    trnpreauthresponses ON trnadmission.AdmissionID = trnpreauthresponses.AdmissionID
INNER JOIN 
    auditemailsent ON auditemailsent.AdmissionID = trnadmission.AdmissionID
WHERE 
    auditemailsent.Type IN ('Enhancement', 'Enhancement Rejection' ,'Enhancement Rejection Reply');

                """
    cursor2.execute(sql_query12)
    rows12 = cursor2.fetchall()
    print(rows12[0][0])

#enhancement query
    sql_query13 = """
select count(*)
from trnadmission adm
    inner join auditpreauthqueries res on res.AdmissionID=adm.AdmissionID
    inner join auditemailsent sen on sen.AdmissionID=adm.AdmissionID
    where ReasonType='Query' and sen.Type='Enhancement' or sen.Type='Enhancement Rejection' or sen.Type='Enhancement Rejection Reply'
                """
    cursor2.execute(sql_query13)
    rows13 = cursor2.fetchall()
    print(rows13[0][0])

#enhancement reply count
    sql_query14 = """
        SELECT COUNT(*)
        FROM auditlogadmission 
        INNER JOIN trnadmission ON trnadmission.ThirdPartyAdmissionID = auditlogadmission.ThirdPartyAdmissionID 
        INNER JOIN auditemailsent ON trnadmission.AdmissionID = auditemailsent.AdmissionID
        WHERE auditemailsent.Type = 'Enhancement Query reply';

                """
    cursor2.execute(sql_query14)
    rows14 = cursor2.fetchall()
    print(rows14[0][0])

#enhancement rejection count
    sql_query15 = """
select count(*) from trnadmission adm
    inner join auditpreauthqueries res on res.AdmissionID=adm.AdmissionID
    inner join auditemailsent sen on sen.AdmissionID=adm.AdmissionID
    where ReasonType='Reject' and sen.Type='Enhancement' or sen.Type='Enhancement Rejection' or sen.Type='Enhancement Rejection Reply'

                """
    cursor2.execute(sql_query15)
    rows15 = cursor2.fetchall()
    print(rows15[0][0])

#enhancement rejection reply
    sql_query16 = """
select count(*) from trnadmission adm
    inner join auditpreauthqueries res on res.AdmissionID=adm.AdmissionID
    inner join auditemailsent sen on sen.AdmissionID=adm.AdmissionID
    where ReasonType='Reject' and sen.Type='Enhancement Rejection Reply'

                """
    cursor2.execute(sql_query16)
    rows16 = cursor2.fetchall()
    print(rows16[0][0])

#final approval
    sql_query17 = """
		SELECT COUNT(*)
        FROM auditlogadmission 
        INNER JOIN trnadmission ON trnadmission.ThirdPartyAdmissionID = auditlogadmission.ThirdPartyAdmissionID 
        INNER JOIN auditemailsent ON trnadmission.AdmissionID = auditemailsent.AdmissionID
        WHERE auditemailsent.Type='Final Approval';

                """
    cursor2.execute(sql_query17)
    rows17 = cursor2.fetchall()
    print(rows17[0][0])


    sql_query18 = """
		SELECT COUNT(*)
        FROM trnadmission 
        left outer JOIN auditemailsent ON trnadmission.AdmissionID = auditemailsent.AdmissionID
        WHERE auditemailsent.Type='Final Approval Query Reply';
                """
    cursor2.execute(sql_query18)
    rows18 = cursor2.fetchall()
    print(rows18[0][0])


    sql_query19 = """
		SELECT COUNT(*)
        FROM trnadmission 
        INNER JOIN auditemailsent ON trnadmission.AdmissionID = auditemailsent.AdmissionID
        WHERE auditemailsent.Type='Final Approval Rejection Reply';

                """
    cursor2.execute(sql_query19)
    rows19 = cursor2.fetchall()
    print(rows19[0][0])


    sql_query20 = """
		SELECT COUNT(*)
        FROM trnadmission  
        INNER JOIN auditemailsent ON trnadmission.AdmissionID = auditemailsent.AdmissionID
        WHERE auditemailsent.Type='Settlement Fightback';

                """
    cursor2.execute(sql_query20)
    rows20 = cursor2.fetchall()
    print(rows20[0][0])


    sql_query21 = """
		SELECT COUNT(*)
        FROM trnadmission 
        INNER JOIN auditemailsent ON trnadmission.AdmissionID = auditemailsent.AdmissionID
        WHERE auditemailsent.Type='Settlement Fightback Query Reply';
                """
    cursor2.execute(sql_query21)
    rows21 = cursor2.fetchall()
    print(rows21[0][0])

    sql_query22 = """
		SELECT COUNT(*)
        FROM trnadmission 
        INNER JOIN auditemailsent ON trnadmission.AdmissionID = auditemailsent.AdmissionID
        WHERE auditemailsent.Type='Settlement Fightback Rejection Reply';
                """
    cursor2.execute(sql_query22)
    rows22 = cursor2.fetchall()
    print(rows22[0][0])


    cursor2.close()
    conn2.close()   


count()