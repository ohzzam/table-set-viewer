import pymysql

con = pymysql.connect(host='127.0.0.1', user='root', password='dhwoan', database='mysql', charset='utf8') 
cur = con.cursor()
#cur.execute("SELECT `DEFAULT_COLLATION_NAME` FROM `information_schema`.`SCHEMATA` WHERE `SCHEMA_NAME`='mysql'")
cur.execute("SHOW /*!50002 GLOBAL */ STATUS")
myresult = cur.fetchall()

for row in myresult:
    print(row)

con.close()