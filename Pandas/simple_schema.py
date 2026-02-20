import pymysql

try:
    con = pymysql.connect(host='127.0.0.1', user='root', password='dhwoan', database='mysql', charset='utf8') 
    cur = con.cursor()
    
    # Get list of all databases
    print("DATABASES:")
    cur.execute("SHOW DATABASES")
    databases = cur.fetchall()
    for db in databases:
        print(f"  - {db[0]}")
    
    # Try to get tables from common databases
    for db_name in ['mysql', 'test']:
        try:
            cur.execute(f"USE {db_name}")
            print(f"\n\nTables in '{db_name}':")
            cur.execute("SHOW TABLES")
            tables = cur.fetchall()
            for table in tables:
                print(f"  - {table[0]}")
        except:
            pass
    
    con.close()
    print("\nDatabase connection successful!")
except Exception as e:
    print(f"Error: {e}")
