import pymysql

con = pymysql.connect(host='127.0.0.1', user='root', password='dhwoan', database='mysql', charset='utf8') 
cur = con.cursor()

# Get list of all databases
print("=" * 50)
print("DATABASES:")
print("=" * 50)
cur.execute("SHOW DATABASES")
databases = cur.fetchall()
for db in databases:
    print(db[0])

# Get schema for each database
print("\n" + "=" * 50)
print("SCHEMA INFORMATION:")
print("=" * 50)

for db_tuple in databases:
    db_name = db_tuple[0]
    if db_name not in ['information_schema', 'performance_schema', 'mysql', 'sys']:
        try:
            cur.execute(f"USE {db_name}")
            print(f"\n--- Database: {db_name} ---")
            
            # Get tables
            cur.execute("SHOW TABLES")
            tables = cur.fetchall()
            for table in tables:
                table_name = table[0]
                print(f"\nTable: {table_name}")
                
                # Get columns
                cur.execute(f"DESCRIBE {table_name}")
                columns = cur.fetchall()
                for col in columns:
                    print(f"  {col[0]:20} {col[1]:20} {col[2]:5} {col[3]:10}")
        except Exception as e:
            print(f"Error accessing {db_name}: {e}")

con.close()
