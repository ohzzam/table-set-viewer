import pymysql
import psycopg2
import pandas as pd
try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None
try:
    import CUBRIDdb
except ImportError:
    CUBRIDdb = None

# 다양한 DB 지원 (MySQL/MariaDB, PostgreSQL)
def get_connection(dbtype, host, port, user, password, db):
    if dbtype == 'mysql':
        return pymysql.connect(host=host, port=port, user=user, password=password, db=db)
    elif dbtype == 'postgresql':
        return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db)
    elif dbtype == 'oracle':
        if cx_Oracle is None:
            raise ImportError('cx_Oracle 패키지가 설치되어 있지 않습니다.')
        dsn = cx_Oracle.makedsn(host, port, db)
        return cx_Oracle.connect(user=user, password=password, dsn=dsn)
    elif dbtype == 'cubrid':
        if CUBRIDdb is None:
            raise ImportError('CUBRID-Python 패키지가 설치되어 있지 않습니다.')
        conn_str = f"CUBRID:{host}:{port}:{db}:::"
        return CUBRIDdb.connect(conn_str, user, password)
    else:
        raise ValueError('지원하지 않는 DB 타입입니다.')

def get_tables(conn, dbtype):
    with conn.cursor() as cur:
        if dbtype == 'mysql':
            cur.execute("SHOW TABLE STATUS")
            return [(row[0], row[17]) for row in cur.fetchall()]
        elif dbtype == 'postgresql':
            cur.execute("SELECT tablename, obj_description(('public.' || tablename)::regclass) FROM pg_tables WHERE schemaname='public'")
            return [(row[0], row[1]) for row in cur.fetchall()]
        elif dbtype == 'oracle':
            cur.execute("SELECT table_name FROM user_tables")
            return [(row[0], '') for row in cur.fetchall()]
        elif dbtype == 'cubrid':
            cur.execute("SELECT class_name FROM db_class WHERE is_system_class = 'NO'")
            return [(row[0], '') for row in cur.fetchall()]

def get_table_schema(conn, dbtype, table):
    with conn.cursor() as cur:
        if dbtype == 'mysql':
            # ...기존코드...
            # ...생략...
            return {
                'columns': columns,
                'indexes': indexes,
                'primary_key': pk,
                'foreign_keys': fk,
                'table_comment': table_comment
            }
        elif dbtype == 'postgresql':
            # ...기존코드...
            # ...생략...
            return {
                'columns': columns,
                'indexes': indexes,
                'primary_key': pk,
                'foreign_keys': fk,
                'table_comment': table_comment
            }
        elif dbtype == 'oracle':
            # 컬럼 정보
            cur.execute(f"SELECT column_name, data_type, nullable, data_default FROM user_tab_columns WHERE table_name = '{table.upper()}'")
            columns = cur.fetchall()
            # 인덱스 정보
            cur.execute(f"SELECT index_name, column_name, uniqueness FROM user_ind_columns JOIN user_indexes USING(index_name) WHERE table_name = '{table.upper()}'")
            indexes = [(table, row[0], row[1], row[2]) for row in cur.fetchall()]
            # PK
            cur.execute(f"SELECT cols.column_name FROM user_constraints cons, user_cons_columns cols WHERE cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.table_name = '{table.upper()}'")
            pk = [row[0] for row in cur.fetchall()]
            # FK
            cur.execute(f"SELECT a.column_name, c_pk.table_name, b.column_name FROM user_cons_columns a JOIN user_constraints c ON a.constraint_name = c.constraint_name JOIN user_constraints c_pk ON c.r_constraint_name = c_pk.constraint_name JOIN user_cons_columns b ON b.constraint_name = c_pk.constraint_name WHERE c.constraint_type = 'R' AND a.table_name = '{table.upper()}'")
            fk = cur.fetchall()
            # 코멘트
            cur.execute(f"SELECT comments FROM user_tab_comments WHERE table_name = '{table.upper()}'")
            r = cur.fetchone()
            table_comment = r[0] if r else ''
            return {
                'columns': columns,
                'indexes': indexes,
                'primary_key': pk,
                'foreign_keys': fk,
                'table_comment': table_comment
            }
        elif dbtype == 'cubrid':
            cur.execute(f"SELECT attr_name, domain, '', IF(is_nullable='NO','NO','YES'), '', def_value, '', '', '' FROM db_attribute WHERE class_name='{table}'")
            columns = cur.fetchall()
            cur.execute(f"SELECT key_attr_name FROM db_index WHERE class_name='{table}' AND is_primary_key='YES'")
            pk = [row[0] for row in cur.fetchall()]
            fk = []
            cur.execute(f"SELECT index_name, key_attr_name, IF(is_unique='YES','Y','') FROM db_index WHERE class_name='{table}'")
            indexes = [(table, row[0], row[1], row[2]) for row in cur.fetchall()]
            comment = ''
            return {
                'columns': columns,
                'indexes': indexes,
                'primary_key': pk,
                'foreign_keys': fk,
                'table_comment': comment
            }

def get_table_ddl(conn, dbtype, table):
    with conn.cursor() as cur:
        if dbtype == 'mysql':
            cur.execute(f"SHOW CREATE TABLE `{table}`")
            return cur.fetchone()[1]
        elif dbtype == 'postgresql':
            cur.execute(f"SELECT 'CREATE TABLE ' || tablename || ' (' || string_agg(column_name || ' ' || data_type, ', ') || ');' FROM information_schema.columns WHERE table_name = '{table}' GROUP BY tablename;")
            return cur.fetchone()[0]
        elif dbtype == 'oracle':
            # 오라클은 DDL 추출이 복잡, 간단히 컬럼 정보로 생성
            cur.execute(f"SELECT column_name, data_type FROM user_tab_columns WHERE table_name = '{table.upper()}'")
            cols = cur.fetchall()
            col_defs = [f"{c[0]} {c[1]}" for c in cols]
            return f"CREATE TABLE {table} (\n  " + ",\n  ".join(col_defs) + "\n);"
        elif dbtype == 'cubrid':
            cur.execute(f"DESCRIBE {table}")
            desc = cur.fetchall()
            lines = [f"{row[0]} {row[1]} {'NOT NULL' if row[2]=='NO' else ''} DEFAULT {row[4]}" for row in desc]
            return f"CREATE TABLE {table} (\n  " + ",\n  ".join(lines) + "\n);"

def export_schema_to_excel(schema_data, filename):
    df = pd.DataFrame(schema_data)
    df.to_excel(filename, index=False)
