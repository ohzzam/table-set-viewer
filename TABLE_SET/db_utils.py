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
            cur.execute(f"SHOW FULL COLUMNS FROM `{table}`")
            columns = cur.fetchall()
            cur.execute(f"SHOW INDEX FROM `{table}`")
            idx_rows = cur.fetchall()
            # 인덱스 정보: (Table, Non_unique, Key_name, Seq_in_index, Column_name, ...)
            # PRIMARY, MUL(자동 생성) 제외, 같은 인덱스 이름은 컬럼명 합치기
            index_dict = {}
            for row in idx_rows:
                key_name = row[2]
                non_unique = row[1]
                # PRIMARY, MUL(자동 생성) 제외: MUL은 일반적으로 Key_name이 'PRIMARY'가 아니면서 Non_unique=1, Unique=0
                # MySQL에서 MUL은 Key_name이 'PRIMARY'가 아니고, Non_unique=1, Unique=0, 그리고 자동 생성된 인덱스임
                # 사용자 생성 인덱스만 포함: Key_name != 'PRIMARY' and (Unique 인덱스 or 일반 인덱스)
                if key_name == 'PRIMARY':
                    continue
                # 자동 생성된 MUL 인덱스(외래키 등)는 MySQL에서 Non_unique=1, Key_name이 컬럼명과 동일한 경우가 많음
                # 일반적으로 사용자가 직접 생성한 인덱스는 Key_name이 컬럼명과 다름
                # 여기서는 Key_name이 컬럼명과 동일하면 제외 (자동 생성 인덱스)
                if key_name == row[4]:
                    continue
                if key_name not in index_dict:
                    index_dict[key_name] = {'columns': [], 'unique': 'Y' if non_unique == 0 else ''}
                index_dict[key_name]['columns'].append(row[4])
            indexes = [(iname, ','.join(data['columns']), data['unique']) for iname, data in index_dict.items()]
            # PK
            pk = [row[0] for row in columns if row[4] == 'PRI']
            # FK (information_schema에서 추출)
            cur.execute(f"""
                SELECT column_name, referenced_table_name, referenced_column_name
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE table_schema=DATABASE() AND table_name=%s AND referenced_table_name IS NOT NULL
            """, (table,))
            fk = cur.fetchall()
            # 코멘트
            cur.execute(f"SELECT table_comment FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='{table}'")
            r = cur.fetchone()
            table_comment = r[0] if r else ''
            return {
                'columns': columns,
                'indexes': indexes,
                'primary_key': pk,
                'foreign_keys': fk,
                'table_comment': table_comment
            }
        elif dbtype == 'postgresql':
            cur.execute(f"SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = '{table}'")
            columns = cur.fetchall()
            # 인덱스 정보
            cur.execute(f"SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{table}'")
            indexes = [(row[0], row[1], '') for row in cur.fetchall()]
            # PK
            cur.execute(f"SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '{table}'::regclass AND i.indisprimary")
            pk = [row[0] for row in cur.fetchall()]
            # FK
            cur.execute(f"SELECT kcu.column_name, ccu.table_name, ccu.column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_name = kcu.table_name JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='{table}'")
            fk = cur.fetchall()
            # 코멘트
            cur.execute(f"SELECT obj_description('{table}'::regclass)")
            r = cur.fetchone()
            table_comment = r[0] if r else ''
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
