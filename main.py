from db_connect import Db

file = 'db.sqlite3'

sql = """
SELECT 
    name
FROM 
    sqlite_schema
WHERE 
    type ='table' AND 
    name NOT LIKE 'sqlite_%';
"""


with Db(file) as conn:
    select = conn.fetchmany_as_dict(sql)
    print(type(select))
    for i in select:
        print(i)
