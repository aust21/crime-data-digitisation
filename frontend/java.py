from configuration import session

session_obj = session()

sql = """
SELECT
    MAX(Count)
FROM crime_data;    
"""

data = session_obj.execute(sql)
res = data.fetchall()
print(res)