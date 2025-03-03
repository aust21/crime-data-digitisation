from configuration import session

session_obj = session()

sql = """
SELECT 
    crime_category,
    COUNT(*) AS total_count
FROM crime_data
GROUP BY crime_category
ORDER BY total_count DESC;
"""

data = session_obj.execute(sql)
res = data.fetchall()
print(res)