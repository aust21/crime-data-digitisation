import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from frontend.configuration import session

def fetch_aggrigated_by_cat():
    session_obj = session()
    query = """
    SELECT crime_category,
        COUNT(*) AS total_count
    FROM crime_data
    GROUP BY crime_category
    ORDER BY total_count DESC;
    """
    result = session_obj.execute(query)
    return result.fetchall()

def fetch_highest_count():
    session_obj = session()
    query = """
        SELECT 
            cd.geography,
            cd.crime_category,
            MAX(crime_count) AS crime_count
        FROM crime_data cd
        GROUP BY cd.crime_category, cd.geography
        ORDER BY crime_count DESC;
        """
    result = session_obj.execute(query)
    return result.fetchall()

def fetch_min_count():
    session_obj = session()
    query = """
            SELECT 
                cd.geography,
                cd.crime_category,
                MIN(crime_count) AS crime_count
            FROM crime_data cd
            GROUP BY cd.crime_category, cd.geography
            ORDER BY crime_count ASC;
            """
    result = session_obj.execute(query)
    return result.fetchall()

def fetch_province_count():
    session_obj = session()
    query = """
            SELECT 
                cd.geography,
                COUNT(*) AS total_occurrence
            FROM crime_data cd
            GROUP BY cd.geography
            ORDER BY cd.geography ASC;
            """
    result = session_obj.execute(query)
    return result.fetchall()