import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from frontend.configuration import session

def fetch_aggrigated_by_cat():
    session_obj = session()
    query = "SELECT * FROM crime_by_category;"
    result = session_obj.execute(query)
    return result.fetchall()
