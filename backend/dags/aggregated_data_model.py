import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Define the SQLAlchemy model
class CrimeByCategory(Base):
    __tablename__ = 'crime_by_category'

    id = Column(Integer, primary_key=True)
    category = Column(String(300))
    total_count = Column(Integer)