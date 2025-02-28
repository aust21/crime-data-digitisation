
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class CrimeByCategory(Base):
    __tablename__ = 'crime_by_category'

    id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String(300))
    geography = Column(Integer)
    financial_year = Column(Integer)
    entry_count = Column(Integer)