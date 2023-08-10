import requests
import pandas as pd
import datetime as datetime

from sqlalchemy import Column, Integer, VARCHAR, create_engine, Date, Boolean, Float, TIMESTAMP
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Currency(Base):
    __tablename__ = 'currency'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    currency = Column(VARCHAR(50), nullable=False)
    value = Column(Float, nullable=False)
    date_save = Column (TIMESTAMP, nullable=False)



class Currency_etl(Base):
    __tablename__ = 'currency_etl'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    currency = Column(VARCHAR(50), nullable=False)
    value = Column(Float, nullable=False)
    date_save = Column (TIMESTAMP, nullable=False)




#Подключение к БД
SQLALCHEMY_DATABASE_URI = f"postgresql://swintus:parol@194.87.102.3:5432/test_database"
engine = create_engine(SQLALCHEMY_DATABASE_URI)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)



session_local = SessionLocal()
data = session_local.query(Currency_etl).all()

for one_currency in data:
    new_record = Currency (
                    currency = one_currency.currency,
                    value = one_currency.value,
                    date_save = one_currency.date_save
                    )
    session_local.add(new_record)

session_local.commit()

