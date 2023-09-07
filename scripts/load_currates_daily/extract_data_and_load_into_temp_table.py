import requests
import pandas as pd
import datetime as datetime

from sqlalchemy import Column, Integer, VARCHAR, create_engine, Date, Boolean, Float, TIMESTAMP
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
import currencyapicom



Base = declarative_base()



class Currency_etl(Base):
    __tablename__ = 'currency_etl'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    currency = Column(VARCHAR(50), nullable=False)
    value = Column(Float, nullable=False)
    date_save = Column (TIMESTAMP, nullable=False)


def money_curs_USD():
    try:
        client = currencyapicom.Client('FifrTL3HIbbHYOwZfeVUa9UMt1G56DBbShsDFXgk')
        result = client.latest('USD', currencies=['RUB'])
        curs_USD = {"USD": result.get('data').get('RUB').get('value')}

    except Exception as e:
        print(e)
    finally:
        return curs_USD

def money_curs_EUR():
    try:
        client = currencyapicom.Client('FifrTL3HIbbHYOwZfeVUa9UMt1G56DBbShsDFXgk')
        result = client.latest('EUR', currencies=['RUB'])
        curs_EUR = {"EUR": result.get('data').get('RUB').get('value')}

    except Exception as e:
        print(e)
    finally:
        return curs_EUR
#
# # #Подключение к БД
SQLALCHEMY_DATABASE_URI = f"postgresql://swintus:parol@194.87.102.3:5432/test_database"
engine = create_engine(SQLALCHEMY_DATABASE_URI)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


new_record =  Currency_etl(
            currency =  "USD",
            value = money_curs_USD() ['USD'],
            date_save = str(datetime.datetime.now().strftime("%Y-%m-%d  %H:%M"))
        )

new_record2 =  Currency_etl(
            currency="EUR",
            value=money_curs_EUR()['EUR'],
            date_save = str(datetime.datetime.now().strftime("%Y-%m-%d  %H:%M"))
        )




session_local = SessionLocal()
session_local.add(new_record)
session_local.add(new_record2)
session_local.commit()

