import requests
import pandas as pd
import datetime as datetime

from sqlalchemy import Column, Integer, VARCHAR, create_engine, Date, Boolean, Float, TIMESTAMP
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()



class Temperature_etl(Base):
    __tablename__ = 'temperature_etl'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    city = Column(VARCHAR(50), nullable=False)
    temperature = Column(Float, nullable=False)
    date_save = Column (TIMESTAMP, nullable=False)




def what_temp(city):
    city = str(city)
    api_key = "073069247d474de0a3c111032232106"
    base_url = "http://api.weatherapi.com/v1"
    result = None

    try:
        parameters = {"key": api_key, "q": city}  # URL parameters
        result = requests.get(f"{base_url}/current.json", params=parameters)
        data = result.json()
        result = {str(data.get("location").get('name')) : str(data.get("current").get("temp_c"))}

    except Exception as e:
        print(e)

    finally:
        return result




def many_cities(cities):
    l_cities = cities.split(", ")
    city_temp_dict = {}

    try:
        for one_city in l_cities:
            data = what_temp(one_city)
            city_temp_dict.update(data)

    except Exception as e:
        print(e)
    return city_temp_dict


# def to_pandas(cities):
#     try:
#        # pd.DataFrame.from_dict(many_cities(cities), orient='index').rename(columns={0:'Temperature'}).to_csv(f'D:\Program Files\PyCharm\Files Pandas\{(str(datetime.datetime.now().strftime("%Y-%m-%d")))}.csv')
#         pd.DataFrame.from_dict(many_cities(cities), orient='index').rename(columns={0: 'Temperature'}).to_csv(f'/test/tempetature {(str(datetime.datetime.now().strftime("%Y-%m-%d")))}.csv')
#         return(many_cities(cities))
#
#     except Exception as e:
#         print(e)


result = many_cities("London, Saint Petersburg, Tel Aviv")

#Подключение к БД
SQLALCHEMY_DATABASE_URI = f"postgresql://swintus:parol@194.87.102.3:5432/test_database"
engine = create_engine(SQLALCHEMY_DATABASE_URI)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)



new_record =  Temperature_etl(
            city =  str(list(result)[0]),
            temperature = (str(result [list(result)[0]])),
            date_save = str(datetime.datetime.now().strftime("%Y-%m-%d  %H:%M"))
        )

new_record2 =  Temperature_etl(
            city =  str(list(result)[1]),
            temperature = (str(result [list(result)[1]])),
            date_save = str(datetime.datetime.now().strftime("%Y-%m-%d  %H:%M"))
        )

new_record3 =  Temperature_etl(
            city =  str(list(result)[2]),
            temperature = (str(result [list(result)[2]])),
            date_save = str(datetime.datetime.now().strftime("%Y-%m-%d  %H:%M"))
        )




session_local = SessionLocal()
session_local.add(new_record)
session_local.add(new_record2)
session_local.add(new_record3)
session_local.commit()

