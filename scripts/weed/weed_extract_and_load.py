# Импорт библиотек
import datetime as datetime
from sqlalchemy import Column, Integer, VARCHAR, create_engine, Date, Boolean, Float, TIMESTAMP
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
import requests

# Таблица
Base = declarative_base()

class Weed_elt (Base):
    __tablename__ = "weed_elt"
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    uid = Column(VARCHAR(50), nullable=True)
    strain = Column(VARCHAR(50), nullable=True)
    cannabinoid_abbreviation = Column(VARCHAR(50), nullable=True)
    cannabinoid = Column(VARCHAR(50), nullable=True)
    terpene = Column(VARCHAR(50), nullable=True)
    medical_use = Column(VARCHAR(50), nullable=True)
    health_benefit = Column(VARCHAR(50), nullable=True)
    category = Column(VARCHAR(50), nullable=True)
    weed_type = Column(VARCHAR(50), nullable=True)
    buzzword = Column(VARCHAR(50), nullable=True)
    brand = Column(VARCHAR(50), nullable=True)
    date_save = Column (TIMESTAMP, nullable=False)



# Получение данных
def weed_info():
    base_url = 'https://random-data-api.com/api/cannabis/random_cannabis?size=10'
    try:
        result = requests.get(f"{base_url}/current.json")
        data = result.json()

    except Exception as e:
        print(e)

    finally:
        return data



#Подключение к БД
#Понятно, что в коде нельзя указывать credentials в таком виде, это сделано для тестового задания, чтобы вы при желании тоже могли подключиться к БД и проверить.
#На рабочем сервере это будет браться из параметров (из файла конфига или из свойств подключения Airflow)
SQLALCHEMY_DATABASE_URI = f"postgresql://swintus:parol@194.87.102.3:5432/test_database"
engine = create_engine(SQLALCHEMY_DATABASE_URI)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# Загрузка данных
try:
    for one_weed in weed_info():

        new_record = Weed_elt (
                    id = one_weed ['id'],
                    uid = one_weed ['uid'],
                    strain = one_weed ['strain'],
                    cannabinoid_abbreviation = one_weed ['cannabinoid_abbreviation'],
                    cannabinoid = one_weed ['cannabinoid'],
                    terpene = one_weed ['terpene'],
                    medical_use = one_weed ['medical_use'],
                    health_benefit = one_weed ['health_benefit'],
                    category = one_weed ['category'],
                    weed_type = one_weed ['type'],
                    buzzword = one_weed ['buzzword'],
                    brand = one_weed ['brand'],
                    date_save = str(datetime.datetime.now().strftime("%Y-%m-%d  %H:%M"))
                )



        session_local = SessionLocal()
        session_local.add(new_record)
        session_local.commit()

except Exception as e:
    print(e)


