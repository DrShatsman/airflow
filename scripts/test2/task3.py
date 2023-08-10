import requests
import pandas as pd
import datetime as datetime

from sqlalchemy import Column, Integer, VARCHAR, create_engine, Date, Boolean, Float, TIMESTAMP
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()



