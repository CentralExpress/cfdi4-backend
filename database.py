from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import config

SQLALCHEMY_DATABASE_URL = f"postgresql://{config.db_usuario}:{config.db_password}@{config.db_direccion}:{config.db_puerto}/{config.db_nombre}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
