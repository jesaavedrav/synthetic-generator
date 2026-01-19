from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config.settings import get_settings

settings = get_settings()

engine = create_engine(settings.sqlalchemy_database_uri, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
