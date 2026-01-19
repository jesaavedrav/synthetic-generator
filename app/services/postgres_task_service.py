from sqlalchemy import create_engine, Column, String, Integer, Text, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import JSONB
from config import get_settings
import json
import datetime

Base = declarative_base()

class TrainingTask(Base):
    __tablename__ = "training_tasks"
    task_id = Column(String, primary_key=True)
    type = Column(String, nullable=False)
    status = Column(String, nullable=False)
    request_data = Column(JSONB, nullable=True)
    progress = Column(Integer, default=0)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

class PostgresTaskService:
    def __init__(self):
        settings = get_settings()
        self.engine = create_engine(
            f"postgresql+psycopg2://{settings.postgres_user}:{settings.postgres_password}"
            f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}",
            pool_pre_ping=True
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def create_task(self, task_id, type_, status, request_data, progress=0, error=None):
        session = self.Session()
        try:
            task = TrainingTask(
                task_id=task_id,
                type=type_,
                status=status,
                request_data=request_data,
                progress=progress,
                error=error,
            )
            session.add(task)
            session.commit()
        finally:
            session.close()

    def update_task(self, task_id, status=None, progress=None, error=None):
        session = self.Session()
        try:
            task = session.query(TrainingTask).filter_by(task_id=task_id).first()
            if not task:
                return
            if status is not None:
                task.status = status
            if progress is not None:
                task.progress = progress
            if error is not None:
                task.error = error
            task.updated_at = datetime.datetime.utcnow()
            session.commit()
        finally:
            session.close()

    def get_task(self, task_id):
        session = self.Session()
        try:
            task = session.query(TrainingTask).filter_by(task_id=task_id).first()
            return task
        finally:
            session.close()

    def list_tasks(self, skip=0, limit=100):
        session = self.Session()
        try:
            tasks = session.query(TrainingTask).order_by(TrainingTask.created_at.desc()).offset(skip).limit(limit).all()
            return tasks
        finally:
            session.close()

    def count_tasks(self):
        session = self.Session()
        try:
            return session.query(TrainingTask).count()
        finally:
            session.close()

    def truncate_table(self, table_name: str):
        session = self.Session()
        try:
            if table_name not in Base.metadata.tables:
                raise ValueError(f"Table '{table_name}' not found in metadata.")
            
            table = Base.metadata.tables[table_name]
            session.execute(table.delete())
            session.commit()
        finally:
            session.close()

    def get_table_names(self):
        return list(Base.metadata.tables.keys())
