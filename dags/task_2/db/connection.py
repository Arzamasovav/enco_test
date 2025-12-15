from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from ..config import settings

alchemy_engine = create_engine(
    f"postgresql+psycopg2://{settings.POSTGRES_DB_USER}"
    f":{settings.POSTGRES_DB_PASSWORD}@{settings.POSTGRES_DB_HOST}"
    f":{settings.POSTGRES_DB_PORT}/{settings.POSTGRES_DB_NAME}",
)

make_session = sessionmaker(alchemy_engine)


@contextmanager
def db_session() -> Generator[Session, None]:
    session = make_session()
    try:
        yield session
    except:
        session.rollback()
        raise
    finally:
        session.close()
