from db.models import Base
from db import settings
from sqlalchemy import create_engine, inspect

def createEngine():
    from db import settings
    engine = create_engine('postgresql://{username}:{password}@{host}:{port}/QQP_2024'.format(**settings.DATABASE['qqp']))
    return engine

def checkTableExists(table_name,engine):
    inspector = inspect(engine)
    return inspector.has_table(table_name)

def createTables(table_name,engine):
    if not checkTableExists(table_name):
        print("Creando tabla Sales")
        Base.metadata.create_all(engine)
    
    else:
        print(f"Table {table_name} already exists.")


