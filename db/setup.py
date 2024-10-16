from db.models import Sales,Base
from db import settings
from sqlalchemy import create_engine, inspect

def create_db_engine():
    from db import settings
    engine = create_engine('postgresql://{username}:{password}@{host}:{port}/qqp_2024'.format(**settings.DATABASE['qqp']))
    return engine


def create_tables(table_name,engine):
    inspector = inspect(engine)
    table_created = inspector.has_table(table_name)

    if not table_created:
        print("Creando tabla Sales")
        Base.metadata.create_all(engine)
    
    else:
        print(f"Table {table_name} already exists.")


