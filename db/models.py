from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Sales(Base):
    __tablename__ = 'STG_Sales'

    ID = Column(Integer(),primary_key=True,autoincrement=True)
    producto = Column(String(65),nullable=True)
    presentacion = Column(String(180),nullable=True)
    marca = Column(String(65),nullable=True)
    categoria = Column(String(65),nullable=True)
    catalogo = Column(String(65),nullable=True)
    precio = Column(Float(18,2),nullable=True)
    fecharegistro = Column(DateTime(),nullable=True)
    cadenacomercial = Column(String(65),nullable=True)
    giro = Column(String(65),nullable=True)
    nombrecomercial = Column(String(120),nullable=True)
    direccion = Column(String(255),nullable=True)
    estado = Column(String(120),nullable=True)
    municipio = Column(String(120),nullable=True)
    latitud = Column(Float(18,6),nullable=True)
    longitud = Column(Float(18,6),nullable=True)
