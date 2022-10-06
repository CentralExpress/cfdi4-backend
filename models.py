from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.sql.expression import null
from database import Base

class Cliente(Base):
    __tablename__ = 'clientes'

    id = Column(Integer, nullable=True)
    sucursal = Column(String, nullable=False, primary_key=True)
    codigo = Column(String, nullable=False, primary_key=True)
    rfc = Column(String, nullable=False)
    razon_social = Column(String, nullable=False)
    vendedor = Column(String, nullable=True)
    cif = Column(Boolean, nullable=False, default=False)
    discrepancias = Column(String, nullable=True)
    verificado = Column(Boolean, nullable=False, default=False)
    oculto = Column(Boolean, nullable=False, default=False)
    comentario = Column(String, nullable=True)

