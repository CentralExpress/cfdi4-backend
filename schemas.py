from pydantic import BaseModel, ValidationError, validator
from datetime import datetime
from typing import Optional
import config

lista_sucursales = ['MXL', 'TIJ', 'ENS', 'HLL', 'OBR', 'LPZ']

class IdCliente(BaseModel):
    
    sucursal: str
    codigo: str

    @validator('sucursal')
    def es_sucursal_valida(cls, v):
        if v not in lista_sucursales:
            raise ValueError('Sucursal Invalida')
        return v

class AltaCliente(IdCliente):

    vendedor: str

    @validator('vendedor')
    def es_vendedor_valido(cls, v):
        if v not in config.LISTA_VENDEDORES:
            raise ValueError('Vendedor Invalido')
        return v

    class Config:
        orm_mode = True

class ListaAltaCliente(BaseModel):
    data: list[AltaCliente]

    class Config:
        orm_mode = True

class RespuestaAltaCliente(BaseModel):

    respuesta: str

    class Config:
        orm_mode = True


class ListaRespuestasAltaCliente(BaseModel):
    data: list[RespuestaAltaCliente]

    class Config:
        orm_mode = True

class OcultarCliente(IdCliente):
    comentario: str

    class Config:
        orm_model = True


class Comentario(IdCliente):
    contenido: str

    class Config:
        orm_model = True


class ResumenData(BaseModel):
    clientes: int
    cif: int
    discrepancias: int
    porcentaje_cif: float
    porcentaje_discrepancias: float

class Resumen(BaseModel):
    alcance: str
    data: ResumenData

class ListaResumen(BaseModel):
    data: list[Resumen]


