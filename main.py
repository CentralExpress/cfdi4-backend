from fastapi import FastAPI, Response, status, HTTPException, Depends 
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import and_
import models, schemas
from database import get_db, engine
import funciones_clientes
import config
import funciones_cifs
import cif_reader
import pandas as pd

models.Base.metadata.create_all(bind=engine)
app = FastAPI()

origins = [
            "http://localhost.tiangolo.com",
                "https://localhost.tiangolo.com",
                    "http://localhost",
                    "https://localhost:8000",
                        "http://localhost:8080",
                        "http://192.168.8.201:8080",
                        "https://192.168.8.201:8080",
                        ]

app.add_middleware(
            CORSMiddleware,
                allow_origins=origins,
                    allow_credentials=True,
                        allow_methods=["*"],
                            allow_headers=["*"],
                            )


@app.post("/alta_cliente", status_code=status.HTTP_201_CREATED, response_model=schemas.RespuestaAltaCliente)
def alta_cliente(alta: schemas.AltaCliente, db: Session = Depends(get_db), raise_exceptions=True):

    cliente_adminpaq = funciones_clientes.get_cliente_adminpaq(alta.sucursal, alta.codigo)
    if not cliente_adminpaq:
        if raise_exceptions:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cliente No Encontrado en ADMINPAQ")
        else:
            return {'respuesta': 'Cliente No Encontrado en ADMINPAQ'}

    nuevo_cliente = models.Cliente(sucursal=alta.sucursal, codigo=alta.codigo, rfc=cliente_adminpaq['rfc'],
            razon_social=cliente_adminpaq['razon_social'], vendedor=alta.vendedor)

    cliente = db.query(models.Cliente).filter(and_(models.Cliente.sucursal == alta.sucursal, 
        models.Cliente.codigo == alta.codigo)).first()

    if cliente:
        if raise_exceptions:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Cliente Ya Existe")
        else:
            return {'respuesta': 'Cliente Ya Existe'}

    db.add(nuevo_cliente)
    db.commit()
    db.refresh(nuevo_cliente)
    
    return {'respuesta':'Cliente Creado Exitosamente'}

@app.post("/alta_cliente_masivo", status_code=status.HTTP_201_CREATED)
async def alta_cliente_masivo(alta_masiva: list,db: Session = Depends(get_db)):
    lista_respuesta = []
    dic_respuesta = dict()
    dic_alta = dict()
    for i in alta_masiva:
        print(i)
        try:
            if len(i) != 3: continue
            dic_alta['sucursal'] = i[0]
            dic_alta['codigo'] = i[1]
            dic_alta['vendedor'] = i[2]
            alta = schemas.AltaCliente(**dic_alta)
            dic_respuesta['alta'] = alta
            dic_respuesta['respuesta'] = alta_cliente(alta, db, raise_exceptions=False)
            lista_respuesta.append(dic_respuesta.copy())
        except Exception as e:
            dic_respuesta['alta'] = dic_alta.copy()
            dic_respuesta['respuesta'] = str(e)
            lista_respuesta.append(dic_respuesta.copy())
    return lista_respuesta

@app.get("/get_vendedores")
async def get_vendedores():
    respuesta = {'objeto': 'Lista de Vendedores','data' : config.LISTA_VENDEDORES}
    return respuesta
    

@app.get("/get_clientes_abiertos")
async def get_clientes_abiertos(db: Session = Depends(get_db)):
    respuesta = {'objeto': 'Lista de Clientes Abiertos'}
    data = db.query(models.Cliente).filter(and_(models.Cliente.verificado == False, 
        models.Cliente.oculto == False)).all()
    for i in range(len(data)):
        data[i].id = i
    respuesta['data'] = data
    return respuesta

@app.get("/get_cliente")
async def get_cliente(sucursal: str, codigo: str, db:Session = Depends(get_db)):
    cliente = db.query(models.Cliente).filter(and_(models.Cliente.sucursal == sucursal, 
        models.Cliente.codigo == codigo)).first()

    if not cliente:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Cliente no Existe")

    return {'id': {'sucursal':sucursal, 'codigo':codigo}, 'cliente': cliente}



@app.post("/verificar_cliente")
def verificar_cliente(ID: schemas.IdCliente, db: Session = Depends(get_db)):
    cliente_query = db.query(models.Cliente).filter(and_(models.Cliente.sucursal == ID.sucursal,
        models.Cliente.codigo == ID.codigo))

    cliente = cliente_query.first()

    if not cliente:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Cliente no Existe")

    cif_existe = funciones_cifs.cif_existe(cliente.rfc)
    #cliente.cif = cif_existe
    cliente_query.update({'cif':cif_existe}, synchronize_session=False)

    if not cif_existe:
        db.commit()
        return {'resultado': 'CIF No Encontrada', 'data': cliente}

    discrepancias = []
    cliente_adminpaq = funciones_clientes.get_cliente_adminpaq(cliente.sucursal, cliente.codigo)
    cif = cif_reader.read_cif(config.CARPETA_CIFS + cliente.rfc + '.pdf')

    for campo in config.CAMPOS_VALIDACION_SAT:
        if cliente_adminpaq[campo] != cif[campo]:
            discrepancias.append(campo)

    if len(discrepancias) == 0:
        #cliente.verificado = True
        #cliente.discrepancias = None
        cliente_query.update({'verificado':True, 'discrepancias':None}, synchronize_session=False)
        db.commit()
        return {'resultado':'Verificacion Completa', 'data': cliente}

    #cliente.discrepancias = ", ".join(discrepancias)
    lista_discrepancias= ", ".join(discrepancias)
    cliente_query.update({'discrepancias':lista_discrepancias}, synchronize_session=False)
    db.commit()
    return {'resultado': 'Discrepancias Encontradas', 'data': cliente}


@app.get("/verificar_todos")
async def verificar_todods(db: Session = Depends(get_db)):
    clientes = db.query(models.Cliente).filter(and_(models.Cliente.verificado == False, 
        models.Cliente.oculto == False)).all()

    respuesta_dic = dict()

    for cliente in clientes:
        cliente_id = schemas.IdCliente(sucursal= cliente.sucursal, codigo= cliente.codigo)
        respuesta_cliente = verificar_cliente(cliente_id, db)
        if respuesta_cliente['resultado'] in respuesta_dic.keys():
            respuesta_dic[respuesta_cliente['resultado']] += 1
        else:
            respuesta_dic[respuesta_cliente['resultado']] = 1

    return respuesta_dic
            


@app.get("/pdfs_nuevos")
async def get_pdfs_nuevos():
    return {'PDFs Nuevos:': funciones_cifs.get_archivos_recibidos()}

@app.post("/procesar_pdfs")
async def procesar_pdfs_nuevos():
    respuesta = {'objeto': 'Respuesta de Procesamiento de PDFs Nuevos'}
    dic = funciones_cifs.procesar_recibidos_nuevos()
    respuesta['data'] = dic
    return respuesta

@app.post("/ocultar_cliente")
async def ocultar_cliente(solicitud: schemas.OcultarCliente, db: Session = Depends(get_db)):
    cliente_query = db.query(models.Cliente).filter(and_(models.Cliente.sucursal == solicitud.sucursal,
        models.Cliente.codigo == solicitud.codigo))

    cliente = cliente_query.first()

    if not cliente:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Cliente no Existe")

    cliente_query.update({'oculto': True, 'comentario': solicitud.comentario}, synchronize_session=False)
    db.commit()

    return {'respuesta': 'Cliente Ocultado Exitosamente'}



@app.get("/get_cif")
async def get_cif(rfc: str):
    if not funciones_cifs.cif_existe(rfc):
        return {'respuesta': 'Cedula No Encontrada'}

    nombre_archivo = config.CARPETA_CIFS + rfc + '.pdf'
    return cif_reader.read_cif(nombre_archivo)


@app.get("/cif/{rfc}")
async def get_cif_pdf(rfc: str):
    if not funciones_cifs.cif_existe(rfc):
        respuesta = "CIF No Encontrada"
        return respuesta

    ruta_archivo = config.CARPETA_CIFS + rfc + '.pdf'
    return FileResponse(ruta_archivo)


@app.put("/cambiar_comentario")
async def cambiar_comentario(comentario: schemas.Comentario, db: Session = Depends(get_db)):
    cliente_query = db.query(models.Cliente).filter(and_(models.Cliente.sucursal == comentario.sucursal,
        models.Cliente.codigo == comentario.codigo))

    cliente = cliente_query.first()

    if not cliente:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Cliente no Existe")

    cliente_query.update({'comentario': comentario.contenido}, synchronize_session=False)
    db.commit()
    return {'respuesta': 'Comentario Cambiado'}

@app.get("/resumen", response_model=schemas.ListaResumen)
async def get_resumen():
    query = "SELECT * FROM clientes;"
    df = pd.read_sql(query, con=engine)
    respuesta = funciones_clientes.resumir_df(df)
    return respuesta

@app.get("/clientes/{vendedor}")
async def get_clientes_de_vendedor(vendedor: str, db: Session = Depends(get_db)):
    if not vendedor in config.LISTA_VENDEDORES:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail='Vendedor No Encontrado')
    
    clientes = db.query(models.Cliente).with_entities(models.Cliente.razon_social).filter(models.Cliente.vendedor == vendedor).all()
    return {'data': {'vendedor': vendedor, 'clientes': clientes}}
