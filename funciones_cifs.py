import os, pyodbc, re, pdfx, shutil
import config

def get_archivos_recibidos():
    return len(os.listdir(config.CARPETA_RECIBIDOS))

def get_cifs():
    return len(os.listdir(config.CARPETA_CIFS))

def cif_existe(rfc):
    ruta_archivo = config.CARPETA_CIFS + rfc + '.pdf'
    return os.path.isfile(ruta_archivo)

def get_cliente_adminpaq(suc, codigo):
    server = 'MAINSERVER\\COMPAC' 
    database = config.sucursales[suc]
    username = config.ADMIN_DB_USUARIO
    password = config.ADMIN_DB_PASSWORD
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password+';TrustServerCertificate=Yes')
    cursor = cnxn.cursor()

    cli = 'admClientes.'
    dom = 'admDomicilios.'
    query = f"""
    SELECT {cli}CCODIGOCLIENTE, {cli}CRAZONSOCIAL, {cli}CRFC, {dom}CNOMBRECALLE, {dom}CNUMEROEXTERIOR, {dom}CNUMEROINTERIOR, 
    {dom}CCOLONIA, {dom}CCODIGOPOSTAL, {dom}CPAIS, {dom}CESTADO, {dom}CCIUDAD, {dom}CMUNICIPIO
    FROM ({cli[:-1]} INNER JOIN {dom[:-1]} ON {cli}CIDCLIENTEPROVEEDOR = {dom}CIDCATALOGO )
    WHERE {cli}CCODIGOCLIENTE = '{codigo}' AND {dom}CTIPOCATALOGO = '1' AND {dom}CTIPODIRECCION = '0';"""
    campos = ['codigo', 'razon_social', 'rfc', 'calle', 'numero_exterior', 'colonia', 'codigo_postal', 'pais', 'estado', 'ciudad', 'municipio']
    cursor.execute(query)
    row = cursor.fetchone()
    return dict(zip(campos, row))

def procesar_recibido(archivo):
    pdf = pdfx.PDFx(archivo)
    texto_pdf = pdf.get_text()

    for texto in config.LISTA_TEXTO_VERIFICACION:
        if not re.search(texto, texto_pdf, re.MULTILINE):
            archivar_recibido(archivo)
            return 'Verificacion CIF Fallada'

    re_rfc = r'([A-Z]{3,4}\d{6}[A-Z\d]{3})\W'

    rfc_match = re.search(re_rfc, texto_pdf, re.MULTILINE)

    if not rfc_match:
        archivar_recibido(archivo)
        return 'RFC no Encontrado'

    rfc_valor = re.search(re_rfc, texto_pdf, re.MULTILINE).group(1)

    origen = archivo
    destino = config.CARPETA_CIFS + rfc_valor + '.pdf'

    if os.path.isfile(destino):
        import cif_reader as cifr
        fecha_destino = cifr.fecha_emision_cif(destino)
        fecha_archivo = cifr.fecha_emision_cif(archivo)
        if fecha_archivo > fecha_destino:
            shutil.copy(archivo, destino)
            return 'Actualizado'
        else:
            archivar_recibido(archivo)
            return 'CIF Existente'
    else:
        shutil.copy(archivo, destino)
        archivar_recibido(archivo)
        return 'CIF Agregada'

    return 'Error Fin de Funcion'

def archivar_recibido(archivo_recibido):
    archivo = archivo_recibido.split('/')[-1]
    if not os.path.isfile(config.CARPETA_ARCHIVADOS + archivo):
        nuevo_nombre = config.CARPETA_ARCHIVADOS + archivo
    else:
        cnt = 1
        nuevo_nombre = config.CARPETA_ARCHIVADOS + archivo.replace('.pdf', f'_{str(cnt)}.pdf')
        while os.path.isfile(nuevo_nombre):
            cnt += 1
            nuevo_nombre = config.CARPETA_ARCHIVADOS + archivo.replace('.pdf', f'_{str(cnt)}.pdf')

    shutil.move(archivo_recibido, nuevo_nombre)
    return

def procesar_recibidos_nuevos(explicar=False):
    lista_recibidos = os.listdir(config.CARPETA_RECIBIDOS)
    respuesta_dic = dict() 

    for archivo in lista_recibidos:
        respuesta = procesar_recibido(config.CARPETA_RECIBIDOS + archivo)
        if not respuesta in respuesta_dic.keys():
            respuesta_dic[respuesta] = 1
        else:
            respuesta_dic[respuesta] += 1

        if explicar:
            print(f'Archivo: {archivo} || Respuesta: {respuesta}')

    return respuesta_dic


if __name__ == '__main__':
    print(f'Archivos en carpeta recibidos: {get_archivos_recibidos()}')
    print(f'Archivos en carpeta cifs: {get_cifs()}')
