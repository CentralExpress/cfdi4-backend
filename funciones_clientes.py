import pyodbc, pandas as pd
import config

def get_cliente_adminpaq(suc, codigo):
    server = 'MAINSERVER\COMPAC' 
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
    WHERE {cli}CCODIGOCLIENTE = '%s' AND {dom}CTIPOCATALOGO = '1' AND {dom}CTIPODIRECCION = '0';"""
    query = query.replace('%s', codigo)
    campos = config.CAMPOS_ADMIN_DB
    cursor.execute(query)
    row = cursor.fetchone()
    if not row:
        return None
    return dict(zip(campos, row))

def resumir_df(df, wrap=True):
    respuesta = dict()
    respuesta['data'] = []
    aux_dict = dict()
    aux_dict['clientes'] = len(df.index)
    aux_dict['cif'] = (df['cif'] == True).sum()
    aux_dict['discrepancias'] = len(df['discrepancias'].dropna())

    aux_dict['porcentaje_cif'] = aux_dict['cif']/aux_dict['clientes']
    aux_dict['porcentaje_discrepancias'] = aux_dict['discrepancias']/aux_dict['clientes']

    if not wrap:
        return {'alcance': 'global', 'data': aux_dict.copy()}
    respuesta['data'].append({'alcance': 'global', 'data': aux_dict.copy()})
    
    for suc in df['sucursal'].unique():
        df_aux = df.loc[df['sucursal'] == suc]
        dict_aux = resumir_df(df_aux, wrap=False)
        dict_aux['alcance'] = suc
        respuesta['data'].append(dict_aux.copy())

    for vendedor in df['vendedor'].unique():
        df_aux = df.loc[df['vendedor'] == vendedor]
        dict_aux = resumir_df(df_aux, wrap=False)
        dict_aux['alcance'] = vendedor
        respuesta['data'].append(dict_aux.copy())


    return respuesta
