import pandas as pd, datetime, pyodbc, logging, csv, re

sucursal = {
	'MXL' : {
		'nombre' : 'adMEXICALI',
		'id_almacen' : '23',
		'clientes_esp' : ['000171']
	},
	'TIJ' : {
		'nombre' : 'adTIJUANA',
		'id_almacen' : '5',
		'clientes_esp' : ['000167']
	},
	'ENS' : {
		'nombre' : 'adENSENADA',
		'id_almacen' : '12',
		'clientes_esp' : []
	},
	'HLL' : {
		'nombre' : 'adHERMOSILLO',
		'id_almacen' : '7',
		'clientes_esp' : ['000735']
	},
	'OBR' : {
		'nombre' : 'adOBGREGON',
		'id_almacen' : '17',
		'clientes_esp' : ['000466']
	},
	'LPZ' : {
		'nombre' : 'adLA_PAZ',
		'id_almacen' : '19',
		'clientes_esp' : ['000171']
	}
}
tablas = {
#Campos Productos
	'productos' : {
	'nombre' : 'admProductos',
	'ID_Producto' : 'CIDPRODUCTO',
	'Codigo_Producto' : 'CCODIGOPRODUCTO',
	'Nombre_Producto' : 'CNOMBREPRODUCTO',
	'P_Mostrador' : 'CPRECIO1',
	'P_Especial' : 'CPRECIO2',
	'P_Especial_Plus': 'CPRECIO3',
	'P_Exclusivo' : 'CPRECIO4',
	'P_Exclusivo_Plus' : 'CPRECIO5',
	'P_Publico' : 'CPRECIO6',
	'P_Select' : 'CPRECIO9',
	'P_Select_Plus' : 'CPRECIO10'
	},
#Campos Agentes
	'agentes' : {
	'nombre' : 'admAgentes',
	'ID_Agente' : 'CIDAGENTE',
	'Codigo_Agente' : 'CCODIGOAGENTE',
	'Nombre_Agente' : 'CNOMBREAGENTE'
	},
#Campos Clientes
	'clientes' : {
	'nombre' : 'admClientes',
	'ID_Cliente' : 'CIDCLIENTEPROVEEDOR',
	'Codigo_Cliente' : 'CCODIGOCLIENTE',
	'Razon_Social' : 'CRAZONSOCIAL',
        'RFC': 'CRFC',
	'Lista_Precio' : 'CLISTAPRECIOCLIENTE',
	'Estado' : 'CESTATUS',
	'Agente_ID' : 'CIDVALORCLASIFCLIENTE2',
	'Dia_ID' : 'CIDVALORCLASIFCLIENTE3'
	},
#Campos Clasificacion
	'clas' : {
	'nombre' : 'admClasificacionesValores',
	'ID_Clas' : 'CIDVALORCLASIFICACION',
	'Clas_Valor' : 'CVALORCLASIFICACION'
	},
#Campos Documentos
	'documentos' : {
	'nombre' : 'admDocumentos',
	'ID_Documento' : 'CIDDOCUMENTO',
	'Tipo_ID' : 'CIDDOCUMENTODE',
	'Concepto' : 'CIDCONCEPTODOCUMENTO',
	'Serie' : 'CSERIEDOCUMENTO',
	'Folio' : 'CFOLIO',
	'Fecha' : 'CFECHA',
	'Cliente_ID' : 'CIDCLIENTEPROVEEDOR',
	'Razon_Social' : 'CRAZONSOCIAL',
	'Agente' : 'CIDAGENTE',
	'Cancelado' : 'CCANCELADO',
	'Neto' : 'CNETO',
	'Impuesto' : 'CIMPUESTO1',
	'Descuento' : 'CDESCUENTOMOV'
	},
#Campos Inventario
	'inventario' : {
	'nombre' : 'vwInventarioActual',
	'ID_Prod' : 'idProd',
	'Codigo' : 'Codigo',
	'Lote' : 'Lote',
	'Caducidad' : 'Caducidad',
	'Cod_Almacen' : 'CodAlmacen',
	'Existencia' : 'Existencia',
	'Estado' : 'Estatus'
	},
#Campos Movimientos
	'movimientos' : {
	'nombre' : 'admMovimientos',
	'Producto_ID' : 'CIDPRODUCTO',
	'Cantidad' : 'CUNIDADES',
	'Precio' : 'CPRECIO',
	'Neto' : 'CNETO',
	'Descuento' : 'CDESCUENTO1',
	'Costo' : 'CCOSTOESPECIFICO',
	'Total' : 'CTOTAL',
	'Fecha' : 'CFECHA'
	}
}

#funcion que regresa relacion entre codigo y nombre de laboratorios
def get_labs(labsFile='labs.csv'):
	with open(labsFile, 'r', encoding='UTF-8') as file:
		data = csv.reader(file, delimiter = ',')
		labs = {row[0]:row[1] for row in data}
		return labs

#funcion que forma coneccion y regresa cursor
def conect(suc):
	import credentials
	server = 'MAINSERVER\\COMPAC' 
	database = sucursal[suc]['nombre']
	username = credentials.username
	password = credentials.password
	cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password+';TrustServerCertificate=Yes')
	return cnxn.cursor()

#intenta traducir entre nombre comun y nombre de base de datos
def traducir(nombre, tabla):
	return tablas[tabla].get(nombre, nombre)

#recibe fecha en el formato de la base de datos y regresa objeto datetime.date
def to_date(date_str):
	year = int(date_str[:4])
	month = int(date_str[5:7])
	day = int(date_str[8:10])
	return datetime.date(year=year, month=month, day=day)

#funcion que recibe un cursor ya ejecutado y colecta el resultado, regresa una lista de listas
def collect(cursor):
	data = []
	row = cursor.fetchone()
	while row:
		data.append(list(row))
		row = cursor.fetchone()
	return data

#funcion que regresa la tabla de clasificaciones en forma de diccionario
def get_class_dic(suc):
	query = """SELECT CIDVALORCLASIFICACION, CVALORCLASIFICACION from admClasificacionesValores"""
	cursor = conect(suc)
	cursor.execute(query)
	dic = {}
	row = cursor.fetchone()
	while row:
		dic[row[0]] = row[1]
		row = cursor.fetchone()
	return dic

#funcion que genera query
#campos : lista de los campos nombres comunes o de base de datos
#tabla : nombre de la tabla
#filtros : lista de touples en el formato (campo, comparacion, valor)
def gen_sql(campos, tabla, filtros=''):
	if isinstance(campos, str):
		campos_str = traducir(campos, tabla)
	else:
	 	campos_str = ','.join(map(traducir, campos, [tabla] * len(campos)))

	if filtros != '':
		filtro_list = []
		for i in filtros:
			filtro_list.append(f" {traducir(i[0], tabla)} {i[1]} \'{i[2]}\' ")
		filtro_str = 'WHERE' + 'AND'.join(filtro_list) +';'
		filtro_str = filtro_str.replace("""'(""", '(')
		filtro_str = filtro_str.replace(""")'""", ')')

	else:
		filtro_str = ''

	return f"SELECT {campos_str} FROM {tablas[tabla]['nombre']} {filtro_str}"

#funcion que regresa df de los productos en la visa Inventario Actual 
#suc : codigo de la sucursal
#caducos : Bool, si True incluye los productos caducos
def get_inventario(suc, caducos=False):
	print(f'getind inv: {suc}')
#nombres de los campos
	campos = list(tablas['inventario'].keys())[1:]
#filtros para almacen principal de la sucursal
	filtros = [('idAlmacen', '=', sucursal[suc]['id_almacen'])]
	if not caducos:
		filtros.append(('Estado', 'IN', """('Vigente', 'N/A')"""))
#generacion del query
	query = gen_sql(campos, 'inventario', filtros)
#obteniendo el cursor
	cursor = conect(suc)
#ejecutando el query
	cursor.execute(query)
#recolectando resultado
	data = collect(cursor)
#creando DF
	df = pd.DataFrame(data, columns=campos)
#agregando columna sucursal
	df['Sucursal'] = suc
#fecha de hoy para columnas calculadas
	today = datetime.datetime.now()
	err_date = datetime.date(year=1900, month=1, day=1)
#formateando columna caducidad a objeto datetime.date
#df['Caducidad'] = pd.to_datetime(df['Caducidad'], infer_datetime_format=True)
#creando columnas calculadas
#	df['Dias'] = df['Caducidad'].apply(lambda x: 0 if x == err_date else (x - today).days)
#df['Caducidad_Urgente'] = df['Dias'] < 31
#df['Caducidad_Proxima'] = df['Dias'] < 91
	return df	

#funcion que regresa inventario de lotes
def get_inventario_sec(suc):
#query particular
	query = f"""SELECT admCapasProducto.CIDPRODUCTO, admProductos.CCODIGOPRODUCTO, admCapasProducto.CNUMEROLOTE,
		 admCapasProducto.CFECHACADUCIDAD, admAlmacenes.CCODIGOALMACEN, admCapasProducto.CEXISTENCIA
	FROM ((admCapasProducto 
	INNER JOIN admProductos ON admProductos.CIDPRODUCTO=admCapasProducto.CIDPRODUCTO)
	INNER JOIN admAlmacenes ON admAlmacenes.CIDALMACEN=admCapasProducto.CIDALMACEN)
	WHERE admCapasProducto.CIDALMACEN = '{sucursal[suc]['id_almacen']}' AND admCapasProducto.CEXISTENCIA > '0';"""
	campos = list(tablas['inventario'].keys())[1:-1]
	cursor = conect(suc)
	cursor.execute(query)
	data = collect(cursor)
	df = pd.DataFrame(data, columns=campos)
	df['Caducidad'] = df['Caducidad'].apply(lambda x:datetime.date(year=x.year, month=x.month, day=x.day))
	df['Estado'] = 'Vigente'
	df['Sucursal'] = suc
	df['err'] = df['Caducidad'] > datetime.date(year=2100, month=1, day=1)
	df = df.loc[~df['err']].drop(columns=['err'])
	today = datetime.date.today()
	df['Dias'] = df['Caducidad'] - today
	df['Caducidad_proxima'] = df['Dias'] < datetime.timedelta(days=90)
	df['Caducidad_urgente'] = df['Dias'] < datetime.timedelta(days=30)
	df['Dias'] = df['Dias'].apply(lambda x: x.days)
	df['Estado'] = df['Dias'].apply(lambda x: 'Vigente' if x > 0 else 'Expirado')
	return df

#funcion que regresa DF de todos los clientes activos con todos los campos
def get_clientes(suc):
	campos = list(tablas['clientes'].keys())[1:]
	filtros = [('CTIPOCLIENTE', '=', '1'), ('CESTATUS', '=', '1')]
	query = gen_sql(campos, 'clientes', filtros)
	cursor = conect(suc)
	cursor.execute(query)
	data = collect(cursor)
	df = pd.DataFrame(data, columns=campos)
	class_dic = get_class_dic(suc)
	df['Agente_ID'] = df['Agente_ID'].apply(lambda x: class_dic.get(x, 'err class'))
	df['Dia_ID'] = df['Dia_ID'].apply(lambda x: class_dic.get(x, 'err class'))
	df = df.drop(columns='Estado')
	return df

#funcion que regresa DF de todods los productos activos
def get_productos(suc):
	campos = list(tablas['productos'].keys())[1:]
	filtros = [('CSTATUSPRODUCTO', '=', '1')]
	query = gen_sql(campos, 'productos', filtros)
	cursor = conect(suc)
	cursor.execute(query)
	data = collect(cursor)
	df = pd.DataFrame(data, columns=campos)
	return df
	
#funcion que regresa DF desde start hasta end
#start : fecha inicial, objeto datetime.date
#end : fecha final, objeto datetime.date
def get_documentos(suc, start, end, componentes=False):
	if isinstance(suc, list):
		dfl = []
		for i in suc:
			df = get_documentos(i, start, end)
			dfl.append(df.copy())
		return pd.concat(dfl)

#todo esto es para creacion del query particular
	doc = 'admDocumentos.'
	prod = 'admProductos.'
	cli = 'admClientes.'
	mov = 'admMovimientos.'
	agen = 'admAgentes.'
	campos = [prod + 'CIDPRODUCTO', prod + 'CCODIGOPRODUCTO', prod + 'CNOMBREPRODUCTO', prod + 'CTIPOPRODUCTO', mov + 'CUNIDADES', mov + 'CPRECIO', mov + 'CNETO', mov + 'CDESCUENTO1',
			mov + 'CIMPUESTO1', mov + 'CTOTAL', mov + 'CFECHA', doc + 'CFOLIO', doc + 'CIDCLIENTEPROVEEDOR', doc + 'CRAZONSOCIAL',
			 agen + 'CNOMBREAGENTE', doc + 'CAFECTADO']
	campos_nombres = ['ID_Producto', 'Codigo', 'Nombre', 'Tipo', 'Cantidad', 'Precio', 'Neto', 'Descuento', 'Impuesto', 'Total', 'Fecha','Folio', 'Cliente', 'Razon Social', 'Agente', 'Estado']
	campos_str = ", ".join(campos)
	format_str = '%Y-%m-%d 00:00:00.000'
	query = f"""SELECT {campos_str}
	FROM (((admMovimientos 
	INNER JOIN admProductos ON admMovimientos.CIDPRODUCTO=admProductos.CIDPRODUCTO)
	INNER JOIN admDocumentos ON admMovimientos.CIDDOCUMENTO=admDocumentos.CIDDOCUMENTO)
	INNER JOIN admAgentes ON admAgentes.CIDAGENTE=admDocumentos.CIDAGENTE)
	WHERE admMovimientos.CIDDOCUMENTODE = '4' AND admMovimientos.CFECHA BETWEEN '{start.strftime(format_str)}' AND '{end.strftime(format_str)}';"""
	cursor = conect(suc)
	cursor.execute(query)
	data = collect(cursor)
#df de los documentos
	df = pd.DataFrame(data, columns=campos_nombres)
#convirtiendo cliente_ID a Codigo de cliente
	clientes_campos = ['ID_Cliente', 'Codigo_Cliente']
	clientes_df = get_clientes(suc).set_index('ID_Cliente')['Codigo_Cliente']
	df = df.join(clientes_df, on='Cliente')
#agregando campos calculados
	df['Tipo'] = df['Tipo'].apply(lambda x: 'Producto' if x == 1 else 'Paquete')
	df['Desc-Bool'] = df['Descuento'] > 0
	df['Sucursal'] = sucursal[suc]['nombre'][2:]
	df['Neto-Desc'] = df['Neto'] - df['Descuento']
	df['Estado'] = df['Estado'].apply(lambda x: 'Afectado' if x == 1 else 'Cancelado')
	df['Sucursal'] = df['Sucursal'].replace('OBGREGON', 'OBREGON')
	lab_dic = get_labs()
	df['Laboratorio'] = df['Codigo'].apply(lambda x: lab_dic.get(re.split('\d+',x)[0], 'No Encontrado'))
	if len(sucursal[suc]['clientes_esp']) == 0:
		df['Cliente_ESP'] = False
	else:
		df['Cliente_ESP'] = df['Codigo_Cliente'].apply(lambda x:x in sucursal[suc]['clientes_esp'])

	if componentes:
		df = add_componentes_paquetes(df, suc)

	return df

#funcion que agrega los componentes de paquetes
#df: Objeto DataFrame de los documentos donde se agregaran los componentes
#suc: codigo de la sucursal
def add_componentes_paquetes(df, suc):
#loc de los registros que son paquetes
	df_paquetes = df.loc[df['Tipo'] == 'Paquete']
#creando query
	comp = 'admComponentesPaquete.'
	prod = 'admProductos.'
	query = f"""SELECT {comp}CIDPAQUETE, {comp}CIDPRODUCTO, {prod}CCODIGOPRODUCTO, {prod}CNOMBREPRODUCTO, {comp}CCANTIDADPRODUCTO 
	FROM admComponentesPaquete
	INNER JOIN admProductos ON admComponentesPaquete.CIDPRODUCTO=admProductos.CIDPRODUCTO"""
	campos = ['ID_Paquete', 'ID_Producto', 'Codigo_Producto', 'Nombre', 'Cantidad']
	cursor = conect(suc)
	cursor.execute(query)
	data = collect(cursor)
#df de todos los componentes de paquetes
	df_componentes = pd.DataFrame(data, columns=campos)

#iterando df_paquetes
	for index, paquete in df_paquetes.iterrows():
#diccionario para insertar en df
		dic_paquete = paquete.to_dict()
#modificando datos del dic
		dic_paquete['Precio'] = 0
		dic_paquete['Neto'] = 0
		dic_paquete['Descuento'] = 0
		dic_paquete['Impuesto'] = 0
		dic_paquete['Total'] = 0
		dic_paquete['Tipo'] = 'Producto' 

#df de los componentes de el paquete actual
		df_componentes_paquete = df_componentes.loc[df_componentes['ID_Paquete'] == dic_paquete['ID_Producto']]
#iterando los componentes de el paquete actual
		for index, componente in df_componentes_paquete.iterrows():
#modificando datos para componente
			dic_paquete['ID_Producto'] = componente['ID_Producto']
			dic_paquete['Codigo'] = componente['Codigo_Producto']
			dic_paquete['Nombre'] = componente['Nombre']
			dic_paquete['Cantidad'] = componente['Cantidad'] * dic_paquete['Cantidad']

#agregando componente a df
			df = df.append(dic_paquete, ignore_index=True)

	return df

#funcion que regresa todos los documentos del mes actual de todas las sucursales
def get_documentos_mes(codigo_suc='', componentes=False):
	end = datetime.date.today()
	start = end.replace(day=1)
	if codigo_suc == '':
		dfl = []
		for suc in sucursal.keys():
			df = get_documentos(suc, start, end, componentes)
			dfl.append(df.copy())
		return pd.concat(dfl)
	else:
		return get_documentos(codigo_suc, start, end, componentes)
