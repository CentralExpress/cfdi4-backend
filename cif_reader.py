import pdfx, re, datetime

def read_cif(pdf_file, debug=False):
	dic_re = {
		'fisico': {
		'rfc' : r'([A-Z]{3,4}\d{6}[A-Z\d]{3})\W',
		'curp': r'Primer Apellido:\W+\w+\W+(\w+)',
		'emision' : 'Emisi.n\W[\w ]+,.+ A ([\w\d ]+\W[\w\d ]+)',
		'nombre' :r'Primer Apellido:\W+[\w :]+\W+[\w :]+\W+[\w :]+\W+([\w ]+)',
		'primer_apellido' :r'Primer Apellido:\W+[\w :]+\W+[\w :]+\W+[\w :]+\W+[\w ]+\W+([\w ]+)',
		'segundo_apellido':r'Primer Apellido:\W+[\w :]+\W+[\w :]+\W+[\w :]+\W+[\w ]+\W+[\w ]+\W+([\w ]+)',
		'fecha_inicio_de_operaciones': r'Fecha inicio de operaciones:\W+([\w\d ]+)\W',
		'status_en_el_padron': r'Estatus en el padr.n:\W+(\w+)\W',
		'fecha_de_ultimo_cambio_de_estado': r'Fecha de .ltimo cambio de estado:\W+([\w\d ]+)\W',
		'razon_social': r'Contribuyentes\n{2}([\w \n]+)Nombre',
		'codigo_postal': r'C.digo Postal:(\d+)',
		'tipo_de_vialidad': r'Tipo de Vialidad:([ \w\(\)\.]+)',
		'calle': r'Nombre de Vialidad:([ \w\.]+)',
		'numero_exterior': r'N.mero Exterior:([ \d]+)',
		'numero_interior': r'N.mero Interior:(.*)',
		'colonia': r'Nombre de la Colonia:([ \w]+)',
		'localidad': r'Nombre de la Localidad:(.*)',
		'entre_calle': r'Entre Calle:(.*)',
		'y_calle': r'Y Calle:(.*)',
		'municipio': r'Territorial:(.*)',
		'entidad_federativa': r'Federativa:([ \w]+)',
		'correo_electronico': r'Correo Electr.nico:\W{2}(.*)',
		'tel_fijo_lada': r'Tel\. Fijo Lada:(.*)',
		'numero_fijo': r'Correo Electr.nico:\W{2}.*\W{2}N.mero:(.*)',
		'tel_movil_lada': r'Tel\. M.vil Lada:(.*)',
		'numero_movil': r'Correo Electr.nico:\W{2}.*\W{2}.*\W{2}N.mero:(.*)'
		},
		'moral': {
		'rfc' : r'([A-Z]{3,4}\d{6}[A-Z\d]{3})\W',
		'razon_social': r'Social:\W{2}([\w ]+)',
		'emision' : 'Emisi.n\W[\w ]+,.+ A ([\w\d ]+\W[\w\d ]+)',
		'regimen_capital': r'Social:\W{2}[\w ]+\W{2}([\w ]+)',
		'fecha_inicio_de_operaciones': r'Fecha inicio de operaciones:\W+([\w\d ]+)\W',
		'status_en_el_padron': r'Estatus en el padr.n:\W+(\w+)\W',
		'fecha_de_ultimo_cambio_de_estado': r'Fecha de .ltimo cambio de estado:\W+([\w\d ]+)\W',
		'nombre_comercial': r'Nombre Comercial:\W+([\w ]+)\W',
		'codigo_postal': r'C.digo Postal:(\d+)',
		'tipo_de_vialidad': r'Tipo de Vialidad:([ \w\(\)\.]+)',
		'calle': r'Nombre de Vialidad:([ \w\.]+)',
		'numero_exterior': r'N.mero Exterior:([ \d]+)',
		'numero_interior': r'N.mero Interior:(.*)',
		'colonia': r'Nombre de la Colonia:([ \w]+)',
		'localidad': r'Nombre de la Localidad:(.*)',
		'entre_calle': r'Entre Calle:(.*)',
		'y_calle': r'Y Calle:(.*)',
		'municipio': r'Territorial:(.*)',
		'entidad_federativa': r'Federativa:([ \w]+)',
		'correo_electronico': r'Correo Electr.nico:\W{2}(.*)',
		'tel_fijo_lada': r'Tel\. Fijo Lada:(.*)',
		'numero_fijo': r'Correo Electr.nico:\W{2}.*\W{2}N.mero:(.*)',

		}
	}

	pdf = pdfx.PDFx(pdf_file)
	text = pdf.get_text()

	curp = re.search(dic_re['fisico']['curp'], text, re.MULTILINE)
	if not curp:
		regimen = 'moral'
	else:
		regimen = 'fisico'

	dic = dic_re[regimen].copy()

	for campo in dic.keys():
		try:
			valor = re.search(dic[campo], text, re.MULTILINE).group(1)
			valor = valor.replace('\n', '')
			dic[campo] = valor.strip()
		except:
			dic[campo] = 'ERROR'

	dic['regimen'] = regimen
	dic['emision'] = dic['emision'].replace('\n', '')
		
	return dic
		
def emision_to_dt(emision_str):
	mes_dic = {'ENERO' : 1,
		'FEBRERO': 2,
		'MARZO': 3,
		'ABRIL': 4,
		'MAYO': 5,
		'JUNIO': 6,
		'JULIO': 7,
		'AGOSTO': 8,
		'SEPTIEMBRE': 9,
		'OCTUBRE': 10,
		'NOVIEMBRE': 11,
		'DICIEMBRE': 12 }

	lista_data = emision_str.replace('\n', '').split('DE')
	dia = int(lista_data[0])
	mes = mes_dic[lista_data[1].strip()]
	year = int(lista_data[2])
	return datetime.date(day=dia, month=mes, year=year)

def fecha_emision_cif(archivo):
	dic = read_cif(archivo)
	return emision_to_dt(dic['emision'])
	
def exportar_texto_cif(archivo, destino):
    pdf = pdfx.PDFx(archivo)
    texto = pdf.get_text()
    with open(destino, 'w') as out_file:
        out_file.write(texto)
    return

	
if __name__ == '__main__':
	import pprint, json
	pprint.pprint(read_cif('doc1.pdf'))
	pprint.pprint(read_cif('CEC_CIF.pdf'))
