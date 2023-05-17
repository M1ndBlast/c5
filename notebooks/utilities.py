import os, math
import pandas as pd
from time import time
from geopy import distance

# Load dataset delitos_2022.xlsx for training and delitos_2023.xlsx for testing
DATA_PATH					= '../data/'
PROMAD_PATH					= 'C:/Users/jhernandeza/Downloads/PROMAD_FULL/'#'//10.19.101.116/6.-Compartida_DGGE/Promad/BASES 2022/'
INCIDENTES_RAW_FILENAME		= DATA_PATH+'C5_PROMAD.pkl'
INCIDENTES_FILENAME			= DATA_PATH+'C5_Delitos.pkl'
CAMARAS_FILENAME			= DATA_PATH+'C5_Camaras.pkl'
INCIDENTES_CAMARAS_FILENAME	= DATA_PATH+'C5_Delitos-Camaras.pkl'

"""	Filtrar por codigo de cierre AFIRMATIVO
		['A'] = Afirmativo
		['N'] = Negativo
		['F'] = Falso
		['I'] = Incompleto
"""
CODIGOS_CIERRE = ['A']

CAMARAS_EXCLUIR = ['MC', 'MT']
SECTORES_INCLUIR = ['MORELOS', 'CONGRESO', 'ALAMEDA', 'CENTRO']

INCIDENTES_C4 = [
	'ROBO-TRANSEÚNTE',
	'ROBO-AUTO PARTES',
	'ROBO-AUTOMOVILISTA',
	'ROBO-VEHÍCULO SIN VIOLENCIA',
	'ROBO-VEHÍCULO CON VIOLENCIA',
	'ROBO-ESTABLECIMIENTO SIN VIOLENCIA',
	'ROBO-ESTABLECIMIENTO CON VIOLENCIA',
	'ROBO-CABLE',
	'ROBO-BIENES GUBERNAMENTALES (COLADERAS CABLE)',
	'ADMINISTRATIVAS-TIRAR BASURA EN VÍA PÚBLICA',
	'ADMINISTRATIVAS-EBRIOS',				#
	'ADMINISTRATIVAS-DROGADOS',
	'ADMINISTRATIVAS-GRAFITIS',
	'ADMINISTRATIVAS-FRANELEROS',
	'ADMINISTRATIVAS-COMERCIO INFORMAL',
	'DISTURBIO-ESCÁNDALO',					#
	'DISTURBIO-RIÑA',						#
]



COLUMNAS = ['folio', 'incidente_c4',  'fecha_creacion', 'sector_inicio', 'delegacion_inicio', 'colonia', 'latitud', 'longitud',]

# DAYS = {'Lunes': 1, 'Martes': 2, 'Miércoles': 3, 'Jueves': 4, 'Viernes': 5, 'Sábado': 6, 'Domingo': 7}


def load_raw_data():
	if os.path.exists(INCIDENTES_RAW_FILENAME):
		print(f'Cargando {INCIDENTES_RAW_FILENAME}')
		df_raw_delitos = pd.read_pickle(INCIDENTES_RAW_FILENAME)
	else:
		print(f'Generando {INCIDENTES_RAW_FILENAME}')
		MESES_DICT = { 'ENE': 1, 'FEB': 2, 'MAR': 3, 'ABR': 4, 'MAY': 5, 'JUN': 6, 'JUL': 7, 'AGO': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DIC': 12 }

		# Imprimir los archivos de la carpeta
		print(f'Buscando archivos de C4 en {PROMAD_PATH}')
		C4_PROMAD_FILES = []

		for file in os.listdir(PROMAD_PATH):
			if os.path.isfile(os.path.join(PROMAD_PATH, file)) and file.endswith(".xlsx"):
				C4_PROMAD_FILES.append(file)

		if len(C4_PROMAD_FILES) == 0:
			raise ReferenceError(f'No se encontró ningún archivos de C4 en {PROMAD_PATH}')
		else:
			print(f'Se encontraron {len(C4_PROMAD_FILES)} archivos de C4 en {PROMAD_PATH}')

		# Ordenar los archivos por fecha using MESES_DICT
		C4_PROMAD_FILES.sort(key=lambda x: (MESES_DICT[x[3:6]]))

		# Guardar los archivos en un dataframe y posteriormente en un pickle y un excel
		print(f'Concatenando archivos de C4 en {PROMAD_PATH}')
		df_raw_delitos = pd.DataFrame()


		tS = time()
		ts_file, te_file = tS, tS
		meantime_file, meancount_file = 0, 0
		total_iterations = len(C4_PROMAD_FILES)

		for i,file in enumerate(C4_PROMAD_FILES):
			# Start time for each file
			meantime_file += round((te_file - ts_file), 2)
			ts_file = time()
			meancount_file += 1

			stimated_time = seconds_to_time(round((meantime_file/(meancount_file | 1)) * (total_iterations - meancount_file),2))

			timecamara_log = f'<{i}/{total_iterations} [{seconds_to_time(meantime_file/(meancount_file | 1))} per file]'
			print(f'\r[{seconds_to_time(round((te_file - tS), 2))} total / {stimated_time} to finish] {timecamara_log} \t\t\t\t', end='\r')
			te_file = time()
			# df_c4_promad = pd.concat([df_c4_promad, pd.read_excel(os.path.join(PROMAD_PATH, file), dtype=str, sheet_name='BASE')])
			df_raw_delitos = pd.concat([df_raw_delitos, pd.read_excel(os.path.join(PROMAD_PATH, file), dtype=str)])
		print(f'Guardando {INCIDENTES_RAW_FILENAME}')
		df_raw_delitos.to_pickle(INCIDENTES_RAW_FILENAME)
		try:
			INCIDENTES_RAW_FILENAME_XLSX = INCIDENTES_RAW_FILENAME.replace('.pkl', '.xlsx')
			df_raw_delitos.to_excel(INCIDENTES_RAW_FILENAME_XLSX)
		except Exception as e:
			print(f'No se pudo guardar el archivo {INCIDENTES_RAW_FILENAME_XLSX}')
			print(e)
			pass
	return df_raw_delitos
		

def load_data():
	# Load delitos dataset
	if os.path.exists(INCIDENTES_FILENAME):
		print(f'Cargando {INCIDENTES_FILENAME}')
		df_delitos = pd.read_pickle(INCIDENTES_FILENAME)
	else:
		df_delitos = load_raw_data()
		print(f'Generando {INCIDENTES_FILENAME}')

		# Imprimir cuales son los valores unicos de la columna sector_inicio y cuantos registros hay de cada uno
		sectores = df_delitos['sector_inicio'].value_counts()
		print(f'Cantidad de registros por sector_inicio: \n[{len(df_delitos["sector_inicio"])}] 100%\n{(sectores/sectores.sum()*100).apply(lambda x: f"{x:.2f}%")}')
		# Solo considerar los registros que tengan alguno de los sectores incluidos en SECTORES_INCLUIR
		df_delitos = df_delitos[df_delitos['sector_inicio'].isin(SECTORES_INCLUIR)]

		# Imprimir cuales son los valores unicos de la columna codigo_cierre y cuantos registros hay de cada uno
		codigo_cierre = df_delitos['codigo_cierre'].value_counts()
		print(f'Cantidad de registros por codigo_cierre: \n[{len(df_delitos["codigo_cierre"])}] 100%\n{(codigo_cierre/codigo_cierre.sum()*100).apply(lambda x: f"{x:.2f}%")}')
		# Solo considerar los registros que tengan "A" (Afirmativo) en la columna codigo_cierre
		df_delitos = df_delitos[df_delitos['codigo_cierre'].isin(CODIGOS_CIERRE)] 


		# Imprimir cuales son los valores unicos de la columna incidente_c4 y cuantos registros hay de cada uno
		tipo_incidente = df_delitos['incidente_c4'].value_counts()
		print(f'Cantidad de registros por tipo de incidente: \n[{len(df_delitos["incidente_c4"])}] 100%\n{(tipo_incidente/tipo_incidente.sum()*100)[tipo_incidente.index.to_series().apply(lambda x: x.upper()).isin(INCIDENTES_C4)].apply(lambda x: f"{x:.2f}%")}')
		# Solo considerar los registros que tengan alguno de los valores de incidentes_c4 en la columna incidente_c4
		df_delitos = df_delitos[df_delitos['incidente_c4'].apply(lambda x: x.upper()).isin(INCIDENTES_C4)]


		print(f'Modificando columnas de {INCIDENTES_FILENAME}')
		# Convertir las columnas fecha_creacion y hora_creacion a datetime y juntarlas en una sola columna
		df_delitos['hora_creacion'] = pd.to_datetime(df_delitos['hora_creacion'], format='%H:%M:%S')
		df_delitos['fecha_creacion'].replace(regex=True, inplace=True, to_replace=r'\s.*$', value=r'')
		df_delitos['fecha_creacion'] = pd.to_datetime(df_delitos['fecha_creacion'], format='%Y/%m/%d')
		df_delitos['fecha_creacion'] = df_delitos['fecha_creacion'] + (df_delitos['hora_creacion'] - df_delitos['hora_creacion'].min())
		df_delitos.drop(columns=['hora_creacion'], inplace=True)


		# convert latitud and longitud to float
		df_delitos['latitud'] = df_delitos['latitud'].astype(float)
		df_delitos['longitud'] = df_delitos['longitud'].astype(float)


		# Save columns and drop the ones that are not needed
		df_delitos = df_delitos[COLUMNAS]


		df_delitos.fillna(0, inplace=True)
		# Create a new column named camera id empty
		df_delitos['id_camara'] = ''


		print(f'Guardando {INCIDENTES_FILENAME}')
		df_delitos.to_pickle(INCIDENTES_FILENAME)
		try:
			INCIDENTES_FILENAME_XLSX = INCIDENTES_FILENAME.replace('.pkl', '.xlsx')
			df_delitos.to_excel(INCIDENTES_FILENAME_XLSX)
		except Exception as e:
			print(f'No se pudo guardar el archivo {INCIDENTES_FILENAME_XLSX}')
			print(e)
			pass
	## Load camaras dataset

	if os.path.exists(CAMARAS_FILENAME):
		print(f'Cargando {CAMARAS_FILENAME}')
		df_camaras = pd.read_pickle(CAMARAS_FILENAME)
	else:	
		print(f'Generando {CAMARAS_FILENAME}')
		df_camaras = pd.read_excel(DATA_PATH+'camaras_01.2023.xlsx', dtype=str, sheet_name='BASE')

		# Solo considerar las filas que tengan alguno de los sectores incluidos en SECTORES_INCLUIR en la columna SECTOR_UPC y que tengan "9m" en la columna TIPO_POSTE 
		df_camaras = df_camaras[(~df_camaras['ID_BCT_O'].str.startswith(tuple(CAMARAS_EXCLUIR))) & (df_camaras['TIPO_POSTE'] == '9m')]

		# Solo considerar los registros que tengan alguno de los sectores incluidos en SECTORES_INCLUIR en la columna SECTOR_UPC
		df_camaras = df_camaras[df_camaras['SECTOR_UPC'].isin(SECTORES_INCLUIR)]

		# Renombrar las columnas ID_BCT_O a id, LATITUD a latitud, LONGITUD a longitud
		df_camaras.rename(columns={'ID_BCT_O':'id','LATITUD':'latitud','LONGITUD':'longitud','SECTOR_UPC':'sector','TIPO_POSTE':'tipo'}, inplace=True)
		df_camaras = df_camaras[['id','latitud','longitud','sector','tipo']]

		# convertir a float las columnas "latitud,longitud" de df_camaras
		df_camaras.latitud = df_camaras.latitud.astype(float)
		df_camaras.longitud = df_camaras.longitud.astype(float)

		print(f'Guardando {CAMARAS_FILENAME}')
		df_camaras.to_pickle(CAMARAS_FILENAME)
		try:
			CAMARAS_FILENAME_XLSX = CAMARAS_FILENAME.replace('.pkl', '.xlsx')
			df_camaras.to_excel(CAMARAS_FILENAME_XLSX)
		except Exception as e:
			print(f'No se pudo guardar el archivo {CAMARAS_FILENAME_XLSX}')
			print(e)
			pass
	return df_delitos, df_camaras


def vinculate_incidentes_camaras(df_incidentes, df_camaras)->pd.DataFrame:
	if os.path.exists(INCIDENTES_CAMARAS_FILENAME):
		print(f'Cargando {INCIDENTES_CAMARAS_FILENAME}')
		dataset = pd.read_pickle(INCIDENTES_CAMARAS_FILENAME)
	else:
		print(f'Generando {INCIDENTES_CAMARAS_FILENAME} con {len(df_camaras)} camaras y {len(df_incidentes)} incidentes')
		# create a new dataframe with the columns of df 
		dataset = pd.DataFrame(columns=df_incidentes.columns)

		tS = time()
		ts_cam, te_cam = tS, tS
		meantime_camara, meancount_camara = 0, 0
		totaliterations = len(df_camaras)


		# obtener latitud y longitud de cada registro de df_camaras
		for index, (_, cam) in enumerate(df_camaras.iterrows()):
			# Start time for each camera
			meantime_camara += round((te_cam - ts_cam), 2)
			ts_cam = time()
			meancount_camara += 1

			stimated_time = seconds_to_time(round((meantime_camara/(meancount_camara | 1)) * (totaliterations - meancount_camara),2))

			timecamara_log = f'<{cam.id} ({index}/{totaliterations}) [{seconds_to_time(meantime_camara/(meancount_camara | 1))} per camera]'
			print(f'\r[{seconds_to_time(round((te_cam - tS), 2))} total / {stimated_time} to finish] {timecamara_log} \t\t\t\t', end='\r')
			te_cam = time()

			center = (cam.latitud, cam.longitud) # latitud y longitud del punto central
			
			radius = 100 # 100m en latitud y longitud = (0.0009, 0.0009) 

			df_quadrant = df_incidentes[(df_incidentes['latitud'] >= cam.latitud-0.0009) & (df_incidentes['latitud'] <= cam.latitud+0.0009) 
						& (df_incidentes['longitud'] >= cam.longitud-0.0009) & (df_incidentes['longitud'] <= cam.longitud+0.0009)]

			df_quadrant = df_quadrant[[distance.distance(center, (lat, lon)).m <= radius for lat, lon in zip(df_quadrant['latitud'], df_quadrant['longitud'])]]

			df_quadrant['id_camara'] = cam.id

			dataset = pd.concat([dataset, df_quadrant], ignore_index=True)

		print(f'\nGuardando {INCIDENTES_CAMARAS_FILENAME}')
		dataset.to_pickle(INCIDENTES_CAMARAS_FILENAME)
		try:
			INCIDENTES_CAMARAS_FILENAME_XLSX = INCIDENTES_CAMARAS_FILENAME.replace('.pkl', '.xlsx')
			dataset.to_excel(INCIDENTES_CAMARAS_FILENAME_XLSX)
		except Exception as e:
			print(f'No se pudo guardar el archivo {INCIDENTES_CAMARAS_FILENAME_XLSX}')
			print(e)
			pass
	return dataset


def generate_redundant_data(data):
	print('Procesando la hora del día...')
	data['hora_dia'] = data['fecha_creacion'].dt.hour
	print('Procesando el día de la semana...')
	data['dia_semana'] = data['fecha_creacion'].dt.dayofweek
	print('Procesando el día...')
	data['dia_mes'] = data['fecha_creacion'].dt.day
	print('Procesando el mes...')
	data['mes_anio'] = data['fecha_creacion'].dt.month

	# Convertir las funciones lambda en una sola funcion que devuelva ambas columnas
	def split_c5(x):
		splits = x.rsplit('-', 1)
		if len(splits) == 1:
			return splits[0], splits[0]
		else:
			return splits[0], splits[1]

	# Aplicar la funcion a la columna 'incidente_c4'
	print('Procesando la categoría y el tipo...')
	data['incidente_categoria'], data['incidente_tipo'] = zip(*data['incidente_c4'].apply(split_c5))

	print('Procesando lo demás...')
	# Aquí, necesitaríamos agregar las columnas faltantes como 
	# 	- densidad poblacional, 
	# 	- índice de criminalidad, 
	# 	- incidentes previos en cada categoría, 
	# 	- factores ambientales 
	# 	- información de puntos de interés.

	return data


def fill_data(df_count, fill_camera, fill_values=0, start_date='2022-01-01', end_date='2023-01-31'):
	df_count = df_count.set_index('fecha_creacion').reindex(pd.date_range(start=start_date, end=end_date)).reset_index().rename(columns={'index': 'fecha_creacion'})
	df_count['count'] = df_count['count'].fillna(fill_values)
	df_count['id_camara'] = df_count['id_camara'].fillna(fill_camera)
	return df_count

# Funcion que convierte los segundos en formato hh:mm:ss
def seconds_to_time(seconds:float):
	hours = math.floor(seconds / 3600)
	seconds = seconds - hours * 3600
	minutes = math.floor(seconds / 60)
	seconds = seconds - minutes * 60
	milliseconds = math.floor((seconds - math.floor(seconds)) * 1000)
	seconds = math.floor(seconds)
	return f'{hours:02d}:{minutes:02d}:{seconds:02d}:{milliseconds:03d}'