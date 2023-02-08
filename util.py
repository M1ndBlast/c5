# Load dataset delitos_2022.xlsx for training and delitos_2023.xlsx for testing

import os
import pandas as pd
from geopy import distance

def load_data():
	# Load delitos dataset
	if os.path.exists('delitos.pkl'):
		df_delitos = pd.read_pickle('delitos.pkl')
	else:
		df_delitos = pd.concat([pd.read_excel('delitos_2022.xlsx'), pd.read_excel('delitos_2023.xlsx')], ignore_index=True)

		df_delitos = df_delitos[df_delitos['codigo_cierre'] == 'A'] # Solo considerar los registros que tengan "A" (Afirmativo) en la columna codigo_cierre
		df_delitos = df_delitos[df_delitos.fecha_creacion != '2021-12-31'] # Remove rows with date 2021-12-31

		"""
			Save rows where incidentes_c4 is
			['Robo-Vehículo sin Violencia', 'Robo-Vehiculo con Violencia',
			'Agresión-Persona', 'Denuncia-Persona Sospechosa',
			'Disturbio-Concentración de Personas', 'Disturbio-Disparos',
			'Robo-Auto partes', 'Robo-Automovilista',
			'Robo-Establecimiento con Violencia',
			'Robo-Establecimiento sin Violencia', 'Robo-Transeúnte',
			'Abandono-Vehículo']
		"""
		df_delitos = df_delitos[df_delitos['incidente_c4'].isin(['Robo-Vehículo sin Violencia', 'Robo-Vehiculo con Violencia',
				'Agresión-Persona', 'Denuncia-Persona Sospechosa',
				'Disturbio-Concentración de Personas', 'Disturbio-Disparos',
				'Robo-Auto partes', 'Robo-Automovilista',
				'Robo-Establecimiento con Violencia',
				'Robo-Establecimiento sin Violencia', 'Robo-Transeúnte',
				'Abandono-Vehículo'])]

		"""
			Save columns and drop the ones that are not needed
			['fecha_creacion', 'hora_creacion',
			'dia_semana', 'incidente_c4', 'colonia', 'delegacion_inicio',
			'sector_inicio', 'latitud', 'longitud']
		"""
		df_delitos.drop(['hora_creacion', 'dia_semana', 'incidente_c4', 'colonia', 'delegacion_inicio',
				'sector_inicio', 'codigo_cierre', 'latitud', 'longitud'], axis=1, inplace=True)
		df_delitos['id_camara'] = ''

		df_delitos = df_delitos.fillna(0, inplace=True)

		# Remove YYYY-MM-DD in field fecha_creacion
		df_delitos['fecha_creacion'] = pd.to_datetime(df_delitos['fecha_creacion'], format='%Y-%m-%d')
		df_delitos['año_creacion'] = df_delitos['fecha_creacion'].dt.year
		df_delitos['mes_creacion'] = df_delitos['fecha_creacion'].dt.month
		df_delitos['dia_creacion'] = df_delitos['fecha_creacion'].dt.day
		df_delitos['dia_semana'] = df_delitos['fecha_creacion'].dt.dayofweek
		df_delitos['semana_creacion'] = df_delitos['fecha_creacion'].dt.week
		# Remove YYYY-MM-DD in field hora_creacion
		df_delitos['hora_creacion'] = df_delitos['hora_creacion'].str[11:13]+':00'
		
		# convert latitud and longitud to float
		df_delitos['latitud'] = df_delitos['latitud'].astype(float)
		df_delitos['longitud'] = df_delitos['longitud'].astype(float)

		df_delitos = df_delitos[['fecha_creacion', 'hora_creacion', 'mes_creacion', 'dia_creacion', 'semana_creacion',
				'dia_semana', 'incidente_c4', 'colonia', 'delegacion_inicio',
				'sector_inicio', 'latitud', 'longitud', 'codigo_cierre']]

		df_delitos.to_pickle('delitos.pkl')
	## Load camaras dataset

	if os.path.exists('camaras.pkl'):
		df_camaras = pd.read_pickle('camaras.pkl')
	else:	
		df_camaras = pd.read_excel('camaras_01.2023.xlsx', dtype=str, sheet_name='BASE')

		df_camaras = df_camaras[['ID_BCT_O','LATITUD','LONGITUD']]
		df_camaras.rename(columns={'LATITUD':'latitud','LONGITUD':'longitud'}, inplace=True) #rename columns LATITUD,LONGITUD to lower case 

		# convertir a float las columnas "LATITUD,LONGITUD" de df_camaras
		df_camaras.latitud = df_camaras.latitud.astype(float)
		df_camaras.longitud = df_camaras.longitud.astype(float)
		df_camaras.to_pickle('camaras.pkl')
	return df_delitos, df_camaras


def preprocess_data(df_incidentes, df_camaras):
	# create a new dataframe with the columns of df 
	dataset = pd.DataFrame(columns=df_incidentes.columns)

	# obtener latitud y longitud de cada registro de df_camaras
	for index, cam in df_camaras.iterrows():
		print(index,"/",len(df_camaras), end="\r")

		center = (cam.LATITUD, cam.LONGITUD) # latitud y longitud del punto central
		
		radius = 100 # 100m en latitud y longitud = (0.0009, 0.0009) 

		df_quadrant = df_incidentes[(df_incidentes['latitud'] >= cam.LATITUD-0.0009) & (df_incidentes['latitud'] <= cam.LATITUD+0.0009) 
					& (df_incidentes['longitud'] >= cam.LONGITUD-0.0009) & (df_incidentes['longitud'] <= cam.LONGITUD+0.0009)]

		df_quadrant = df_quadrant[[distance.distance(center, (lat, lon)).m <= radius for lat, lon in zip(df_quadrant['latitud'], df_quadrant['longitud'])]]

		df_quadrant['id_camara'] = cam.ID_BCT_O

		dataset = pd.concat([dataset, df_quadrant], ignore_index=True)
		dataset.to_pickle('dataset.pkl')
	return dataset
