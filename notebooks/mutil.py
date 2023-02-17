import os
import pandas as pd
from geopy import distance

# Load dataset delitos_2022.xlsx for training and delitos_2023.xlsx for testing
PATH_DATA = '../data/'

def load_data():
	# Load delitos dataset
	if os.path.exists(PATH_DATA+'delitos.pkl'):
		df_delitos = pd.read_pickle(PATH_DATA+'delitos.pkl')
	else:
		df_delitos = pd.concat([pd.read_excel(PATH_DATA+'delitos_2022.xlsx'), pd.read_excel(PATH_DATA+'delitos_2023.xlsx')], ignore_index=True)

		df_delitos['fecha_creacion'] = pd.to_datetime(df_delitos['fecha_creacion'], format='%Y-%m-%d')
		df_delitos['hora_creacion'].replace(regex=True, inplace=True, to_replace=r'^[\d\-]+ ', value=r'')
		df_delitos['hora_creacion'] = pd.to_datetime(df_delitos['hora_creacion'], format='%H:%M:%S')
		df_delitos['fecha_creacion'] = df_delitos['fecha_creacion'] + (df_delitos['hora_creacion'] - df_delitos['hora_creacion'].min())
		
		df_delitos = df_delitos[df_delitos['codigo_cierre'] == 'A'] # Solo considerar los registros que tengan "A" (Afirmativo) en la columna codigo_cierre
	
		df_delitos = df_delitos[df_delitos['fecha_creacion'].dt.year >= 2022]

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
		df_delitos = df_delitos[df_delitos.incidente_c4.isin([
			'Robo-Vehículo sin Violencia', 'Robo-Vehiculo con Violencia', 
			'Agresión-Persona', 'Denuncia-Persona Sospechosa', 
			'Disturbio-Concentración de Personas', 'Disturbio-Disparos', 
			'Robo-Auto partes', 'Robo-Automovilista', 
			'Robo-Establecimiento con Violencia', 'Robo-Establecimiento sin Violencia', 
			'Robo-Transeúnte', 'Abandono-Vehículo'
		])]

		"""
			Save columns and drop the ones that are not needed
			['fecha_creacion', 'hora_creacion',
			'dia_semana', 'incidente_c4', 'colonia', 'delegacion_inicio',
			'sector_inicio', 'latitud', 'longitud']
		"""
		df_delitos = df_delitos[[
			'folio',
			'fecha_creacion', 'hora_creacion', 'incidente_c4', 
			'colonia', 'delegacion_inicio', 'sector_inicio', 
			'latitud', 'longitud'
		]]
		df_delitos.fillna(0, inplace=True)

		# Create a new column named camera id empty
		df_delitos['id_camara'] = ''

		# Remove YYYY-MM-DD in field fecha_creacion
		df_delitos['año_creacion'] = df_delitos['fecha_creacion'].dt.year
		df_delitos['mes_creacion'] = df_delitos['fecha_creacion'].dt.month
		df_delitos['dia_creacion'] = df_delitos['fecha_creacion'].dt.day
		df_delitos['dia_semana'] = df_delitos['fecha_creacion'].dt.dayofweek
		df_delitos['semana_creacion'] = df_delitos['fecha_creacion'].dt.isocalendar().week
		df_delitos['hora_creacion'] = df_delitos['hora_creacion'].dt.hour
		
		# convert latitud and longitud to float
		df_delitos['latitud'] = df_delitos['latitud'].astype(float)
		df_delitos['longitud'] = df_delitos['longitud'].astype(float)

		df_delitos.to_pickle(PATH_DATA+'delitos.pkl')
	## Load camaras dataset

	if os.path.exists(PATH_DATA+'camaras.pkl'):
		df_camaras = pd.read_pickle(PATH_DATA+'camaras.pkl')
	else:	
		df_camaras = pd.read_excel(PATH_DATA+'camaras_01.2023.xlsx', dtype=str, sheet_name='BASE')

		df_camaras = df_camaras[['ID_BCT_O','latitud','longitud']]
		df_camaras.rename(columns={'latitud':'latitud','longitud':'longitud'}, inplace=True) #rename columns latitud,longitud to lower case 

		# convertir a float las columnas "latitud,longitud" de df_camaras
		df_camaras.latitud = df_camaras.latitud.astype(float)
		df_camaras.longitud = df_camaras.longitud.astype(float)
		df_camaras.to_pickle(PATH_DATA+'camaras.pkl')
	return df_delitos, df_camaras


def preprocess_data(df_incidentes, df_camaras)->pd.DataFrame:

	if os.path.exists(PATH_DATA+'dataset.pkl'):
		dataset = pd.read_pickle(PATH_DATA+'dataset.pkl')
	else:
		# create a new dataframe with the columns of df 
		dataset = pd.DataFrame(columns=df_incidentes.columns)

		# obtener latitud y longitud de cada registro de df_camaras
		for index, cam in df_camaras.iterrows():
			print(index,"/",len(df_camaras), end="\r")

			center = (cam.latitud, cam.longitud) # latitud y longitud del punto central
			
			radius = 100 # 100m en latitud y longitud = (0.0009, 0.0009) 

			df_quadrant = df_incidentes[(df_incidentes['latitud'] >= cam.latitud-0.0009) & (df_incidentes['latitud'] <= cam.latitud+0.0009) 
						& (df_incidentes['longitud'] >= cam.longitud-0.0009) & (df_incidentes['longitud'] <= cam.longitud+0.0009)]

			df_quadrant = df_quadrant[[distance.distance(center, (lat, lon)).m <= radius for lat, lon in zip(df_quadrant['latitud'], df_quadrant['longitud'])]]

			df_quadrant['id_camara'] = cam.ID_BCT_O

			dataset = pd.concat([dataset, df_quadrant], ignore_index=True)
			dataset.to_pickle(PATH_DATA+'dataset.pkl')
	return dataset


def fill_data(df_count, fill_camera, fill_values=0, start_date='2022-01-01', end_date='2023-01-31'):
	df_count = df_count.set_index('fecha_creacion').reindex(pd.date_range(start=start_date, end=end_date)).reset_index().rename(columns={'index': 'fecha_creacion'})
	df_count['count'] = df_count['count'].fillna(fill_values)
	df_count['id_camara'] = df_count['id_camara'].fillna(fill_camera)
	return df_count