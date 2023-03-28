import pandas as pd
DATA_PATH = '../data'

# Cargar el archivo
print('Cargando el archivo...')
data = pd.read_pickle(DATA_PATH+"/C4_2022 PROMAD.pkl")

# Filtrar y renombrar las columnas necesarias
print('Filtrando y renombrando las columnas...')
data = data[['latitud', 'longitud', 'hora_creacion', 'dia_semana', 'fecha_creacion', 'incidente_c4', 'colonia', 'delegacion_inicio']]

# Convertir 'hora_creacion' a un objeto datetime
print('Convirtiendo hora a datetime...')
data['hora_creacion'] = pd.to_datetime(data['hora_creacion'])
# Convertir 'fecha_creacion' a un objeto datetime
print('Convirtiendo fecha a datetime...')
data['fecha_creacion'] = pd.to_datetime(data['fecha_creacion'])

# Extraer la información de la hora del día a partir de 'hora_creacion'
print('Procesando la hora del día...')
data['hora_dia'] = data['hora_creacion'].apply(lambda x: x.hour)

# Convertir 'dia_semana' en una variable numérica
days = {'Lunes': 1, 'Martes': 2, 'Miércoles': 3, 'Jueves': 4, 'Viernes': 5, 'Sábado': 6, 'Domingo': 7}
data['dia_semana'] = data['dia_semana'].map(days)

# Extraer el día y el mes a partir de 'fecha_creacion'
print('Procesando el día...')
data['dia_mes'] = data['fecha_creacion'].apply(lambda x: x.day)
print('Procesando el mes...')
data['mes_anio'] = data['fecha_creacion'].apply(lambda x: x.month)

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

# Preprocesar y agregar las columnas faltantes como se describió anteriormente

# Eliminar las columnas ya procesadas
data.drop(['hora_creacion', 'fecha_creacion'], axis=1, inplace=True)

# Guardar el conjunto de datos preprocesado en un archivo CSV
data.to_csv(DATA_PATH+'/datos_preprocesados.csv', index=False)

