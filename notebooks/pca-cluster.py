import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler


# Leemos los datos del archivo \\C4wadpninv004\ANALISIS II-DGGE\02. SME\GUSTAVO\B200m_CONSIGNADAS.csv
DIR_PATH = '\\\\C4wadpninv004\\ANALISIS II-DGGE\\02. SME\\GUSTAVO\\'
FILE_PATH = DIR_PATH + 'B200m_CONSIGNADAS.csv'
data = pd.read_csv(FILE_PATH, sep=',', encoding='latin-1')

# AGRUPACIONES: Los siguientes agrupaciones son para unir las columnas de la base de datos y sumar sus valores
agrupaciones = [['C09-CUARTELES DE LA POLICÍA AUXILIAR','C10-CUARTELES PBI','C11-CUARTELES PGJ','C16-JUZGADOS CIVILES Y PENALES','C20-MINISTERIOR PUBLICOS','C21-MODULOS SSP',],

['C05-CENTRALES CAMIONERAS','C19-METROBUS','C31-TREN LIGERO','C42-TROLEBUS','C43-TURIBUS','C72-ACCESOS METRO','C81-ESTACIONES DE CABLEBUS',],

['C06-CENTROS COMERCIALES','C38-MERCADOS PUBLICOS','C46-ESTABLECIMIENTOS MERCANTILES','C49-TIENDAS DEPARTAMENTALES','C53-CINES','C62-OXXO',],

['C22-MONUMENTOS HISTORICOS', 'C57-EVENTOS MASIVOS', 'C73-ATRACTIVOS TURISTICOS', 'C78-ZONAS ARQUEOLOGICAS',],

['C58-CASAS Y CENTROS DE CULTURA','C59-MUSEOS Y TEATROS','C75-FONOTECAS','C76-FOTOTECA','C77-GALERIAS', 'C26-PARQUES Y RECREACION',],

['C23-NOTARIAS','C24-OFICINAS DE GOBIERNO','C27-RECLUSORIOS','C30-TESORERIAS','C82-ALCALDIAS', 'C01-BANCOS Y CAJEROS'],

['C15-HOSPITALES','C45-CENTROS DE SALUD Y CLINICAS',],

['C44-GUARDERIAS', 'C67-CENTROS PILARES', ],#'C13-EDUCACIÓN',],

['C33-IGLESIAS Y TEMPLOS',],

['C51-EDIFICIOS',],]

# Cada nueva columna es la suma de las columnas que se encuentran en la lista de agrupaciones con nombre GRUPO-k
for i in range(len(agrupaciones)):
	data['GRUPO-'+str(i+1)] = data[agrupaciones[i]].sum(axis=1)
	# Eliminamos las columnas que ya no se van a utilizar
	data = data.drop(agrupaciones[i], axis=1)

# Normalizar los datos
scaler = StandardScaler()
data_normalized = scaler.fit_transform(data)

# Aplicar PCA
pca = PCA(n_components=7)
data_pca = pca.fit_transform(data_normalized)

# Aplicar K-means
k = 3
kmeans = KMeans()#(n_clusters=k)
kmeans.fit(data_pca)

# Asignar etiquetas de cluster y visualizar
labels = kmeans.labels_

plt.scatter(data_pca[:, 0], data_pca[:, 1], c=labels, cmap='viridis')
plt.xlabel('Componente principal 1')
plt.ylabel('Componente principal 2')
plt.title('Clustering utilizando PCA y K-means')
plt.show()
