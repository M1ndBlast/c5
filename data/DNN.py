"""
latitud,longitud,hora_dia,dia_semana,dia_mes,mes_anio,densidad_poblacional,indice_criminalidad,incidentes_categoria1,incidentes_categoria2,incidentes_categoria3,incidentes_categoria4,incidentes_categoria5,incidentes_categoria6,incidentes_categoria7,incidentes_categoria8,incidentes_categoria9,incidentes_categoria10,iluminacion,condiciones_meteorologicas,punto_interes1,punto_interes2,punto_interes3,tipo_calle,probabilidad_incidente
19.4326,-99.1332,14,3,15,6,15250,10.5,12,5,8,20,15,7,10,6,2,5,2,1,1,0,1,2,0.65
19.3899,-99.1733,8,5,28,11,17800,12.0,8,10,15,18,12,9,11,8,3,6,3,3,0,1,0,1,0.75

"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import ModelCheckpoint

# Leer los datos preprocesados desde el archivo CSV
data = pd.read_csv('datos_preprocesados.csv')

# Aplicar la codificación one-hot a las columnas categóricas
categorical_columns = ['hora_dia', 'dia_semana', 'dia_mes', 'mes_anio', 'iluminacion', 'condiciones_meteorologicas', 'tipo_calle']
data = pd.get_dummies(data, columns=categorical_columns)

# Separar los datos en características (X) y etiquetas (y)
X = data.drop('probabilidad_incidente', axis=1)
y = data['probabilidad_incidente']

# Dividir los datos en conjuntos de entrenamiento, validación y prueba
X_train, X_temp, y_train, y_temp = train_test_split(X, y, test_size=0.2, random_state=42)
X_val, X_test, y_val, y_test = train_test_split(X_temp, y_temp, test_size=0.5, random_state=42)

# Normalizar los datos
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_val = scaler.transform(X_val)
X_test = scaler.transform(X_test)

# Crear el modelo de red neuronal
model = Sequential()
model.add(Dense(64, activation='relu', input_shape=(X_train.shape[1],)))
model.add(Dense(64, activation='relu'))
model.add(Dense(1, activation='sigmoid'))

# Compilar el modelo
optimizer = Adam(learning_rate=0.001)
model.compile(optimizer=optimizer, loss='binary_crossentropy', metrics=['accuracy'])

# Crear el callback para guardar el modelo en cada mejora
checkpoint_filepath = 'mejor_modelo.h5'
checkpoint_callback = ModelCheckpoint(
    filepath=checkpoint_filepath,
    monitor='val_loss',
    mode='min',
    save_best_only=True,
    save_weights_only=False,
    verbose=1
)

# Entrenar el modelo
model.fit(
    X_train, y_train,
    validation_data=(X_val, y_val),
    epochs=50,
    batch_size=32,
    callbacks=[checkpoint_callback]
)

# Cargar el mejor modelo guardado
model.load_weights(checkpoint_filepath)

# Evaluar el modelo en el conjunto de prueba
test_loss, test_accuracy = model.evaluate(X_test, y_test)
print(f'Test loss: {test_loss}, Test accuracy: {test_accuracy}')

# Aplicar el modelo a nuevos datos
new_data = np.array([[..., ...], [..., ...]])  # Reemplazar con datos reales
new_data_normalized = scaler.transform(new_data)
predictions = model.predict(new_data_normalized)

# Imprimir las predicciones
print(f'Probabilidades de incidentes para los nuevos datos: {predictions}')
