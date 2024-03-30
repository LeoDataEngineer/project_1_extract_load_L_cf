import pandas as pd
from datetime import datetime
import os



# Credenciales API guardadas en github en sus variables de entorno
client_id = os.environ['CLIENT_ID']
client_secret = os.environ['CLIENT_SECRET']
# URLs de los endpoints
endpoint_1 = f'https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationInformation?client_id={client_id}&client_secret={client_secret}'
endpoint_2 = f"https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationStatus?client_id={client_id}&client_secret={client_secret}"

##########################################################################################################

# Obtenemos los datos, lipieza y crear un dataframe
def get_and_clean_df(endpoint):
    json = pd.read_json(endpoint)
    df = pd.DataFrame(json['data'][0])

    if endpoint == endpoint_1:
        columnas_a_eliminar = ['physical_configuration', 'altitude', 'is_charging_station',
                               'obcn', '_ride_code_support', 'rental_uris', 'cross_street']

        # Eliminar las columnas especificadas
        df = df.drop(columns=columnas_a_eliminar)
        df['station_id'] = df['station_id'].astype('int64')
    elif endpoint == endpoint_2:
        columnas_a_eliminar = [
            'num_bikes_available_types', 'is_charging_station', 'traffic']

        # Eliminar las columnas especificadas
        df = df.drop(columns=columnas_a_eliminar)

        df['last_reported'] = pd.to_datetime(df['last_reported'], unit='s')

       # Obtener la fecha actual
        current_date = pd.Timestamp(datetime.now().date())

       # Reemplazar NaT con la fecha actual en 'last_reported'
        df['last_reported'] = df['last_reported'].fillna(current_date)

        df['last_reported'] = df['last_reported'].astype('int64') // 10**9

        df['station_id'] = df['station_id'].astype('int64')
        
    return df


# creamos los dataframe en archivos csv
c= 0
endpoints =[endpoint_1, endpoint_2]
for endpoint in endpoints:
    c += 1
    df= get_and_clean_df(endpoint)
    file_name = f'endpoint_{c}.csv'  
    df.to_csv(file_name, index=False)

#############################################################################################

