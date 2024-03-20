import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import os

# Credenciales API
#client_id = 'eeea2fd521514498a37629a810012185'
#client_secret = '14C6598f35E2498185685Ccfc6b2b372'
client_id = os.environ['CLIENT_ID']
client_secret = os.environ['CLIENT_SECRET']
# URLs de los endpoints
endpoint_1 = f'https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationInformation?client_id={client_id}&client_secret={client_secret}'
endpoint_2 = f"https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationStatus?client_id={client_id}&client_secret={client_secret}"

##########################################################################################################


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


#############################################################################################

def conectar_snowflake():
    """Función para conectar a Snowflake"""
    conn = snowflake.connector.connect(
        user='leo',
        password='Leomar161679',
        account='hw55128.us-east-2.aws',
        warehouse='COMPUTE_WH',
        database='BICI',
        schema='PUBLIC'
    )
    return conn

############################################################################


def crear_tabla_estacion(conn):
    """Crea la tabla 'estacion' si no existe"""
    cur = conn.cursor()
    try:
        # Primero, intenta eliminar la tabla si existe
        cur.execute("DROP TABLE IF EXISTS estacion")
        # Luego, crea la nueva tabla
        cur.execute("""
                    CREATE TABLE estacion (
                        station_id INT,
                        name VARCHAR(200),
                        lat FLOAT,
                        lon FLOAT,
                        address VARCHAR(200),
                        post_code VARCHAR(10),
                        capacity INT,
                        rental_methods VARCHAR(100),
                        groups VARCHAR(100),
                        nearby_distance FLOAT,
                        created_at TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP()
                    )
                    """)
        # Confirma los cambios
        conn.commit()
        print("Tabla 'estacion' creada exitosamente.")
    except Exception as e:
        print("Error al crear la tabla:", e)
    finally:
        cur.close()


##################################################################################################

def crear_tabla_num_bici(conn):
    """Crea la tabla 'num_bici' si no existe"""
    cur = conn.cursor()
    try:
        # Primero, intenta eliminar la tabla si existe
        cur.execute("DROP TABLE IF EXISTS num_bici")
        # Luego, crea la nueva tabla
        cur.execute("""
                    CREATE TABLE num_bici (
                        station_id INT,
                        num_bikes_available INT,
                        num_bikes_disabled INT,
                        num_docks_available INT,
                        num_docks_disabled INT,
                        last_reported TIMESTAMP,
                        status VARCHAR(50),
                        is_installed INT,
                        is_renting INT,
                        is_returning INT,
                        created_at TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP()
                    )
                    """)
        # Confirma los cambios
        conn.commit()
        print("Tabla 'num_bici' creada exitosamente.")
    except Exception as e:
        print("Error al crear la tabla:", e)
    finally:
        cur.close()

###################################################################################################


def cargar_datos_db_estacion(conn, df):
    """Carga datos desde un DataFrame de pandas a la tabla 'estacion' en Snowflake"""
    write_pandas(conn, df, 'ESTACION')
    print("DataFrame 'estacion' cargado en Snowflake.")


def cargar_datos_db_num_bici(conn, df):
    """Carga datos desde un DataFrame de pandas a la tabla 'num_bici' en Snowflake"""
    write_pandas(conn, df, 'NUM_BICI')
    print("DataFrame  'num_bici' cargado en Snowflake.")


###############################################################################################

def main():
    conn = conectar_snowflake()

    df_1 = get_and_clean_df(endpoint_1)
    df_1.columns = df_1.columns.str.upper()

    df_2 = get_and_clean_df(endpoint_2)
    df_2.columns = df_2.columns.str.upper()

    crear_tabla_estacion(conn)
    crear_tabla_num_bici(conn)

    cargar_datos_db_estacion(conn, df_1)
    cargar_datos_db_num_bici(conn, df_2)

    # Cerrar la conexión
    conn.close()


if __name__ == "__main__":
    main()
