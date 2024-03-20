import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import os


#############################################################################################

# Creamos la conexion a la base de datos
def conectar_snowflake():
    """Función para conectar a Snowflake"""
    
    conn = snowflake.connector.connect(
        user=os.environ['SNOWSQL_USER'],
        password=os.environ['SNOWSQL_PWD'],
        account=os.environ['SNOWSQL_ACCOUNT'],
        warehouse='COMPUTE_WH',
        database='BICI',
        schema='PUBLIC'
    
    )
    return conn

############################################################################

# CReamos tablas en la base de datos 
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

# Cargamos las tablas con los datos 
def cargar_datos_db_estacion(conn, df):
    """Carga datos desde un DataFrame de pandas a la tabla 'estacion' en Snowflake"""
    write_pandas(conn, df, 'ESTACION')
    print("DataFrame 'estacion' cargado en Snowflake.")


def cargar_datos_db_num_bici(conn, df):
    """Carga datos desde un DataFrame de pandas a la tabla 'num_bici' en Snowflake"""
    write_pandas(conn, df, 'NUM_BICI')
    print("DataFrame  'num_bici' cargado en Snowflake.")


###############################################################################################

# Funcion principal donde hacemos llamado las funciones anteriores
def main():
    conn = conectar_snowflake()
    
    
    current_dir = os.getcwd()

    # Cargar el archivo CSV desde el directorio actual
    file_path_1 = os.path.join(current_dir, 'endpoint_1.csv')
    
    file_path_2 = os.path.join(current_dir, 'endpoint_2.csv')
    
    
    df_1 = pd.read_csv(file_path_1)
    df_1.columns = df_1.columns.str.upper()
    
  

    df_2 = pd.read_csv(file_path_2)
    df_2.columns = df_2.columns.str.upper()

    crear_tabla_estacion(conn)
    crear_tabla_num_bici(conn)

    cargar_datos_db_estacion(conn, df_1)
    cargar_datos_db_num_bici(conn, df_2)

    # Cerrar la conexión
    conn.close()


if __name__ == "__main__":
    main()