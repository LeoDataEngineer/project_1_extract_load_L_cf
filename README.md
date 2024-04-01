# Nuevo Servicio para la Aplicación Móvil. 
## Para implementar este servicio, se requiere consumir una API que proporciona dos endpoints con la información necesaria. Los datos obtenidos se almacenarán en una instancia de base de datos en la nube para su posterior uso y todo el proceso CI/CD se usara Github Action.

Este proyecto se centra en la extracción, limpieza básica y carga de datos de dos endpoints de API en una base de datos Snowflake.

## Tecnologías y bibliotecas utilizadas

- Python
- Pandas
- Snowflake Connector for Python

## Proceso

### Extracción y limpieza de datos

Los datos se obtienen de dos endpoints de API diferentes:

Se obtiene los datos de la estacion:

`endpoint_1 = f'https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationInformation?client_id={client_id}&client_secret={client_secret}'`

Se obtiene la disponiblidad de bicicletas:

`endpoint_2 = f"https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationStatus?client_id={client_id}&client_secret={client_secret}"`

La función `get_and_clean_df()` se utiliza para extraer y limpiar los datos básicos, y devuelve un dataframe limpio para cada endpoints. Luego, esos dataframe se convierten en archivos csv. Puedes encontrar más detalles en el siguiente enlace:

- [get_and_clean_df](https://github.com/leodataengineer/project_1_extract_load_l_cf/tree/main/project_final_extract.py#L17L47)

### Conexión a Snowflake

La función `conectar_snowflake()` se utiliza para establecer una conexión con la base de datos Snowflake. Más detalles en:

- [conectar_snowflake](https://github.com/leodataengineer/project_1_extract_load_l_cf/tree/main/project_final_load.py#L11L23)

### Creación de tablas en Snowflake

Las funciones `crear_tabla_estacion()` y `crear_tabla_num_bici()` se utilizan para crear las tablas 'ESTACION' y 'NUM_BICI' en Snowflake si aún no existen. Puedes encontrar más detalles en:

- [crear_tabla_estacion](https://github.com/leodataengineer/project_1_extract_load_l_cf/tree/main/project_final_load.py#L28L56)
- [crear_tabla_num_bici](https://github.com/leodataengineer/project_1_extract_load_l_cf/tree/main/project_final_load.py#L60L88)

### Carga de datos en Snowflake

Los datos limpios se cargan en dos tablas de Snowflake, 'ESTACION' y 'NUM_BICI'. Las funciones `cargar_datos_db_estacion()` y `cargar_datos_db_num_bici()` se utilizan para cargar los datos en las respectivas tablas. Más detalles en:

- [cargar_datos_db_estacion](https://github.com/leodataengineer/project_1_extract_load_l_cf/tree/main/project_final_load.py#L93L96)
- [cargar_datos_db_num_bici](https://github.com/leodataengineer/project_1_extract_load_l_cf/tree/main/project_final_load.py#L99L102)

### Función principal

La función `main()` coordina todo el proceso, incluyendo la conexión a Snowflake, la creación de tablas, la carga de datos y el cierre de la conexión. Puedes encontrar más detalles en:

- [main](https://github.com/leodataengineer/project_1_extract_load_l_cf/tree/main/project_final_load.py#L108L135)

### El archivo yaml en Github Action: Crea los pasos para la ejecución de los archivos de forma automatizada
- [Extract_load_workflow](https://github.com/leodataengineer/project_1_extract_load_l_cf/tree/main/.github/workflows/extract_load.yml#L1L77)