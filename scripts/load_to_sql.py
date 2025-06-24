import pandas as pd
from sqlalchemy import create_engine
from transform import transform_all

def create_connection(server='localhost', database='NetflixDB', username=None, password=None):
    """
    Crea conexión a SQL Server usando SQLAlchemy.
    
    Args:
        server: Servidor SQL Server
        database: Nombre de la base de datos
        username: Usuario (None para autenticación Windows)
        password: Contraseña (None para autenticación Windows)
    
    Returns:
        Engine de SQLAlchemy
    """
    driver = 'ODBC Driver 17 for SQL Server'
    
    if username and password:
        # Autenticación SQL Server
        connection_string = (
            f"mssql+pyodbc://{username}:{password}@{server}/{database}?"
            f"driver={driver.replace(' ', '+')}"
        )
    else:
        # Autenticación Windows
        connection_string = (
            f"mssql+pyodbc://@{server}/{database}?"
            f"driver={driver.replace(' ', '+')}&trusted_connection=yes"
        )
    
    return create_engine(connection_string)

def load_tables_to_sql(tables_dict, engine):
    """
    Carga los DataFrames transformados a las tablas de SQL Server.
    
    Args:
        tables_dict: Diccionario con los DataFrames transformados
        engine: Engine de SQLAlchemy
    """
    # Orden de inserción: primero entidades, luego relaciones
    insert_order = ['titles', 'directors', 'casts', 'genres', 
                   'title_director', 'title_cast', 'title_genre']
    
    for table_name in insert_order:
        if table_name in tables_dict:
            df = tables_dict[table_name]
            try:
                df.to_sql(table_name, engine, if_exists='append', index=False)
                print(f"{table_name}: {len(df)} registros insertados")
            except Exception as e:
                print(f"Error insertando {table_name}: {str(e)}")

def main(df_clean, server='localhost', database='NetflixDB', username=None, password=None):
    """
    Función principal que transforma y carga los datos.
    
    Args:
        df_clean: DataFrame limpio de Netflix
        server: Servidor SQL Server
        database: Nombre de la base de datos
        username: Usuario SQL Server (opcional)
        password: Contraseña SQL Server (opcional)
    """
    print("Iniciando proceso de transformación y carga...")
    
    # 1. Transformar datos usando el módulo transform
    print("Transformando datos...")
    tables = transform_all(df_clean)
    
    # Mostrar resumen de transformación
    print("\nResumen de transformación:")
    for table_name, df in tables.items():
        print(f"   - {table_name}: {len(df)} registros")
    
    # 2. Crear conexión a SQL Server
    print(f"\nConectando a SQL Server ({server}/{database})...")
    try:
        engine = create_connection(server, database, username, password)
        print("Conexión establecida")
    except Exception as e:
        print(f"Error de conexión: {str(e)}")
        return False
    
    # 3. Cargar datos a SQL Server
    print("\nCargando datos a SQL Server...")
    load_tables_to_sql(tables, engine)
    
    print("\nProceso completado exitosamente!")
    return True

if __name__ == "__main__":
    df_clean = transform_all()
    main(df_clean)