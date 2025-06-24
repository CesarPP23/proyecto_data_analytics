import pandas as pd
from sqlalchemy import create_engine
from transform import transform_all

def create_connection(server='localhost\\SQLEXPRESS', database='NetflixDB', username=None, password=None):
    """
    Crea conexión a SQL Server usando SQLAlchemy.
    
    Args:
        server: Servidor SQL Server (por defecto localhost\SQLEXPRESS)
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
    
    print("💾 Cargando datos a SQL Server...")
    for table_name in insert_order:
        if table_name in tables_dict:
            df = tables_dict[table_name]
            try:
                df.to_sql(table_name, engine, if_exists='append', index=False)
                print(f"   ✅ {table_name}: {len(df)} registros insertados")
            except Exception as e:
                print(f"   ❌ Error insertando {table_name}: {str(e)}")
                # Continúa con las siguientes tablas aunque una falle
                continue

def test_connection(server='localhost\\SQLEXPRESS', database='NetflixDB', username=None, password=None):
    """
    Prueba la conexión a SQL Server antes de cargar datos.
    
    Returns:
        True si la conexión es exitosa, False en caso contrario
    """
    try:
        engine = create_connection(server, database, username, password)
        with engine.connect() as conn:
            result = conn.execute("SELECT 1")
            return True
    except Exception as e:
        print(f"❌ Error de conexión: {str(e)}")
        return False

def main(server='localhost\\SQLEXPRESS', database='NetflixDB', username=None, password=None):
    """
    Función principal que transforma y carga los datos.
    
    Args:
        server: Servidor SQL Server
        database: Nombre de la base de datos
        username: Usuario SQL Server (opcional)
        password: Contraseña SQL Server (opcional)
    """
    print("🚀 Iniciando proceso ETL completo...")
    
    try:
        # 1. Probar conexión primero
        print(f"🔗 Probando conexión a {server}/{database}...")
        if not test_connection(server, database, username, password):
            print("❌ No se pudo establecer conexión. Verifica:")
            print("   - Que SQL Server esté ejecutándose")
            print("   - Que la base de datos 'NetflixDB' exista")
            print("   - Que el driver ODBC esté instalado")
            return False
        print("✅ Conexión exitosa")
        
        # 2. Transformar datos usando el módulo transform
        print("\n📊 Transformando datos...")
        tables = transform_all()
        
        # Mostrar resumen de transformación
        print("\n📋 Resumen de transformación:")
        total_records = 0
        for table_name, df in tables.items():
            print(f"   - {table_name}: {len(df)} registros")
            total_records += len(df)
        print(f"   📊 Total de registros a insertar: {total_records:,}")
        
        # 3. Crear conexión a SQL Server
        print(f"\n🔗 Conectando a SQL Server para carga de datos...")
        engine = create_connection(server, database, username, password)
        
        # 4. Cargar datos a SQL Server
        load_tables_to_sql(tables, engine)
        
        print("\n🎉 Proceso ETL completado exitosamente!")
        print("✅ Todos los datos han sido cargados a la base de datos NetflixDB")
        return True
        
    except Exception as e:
        print(f"\n❌ Error en el proceso ETL: {str(e)}")
        print("💡 Sugerencias:")
        print("   - Verifica que la base de datos NetflixDB exista")
        print("   - Asegúrate de que las tablas estén creadas")
        print("   - Revisa que los datos de origen estén disponibles")
        return False

if __name__ == "__main__":
    # Opción 1: Ejecutar con autenticación Windows (por defecto)
    success = main()
     
    # Opción 2: Ejecutar con credenciales específicas (descomenta si es necesario)
    # success = main(
    #     server='localhost\\SQLEXPRESS',
    #     database='NetflixDB',
    #     username='tu_usuario',
    #     password='tu_contraseña'
    # )
    
    # Opción 3: Ejecutar con servidor remoto (descomenta si es necesario)
    # success = main(
    #     server='192.168.1.100\\SQLEXPRESS',  # IP del servidor remoto
    #     database='NetflixDB'
    # )
    
    if success:
        print("\n🎯 ¡Listo! Puedes verificar los datos en SQL Server Management Studio")
    else:
        print("\n⚠️  El proceso falló. Revisa los errores anteriores.")