import pandas as pd
from .normalize_string import normalize_string_values

def cargar_dataset():
    # Cargar el dataset original
    df = pd.read_csv('../../data/netflix_titles.csv')
    return df

def limpiar_dataset(df):
    # Limpieza de duplicados en este caso no es necesario, pero se debe hacer por buenas prácticas de programación
    df = df.drop_duplicates(subset=['show_id'])

    # 7. Estandarización de fechas
    df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')

    # 7. Separación de 'duration' en cantidad y tipo
    df[['duration_int', 'duration_type']] = df['duration'].str.extract(r'(\d+)\s*(\w+)')
    df['duration_int'] = pd.to_numeric(df['duration_int'], errors='coerce')

    # 8. Limpieza de espacios y nulos en texto
    columnas_string = ['show_id','type','title', 'director', 'cast', 'country', 'rating', 'listed_in', 'description']
    for col in columnas_string:
        df[col] = df[col].apply(normalize_string_values)

    return df

def main():
    # 1. Cargar el dataset
    df = cargar_dataset()
    print("Dataset original cargado con éxito")

    # 2. Limpiar el dataset
    df = limpiar_dataset(df)

    # 3. Mostrar las primeras filas del dataframe limpio
    print("Dataset limpio")
    print(df.head())

    # 10. Guardar el dataset limpio
    df.to_csv('../data/netflix_titles_clean.csv', index=False)
    print("\nDataset limpio guardado en '../data/netflix_titles_clean.csv'")
    
if __name__ == "__main__":
    main()
    print("Script de limpieza ejecutado correctamente")