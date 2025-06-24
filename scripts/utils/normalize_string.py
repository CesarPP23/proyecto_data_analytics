import re
import pandas as pd
# Cargar funciones predefinidas para limpiar datos tipo string
def normalize_string_values(s):
    if pd.isna(s) or str(s).lower() == 'nan':
        return pd.NA
    # Elimina espacios al inicio/final y múltiples espacios
    s = str(s).strip()
    s = re.sub(r'\s+', ' ', s)
    # Elimina espacios antes y después de comas
    s = re.sub(r'\s*,\s*', ', ', s)
    # Si queda vacío, lo convierte a NA
    if s == '':
        return pd.NA
    return s