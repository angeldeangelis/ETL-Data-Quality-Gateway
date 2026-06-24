import logging
from pathlib import Path
import pandas as pd
# Importamos el módulo que construiste y blindaste en el Bloque 1
from file_discovery import get_valid_csv_files

# Configuración del sistema de monitoreo corporativo
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DataIngestionCore")

def process_and_clean_payloads(csv_paths: list[Path]) -> pd.DataFrame:
    """
    Toma una lista de rutas de archivos CSV, los lee, aplica reglas estrictas
    de limpieza de datos y los unifica en un solo DataFrame estructurado.
    """
    cleaned_dataframes = []
    
    # Columnas que el contrato de negocio exige que existan en el CSV
    REQUIRED_COLUMNS = ['transaction_id', 'product_name', 'revenue']
    
    for file_path in csv_paths:
        try:
            logger.info(f"Iniciando extracción de Pandas para: {file_path.name}")
            
            # Cargamos el archivo en memoria
            df = pd.read_csv(file_path)
            
            # AUDITORÍA DE COLUMNAS: Validamos que el archivo venga con la estructura correcta
            if not all(col in df.columns for col in REQUIRED_COLUMNS):
                logger.error(f"ARCHIVO RECHAZADO: Estructura de columnas inválida en {file_path.name}")
                continue # Saltamos este archivo corrupto y seguimos con el siguiente
                
            # CONTROL DE CALIDAD 1: Tratamiento de Vacíos
            # Eliminamos filas donde la columna 'revenue' o 'transaction_id' sea nula (NaN).
            initial_row_count = len(df)
            df = df.dropna(subset=['transaction_id', 'revenue'])
            dropped_rows_nan = initial_row_count - len(df)
            
            if dropped_rows_nan > 0:
                logger.warning(f"Filas con valores nulos eliminadas en {file_path.name}: {dropped_rows_nan}")
                
            # CONTROL DE CALIDAD 2: Deduplicación por Clave de Negocio
            # Si un 'transaction_id' se repite, mantenemos la primera aparición ('first') y borramos el clon.
            row_count_before_dedup = len(df)
            df = df.drop_duplicates(subset=['transaction_id'], keep='first')
            dropped_rows_dup = row_count_before_dedup - len(df)
            
            if dropped_rows_dup > 0:
                logger.warning(f"Transacciones duplicadas eliminadas en {file_path.name}: {dropped_rows_dup}")
                
            # Si el DataFrame sobrevivió a la purga con datos válidos, lo guardamos en la cola
            if not df.empty:
                cleaned_dataframes.append(df)
                
        except Exception as e:
            # Si el archivo está dañado a nivel de bytes o encriptado, Pandas fallará. 
            # Capturamos el error aquí para que el pipeline completo NO colapse.
            logger.error(f"Falla crítica procesando el archivo {file_path.name}: {str(e)}")
            
    # CONSOLIDACIÓN FINAL
    if cleaned_dataframes:
        # Combinamos todos los DataFrames de la lista en una sola matriz unificada
        # ignore_index=True reconstruye el índice de filas de 0 a N de forma ordenada
        master_df = pd.concat(cleaned_dataframes, ignore_index=True)
        return master_df
    else:
        logger.warning("No se consolidaron datos. Todos los DataFrames estaban vacíos o corruptos.")
        return pd.DataFrame() # Retornamos un DataFrame vacío estructurado para evitar errores aguas abajo

if __name__ == "__main__":
    # Determinamos la raíz dinámica del entorno actual
    BASE_DIR = Path(__file__).resolve().parent
    SOURCE_FOLDER = BASE_DIR / "raw_data"
    OUTPUT_FOLDER = BASE_DIR / "processed_data"
    
    # 1. Activamos el Radar del Bloque 1
    discovered_payloads = get_valid_csv_files(SOURCE_FOLDER)
    
    if discovered_payloads:
        # 2. Ejecutamos el Motor de Limpieza de Pandas
        final_master_data = process_and_clean_payloads(discovered_payloads)
        
        if not final_master_data.empty:
            # 3. Almacenamiento Seguro (Capa de Carga / Load)
            OUTPUT_FOLDER.mkdir(exist_ok=True)
            output_path = OUTPUT_FOLDER / "cleaned_master.csv"
            
            # index=False evita que Pandas guarde la columna innecesaria de índices numéricos
            final_master_data.to_csv(output_path, index=False)
            logger.info(f"=== ETL COMPLETO: Datos unificados de alta calidad guardados en -> {output_path} ===")
    else:
        logger.error("Proceso ETL abortado: El radar de descubrimiento no encontró archivos fuente válidos.")