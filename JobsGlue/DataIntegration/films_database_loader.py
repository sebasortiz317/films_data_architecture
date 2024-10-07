from typing import List, Dict
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame
from excel_sheet_loader import ExcelSheetLoader
from data_quality_utils import DataQualityUtils
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp, col, trim, lit, when

class FilmsDatabaseLoader:
    """
    Clase para manejar la carga y transformación de todas las hojas de la base de datos de películas.
    """
    def __init__(self, glue_context: GlueContext, excel_path: str, sheet_names: List[str], transformers_map: Dict[str, 'Transformer'], s3_output_path: str, glue_database: str, partition_fields: Dict[str, List[str]] = None):
        """
        :param glue_context: Contexto de Glue
        :param excel_path: Ruta del archivo Excel en S3
        :param sheet_names: Lista con los nombres de las hojas a cargar
        :param transformers_map: Diccionario que mapea cada hoja a su transformador específico
        :param s3_output_path: Ruta base en S3 para almacenar los resultados
        :param glue_database: Nombre de la base de datos en Glue
        :param partition_fields: Diccionario de campos de partición por hoja
        """
        self.glue_context = glue_context
        self.excel_path = excel_path
        self.sheet_names = sheet_names
        self.transformers_map = transformers_map
        self.loaders = {sheet: ExcelSheetLoader(glue_context, excel_path, sheet) for sheet in sheet_names}
        self.s3_output_path = s3_output_path
        self.partition_fields = partition_fields or {}
        self.glue_database = glue_database

    def convert_timestamp_to_date(self, df: DataFrame, partition_fields: List[str]):
        """
        Convierte campos de tipo Timestamp a formato Date (yyyy-MM-dd) en una nueva columna con sufijo _partition si están en la lista de particiones.

        :param df: DataFrame a procesar
        :param partition_fields: Lista de campos de partición
        :return: Tuple (DataFrame actualizado, Lista de campos de partición con nuevos nombres)
        """
        updated_partition_fields = []

        for field in partition_fields:
            if field in df.columns and isinstance(df.schema[field].dataType, TimestampType):
                df = df.withColumn(f"{field}_partition", col(field).cast("date"))
                updated_partition_fields.append(f"{field}_partition")
            else:
                updated_partition_fields.append(field)

        return df, updated_partition_fields

    def save_to_s3_and_catalog(self, df: DataFrame, output_path: str, table_name: str, partition_fields: List[str], database: str):
        """
        Guarda el DataFrame en S3 en formato Parquet y registra la metadata en el catálogo de Glue.
        
        :param df: DataFrame a guardar
        :param output_path: Ruta en S3 donde se guardará el archivo Parquet
        :param table_name: Nombre de la tabla a registrar en el catálogo de Glue
        :param partition_fields: Lista de campos de partición (puede estar vacía si no hay particiones)
        :param database: Nombre de la base de datos en Glue
        """

        print(f"Registrando metadata en el catálogo de Glue para la tabla {table_name}")
        
        sink = self.glue_context.getSink(
            connection_type="s3", 
            path=output_path, 
            enableUpdateCatalog=True, 
            partitionKeys=partition_fields if partition_fields else []
        )
        sink.setCatalogInfo(catalogDatabase=database, catalogTableName=table_name)
        sink.setFormat("glueparquet")
        sink.writeFrame(DynamicFrame.fromDF(df, self.glue_context, table_name))
        
    def run(self):
        """
        Ejecuta la carga y transformación de todas las hojas.
        """
        for sheet_name in self.sheet_names:
            try:
                print(f"Cargando la hoja: {sheet_name}")
                loader = self.loaders[sheet_name]
                dynamic_frame = loader.load_sheet()
                
                transformer = self.transformers_map.get(sheet_name)
                
                df_dataquality_error = None
                
                if transformer:
                    print(f"Aplicando transformaciones para la hoja: {sheet_name}")
                    dynamic_frame, df_dataquality_error = transformer.transform(dynamic_frame)
                else:
                    print(f"No hay transformador definido para la hoja: {sheet_name}")
                
                df_valid = dynamic_frame.toDF()

                partition_fields = self.partition_fields.get(sheet_name, [])

                if partition_fields:
                    df_valid, updated_partition_fields = self.convert_timestamp_to_date(df_valid, partition_fields)
                    partition_fields = updated_partition_fields
                
                valid_output_path = f"{self.s3_output_path}/{sheet_name}/"
                self.save_to_s3_and_catalog(df_valid, valid_output_path, sheet_name, partition_fields, self.glue_database)
                
                if df_dataquality_error is not None:
                    df_error = df_dataquality_error.toDF()
                    error_output_path = f"{self.s3_output_path}/{sheet_name}_errors/" 
                    self.save_to_s3_and_catalog(df_error, error_output_path, f"{sheet_name}_errors", [], self.glue_database)
                else:
                    print(f"No se encontraron errores de calidad de datos para la hoja {sheet_name}")
            
            except Exception as e:
                print(f"Error al cargar o transformar la hoja {sheet_name}: {e}")
