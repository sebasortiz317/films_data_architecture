from pyspark.sql.functions import to_timestamp, col, trim, lit, when
from pyspark.sql.types import *
from pyspark.sql import DataFrame

class DataQualityUtils:
    
    @staticmethod
    def validate_and_cast(df: DataFrame, column_name: str, cast_type, error_message: str):
        """
        Valida y castea una columna a un tipo específico. Si el casteo falla, separa las filas con errores.
        
        :param df: DataFrame original
        :param column_name: Nombre de la columna a validar y castear
        :param cast_type: Tipo de dato al que se debe castear la columna
        :param error_message: Mensaje de error de calidad de datos en caso de fallo
        :return: Un tuple de DataFrames (df_valid, df_error)
        """
        df_valid = df.withColumn(f'{column_name}_clean', df[column_name].cast(cast_type))
        
        df_error = df_valid.filter(col(f'{column_name}_clean').isNull()).withColumn('data_quality_error', lit(error_message))
        
        df_valid = df_valid.filter(col(f'{column_name}_clean').isNotNull())
        
        df_valid = df_valid.drop(f'{column_name}_clean').withColumn(column_name, df[column_name].cast(cast_type))
        df_error = df_error.drop(f'{column_name}_clean')
        
        return df_valid, df_error

    @staticmethod
    def trim_column_names(df: DataFrame) -> DataFrame:
        """
        Aplica trim a los nombres de las columnas del DataFrame.
        
        :param df: DataFrame original
        :return: DataFrame con nombres de columnas sin espacios en los extremos
        """
        for column_name in df.columns:
            new_column_name = column_name.strip()
            df = df.withColumnRenamed(column_name, new_column_name)
        return df
    
    @staticmethod
    def trim_string_columns(df: DataFrame) -> DataFrame:
        """
        Aplica trim a todas las columnas de tipo String en el DataFrame.
        
        :param df: DataFrame original
        :return: DataFrame con todas las columnas de tipo String con espacios removidos
        """
        for column_name in df.columns:
            if isinstance(df.schema[column_name].dataType, StringType):
                df = df.withColumn(column_name, trim(col(column_name)))
        return df
    
    @staticmethod
    def replace_null_string_with_spark_null(df: DataFrame) -> DataFrame:
        """
        Detecta valores 'NULL' (en texto) en columnas de tipo String y los convierte en null de Spark.
        """
        for column_name in df.columns:
            if isinstance(df.schema[column_name].dataType, StringType):
                df = df.withColumn(
                    column_name, 
                    when(col(column_name) == 'NULL', lit(None)).otherwise(col(column_name))
                )
        return df
    
    @staticmethod
    def remove_columns_with_all_nulls(df: DataFrame) -> DataFrame:
        """
        Elimina las columnas que contienen solo valores nulos.
        
        :param df: DataFrame original
        :return: DataFrame sin columnas que contienen solo valores nulos
        """
        for column_name in df.columns:
            non_null_count = df.filter(col(column_name).isNotNull()).count()
            if non_null_count == 0:
                df = df.drop(column_name)
        return df
    
    @staticmethod
    def combine_dataframes(dfs: list) -> DataFrame:
        """
        Combina una lista de DataFrames de Spark que tengan la misma estructura, 
        ignorando aquellos que sean None.

        :param dfs: Lista de DataFrames a combinar.
        :return: Un DataFrame combinado o None si todos los DataFrames son None.
        """
        valid_dfs = [df for df in dfs if df is not None]

        if not valid_dfs:
            return None

        combined_df = valid_dfs[0]
        for df in valid_dfs[1:]:
            combined_df = combined_df.unionByName(df)

        return combined_df
