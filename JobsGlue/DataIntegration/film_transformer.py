from transformer import Transformer
from awsglue.dynamicframe import DynamicFrame
from data_quality_utils import DataQualityUtils
from pyspark.sql.types import TimestampType, IntegerType, DoubleType

class FilmTransformer(Transformer):
    def transform(self, dynamic_frame: DynamicFrame):
        df = dynamic_frame.toDF()
        
        df = DataQualityUtils.trim_column_names(df)
        
        df = DataQualityUtils.trim_string_columns(df)
        
        df = DataQualityUtils.replace_null_string_with_spark_null(df)
        
        df = DataQualityUtils.remove_columns_with_all_nulls(df)
        
        df_last_update_error = None 
        df_release_year_error = None
        df_rental_rate_error = None
        df_replacement_cost_error = None
        
        if 'last_update' in df.columns:
            df, df_last_update_error = DataQualityUtils.validate_and_cast(
                df,
                'last_update',
                TimestampType(),
                'Error en last_update: no es Timestamp válido'
            )
        else:
            print("Advertencia: La columna 'last_update' no existe en 'film'.")
        
        if 'release_year' in df.columns:
            df, df_release_year_error = DataQualityUtils.validate_and_cast(
                df,
                'release_year',
                IntegerType(),
                'Error en release_year: no es entero válido'
            )
        else:
            print("Advertencia: La columna 'release_year' no existe en 'film'.")
            
        if 'rental_rate' in df.columns:
            df, df_rental_rate_error = DataQualityUtils.validate_and_cast(
                df,
                'rental_rate',
                DoubleType(),
                'Error en rental_rate: no es Double válido'
            )
        else:
            print("Advertencia: La columna 'rental_rate' no existe en 'film'.")
            
        if 'replacement_cost' in df.columns:
            df, df_replacement_cost_error = DataQualityUtils.validate_and_cast(
                df,
                'replacement_cost',
                DoubleType(),
                'Error en replacement_cost: no es Double válido'
            )
        else:
            print("Advertencia: La columna 'replacement_cost' no existe en 'film'.")
    
        dynamic_frame_valid = DynamicFrame.fromDF(df, dynamic_frame.glue_ctx, dynamic_frame.name)

        df_dataquality_error = DataQualityUtils.combine_dataframes([df_last_update_error, df_release_year_error, df_rental_rate_error, df_replacement_cost_error])
        
        dynamic_frame_dataquality_error = None
        if df_dataquality_error is not None:
            dynamic_frame_dataquality_error = DynamicFrame.fromDF(df_dataquality_error, dynamic_frame.glue_ctx, "df_dataquality_error")
        else:
            print("No se encontraron errores de calidad de datos.")
        
        return dynamic_frame_valid, dynamic_frame_dataquality_error
