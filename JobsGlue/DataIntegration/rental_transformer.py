# rental_transformer.py

from transformer import Transformer
from awsglue.dynamicframe import DynamicFrame
from data_quality_utils import DataQualityUtils
from pyspark.sql.types import TimestampType

class RentalTransformer(Transformer):
    """
    Transformador específico para la hoja 'rental'.
    """
    def transform(self, dynamic_frame: DynamicFrame):
        df = dynamic_frame.toDF()

        df = DataQualityUtils.trim_column_names(df)

        df = DataQualityUtils.trim_string_columns(df)
        
        df = DataQualityUtils.replace_null_string_with_spark_null(df)
        
        df = DataQualityUtils.remove_columns_with_all_nulls(df)
        
        df_last_update_error = None
        df_rental_date_error = None
        df_return_date_error = None
        
        if 'last_update' in df.columns:
            df, df_last_update_error = DataQualityUtils.validate_and_cast(
                df,
                'last_update',
                TimestampType(),
                'Error en last_update: no es Timestamp válido'
            )
        else:
            print("Advertencia: La columna 'last_update' no existe en 'rental'.")
            
        if 'rental_date' in df.columns:
            df, df_rental_date_error = DataQualityUtils.validate_and_cast(
                df,
                'rental_date',
                TimestampType(),
                'Error en rental_date: no es Timestamp válido'
            )
        else:
            print("Advertencia: La columna 'rental_date' no existe en 'rental'.")
            
        if 'return_date' in df.columns:
            df, df_return_date_error = DataQualityUtils.validate_and_cast(
                df,
                'return_date',
                TimestampType(),
                'Error en return_date: no es Timestamp válido'
            )
        else:
            print("Advertencia: La columna 'return_date' no existe en 'rental'.")
        
        dynamic_frame_valid = DynamicFrame.fromDF(df, dynamic_frame.glue_ctx, dynamic_frame.name)

        df_dataquality_error = DataQualityUtils.combine_dataframes([df_last_update_error, df_rental_date_error, df_return_date_error])
        
        dynamic_frame_dataquality_error = None
        if df_dataquality_error is not None:
            dynamic_frame_dataquality_error = DynamicFrame.fromDF(df_dataquality_error, dynamic_frame.glue_ctx, "df_dataquality_error")
        else:
            print("No se encontraron errores de calidad de datos.")
        
        return dynamic_frame_valid, dynamic_frame_dataquality_error
