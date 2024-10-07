# store_transformer.py

from transformer import Transformer
from awsglue.dynamicframe import DynamicFrame
from data_quality_utils import DataQualityUtils
from pyspark.sql.types import TimestampType

class StoreTransformer(Transformer):
    """
    Transformador específico para la hoja 'store'.
    """
    def transform(self, dynamic_frame: DynamicFrame):
        df = dynamic_frame.toDF()
        
        df = DataQualityUtils.trim_column_names(df)
        
        df = DataQualityUtils.trim_string_columns(df)
        
        df = DataQualityUtils.replace_null_string_with_spark_null(df)
        
        df = DataQualityUtils.remove_columns_with_all_nulls(df)
        
        df_last_update_error = None
        
        if 'last_update' in df.columns:
            df, df_last_update_error = DataQualityUtils.validate_and_cast(
                df,
                'last_update',
                TimestampType(),
                'Error en last_update: no es Timestamp válido'
            )
        else:
            print("Advertencia: La columna 'last_update' no existe en 'store'.")
        
        dynamic_frame_valid = DynamicFrame.fromDF(df, dynamic_frame.glue_ctx, dynamic_frame.name)

        df_dataquality_error = DataQualityUtils.combine_dataframes([df_last_update_error])
        
        dynamic_frame_dataquality_error = None
        if df_dataquality_error is not None:
            dynamic_frame_dataquality_error = DynamicFrame.fromDF(df_dataquality_error, dynamic_frame.glue_ctx, "df_dataquality_error")
        else:
            print("No se encontraron errores de calidad de datos.")
        
        return dynamic_frame_valid, dynamic_frame_dataquality_error
