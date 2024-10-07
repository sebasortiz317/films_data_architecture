from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame

class ExcelSheetLoader:
    """
    Clase para cargar una hoja específica de un archivo Excel almacenado en S3.
    """
    def __init__(self, glue_context: GlueContext, excel_path: str, sheet_name: str):
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.excel_path = excel_path
        self.sheet_name = sheet_name

    def load_sheet(self) -> DynamicFrame:
        """
        Carga la hoja especificada y la convierte en un DynamicFrame.
        """
        data_address = f"'{self.sheet_name}'!A1"
        df = self.spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("dataAddress", data_address) \
            .load(self.excel_path)
        
        dynamic_frame = DynamicFrame.fromDF(df, self.glue_context, self.sheet_name)
        return dynamic_frame
