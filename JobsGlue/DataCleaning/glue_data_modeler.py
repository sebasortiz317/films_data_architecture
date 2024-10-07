# glue_data_modeler.py

from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

class GlueDataModeler:
    def __init__(self, glue_context: GlueContext, database_name: str, s3_output_path: str):
        """
        Inicializa el modelador de datos con el contexto de Glue, el nombre de la base de datos y el S3 de salida.
        
        :param glue_context: El contexto de Glue
        :param database_name: El nombre de la base de datos de Glue en el catálogo
        :param s3_output_path: Ruta base en S3 para almacenar los resultados
        """
        self.glue_context = glue_context
        self.database_name = database_name
        self.s3_output_path = s3_output_path
        self.spark = glue_context.spark_session

    def load_table(self, table_name: str) -> DataFrame:
        """
        Carga una tabla desde el catálogo de Glue y la registra como una vista temporal para SQL.
        
        :param table_name: Nombre de la tabla a cargar
        :return: DataFrame de la tabla cargada
        """
        print(f"Cargando la tabla {table_name} desde Glue Catalog.")
        df = self.glue_context.create_dynamic_frame.from_catalog(database=self.database_name, table_name=table_name).toDF()
        df.createOrReplaceTempView(table_name)
        return df

    def run_query(self, query: str) -> DataFrame:
        """
        Ejecuta una consulta SQL en Spark y devuelve un DataFrame con los resultados.
        
        :param query: Consulta SQL a ejecutar
        :return: DataFrame con los resultados de la consulta
        """
        print(f"Ejecutando consulta SQL:\n{query}")
        return self.spark.sql(query)

    def save_result(self, df: DataFrame, model_name: str, partition_keys: list = None):
        """
        Guarda el resultado de un DataFrame en formato Parquet en S3 y registra la tabla en Glue Catalog.
        
        :param df: DataFrame con los resultados de la consulta
        :param model_name: Nombre del modelo para definir la ruta de salida
        :param partition_keys: Lista de llaves de partición, si es aplicable
        """
        output_path = f"{self.s3_output_path}/{model_name}/"
        print(f"Guardando los resultados en {output_path}")

        # Convertir DataFrame a DynamicFrame para registrar en Glue Catalog
        dynamic_frame = DynamicFrame.fromDF(df, self.glue_context, model_name)
        
        # Registrar la tabla en Glue Catalog
        print(f"Registrando metadata en Glue Catalog para {model_name}")
        sink = self.glue_context.getSink(
            connection_type="s3",
            path=output_path,
            enableUpdateCatalog=True,
            partitionKeys=partition_keys if partition_keys else []
        )
        sink.setCatalogInfo(
            catalogDatabase=self.database_name,
            catalogTableName=model_name
        )
        sink.setFormat("glueparquet")
        sink.writeFrame(dynamic_frame)

    def create_inventory_model(self):
        """
        Crea y guarda el modelo 'inventory_model'.
        """
        inventory_model_query = """
        SELECT
            i.inventory_id,
            i.store_id,
            i.film_id,
            f.title,
            f.release_year,
            f.rental_duration,
            f.rental_rate,
            f.length AS duration_minutes,
            f.replacement_cost,
            f.rating,
            f.special_features
        FROM
            inventory i
        LEFT JOIN store s
            ON i.store_id = s.store_id
        LEFT JOIN film f
            ON i.film_id = f.film_id
        """
        df = self.run_query(inventory_model_query)
        self.save_result(df, "inventory_model")

    def create_rental_model(self):
        """
        Crea y guarda el modelo 'rental_model'.
        """
        rental_model_query = """
        SELECT
            r.rental_id,
            r.inventory_id,
            r.customer_id,
            r.staff_id,
            i.store_id,
            i.film_id,
            r.rental_date,
            r.return_date,
            CONCAT(c.first_name, ' ', c.last_name) AS full_name,
            c.email,
            c.active,
            c.create_date,
            f.title,
            f.release_year,
            f.rental_duration,
            f.rental_rate,
            f.length AS duration_minutes,
            f.replacement_cost,
            f.rating,
            f.special_features
        FROM
            rental r
        LEFT JOIN customer c
            ON c.customer_id = r.customer_id
        LEFT JOIN inventory i
            ON i.inventory_id = r.inventory_id
        LEFT JOIN film f
            ON f.film_id = i.film_id
        """
        df = self.run_query(rental_model_query)
        self.save_result(df, "rental_model")
