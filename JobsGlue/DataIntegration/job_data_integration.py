import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from films_database_loader import FilmsDatabaseLoader
from film_transformer import FilmTransformer
from inventory_transformer import InventoryTransformer
from rental_transformer import RentalTransformer
from customer_transformer import CustomerTransformer
from store_transformer import StoreTransformer

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    excel_file_path = "s3://flypass-test/raw/films_database/Films.xlsx"
    
    sheet_names = ["film", "inventory", "rental", "customer", "store"]
    
    transformers_map = {
        "film": FilmTransformer(),
        "inventory": InventoryTransformer(),
        "rental": RentalTransformer(),
        "customer": CustomerTransformer(),
        "store": StoreTransformer()
    }
    
    partition_fields = {
        'rental': ['rental_date']
    }
    
    s3_output_path = "s3://flypass-test/processed"
    
    db_loader = FilmsDatabaseLoader(
        glue_context=glue_context,
        excel_path=excel_file_path,
        sheet_names=sheet_names,
        transformers_map=transformers_map,
        s3_output_path=s3_output_path,
        glue_database="films_database",
        partition_fields=partition_fields
    )

    db_loader.run()
    
    job.commit()

if __name__ == "__main__":
    main()
