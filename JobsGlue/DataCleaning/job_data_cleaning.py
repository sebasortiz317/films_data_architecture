import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from glue_data_modeler import GlueDataModeler

def main():

    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    S3_OUTPUT_PATH = "s3://flypass-test/processed"
    DATABASE_NAME = "films_database"
    
    modeler = GlueDataModeler(glue_context, DATABASE_NAME, S3_OUTPUT_PATH)
    
    modeler.load_table("inventory")
    modeler.load_table("store")
    modeler.load_table("film")
    modeler.load_table("rental")
    modeler.load_table("customer")
    
    modeler.create_inventory_model()
    modeler.create_rental_model()
    
    job.commit()

if __name__ == "__main__":
    main()
