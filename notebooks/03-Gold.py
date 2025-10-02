from pyspark.sql.functions import when, col, udf
from pyspark.sql.types import StringType
import reverse_geocoder as rg
from datetime import date, timedelta

df = spark.read.parquet(silver_data).filter(col('time') > start_date)

def get_country_code(lat, lon):
try:
        coordinates = (float(lat), float(lon))
        result = rg.search(coordinates)[0].get('cc')
        print(f"Processed coordinates: {coordinates} -> {result}")
        return result
    except Exception as e:
        print(f"Error processing coordinates: {lat}, {lon} -> {str(e)}")
        return None


    get_country_code_udf = udf(get_country_code, StringType())
    df_with_location = \
                df.\
                    withColumn("country_code", get_country_code_udf(col("latitude"), col("longitude")))
     df_with_location_sig_class = \
                            df_with_location.\
                                withColumn('sig_class', 
                                            when(col("sig") < 100, "Low").\
                                            when((col("sig") >= 100) & (col("sig") < 500), "Moderate").\
                                            otherwise("High")
                    gold_output_path = f"{gold_adls}earthquake_events_gold/"
df_with_location_sig_class.write.mode('append').parquet(gold_output_path))