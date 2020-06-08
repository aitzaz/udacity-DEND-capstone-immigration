import configparser
import logging

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Set logging config
logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

# Read config
config = configparser.ConfigParser()
config.read('capstone.cfg')

I94_DATA_FILE_PATH = config['DATA']['I94_APR_DATA_FILE_PATH']
I94_LOCAL_DATA_DIR = config['DATA']['I94_LOCAL_DATA_DIR']
DEMOGRAPHICS_DATA_PATH = config['DATA']['SUPPLEMENTARY_DATASETS_DIR'] + 'us-cities-demographics.csv'
SAS_LABELS_DESCRIPTION_FILE_PATH = config['DATA']['SAS_LABELS_DESCRIPTION_FILE_PATH']
OUTPUT_DATA_DIR = config['DATA']['OUTPUT_DATA_DIR']


def main() -> None:
    """Main function to drive etl process."""

    spark = get_spark_session()

    # Load datasets into spark dataframes
    logger.info("Loading datasets into Spark dataframes")
    immigration_df = get_immigration_data(spark)
    us_demographics_df = get_us_demographics_data(spark)
    countries_df = get_i94_countries(spark)
    ports_df = get_i94_ports(spark)
    states_df = get_i94_states(spark)
    travel_modes_df = get_i94_travel_modes(spark)
    visa_categories_df = get_i94_visas(spark)
    logger.info("All datasets loaded in spark dataframes")

    # clean datasets
    logger.info("Starting data cleaning")
    cleaned_immigration_df = clean_immigration_data(immigration_df)
    cleaned_us_demographics_df = clean_us_demographics_data(us_demographics_df)
    cleaned_ports_df = clean_ports_data(ports_df)
    cleaned_countries_df = clean_countries_data(countries_df)
    cleaned_states_df = clean_states_data(states_df)
    logger.info("Cleaned all datasets")

    # create immigrations fact table
    immigration_fact_table = create_immigration_fact_table(spark, cleaned_immigration_df, cleaned_countries_df,
                                                           cleaned_states_df, cleaned_ports_df, visa_categories_df,
                                                           travel_modes_df)
    logger.info("Immigrations fact table created")

    # create port demographics dimension table
    city_demographics_table = create_city_demographics_dim_table(spark, cleaned_us_demographics_df, cleaned_ports_df)
    logger.info("City demographics dim table created")

    # data quality checks
    if immigration_fact_table.count() == 0:
        Exception("Invalid dataset. Immigrations fact table is empty.")

    if city_demographics_table.count() == 0:
        Exception("Invalid dataset. City demographics dim table is empty.")

    if cleaned_ports_df.count() == 0:
        Exception("Invalid dataset. Port dim table is empty.")

    if cleaned_countries_df.count() == 0:
        Exception("Invalid dataset. Country dim table is empty.")

    if cleaned_states_df.count() == 0:
        Exception("Invalid dataset. US State dim table is empty.")

    if visa_categories_df.count() == 0:
        Exception("Invalid dataset. Visa Category dim table is empty.")

    if travel_modes_df.count() == 0:
        Exception("Invalid dataset. Travel mode dim table is empty.")

    logger.info("Data quality checks passed")

    # Save tables in parquet format
    logger.info("Writing data in parquet format")
    Path(OUTPUT_DATA_DIR).mkdir(parents=True, exist_ok=True)

    immigration_fact_table.write.mode('overwrite').partitionBy('entry_year', 'entry_month', 'port_code').parquet(
        OUTPUT_DATA_DIR + "fact_immigrations.parquet")
    city_demographics_table.write.mode('overwrite').partitionBy('state_code').parquet(
        OUTPUT_DATA_DIR + "dim_city_demographics.parquet")
    cleaned_countries_df.write.mode('overwrite').parquet(OUTPUT_DATA_DIR + "dim_country.parquet")
    cleaned_states_df.write.mode('overwrite').parquet(OUTPUT_DATA_DIR + "dim_us_state.parquet")
    cleaned_ports_df.write.mode('overwrite').parquet(OUTPUT_DATA_DIR + "dim_ports.parquet")
    travel_modes_df.write.mode('overwrite').parquet(OUTPUT_DATA_DIR + "dim_travel_mode.parquet")
    visa_categories_df.write.mode('overwrite').parquet(OUTPUT_DATA_DIR + "dim_visa_category.parquet")

    logger.info("Data saved in output dir in parquet format")
    logger.info("Job completed!")


def get_spark_session() -> SparkSession:
    """Create and return spark session object."""
    spark = SparkSession.builder \
        .appName("Capstone Project - Immigration Dataset") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()

    # # To limit data shuffling for smaller datasets
    # spark_shuffle_partitions: int = int(config['COMMON']['NUM_SPARK_SHUFFLE_PARTITIONS'])
    # spark.conf.set("spark.sql.shuffle.partitions", spark_shuffle_partitions)

    logger.info("Spark session created")
    return spark


def get_data_from_sas_labels_file(label_name: str) -> List[Tuple[str, str]]:
    """
    Utility function to convert sas column label descriptions into tuples.
    :param label_name: Label of the column to transform
    :return: List of Tuple(code, value)
    """
    with open(SAS_LABELS_DESCRIPTION_FILE_PATH) as labels_file:
        file_data = labels_file.read()

    # Remove anything other than label data
    label_data = file_data[file_data.index(label_name):]
    label_data = label_data[:label_data.index(';')]

    lines = label_data.split('\n')
    code_value_pairs = list()
    for line in lines:
        parts = line.split('=')
        if len(parts) != 2:
            # Skip comment or other lines with no codes mapping
            continue
        code = parts[0].strip().strip("'")
        value = parts[1].strip().strip("'")
        code_value_pairs.append((code, value,))

    return code_value_pairs


def get_immigration_data(spark: SparkSession) -> DataFrame:
    """
    Reads immigration data SAS file specified in config and returns in a spark DataFrame.
    In case SAS file doesn't exist, read data from `sas_data` dir in parquet format.
    """
    if Path(I94_DATA_FILE_PATH).exists():
        logger.info(f"Reading SAS7BDAT file form path: {I94_DATA_FILE_PATH}")
        return spark.read.format('com.github.saurfang.sas.spark').load(I94_DATA_FILE_PATH)
    else:
        logger.info(f"Reading PARQUET data form path: {I94_LOCAL_DATA_DIR}")
        return spark.read.parquet(I94_LOCAL_DATA_DIR)


def get_us_demographics_data(spark: SparkSession) -> DataFrame:
    """Read US demographics csv file with explicit schema."""
    schema = StructType([
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("median_age", DoubleType()),
        StructField("male_population", IntegerType()),
        StructField("female_population", IntegerType()),
        StructField("total_population", IntegerType()),
        StructField("number_of_veterans", IntegerType()),
        StructField("foreign_born", IntegerType()),
        StructField("average_household_size", DoubleType()),
        StructField("state_code", StringType()),
        StructField("race", StringType()),
        StructField("count", IntegerType())
    ])

    return spark.read.csv(DEMOGRAPHICS_DATA_PATH, sep=';', header=True, schema=schema)


def get_i94_countries(spark: SparkSession) -> DataFrame:
    """Maps country code to country names and returns as a dataframe."""
    country_code_to_name_pairs = get_data_from_sas_labels_file('I94RES')
    schema = StructType([
        StructField("country_code", StringType()),
        StructField("country_name", StringType())
    ])

    return spark.createDataFrame(
        data=country_code_to_name_pairs,
        schema=schema
    )


def get_i94_ports(spark: SparkSession) -> DataFrame:
    """Translates port codes in SAS Labels Description file into a dataframe."""
    port_code_to_name_pairs = get_data_from_sas_labels_file('I94PORT')
    schema = StructType([
        StructField("port_code", StringType()),
        StructField("port_name", StringType())
    ])

    return spark.createDataFrame(
        data=port_code_to_name_pairs,
        schema=schema
    )


def get_i94_states(spark: SparkSession) -> DataFrame:
    """Maps state codes to state names and returns as a dataframe."""
    state_code_to_name_pairs = get_data_from_sas_labels_file('I94ADDR')
    schema = StructType([
        StructField("state_code", StringType()),
        StructField("state_name", StringType())
    ])

    return spark.createDataFrame(
        data=state_code_to_name_pairs,
        schema=schema
    )


def get_i94_travel_modes(spark: SparkSession) -> DataFrame:
    """Maps travel mode ids to respective names and returns as a dataframe."""
    mode_code_to_name_pairs = get_data_from_sas_labels_file('I94MODE')
    schema = StructType([
        StructField("mode_id", StringType()),
        StructField("mode_name", StringType())
    ])

    return spark.createDataFrame(
        data=mode_code_to_name_pairs,
        schema=schema
    )


def get_i94_visas(spark: SparkSession) -> DataFrame:
    """Maps visa category codes to category name as returns as a dataframe."""
    visa_category_code_to_name_pairs = get_data_from_sas_labels_file('I94VISA')
    schema = StructType([
        StructField("visa_category_id", StringType()),
        StructField("visa_category", StringType())
    ])

    return spark.createDataFrame(
        data=visa_category_code_to_name_pairs,
        schema=schema
    )


def clean_immigration_data(immigration_df: DataFrame) -> DataFrame:
    """
    Transform arrival date, departure date to regular dates and clean birth year column to ignore invalid values.
    """
    # Convert SAS dates to Python dates as it counts days from 1-1-1960
    get_isoformat_date = udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(x)).isoformat() if x else None)
    get_valid_birth_year = udf(lambda yr: yr if (yr and 1900 <= yr <= 2016) else None)

    return immigration_df \
        .withColumn('arrdate', get_isoformat_date(immigration_df.arrdate)) \
        .withColumn('depdate', get_isoformat_date(immigration_df.depdate)) \
        .withColumn("biryear", get_valid_birth_year(immigration_df.biryear)) \
        .dropDuplicates()


def clean_us_demographics_data(us_demographics_df: DataFrame) -> DataFrame:
    """Clean US demographics dataset."""
    return us_demographics_df \
        .dropna() \
        .dropDuplicates()


def clean_ports_data(ports_df: DataFrame) -> DataFrame:
    """Clean ports data by splitting port name into city name and state code."""
    get_city_name = udf(lambda port_name: port_name.split(',')[0].strip() if port_name else None)
    get_state_code = udf(lambda port_name: port_name.split(',')[1].strip()
                                        if (port_name and len(port_name.split(',')) > 1) else None)

    return ports_df \
        .withColumn('city', get_city_name(ports_df.port_name)) \
        .withColumn('state_code', get_state_code(ports_df.port_name)) \
        .drop('port_name') \
        .dropna() \
        .dropDuplicates()


def clean_countries_data(countries_df: DataFrame) -> DataFrame:
    """Clean I94 countries data by replacing invalid countries name with NA."""
    return countries_df \
        .withColumn('country_name', regexp_replace('country_name', '^No Country.*|INVALID.*|Collapsed.*', 'NA'))


def clean_states_data(states_df: DataFrame) -> DataFrame:
    """Clean states data."""
    return states_df \
        .filter('state_code != "99"')


def create_immigration_fact_table(spark: SparkSession, immigration_df: DataFrame,
                                  countries_df: DataFrame, us_states_df: DataFrame, ports_df: DataFrame,
                                  visa_categories_df: DataFrame, travel_modes_df: DataFrame) -> DataFrame:
    """
    Creates query to generate immigrations fact table by joining different dataframes.
    """
    immigration_df.createOrReplaceTempView('staging_immigration_data')
    us_states_df.createOrReplaceTempView('staging_us_states')
    visa_categories_df.createOrReplaceTempView('staging_visa_categories')
    travel_modes_df.createOrReplaceTempView('staging_travel_modes')
    ports_df.createOrReplaceTempView('staging_ports')
    countries_df.createOrReplaceTempView('staging_countries')

    return spark.sql("""
            SELECT
                sid.cicid AS cicid,
                sid.i94yr AS entry_year,
                sid.i94mon AS entry_month,
                sc.country_code AS origin_country_code,
                sp.port_code AS port_code,
                sid.arrdate AS arrival_date,
                stm.mode_id AS travel_mode_code,
                sus.state_code AS us_state_code,
                sid.depdate AS departure_date,
                sid.i94bir AS age,
                svc.visa_category_id AS visa_category_code,
                sid.occup AS occupation,
                sid.gender AS gender,
                sid.biryear AS birth_year,
                sid.dtaddto AS entry_date,
                sid.airline AS airline,
                sid.admnum AS admission_number,
                sid.fltno AS flight_number,
                sid.visatype AS visa_type
            FROM staging_immigration_data sid
                LEFT JOIN staging_countries sc ON sc.country_code = sid.i94res
                LEFT JOIN staging_ports sp ON sp.port_code = sid.i94port
                LEFT JOIN staging_us_states sus ON sus.state_code = sid.i94addr
                LEFT JOIN staging_visa_categories svc ON svc.visa_category_id = sid.i94visa
                LEFT JOIN staging_travel_modes stm ON stm.mode_id = sid.i94mode
            WHERE 
                sc.country_code IS NOT NULL AND
                sp.port_code IS NOT NULL AND
                sus.state_code IS NOT NULL AND
                stm.mode_id IS NOT NULL AND
                svc.visa_category_id IS NOT NULL
        """)


def create_city_demographics_dim_table(spark: SparkSession, us_demographics_df: DataFrame,
                                       ports_df: DataFrame) -> DataFrame:
    """
    Demographics dataset contains multiple entries for a city. This function aggregates those rows and
    creates city demographics dimensional table by combining ports and aggregated demographics datasets.
    """
    us_demographics_df.createOrReplaceTempView('staging_us_demographics')
    ports_df.createOrReplaceTempView('staging_ports')

    aggregated_df = spark.sql("""
            SELECT
                sud.city,
                sud.state_code,
                SUM(sud.male_population) AS male_population,
                SUM(sud.female_population) AS female_population,
                SUM(sud.total_population) AS total_population,
                SUM(sud.number_of_veterans) AS number_of_veterans,
                SUM(sud.foreign_born) AS num_foreign_born
            FROM staging_us_demographics sud
            GROUP BY sud.city, sud.state_code
        """)

    aggregated_df.createOrReplaceTempView('combined_demographics')
    return spark.sql("""
            SELECT
                sp.port_code AS port_code,
                cd.*
            FROM staging_ports sp
                JOIN combined_demographics cd 
                    ON lower(cd.city) = lower(sp.city) AND cd.state_code = sp.state_code
        """)


if __name__ == '__main__':
    main()
