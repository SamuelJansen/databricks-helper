from python_helper import DateTimeHelper, StringHelper, Constant, ObjectHelper, log, EnvironmentHelper
from decimal import Decimal


from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType, IntegerType, DateType, TimestampType


def prefix_with_zeros_as_string(day_as_int):
    return f'{day_as_int:0>2}'


ONE_MILION = 1000000
VERY_LOW_DECIMAL_VALUE = 1 / (100 * ONE_MILION)

NULL_QUERY = 'null'

FIRST_MONTH_DAY = 1
MINIMUM_LAST_MONTH_DAY = 28
FIRST_MONTH_DAY_AS_STRING = prefix_with_zeros_as_string(FIRST_MONTH_DAY) ###- '01'
MINIMUM_LAST_MONTH_DAY_AS_STRING = prefix_with_zeros_as_string(MINIMUM_LAST_MONTH_DAY) ###- '28'


def build_first_month_date_given_date_as_string_list(given_date_as_string_list):
    return DateTimeHelper.dateOf(
        DateTimeHelper.of(
            f'''{
                given_date_as_string_list[0]
            }{Constant.DASH}{
                given_date_as_string_list[1]
            }{Constant.DASH}{FIRST_MONTH_DAY_AS_STRING}{Constant.SPACE}{
                DateTimeHelper.DEFAULT_TIME_BEGIN
            }'''
        )
    )


def get_first_month_date(given_date):
    given_date_as_string_list = str(given_date).split(Constant.DASH)
    return build_first_month_date_given_date_as_string_list(given_date_as_string_list)


def get_last_month_date(given_date):
    return DateTimeHelper.dateOf(
        DateTimeHelper.minusDays(
            DateTimeHelper.plusMonths(
                DateTimeHelper.of(date=f'{get_first_month_date(given_date)}'),
                months=1
            ),
            days=1
        )
    )


def get_last_month_day(given_datetime):
    return int(str(get_last_month_date(given_datetime)).split(Constant.DASH)[-1])


def get_year_dash_month(given_datetime):
    # The idea here is to extract year:
    # Example: 2023-05-12 -> 2023-05
    return StringHelper.join(
        str(given_datetime).split(Constant.DASH)[:-1],
        character = Constant.DASH
    )


def get_year(given_date):
    # The idea here is to extract year:
    # Example: 2023-05-12 -> 2023
    return str(given_date).split(Constant.DASH)[0]


def get_month(given_date):
    # The idea here is to extract month from a date:
    # Example: 2023-05-12 -> 05
    return str(given_date).split(Constant.DASH)[1]


def get_month_dash_day(given_date):
    # The idea here is to extract month dash day from a date:
    # Example: 2023-05-12 -> 05-12
    return StringHelper.join(
        str(given_date).split(Constant.DASH)[-2:],
        character = Constant.DASH
    )


def remove_leading_zeros(integer_as_string):
    while integer_as_string.startswith('0') and 1 < len(integer_as_string):
        integer_as_string = integer_as_string[1:]
    return integer_as_string


def get_distinct_and_ordered(given_list):
    return ObjectHelper.deepSort(list(set(given_list)))


def query_value_as_string_is_not_null(query_value_as_string):
    return (
        not query_value_as_string == NULL_QUERY and 
        query_value_as_string is not None
    )


def get_query_value_or_null(given_query_value):
    return given_query_value if ObjectHelper.isNotNone(given_query_value) else NULL_QUERY


def print_attribute(attribute_name, attribute_value):
    print(f'''{attribute_name}: {attribute_value}''')


def get_monetary_decimal_type():
    return DecimalType(32, 2)


def get_percentual_decimal_type():
    return DecimalType(32, 4)


def to_monetary_decimal(float_value):
    return Decimal(float_value).quantize(Decimal('.01'))
                                         

def to_percentual_decimal(float_value):
    return Decimal(float_value).quantize(Decimal('.0001'))


def parse_column_name(column_name):
    parsed_column_name = column_name.lower()
    parsed_column_name = parsed_column_name.replace('(-)', 'menos').replace(' ', '_').replace('-', '_')
    parsed_column_name = parsed_column_name.replace('%', 'percentual')
    parsed_column_name = parsed_column_name.replace('á', 'a').replace('ã', 'a').replace('â', 'a')
    parsed_column_name = parsed_column_name.replace('é', 'e').replace('ê', 'e')
    parsed_column_name = parsed_column_name.replace('í', 'i')
    parsed_column_name = parsed_column_name.replace('ó', 'o').replace('õ', 'o')
    parsed_column_name = parsed_column_name.replace('ú', 'u')
    parsed_column_name = parsed_column_name.replace('_da_', '_')
    parsed_column_name = parsed_column_name.replace('_de_', '_')
    parsed_column_name = parsed_column_name.replace('_do_', '_')
    return parsed_column_name


def wrap_column_name(column_name):
    return f'`{column_name}`'


def to_query_string_value(value):
    return value if str(value).startswith(Constant.SINGLE_QUOTE) else f'{Constant.SINGLE_QUOTE}{value}{Constant.SINGLE_QUOTE}'


def date_to_query_date(given_date):
    ###- f'{Constant.SINGLE_QUOTE}{given_date}{Constant.SINGLE_QUOTE}'
    return to_query_string_value(given_date) 


def build_month_dash_day_query_from_date(given_date):
    # The idea here is to extract month dash day from a date for queries:
    # Example: 2023-05-14 -> '05-14'
    return StringHelper.join(
        [
            Constant.SINGLE_QUOTE,
            get_month_dash_day(given_date),
            Constant.SINGLE_QUOTE
        ]
    )


def build_month_dash_day_query(date_column_name):
    # The idea here is to extract month dash day from a date:
    # Example: 2023-05-14 -> 05-14
    return f'SUBSTRING({cast_to_string_query(date_column_name, 10)}, 6, 5)'


def build_month_query(date_column_name):
    # The idea here is to extract month from a date:
    # Example: 2023-05-14 -> 05
    return f'SUBSTRING({cast_to_string_query(date_column_name, 10)}, 6, 2)'


def list_to_query_in_integer_list(given_list):
    return StringHelper.join(
        [
            Constant.OPEN_TUPLE,
            StringHelper.join([str(element) for element in given_list], character = Constant.COMA),
            Constant.CLOSE_TUPLE
        ],
        character = Constant.BLANK
    )


def list_to_query_in_string_list(given_list):
    return list_to_query_in_integer_list([
        f'{Constant.SINGLE_QUOTE}{i}{Constant.SINGLE_QUOTE}'
        for i in given_list
    ])


def cast_to_string_query(column_name, size):
    return f'CAST({column_name} AS VARCHAR({size}))'


def replace_if_empty(column_name, defautl_value):
    return f'''nvl(NULLIF({column_name},''), {defautl_value})'''


def cast_to_2_digits_decimal(given_query):
    return f'CAST(({given_query}) AS DECIMAL(32,2))'


def cast_to_percentual_decimal(given_query):
    return cast_to_2_digits_decimal(f'''100.0 * {replace_if_empty(given_query, '0')}''')


def cast_to_monetary_decimal(given_query):
    return cast_to_2_digits_decimal(given_query)


def cast_to_integer(given_query):
    return f'CAST({given_query} AS INT)'
    

def concat_query_date(year_column_name_or_value, month_column_name_or_value, day_column_name_or_value):
    return f'''concat_ws('{Constant.DASH}', {
        cast_to_integer(year_column_name_or_value)
    }, lpad({
        cast_to_integer(month_column_name_or_value)
    }, 2, '0'), lpad({
        cast_to_integer(day_column_name_or_value)
    }, 2, '0'))'''


def get_distinct_integer_collection_from_table(table_name, column_name):
    try:
        return get_distinct_and_ordered([
            int(cd_as_string)
            for cd_as_string in spark_sql(f'''
                SELECT
                    collect_set({cast_to_integer(f'tbl.{column_name}')}) AS {column_name}_set
                FROM {table_name} tbl
                '''
                # , show_query=False, show_dataframe=False
            ).select(f'{column_name}_set').first()[0]
            if query_value_as_string_is_not_null(cd_as_string)
        ])
    except Exception as exception:
        log.failure(get_distinct_integer_collection_from_table, 'Not possible to extract collection. Returning empty collection by default', exception=exception, muteStackTrace=True)
        return []
    

def get_distinct_integer_collection_from_table_by_cd(cd, table_name, cd_column_name, column_name):
    try:
        return get_distinct_and_ordered([
            int(cd_as_string)
            for cd_as_string in spark_sql(f'''
                SELECT
                    collect_set({cast_to_integer(f'tbl.{column_name}')}) AS {column_name}_set
                FROM {table_name} tbl
                WHERE (
                    {cast_to_integer(f'tbl.{cd_column_name}')} = {cast_to_integer(cd)}
                    AND NOT {replace_if_empty(f'tbl.{column_name}', NULL_QUERY)} IS NULL
                )
                '''
                # , show_query=False, show_dataframe=False
            ).select(f'{column_name}_set').first()[0]
            if query_value_as_string_is_not_null(cd_as_string)
        ])
    except Exception as exception:
        log.failure(get_distinct_integer_collection_from_table_by_cd, 'Not possible to extract collection. Returning empty collection by default', exception=exception, muteStackTrace=True)
        return []


def display_spark_dataframe(spark_df: DataFrame, *args, **kwargs) -> DataFrame:
    ###- Here, display() is a builting function in databricks
    display(spark_df, *args, **kwargs)
    return spark_df


def display_query(givenQuery: str, show_query=True) -> str:
    if show_query:
        print(givenQuery)
    return givenQuery

def get_spark_session(spark_session: SparkSession = None) -> SparkSession: 
    return spark_session if ObjectHelper.isNotNone(spark_session) else spark


def spark_sql(*agrs, show_dataframe=True, spark_session: SparkSession = None, **kwargs) -> DataFrame:
    df = get_spark_session(spark_session=spark_session).sql(display_query(*agrs, **kwargs))
    if show_dataframe:
        display_spark_dataframe(df)
    return df

def spark_create_or_replace_temp_view_from_sql(*agrs, view_name=None, **kwargs) -> DataFrame:
    df = spark_sql(*agrs, **kwargs)
    df.createOrReplaceTempView(view_name)
    return df


def spark_big_sql(*agrs, spark_sql_caller=spark_sql, **kwargs):
    df = spark_sql_caller(*agrs, **kwargs)
    print(f'Rows count: {df.count()}')
    return df


def spark_spark_create_or_replace_temp_view_from_big_sql(*agrs, **kwargs) -> DataFrame:
    return spark_big_sql(*agrs, spark_sql_caller=spark_create_or_replace_temp_view_from_sql, **kwargs)
    

def spark_createDataFrame(*agrs, show_dataframe=True, order_by=None, spark_session: SparkSession = None, **kwargs) -> DataFrame:
    df = get_spark_session(spark_session=spark_session).createDataFrame(*agrs, **kwargs)
    if ObjectHelper.isNotEmpty(order_by):
        df = df.orderBy(*order_by)
    if show_dataframe:
        display_spark_dataframe(df)
    return df


def override_table_and_schema(spark_df, table_name):
    return save_as_table(to_spark_df_override_delta_mode(spark_df).option('overwriteSchema', 'true'), table_name)


def override_table(spark_df, table_name):
    return save_as_table(to_spark_df_override_delta_mode(spark_df), table_name)


def to_spark_df_override_delta_mode(spark_df):
    if 0 >= spark_df.count():
        raise Exception('spark dataframe cannot be empty')
    return spark_df.write.format('delta').mode('overwrite')


def save_as_table(spark_df_override_delta_mode, table_name):
    return spark_df_override_delta_mode.saveAsTable(table_name)