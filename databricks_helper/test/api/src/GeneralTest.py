from python_helper import Test
from python_helper import DateTimeHelper



@Test()
def general_tests():
    
    from databricks_helper import (

        ONE_MILION,
        VERY_LOW_DECIMAL_VALUE,
        NULL_QUERY,
        FIRST_MONTH_DAY,
        MINIMUM_LAST_MONTH_DAY,
        FIRST_MONTH_DAY_AS_STRING,
        MINIMUM_LAST_MONTH_DAY_AS_STRING,

        get_spark_session,
        get_display_spark_dataframe_caller,

        set_default_spark_session,
        set_default_display_spark_dataframe,

        spark_col,
        spark_sum,
        spark_round,
        StructType,
        StructField,
        StringType,
        DoubleType,
        DecimalType,
        IntegerType,
        DateType,
        TimestampType,
        Decimal,

        DataFrame,
        SparkSession,

        two_digits_prefixed_with_zeros_as_string,
        build_first_month_date_given_date_as_string_list,
        get_first_month_date,
        get_last_month_date,
        get_last_month_day,
        get_year_dash_month,
        get_year,
        get_month,
        get_month_dash_day,
        remove_leading_zeros,
        get_distinct_and_ordered,
        query_value_as_string_is_not_null,
        get_query_value_or_null,
        print_attribute,
        get_monetary_decimal_type,
        get_percentual_decimal_type,
        to_monetary_decimal,
        to_percentual_decimal,
        parse_column_name,
        wrap_column_name,
        to_query_string_value,
        date_to_query_date,
        build_month_dash_day_query_from_date,
        build_month_dash_day_query,
        build_month_query,
        list_to_query_in_integer_list,
        list_to_query_in_string_list,
        cast_to_query_string,
        replace_if_empty_query,
        cast_to_query_2_digits_decimal,
        cast_to_query_percentual_decimal,
        cast_to_query_monetary_decimal,
        cast_to_query_integer,
        concat_query,
        concat_dash_query,
        concat_query_date,
        get_distinct_integer_collection_from_table,
        get_distinct_integer_collection_from_table_by_cd,
        query_distinct_collection,
        display_spark_dataframe,
        display_query,
        spark_sql,
        spark_create_or_replace_temp_view_from_sql,
        spark_big_sql,
        spark_create_or_replace_temp_view_from_big_sql,
        spark_createDataFrame,
        override_table_and_schema,
        override_table,
        to_spark_df_override_delta_mode,
        save_as_table
    )



    ###- Manipulaçao de datas

    ###- get_first_month_date
    assert DateTimeHelper.dateOf('2003-01-01 01:00:00') == get_first_month_date(DateTimeHelper.dateOf('2003-01-31 01:00:00')), get_first_month_date(DateTimeHelper.dateOf('2003-01-31 01:00:00'))
    assert DateTimeHelper.dateOf('2003-01-01 01:00:00') == get_first_month_date(DateTimeHelper.dateOf('2003-01-13 01:00:00')), get_first_month_date(DateTimeHelper.dateOf('2003-01-13 01:00:00'))
    assert DateTimeHelper.dateOf('2003-02-01 01:00:00') == get_first_month_date(DateTimeHelper.dateOf('2003-02-01 01:00:00')), get_first_month_date(DateTimeHelper.dateOf('2003-02-01 01:00:00'))

    ###- get_last_month_date
    assert DateTimeHelper.dateOf('2003-01-31 01:00:00') == get_last_month_date(DateTimeHelper.dateOf('2003-01-02 01:00:00')), get_last_month_date(DateTimeHelper.dateOf('2003-01-02 01:00:00'))
    assert DateTimeHelper.dateOf('2003-01-31 01:00:00') == get_last_month_date(DateTimeHelper.dateOf('2003-01-31 01:00:00')), get_last_month_date(DateTimeHelper.dateOf('2003-01-31 01:00:00'))

    ###- get_last_month_day
    assert 31 == get_last_month_day(DateTimeHelper.dateOf('2003-01-02 01:00:00')), get_last_month_day(DateTimeHelper.dateOf('2003-01-02 01:00:00'))
    assert 31 == get_last_month_day(DateTimeHelper.dateOf('2003-01-31 01:00:00')), get_last_month_day(DateTimeHelper.dateOf('2003-01-31 01:00:00'))

    ###- get_year_dash_month
    assert '2003-12' == get_year_dash_month(DateTimeHelper.dateOf('2003-12-01 01:00:00')), get_year_dash_month(DateTimeHelper.dateOf('2003-12-01 01:00:00'))
    assert '2003-02' == get_year_dash_month(DateTimeHelper.dateOf('2003-02-01 01:00:00')), get_year_dash_month(DateTimeHelper.dateOf('2003-02-01 01:00:00'))

    ###- get_year
    assert '2003' == get_year(DateTimeHelper.dateOf('2003-12-01 01:00:00')), get_year(DateTimeHelper.dateOf('2003-12-01 01:00:00'))
    assert '2003' == get_year(DateTimeHelper.dateOf('2003-02-01 01:00:00')), get_year(DateTimeHelper.dateOf('2003-02-01 01:00:00'))

    ###- get_month
    assert '12' == get_month(DateTimeHelper.dateOf('2003-12-01 01:00:00')), get_month(DateTimeHelper.dateOf('2003-12-01 01:00:00'))
    assert '02' == get_month(DateTimeHelper.dateOf('2003-02-01 01:00:00')), get_month(DateTimeHelper.dateOf('2003-02-01 01:00:00'))

    ###- get_year
    assert '12-01' == get_month_dash_day(DateTimeHelper.dateOf('2003-12-01 01:00:00')), get_month_dash_day(DateTimeHelper.dateOf('2003-12-01 01:00:00'))
    assert '02-01' == get_month_dash_day(DateTimeHelper.dateOf('2003-02-01 01:00:00')), get_month_dash_day(DateTimeHelper.dateOf('2003-02-01 01:00:00'))







    ###- Manipulação de queries

    ###- parse_column_name
    assert 'percentual_diario' == parse_column_name('% de Diário')

    ###- wrap_column_name
    assert '`column_name`' == wrap_column_name('column_name'), wrap_column_name('column_name')

    ###- date_to_query_date
    assert "-->'2003-03-01 01:00:00.000'" == '-->' + date_to_query_date('2003-03-01 01:00:00.000'), '-->' + date_to_query_date('2003-03-01 01:00:00.000')

    ###- build_month_dash_day_query_from_date
    assert "-->'03-01 01:00:00'" == '-->' + build_month_dash_day_query_from_date(DateTimeHelper.of('2003-03-01 01:00:00')), '-->' + build_month_dash_day_query_from_date(DateTimeHelper.of('2003-03-01 01:00:00'))

    ###- build_month_dash_day_query
    assert 'SUBSTRING(CAST(date_column_name AS VARCHAR(10)), 6, 5)' == build_month_dash_day_query('date_column_name'), build_month_dash_day_query('date_column_name')

    ###- list_to_query_in_integer_list
    assert '(1,2,3,4,5)' == list_to_query_in_integer_list([1,2,3,4,5]), list_to_query_in_integer_list([1,2,3,4,5])

    ###- list_to_query_in_string_list
    assert '''('1','2','3','4','5')''' == list_to_query_in_string_list([1,2,3,4,5]), list_to_query_in_string_list([1,2,3,4,5])

    ###- cast_to_string_query
    assert 'CAST(column_name AS VARCHAR(9))' == cast_to_query_string('column_name', 9), cast_to_query_string('column_name', 9)

    ###- replace_if_empty
    assert '''nvl(NULLIF(column_name,''), defautl_value)''' == replace_if_empty_query('column_name', 'defautl_value'), replace_if_empty_query('column_name', 'defautl_value')

    ###- cast_to_2_digits_decimal
    assert 'CAST((given_query) AS DECIMAL(32,2))' == cast_to_query_2_digits_decimal('given_query'), cast_to_query_2_digits_decimal('given_query')
    assert '''CAST((nvl(NULLIF(column_name,''), defautl_value)) AS DECIMAL(32,2))''' == cast_to_query_2_digits_decimal(replace_if_empty_query('column_name', 'defautl_value')), cast_to_query_2_digits_decimal(replace_if_empty_query('column_name', 'defautl_value'))

    ###- cast_to_percentual_decimal
    assert '''CAST((100.0 * nvl(NULLIF(given_query,''), 0)) AS DECIMAL(32,2))''' == cast_to_query_percentual_decimal('given_query'), cast_to_query_percentual_decimal('given_query')

    ###- cast_to_monetary_decimal
    assert 'CAST((given_query) AS DECIMAL(32,2))' == cast_to_query_monetary_decimal('given_query'), cast_to_query_monetary_decimal('given_query')

    ###- cast_to_integer
    assert 'CAST(given_query AS INT)' == cast_to_query_integer('given_query'), cast_to_query_integer('given_query')

    ###- concat_query_date
    assert (
        '''concat_ws('-', CAST(year_column_name_or_value AS INT), lpad(CAST(month_column_name_or_value AS INT), 2, '0'), lpad(CAST(day_column_name_or_value AS INT), 2, '0'))'''
    ) == (
        concat_query_date('year_column_name_or_value', 'month_column_name_or_value', 'day_column_name_or_value')
    ), concat_query_date('year_column_name_or_value', 'month_column_name_or_value', 'day_column_name_or_value')