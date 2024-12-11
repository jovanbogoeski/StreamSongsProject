from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, date_format
from datetime import datetime, timedelta

if __name__ == "__main__":
    print("Starting Date Dimension ETL for 2021 with Hourly Granularity...")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("Date Dimension ETL").getOrCreate()

    # Paths
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_datetime_2021_hourly"

    # Step 1: Define the date range for 2021
    startdate = datetime.strptime("2021-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    enddate = datetime.strptime("2021-12-31 23:00:00", "%Y-%m-%d %H:%M:%S")

    # Step 2: Define column names and transformation rules
    column_rule_df = spark.createDataFrame(
        [
            ("DateSK", "cast(date_format(date, 'yyyyMMddHH') as long)"),  # 2021010100
            ("Year", "year(date)"),  # 2021
            ("Quarter", "quarter(date)"),  # 1
            ("Month", "month(date)"),  # 1
            ("Day", "day(date)"),  # 1
            ("Hour", "hour(date)"),  # 0
            ("Week", "weekofyear(date)"),  # 1
            ("QuarterNameLong", "date_format(date, 'QQQQ')"),  # 1st quarter
            ("QuarterNameShort", "date_format(date, 'QQQ')"),  # Q1
            ("QuarterNumberString", "date_format(date, 'QQ')"),  # 01
            ("MonthNameLong", "date_format(date, 'MMMM')"),  # January
            ("MonthNameShort", "date_format(date, 'MMM')"),  # Jan
            ("MonthNumberString", "date_format(date, 'MM')"),  # 01
            ("DayNumberString", "date_format(date, 'dd')"),  # 01
            ("WeekNameLong", "concat('week', lpad(weekofyear(date), 2, '0'))"),  # week 01
            ("WeekNameShort", "concat('w', lpad(weekofyear(date), 2, '0'))"),  # w01
            ("WeekNumberString", "lpad(weekofyear(date), 2, '0')"),  # 01
            ("DayOfWeek", "dayofweek(date)"),  # 1
            ("YearMonthString", "date_format(date, 'yyyy/MM')"),  # 2021/01
            ("DayOfWeekNameLong", "date_format(date, 'EEEE')"),  # Sunday
            ("DayOfWeekNameShort", "date_format(date, 'EEE')"),  # Sun
            ("DayOfMonth", "cast(date_format(date, 'd') as int)"),  # 1
            ("DayOfYear", "cast(date_format(date, 'D') as int)"),  # 1
        ],
        ["new_column_name", "expression"],
    )

    # Step 3: Generate a range of timestamps for 2021 with hourly granularity
    start = int(startdate.timestamp())
    stop = int(enddate.timestamp()) + 3600  # Include the final hour
    df_ref_date = spark.range(start, stop, 3600).select(
        col("id").cast("timestamp").alias("date")
    )

    # Step 4: Apply transformation rules to add columns
    for row in column_rule_df.collect():
        new_column_name = row["new_column_name"]
        expression = expr(row["expression"])
        df_ref_date = df_ref_date.withColumn(new_column_name, expression)

    # Step 5: Save the result to Parquet
    try:
        df_ref_date.write.mode("overwrite").partitionBy("Year", "Month", "Day").parquet(output_path)
        print(f"Date dimension for 2021 with hourly granularity saved successfully to {output_path}")
    except Exception as e:
        print(f"Error saving date dimension for 2021 with hourly granularity: {e}")
        exit(1)

    # Stop SparkSession
    spark.stop()
    print("Date Dimension ETL for 2021 with Hourly Granularity completed.")
