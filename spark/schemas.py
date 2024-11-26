from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType
# Define schemas based on your topics
auth_events_schema = StructType([
    StructField("ts", LongType(), True),             # int64
    StructField("sessionId", IntegerType(), True),      # int64
    StructField("level", StringType(), True),        # object (string)
    StructField("itemInSession", IntegerType(), True),  # int64
    StructField("city", StringType(), True),         # object (string)
    StructField("zip", LongType(), True),            # int64
    StructField("state", StringType(), True),        # object (string)
    StructField("userAgent", StringType(), True),    # object (string)
    StructField("lon", DoubleType(), True),          # float64
    StructField("lat", DoubleType(), True),          # float64
    StructField("userId", IntegerType(), True),       # float64
    StructField("lastName", StringType(), True),     # object (string)
    StructField("firstName", StringType(), True),    # object (string)
    StructField("gender", StringType(), True),       # object (string)
    StructField("registration", LongType(), True), # float64
    StructField("success", BooleanType(), True)      # bool
])

listen_events_schema = StructType([
    StructField("artist", StringType(), True),          # object (string)
    StructField("song", StringType(), True),            # object (string)
    StructField("duration", DoubleType(), True),        # float64
    StructField("ts", LongType(), True),                # int64 (could be converted to TimestampType if needed)
    StructField("sessionId", IntegerType(), True),         # int64
    StructField("auth", StringType(), True),            # object (string)
    StructField("level", StringType(), True),           # object (string)
    StructField("itemInSession", IntegerType(), True),     # int64
    StructField("city", StringType(), True),            # object (string)
    StructField("zip", LongType(), True),               # int64
    StructField("state", StringType(), True),           # object (string)
    StructField("userAgent", StringType(), True),       # object (string)
    StructField("lon", DoubleType(), True),             # float64
    StructField("lat", DoubleType(), True),             # float64
    StructField("userId", LongType(), True),            # int64
    StructField("lastName", StringType(), True),        # object (string)
    StructField("firstName", StringType(), True),       # object (string)
    StructField("gender", StringType(), True),          # object (string)
    StructField("registration", LongType(), True)       # int64
])


page_view_events_schema = StructType([
    StructField("ts", LongType(), True),                # int64 (could be converted to TimestampType if needed)
    StructField("sessionId", IntegerType(), True),         # int64
    StructField("page", StringType(), True),            # object (string)
    StructField("auth", StringType(), True),            # object (string)
    StructField("method", StringType(), True),          # object (string)
    StructField("status", IntegerType(), True),            # int64
    StructField("level", StringType(), True),           # object (string)
    StructField("itemInSession", LongType(), True),     # int64
    StructField("city", StringType(), True),            # object (string)
    StructField("zip", LongType(), True),               # int64
    StructField("state", StringType(), True),           # object (string)
    StructField("userAgent", StringType(), True),       # object (string)
    StructField("lon", DoubleType(), True),             # float64
    StructField("lat", DoubleType(), True),             # float64
    StructField("userId", LongType(), True),          # float64 (if it should be int64, use LongType)
    StructField("lastName", StringType(), True),        # object (string)
    StructField("firstName", StringType(), True),       # object (string)
    StructField("gender", StringType(), True),          # object (string)
    StructField("registration", DoubleType(), True),    # float64
    StructField("artist", StringType(), True),          # object (string)
    StructField("song", StringType(), True),            # object (string)
    StructField("duration", DoubleType(), True)         # float64
])

status_change_events_schema = StructType([
    StructField("ts", LongType(), True),                # int64 (could be converted to TimestampType if needed)
    StructField("sessionId", IntegerType(), True),         # int64
    StructField("auth", StringType(), True),            # object (string)
    StructField("level", StringType(), True),           # object (string)
    StructField("itemInSession", IntegerType(), True),     # int64
    StructField("city", StringType(), True),            # object (string)
    StructField("zip", LongType(), True),               # int64
    StructField("state", StringType(), True),           # object (string)
    StructField("userAgent", StringType(), True),       # object (string)
    StructField("lon", DoubleType(), True),             # float64
    StructField("lat", DoubleType(), True),             # float64
    StructField("userId", IntegerType(), True),            # int64
    StructField("lastName", StringType(), True),        # object (string)
    StructField("firstName", StringType(), True),       # object (string)
    StructField("gender", StringType(), True),          # object (string)
    StructField("registration", LongType(), True)       # int64
])

schema_mapping = {
    "auth_events": auth_events_schema,
    "listen_events": listen_events_schema,
    "page_view_events": page_view_events_schema,
    "status_change_events": status_change_events_schema
}