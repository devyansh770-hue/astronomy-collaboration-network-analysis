# ================================================================
# PROJECT 63: ASTRONOMY COLLABORATION NETWORK ANALYSIS
# TASK 3: IDENTIFY RESEARCHERS WITH MOST CONNECTIONS
# Dataset: ca-AstroPh
# ================================================================

import os

# Configure Java 17 locally
os.environ["JAVA_HOME"] = (
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.21.9-hotspot"
)

os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
)

os.environ.pop("SPARK_HOME", None)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, split, regexp_replace,
    size, least, greatest, count,
    coalesce, lit, desc
)

# ------------------------------------------------
# 1. INITIALIZE SPARK
# ------------------------------------------------

spark = SparkSession.builder \
    .appName("AstroPh_Top_Researchers") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\nSpark initialized successfully!")
print("Spark Version:", spark.version)

# ------------------------------------------------
# 2. LOAD DATASET
# ------------------------------------------------

DATA_PATH = (
    r"C:\Users\Devyansh verma\OneDrive\Desktop"
    r"\INT315_Astr\dataset\ca-AstroPh_edges.txt"
)

print("\nLoading collaboration dataset...")

raw_df = spark.read.text(DATA_PATH)

data_lines = raw_df.filter(
    (~trim(col("value")).startswith("#")) &
    (trim(col("value")) != "")
)

parsed_df = data_lines.withColumn(
    "parts",
    split(
        trim(regexp_replace(col("value"), r"\s+", " ")),
        " "
    )
)

valid_df = parsed_df.filter(
    size(col("parts")) == 2
)

all_edges = valid_df.select(
    col("parts").getItem(0).cast("long").alias("src"),
    col("parts").getItem(1).cast("long").alias("dst")
).filter(
    col("src").isNotNull() & col("dst").isNotNull()
)

# ------------------------------------------------
# 3. CONSTRUCT UNDIRECTED GRAPH
# ------------------------------------------------

vertices_df = (
    all_edges.select(col("src").alias("id"))
    .union(all_edges.select(col("dst").alias("id")))
    .distinct()
)

non_self_edges = all_edges.filter(
    col("src") != col("dst")
)

unique_directed_edges = non_self_edges.dropDuplicates(
    ["src", "dst"]
)

edges_df = unique_directed_edges.select(
    least(col("src"), col("dst")).alias("src"),
    greatest(col("src"), col("dst")).alias("dst")
).dropDuplicates(["src", "dst"])

# ------------------------------------------------
# 4. CALCULATE DEGREE
# ------------------------------------------------

print("\nCalculating researcher connections...")

src_degrees = edges_df.select(
    col("src").alias("id")
)

dst_degrees = edges_df.select(
    col("dst").alias("id")
)

all_connections = src_degrees.union(dst_degrees)

degree_df = all_connections.groupBy("id").agg(
    count("*").alias("degree")
)

degree_df = vertices_df.join(
    degree_df,
    on="id",
    how="left"
).withColumn(
    "degree",
    coalesce(col("degree"), lit(0))
)

total_vertices = vertices_df.count()

degree_df = degree_df.withColumn(
    "degree_centrality",
    col("degree") / lit(total_vertices - 1)
)

# ------------------------------------------------
# 5. FIND TOP 10 RESEARCHERS
# ------------------------------------------------

print("\nFinding the 10 most connected researchers...")

top_10 = degree_df.orderBy(
    desc("degree"),
    col("id").asc()
).limit(10)

print("\n" + "=" * 65)
print("             TOP 10 MOST CONNECTED RESEARCHERS")
print("=" * 65)

top_10.show(10, truncate=False)

# ------------------------------------------------
# 6. FIND THE HIGHEST DEGREE
# ------------------------------------------------

highest_degree = degree_df.agg(
    {"degree": "max"}
).collect()[0][0]

print("\nHighest Degree:", highest_degree)

print("\nRESEARCHERS WITH THE HIGHEST DEGREE")

degree_df.filter(
    col("degree") == highest_degree
).show(truncate=False)

# ------------------------------------------------
# 7. SUMMARY
# ------------------------------------------------

print("\n" + "=" * 65)
print("                  ANALYSIS SUMMARY")
print("=" * 65)

print("Total Researchers:", total_vertices)
print("Total Undirected Edges:", edges_df.count())
print("Highest Number of Collaborators:", highest_degree)

print("\nTOP RESEARCHER IDENTIFICATION COMPLETED!")
print("=" * 65)