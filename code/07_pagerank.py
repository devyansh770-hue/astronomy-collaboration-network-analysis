
# ================================================================
# PROJECT 63: ASTRONOMY COLLABORATION NETWORK ANALYSIS
# TASK 5: PAGERANK ANALYSIS
# Dataset: ca-AstroPh
# ================================================================

import os
import sys

# ------------------------------------------------
# 1. CONFIGURATION
# ------------------------------------------------

DATASET_PATH = (
    r"C:\Users\Devyansh verma\OneDrive\Desktop"
    r"\INT315_Astr\dataset\ca-AstroPh_edges.txt"
)

CHECKPOINT_PATH = (
    r"C:\Users\Devyansh verma\OneDrive\Desktop"
    r"\INT315_Astr\checkpoints"
)

os.makedirs(CHECKPOINT_PATH, exist_ok=True)

# Configure Java 17 locally
os.environ["JAVA_HOME"] = (
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.21.9-hotspot"
)

os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
)

# Avoid conflict with existing Spark 3.5.3
os.environ.pop("SPARK_HOME", None)

# Use Spyder's Python interpreter
PYTHON_PATH = sys.executable
os.environ["PYSPARK_PYTHON"] = PYTHON_PATH

if not os.path.exists(DATASET_PATH):
    raise FileNotFoundError("Dataset file not found!")

# ------------------------------------------------
# 2. IMPORT LIBRARIES
# ------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    split,
    least,
    greatest,
    count,
    desc,
    asc
)

from graphframes import GraphFrame

# ------------------------------------------------
# 3. INITIALIZE SPARK
# ------------------------------------------------

spark = SparkSession.builder \
    .appName("AstroPh_PageRank_Analysis") \
    .master("local[2]") \
    .config(
        "spark.jars.packages",
        "io.graphframes:graphframes-spark4_2.13:0.12.3"
    ) \
    .config("spark.pyspark.python", PYTHON_PATH) \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

spark.sparkContext.setCheckpointDir(CHECKPOINT_PATH)

print("Spark Version:", spark.version)

# ------------------------------------------------
# 4. LOAD DATASET
# ------------------------------------------------

print("\nLoading AstroPh dataset...")

raw_df = spark.read.text(DATASET_PATH)

data_df = raw_df.filter(
    (~trim(col("value")).startswith("#")) &
    (trim(col("value")) != "")
)

split_df = data_df.withColumn(
    "parts",
    split(trim(col("value")), r"\s+")
)

valid_df = split_df.filter(
    col("parts").getItem(0).cast("long").isNotNull() &
    col("parts").getItem(1).cast("long").isNotNull()
)

parsed_df = valid_df.select(
    col("parts").getItem(0).cast("long").alias("src"),
    col("parts").getItem(1).cast("long").alias("dst")
)

print("Valid edge records:", parsed_df.count())

# ------------------------------------------------
# 5. CREATE VERTICES
# ------------------------------------------------

vertices_df = (
    parsed_df.select(col("src").alias("id"))
    .union(parsed_df.select(col("dst").alias("id")))
    .distinct()
)

print("Total researchers:", vertices_df.count())

# ------------------------------------------------
# 6. CREATE UNDIRECTED EDGES
# ------------------------------------------------

non_self_edges = parsed_df.filter(
    col("src") != col("dst")
)

edges_df = non_self_edges.select(
    least(col("src"), col("dst")).alias("src"),
    greatest(col("src"), col("dst")).alias("dst")
).distinct()

print("Total undirected edges:", edges_df.count())


# ------------------------------------------------
# 7. CONSTRUCT BIDIRECTIONAL GRAPH FOR PAGERANK
# ------------------------------------------------

print("\nConstructing bidirectional GraphFrame...")

# Original edge: src -> dst
forward_edges = edges_df.select(
    col("src"),
    col("dst")
)

# Reverse edge: dst -> src
reverse_edges = edges_df.select(
    col("dst").alias("src"),
    col("src").alias("dst")
)

# Combine both directions
pagerank_edges = forward_edges.union(reverse_edges)

# Construct GraphFrame using bidirectional edges
g = GraphFrame(vertices_df, pagerank_edges)

print("Bidirectional GraphFrame created successfully!")

print("Total vertices:", g.vertices.count())
print("Total directed edges for PageRank:", g.edges.count())


# ------------------------------------------------
# 8. CALCULATE PAGERANK
# ------------------------------------------------

print("\nCalculating PageRank...")

pagerank_graph = g.pageRank(
    resetProbability=0.15,
    maxIter=10
)

pagerank_df = pagerank_graph.vertices.select(
    "id",
    "pagerank"
)

print("PageRank calculation completed!")

# ------------------------------------------------
# 9. CALCULATE DEGREE FOR COMPARISON
# ------------------------------------------------

src_degree = edges_df.select(
    col("src").alias("id")
)

dst_degree = edges_df.select(
    col("dst").alias("id")
)

degree_df = (
    src_degree.union(dst_degree)
    .groupBy("id")
    .agg(count("*").alias("degree"))
)

# Include researchers with zero degree
degree_df = vertices_df.join(
    degree_df,
    on="id",
    how="left"
).fillna({"degree": 0})

# ------------------------------------------------
# 10. TOP 10 RESEARCHERS BY PAGERANK
# ------------------------------------------------

print("\nFinding top 10 researchers by PageRank...")

top_pagerank = (
    pagerank_df.join(degree_df, on="id")
    .orderBy(
        desc("pagerank"),
        asc("id")
    )
    .limit(10)
)

print("\n" + "=" * 65)
print("TOP 10 RESEARCHERS BY PAGERANK")
print("=" * 65)

top_pagerank.show(
    10,
    truncate=False
)

# ------------------------------------------------
# 11. TOP 10 RESEARCHERS BY DEGREE
# ------------------------------------------------

top_degree = degree_df.orderBy(
    desc("degree"),
    asc("id")
).limit(10)

print("\n" + "=" * 65)
print("TOP 10 RESEARCHERS BY DEGREE")
print("=" * 65)

top_degree.show(
    10,
    truncate=False
)

# ------------------------------------------------
# 12. COMPARE BOTH RANKINGS
# ------------------------------------------------

print("\n" + "=" * 65)
print("COMPARISON OF TOP 10 RESEARCHERS")
print("=" * 65)

common_researchers = top_pagerank.select(
    "id"
).join(
    top_degree.select("id"),
    on="id",
    how="inner"
)

overlap_count = common_researchers.count()

print(
    "Researchers appearing in both top 10 lists:",
    overlap_count
)

print("\nCommon researchers:")

common_researchers.orderBy("id").show(
    truncate=False
)

# ------------------------------------------------
# 13. FINAL SUMMARY
# ------------------------------------------------

print("\n" + "=" * 65)
print("PAGERANK ANALYSIS SUMMARY")
print("=" * 65)

print("Total researchers:", vertices_df.count())
print("Total undirected edges:", edges_df.count())
print("PageRank iterations: 10")
print("Reset probability: 0.15")
print("Common researchers in both top 10 lists:", overlap_count)

print("\nTASK 5 COMPLETED SUCCESSFULLY!")

# ------------------------------------------------
# 14. STOP SPARK
# ------------------------------------------------

spark.stop()
