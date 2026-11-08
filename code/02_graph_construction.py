
# ================================================================
# PROJECT 63: ASTRONOMY COLLABORATION NETWORK ANALYSIS
# TASK 1: CONSTRUCT AN UNDIRECTED GRAPH
# Dataset: ca-AstroPh
# ================================================================

import os

# 1. Configure Java 17 for this project
os.environ["JAVA_HOME"] = (
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.21.9-hotspot"
)

os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
)

# Avoid conflict with existing Spark 3.5.3
os.environ.pop("SPARK_HOME", None)


# 2. Import PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, split, regexp_replace,
    size, least, greatest
)


# 3. Create Spark Session
spark = SparkSession.builder \
    .appName("AstroPh_Graph_Construction") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\nSpark initialized successfully!")
print("Spark Version:", spark.version)


# 4. Dataset path
DATA_PATH = (
    r"C:\Users\Devyansh verma\OneDrive\Desktop"
    r"\INT315_Astr\dataset\ca-AstroPh_edges.txt"
)


# 5. Load dataset
print("\nLoading collaboration dataset...")

raw_df = spark.read.text(DATA_PATH)


# 6. Remove comments and empty lines
data_lines = raw_df.filter(
    (~trim(col("value")).startswith("#")) &
    (trim(col("value")) != "")
)


# 7. Extract researcher IDs
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


# 8. Construct vertices BEFORE removing self-loops
# This preserves all researchers, including isolated vertices.

vertices_df = (
    all_edges.select(col("src").alias("id"))
    .union(all_edges.select(col("dst").alias("id")))
    .distinct()
)


# 9. Remove self-loops for graph analysis
non_self_edges = all_edges.filter(
    col("src") != col("dst")
)


# 10. Remove duplicate directed records
unique_directed_edges = non_self_edges.dropDuplicates(
    ["src", "dst"]
)


# 11. Convert edges into undirected form
# Store the smaller researcher ID in src
# and the larger researcher ID in dst.

edges_df = unique_directed_edges.select(
    least(col("src"), col("dst")).alias("src"),
    greatest(col("src"), col("dst")).alias("dst")
).dropDuplicates(["src", "dst"])


# 12. Construct the graph data structure
# vertices_df represents graph vertices.
# edges_df represents graph edges.

total_vertices = vertices_df.count()
total_edges = edges_df.count()


# 13. Display graph information
print("\n" + "=" * 55)
print("       UNDIRECTED GRAPH CONSTRUCTION REPORT")
print("=" * 55)

print("\nVERTICES (RESEARCHERS)")
print("Total Vertices:", total_vertices)

print("\nEDGES (COLLABORATIONS)")
print("Total Undirected Edges:", total_edges)

print("\nFIRST 10 VERTICES")
vertices_df.show(10, truncate=False)

print("\nFIRST 10 EDGES")
edges_df.show(10, truncate=False)


# 14. Verify graph construction
print("\n" + "=" * 55)
print("              GRAPH VERIFICATION")
print("=" * 55)

if total_vertices == 18772:
    print("Vertex count verified successfully!")
else:
    print("Vertex count mismatch!")

if total_edges == 198050:
    print("Edge count verified successfully!")
else:
    print("Edge count mismatch!")

print("\nUNDIRECTED GRAPH CONSTRUCTION COMPLETED!")
print("=" * 55)
