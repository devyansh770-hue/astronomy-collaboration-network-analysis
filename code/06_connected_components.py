
# ================================================================
# PROJECT 63: ASTRONOMY COLLABORATION NETWORK ANALYSIS
# TASK 4: CONNECTED COMPONENTS ANALYSIS
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

# Configure Java 17 locally for this project
os.environ["JAVA_HOME"] = (
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.21.9-hotspot"
)

os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
)

# Avoid conflict with the existing Spark 3.5.3 installation
os.environ.pop("SPARK_HOME", None)

# Use the Python interpreter running Spyder
PYTHON_PATH = sys.executable
os.environ["PYSPARK_PYTHON"] = PYTHON_PATH

print("Python executable:", PYTHON_PATH)

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
    coalesce
)

from graphframes import GraphFrame

# ------------------------------------------------
# 3. INITIALIZE SPARK
# ------------------------------------------------

spark = SparkSession.builder \
    .appName("AstroPh_Connected_Components") \
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

print("\nSpark Version:", spark.version)
print("Checkpoint directory configured.")

# ------------------------------------------------
# 4. LOAD ORIGINAL DATASET
# ------------------------------------------------

print("\nLoading AstroPh dataset...")

raw_df = spark.read.text(DATASET_PATH)

# Remove comment lines and blank lines
data_df = raw_df.filter(
    (~trim(col("value")).startswith("#")) &
    (trim(col("value")) != "")
)

# Split each line into source and destination IDs
split_df = data_df.withColumn(
    "parts",
    split(trim(col("value")), r"\s+")
)

# Keep valid rows containing at least two values
valid_df = split_df.filter(
    col("parts").getItem(0).cast("long").isNotNull() &
    col("parts").getItem(1).cast("long").isNotNull()
)

# Extract researcher IDs
parsed_df = valid_df.select(
    col("parts").getItem(0).cast("long").alias("src"),
    col("parts").getItem(1).cast("long").alias("dst")
)

print("Valid edge records:", parsed_df.count())

# ------------------------------------------------
# 5. CONSTRUCT VERTICES
# ------------------------------------------------

# Include endpoints from all valid records, including
# self-loops, so no researcher is accidentally lost.

vertices_df = (
    parsed_df.select(col("src").alias("id"))
    .union(parsed_df.select(col("dst").alias("id")))
    .distinct()
)

print("Total researchers:", vertices_df.count())

# ------------------------------------------------
# 6. CONSTRUCT UNDIRECTED EDGES
# ------------------------------------------------

# Remove self-loops
non_self_edges = parsed_df.filter(
    col("src") != col("dst")
)

# Canonical representation of undirected edges
edges_df = non_self_edges.select(
    least(col("src"), col("dst")).alias("src"),
    greatest(col("src"), col("dst")).alias("dst")
).distinct()

print("Total undirected edges:", edges_df.count())

# ------------------------------------------------
# 7. CREATE GRAPHFRAME
# ------------------------------------------------

print("\nConstructing GraphFrame...")

g = GraphFrame(vertices_df, edges_df)

print("GraphFrame created successfully!")

# ------------------------------------------------
# 8. CALCULATE CONNECTED COMPONENTS
# ------------------------------------------------

print("\nCalculating connected components...")

components_df = g.connectedComponents()

# Each researcher gets a component ID
print("Connected components calculated successfully!")

# ------------------------------------------------
# 9. COUNT RESEARCHERS IN EACH COMPONENT
# ------------------------------------------------

component_sizes = components_df.groupBy(
    "component"
).agg(
    count("*").alias("researcher_count")
).orderBy(
    desc("researcher_count"),
    col("component")
)

# Total number of connected components
total_components = component_sizes.count()

# Largest component size
largest_component = component_sizes.agg(
    {"researcher_count": "max"}
).first()[0]

# Number of components containing only one researcher
single_researcher_components = component_sizes.filter(
    col("researcher_count") == 1
).count()

# Components other than the largest component
other_components = total_components - 1

# ------------------------------------------------
# 10. DISPLAY RESULTS
# ------------------------------------------------

print("\n" + "=" * 60)
print("CONNECTED COMPONENTS ANALYSIS RESULTS")
print("=" * 60)

print("\nTotal researchers:", vertices_df.count())
print("Total undirected edges:", edges_df.count())

print("\nTotal connected components:", total_components)

print(
    "Researchers in largest component:",
    largest_component
)

print(
    "Components other than the largest:",
    other_components
)

print(
    "Single-researcher components:",
    single_researcher_components
)

# ------------------------------------------------
# 11. DISPLAY SIZE OF EVERY COMPONENT
# ------------------------------------------------

print("\n" + "=" * 60)
print("NUMBER OF RESEARCHERS IN EACH COMPONENT")
print("=" * 60)

component_sizes.show(
    total_components,
    truncate=False
)

# ------------------------------------------------
# 12. FINAL VERIFICATION
# ------------------------------------------------

total_researchers_in_components = component_sizes.agg(
    {"researcher_count": "sum"}
).first()[0]

print("\n" + "=" * 60)
print("FINAL VERIFICATION")
print("=" * 60)

print(
    "Researchers in original graph:",
    vertices_df.count()
)

print(
    "Researchers across all components:",
    total_researchers_in_components
)

if vertices_df.count() == total_researchers_in_components:
    print("VERIFICATION SUCCESSFUL!")
else:
    print("VERIFICATION FAILED!")

print("\nTASK 4 COMPLETED!")

# ------------------------------------------------
# 13. STOP SPARK
# ------------------------------------------------

spark.stop()
