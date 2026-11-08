
# ================================================================
# INT315 PROJECT
# ASTRONOMY COLLABORATION NETWORK ANALYSIS
# Dataset: ca-AstroPh
#
# PHASE 1: Spark Initialization
# PHASE 2: Dataset Loading
# PHASE 3: Data Cleaning
# PHASE 4: Duplicate Edge Analysis
# PHASE 5: Undirected Edge Preparation
# ================================================================


# ================================================================
# 1. CONFIGURE JAVA 17 FOR THIS PROJECT ONLY
# ================================================================

import os

os.environ["JAVA_HOME"] = (
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.21.9-hotspot"
)

os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
)

# Prevent conflict with existing Spark 3.5.3
os.environ.pop("SPARK_HOME", None)


# ================================================================
# 2. IMPORT PYSPARK LIBRARIES
# ================================================================

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    trim,
    split,
    regexp_replace,
    size,
    least,
    greatest
)


# ================================================================
# 3. CREATE SPARK SESSION
# ================================================================

spark = SparkSession.builder \
    .appName("AstroPh_Network_Analysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n========================================")
print("SPARK INITIALIZED SUCCESSFULLY!")
print("Spark Version:", spark.version)
print("========================================")


# ================================================================
# 4. DEFINE DATASET LOCATION
# ================================================================

DATA_PATH = (
    r"C:\Users\Devyansh verma\OneDrive\Desktop"
    r"\INT315_Astr\dataset\ca-AstroPh_edges.txt"
)

print("\nDataset Location:")
print(DATA_PATH)


# ================================================================
# 5. LOAD ORIGINAL DATASET
# ================================================================

print("\nLoading Astronomy Collaboration Dataset...")

raw_df = spark.read.text(DATA_PATH)

print("Dataset loaded successfully!")


# ================================================================
# 6. REMOVE COMMENTS AND EMPTY LINES
# ================================================================

data_lines = raw_df.filter(
    (~trim(col("value")).startswith("#")) &
    (trim(col("value")) != "")
)

print("Comments and empty lines removed.")


# ================================================================
# 7. SPLIT RESEARCHER IDs INTO SOURCE AND DESTINATION
# ================================================================

parsed_df = data_lines.withColumn(
    "parts",
    split(
        trim(regexp_replace(col("value"), r"\s+", " ")),
        " "
    )
)


# ================================================================
# 8. IDENTIFY VALID EDGE RECORDS
# ================================================================

valid_df = parsed_df.filter(
    size(col("parts")) == 2
)

# Extract source and destination IDs
edges_df = valid_df.select(
    col("parts").getItem(0).cast("long").alias("src"),
    col("parts").getItem(1).cast("long").alias("dst")
)

# Remove records containing invalid numeric IDs
invalid_numeric_df = edges_df.filter(
    col("src").isNull() | col("dst").isNull()
)

invalid_numeric_count = invalid_numeric_df.count()

clean_edges = edges_df.filter(
    col("src").isNotNull() & col("dst").isNotNull()
)


# ================================================================
# 9. COUNT ORIGINAL EDGE RECORDS
# ================================================================

total_original_records = raw_df.count()

total_data_lines = data_lines.count()

total_valid_records = clean_edges.count()


# ================================================================
# 10. IDENTIFY SELF-LOOPS
# ================================================================

# Preserve all valid records before removing self-loops
all_valid_edges = clean_edges

self_loop_df = all_valid_edges.filter(
    col("src") == col("dst")
)

self_loop_count = self_loop_df.count()

# Count distinct researchers involved in self-loops
self_loop_researchers = self_loop_df.select(
    col("src").alias("id")
).distinct().count()


# ================================================================
# 11. CONSTRUCT ALL UNIQUE RESEARCHERS
# ================================================================

# IMPORTANT:
# Build vertices BEFORE removing self-loops.
# This preserves researchers who only appear in self-loops.

vertices_df = (
    all_valid_edges.select(
        col("src").alias("id")
    )
    .union(
        all_valid_edges.select(
            col("dst").alias("id")
        )
    )
    .distinct()
)

total_nodes = vertices_df.count()


# ================================================================
# 12. REMOVE SELF-LOOPS FOR GRAPH ANALYSIS
# ================================================================

non_self_edges = all_valid_edges.filter(
    col("src") != col("dst")
)


# ================================================================
# 13. REMOVE DUPLICATE DIRECTED EDGES
# ================================================================

unique_directed_df = non_self_edges.dropDuplicates(
    ["src", "dst"]
)

unique_directed_count = unique_directed_df.count()


# ================================================================
# 14. CONSTRUCT UNDIRECTED GRAPH EDGES
# ================================================================

undirected_df = unique_directed_df.select(

    least(
        col("src"),
        col("dst")
    ).alias("src"),

    greatest(
        col("src"),
        col("dst")
    ).alias("dst")

).dropDuplicates(["src", "dst"])

total_undirected_edges = undirected_df.count()


# ================================================================
# 15. VERIFY ORIGINAL DATASET EDGE COUNT
# ================================================================

# Count unique undirected pairs INCLUDING self-loops.
# This is for comparison with the original dataset statistics.

reference_edges_df = all_valid_edges.select(
    least(col("src"), col("dst")).alias("src"),
    greatest(col("src"), col("dst")).alias("dst")
).dropDuplicates(["src", "dst"])

reference_edge_count = reference_edges_df.count()


# ================================================================
# 16. DISPLAY FINAL DATA QUALITY REPORT
# ================================================================

print("\n")
print("=" * 60)
print("          FINAL ASTRONOMY DATASET REPORT")
print("=" * 60)

print("\nORIGINAL DATASET")
print("Original Records:", total_original_records)
print("Data Records:", total_data_lines)
print("Valid Numeric Records:", total_valid_records)

print("\nDATA CLEANING")
print("Invalid Numeric Records:", invalid_numeric_count)
print("Self-Loops:", self_loop_count)
print("Researchers in Self-Loops:", self_loop_researchers)
print("Unique Directed Edges:", unique_directed_count)

print("\nGRAPH STATISTICS")
print("Unique Researchers (Nodes):", total_nodes)
print("Undirected Edges (Without Self-Loops):",
      total_undirected_edges)

print("Unique Undirected Edges (Including Self-Loops):",
      reference_edge_count)


# ================================================================
# 17. DISPLAY GRAPH DATA
# ================================================================

print("\nFIRST 10 UNDIRECTED EDGES")
undirected_df.show(10, truncate=False)

print("\nFIRST 10 RESEARCHERS")
vertices_df.show(10, truncate=False)


# ================================================================
# 18. FINAL VERIFICATION
# ================================================================

print("\n")
print("=" * 60)
print("                VERIFICATION")
print("=" * 60)

print("Expected Researchers: 18772")
print("Actual Researchers:", total_nodes)

print("Expected Original Undirected Edges: 198110")
print("Actual Original Undirected Edges:", reference_edge_count)

if total_nodes == 18772:
    print("\nNODE COUNT VERIFIED!")
else:
    print("\nNODE COUNT NEEDS INVESTIGATION!")

if reference_edge_count == 198110:
    print("ORIGINAL EDGE COUNT VERIFIED!")
else:
    print("EDGE COUNT NEEDS INVESTIGATION!")

print("\nGRAPH EDGE COUNT AFTER SELF-LOOP REMOVAL:",
      total_undirected_edges)

print("\nDATASET VERIFICATION COMPLETED!")
print("=" * 60)
