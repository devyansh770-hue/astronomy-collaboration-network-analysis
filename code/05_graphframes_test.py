
# ================================================================
# GRAPHFRAMES TEST - CORRECTED VERSION
# ================================================================

import os
import sys

CHECKPOINT_PATH = (
    r"C:\Users\Devyansh verma\OneDrive\Desktop"
    r"\INT315_Astr\checkpoints"
)

os.makedirs(CHECKPOINT_PATH, exist_ok=True)
# 1. Configure Java 17
os.environ["JAVA_HOME"] = (
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.21.9-hotspot"
)

os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
)

os.environ.pop("SPARK_HOME", None)

# 2. Automatically detect Spyder's actual Python executable
PYTHON_PATH = sys.executable

print("Python executable:", PYTHON_PATH)
print("Python executable exists:", os.path.exists(PYTHON_PATH))

if not os.path.exists(PYTHON_PATH):
    raise FileNotFoundError("Spyder Python executable was not found!")

# Configure Spark workers
os.environ["PYSPARK_PYTHON"] = PYTHON_PATH

# 3. Import libraries
from pyspark.sql import SparkSession
from graphframes import GraphFrame

# 4. Initialize Spark
spark = SparkSession.builder \
    .appName("AstroPh_GraphFrames_Test") \
    .master("local[2]") \
    .config(
        "spark.jars.packages",
        "io.graphframes:graphframes-spark4_2.13:0.12.3"
    ) \
    .config("spark.pyspark.python", PYTHON_PATH) \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\nSpark Version:", spark.version)

# 5. Create test DataFrames using Spark itself.
# This avoids Python-list-based DataFrame creation.

vertices = spark.range(1, 5).selectExpr("id")

edges = spark.range(1, 3).selectExpr(
    "id AS src",
    "id + 1 AS dst"
)

# 6. Construct GraphFrame
g = GraphFrame(vertices, edges)

# 7. Verify GraphFrames
print("\nGraphFrames JVM dependency loaded successfully!")

print("Test vertices:", g.vertices.count())
print("Test edges:", g.edges.count())

# 8. Test connected components algorithm
print("\nTesting connected components...")

#================================
# Configure checkpoint directory for GraphFrames
spark.sparkContext.setCheckpointDir(CHECKPOINT_PATH)

print("Checkpoint directory configured successfully!")
#========================================
components = g.connectedComponents()

components.show(truncate=False)

print("\nGRAPHFRAMES TEST COMPLETED SUCCESSFULLY!")
