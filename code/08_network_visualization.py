
# ================================================================
# PROJECT 63: ASTRONOMY COLLABORATION NETWORK ANALYSIS
# TASK 6: NETWORK VISUALIZATION
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

OUTPUT_FOLDER = (
    r"C:\Users\Devyansh verma\OneDrive\Desktop"
    r"\INT315_Astr\outputs"
)

CHECKPOINT_PATH = (
    r"C:\Users\Devyansh verma\OneDrive\Desktop"
    r"\INT315_Astr\checkpoints"
)

os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(CHECKPOINT_PATH, exist_ok=True)

# Configure Java 17 locally
os.environ["JAVA_HOME"] = (
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.21.9-hotspot"
)

os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
)

os.environ.pop("SPARK_HOME", None)

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

import networkx as nx
import matplotlib.pyplot as plt

# ------------------------------------------------
# 3. INITIALIZE SPARK
# ------------------------------------------------

spark = SparkSession.builder \
    .appName("AstroPh_Network_Visualization") \
    .master("local[2]") \
    .config(
        "spark.jars.packages",
        "io.graphframes:graphframes-spark4_2.13:0.12.3"
    ) \
    .config("spark.pyspark.python", PYTHON_PATH) \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

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
# 7. CALCULATE DEGREE OF EACH RESEARCHER
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

degree_df = vertices_df.join(
    degree_df,
    on="id",
    how="left"
).fillna({"degree": 0})

# ------------------------------------------------
# 8. SELECT TOP 30 RESEARCHERS
# ------------------------------------------------

print("\nSelecting top 30 researchers by degree...")

top_researchers = degree_df.orderBy(
    desc("degree"),
    asc("id")
).limit(30)

top_researchers.show(30, truncate=False)

top_ids = [
    row["id"]
    for row in top_researchers.select("id").collect()
]

print("Selected researchers:", len(top_ids))

# ------------------------------------------------
# 9. SELECT COLLABORATIONS BETWEEN TOP 30
# ------------------------------------------------

print("\nFinding collaborations between selected researchers...")

top_edges_df = edges_df.filter(
    col("src").isin(top_ids) &
    col("dst").isin(top_ids)
)

top_edges = top_edges_df.collect()

print("Collaborations among top 30:", len(top_edges))

# ------------------------------------------------
# 10. CREATE NETWORKX GRAPH
# ------------------------------------------------

print("\nCreating NetworkX graph...")

G = nx.Graph()

# Add the top 30 researchers as nodes
for row in top_researchers.collect():
    G.add_node(
        row["id"],
        degree=row["degree"]
    )

# Add collaboration edges
for row in top_edges:
    G.add_edge(
        row["src"],
        row["dst"]
    )

print("NetworkX graph created successfully!")

print("Number of nodes:", G.number_of_nodes())
print("Number of edges:", G.number_of_edges())

# ------------------------------------------------
# 11. VISUALIZE NETWORK
# ------------------------------------------------

print("\nGenerating network visualization...")

plt.figure(figsize=(16, 12))

# Arrange nodes using spring layout
pos = nx.spring_layout(
    G,
    seed=42,
    k=0.8,
    iterations=100
)

# Node size is based on original degree
node_sizes = [
    G.nodes[node]["degree"] * 8
    for node in G.nodes()
]

# Draw collaboration edges
nx.draw_networkx_edges(
    G,
    pos,
    width=1.0,
    alpha=0.5
)

# Draw researchers
nx.draw_networkx_nodes(
    G,
    pos,
    node_size=node_sizes,
    alpha=0.85
)

# Display researcher IDs
nx.draw_networkx_labels(
    G,
    pos,
    font_size=8,
    font_weight="bold"
)

plt.title(
    "AstroPh Collaboration Network - Top 30 Researchers",
    fontsize=16
)

plt.figtext(
    0.5,
    0.02,
    "Node size represents total researcher degree. "
    "Edges show collaborations between selected researchers.",
    ha="center",
    fontsize=10
)

plt.axis("off")
plt.tight_layout()

# ------------------------------------------------
# 12. SAVE VISUALIZATION
# ------------------------------------------------

OUTPUT_PATH = os.path.join(
    OUTPUT_FOLDER,
    "AstroPh_Top30_Network.png"
)

plt.savefig(
    OUTPUT_PATH,
    dpi=300,
    bbox_inches="tight"
)

print("\nVisualization saved successfully!")
print("Image location:", OUTPUT_PATH)

plt.show()

# ------------------------------------------------
# 13. FINAL SUMMARY
# ------------------------------------------------

print("\n" + "=" * 60)
print("NETWORK VISUALIZATION SUMMARY")
print("=" * 60)

print("Original researchers:", vertices_df.count())
print("Original undirected edges:", edges_df.count())
print("Researchers visualized:", G.number_of_nodes())
print("Collaborations visualized:", G.number_of_edges())

print("\nTASK 6 COMPLETED SUCCESSFULLY!")

# ------------------------------------------------
# 14. STOP SPARK
# ------------------------------------------------

spark.stop()
