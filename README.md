# 🌌 Astronomy Collaboration Network Analysis (ca-AstroPh)
### Academic Project (INT315: Big Data & Graph Analytics)

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5%2B%20%2F%204.0-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![GraphFrames](https://img.shields.io/badge/GraphFrames-0.12.3-orange)](https://graphframes.github.io/graphframes/)
[![NetworkX](https://img.shields.io/badge/NetworkX-3.0%2B-black)](https://networkx.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 📌 Project Overview

This repository contains the end-to-end implementation of **Project 63: Astronomy Collaboration Network Analysis**, developed as an academic coursework project for **INT315**.

The project analyzes the large-scale **ArXiv AstroPhysics (ca-AstroPh)** co-authorship collaboration network from the Stanford Network Analysis Platform (SNAP). Using **Apache Spark**, **PySpark**, and **GraphFrames**, this project processes large-scale distributed graph data, computes network centrality measures, detects connected communities, identifies influential astronomers via **PageRank**, and visualizes key collaboration hubs.

---

## 📊 Dataset Description

The dataset covers scientific collaborations from the **ArXiv Astrophysics** section:
- **Source**: [Stanford SNAP ca-AstroPh Dataset](https://snap.stanford.edu/data/ca-AstroPh.html)
- **Nodes (Authors/Researchers)**: `18,772`
- **Edges (Collaborations)**: `396,160` (co-authored at least one research paper)
- **Graph Type**: Undirected, unweighted collaboration network

---

## 🛠️ Tech Stack & Prerequisites

- **Language**: Python 3.10+
- **Big Data Engine**: Apache Spark (PySpark)
- **Graph Processing**: GraphFrames (`io.graphframes:graphframes-spark4_2.13:0.12.3`)
- **Network Analysis & Visualization**: NetworkX, Matplotlib
- **Java Runtime**: OpenJDK 17 (Eclipse Adoptium)

---

## 📂 Project Architecture & Pipeline

The pipeline is organized in modular, sequential execution scripts inside the [`code/`](code/) directory:

```
INT315_Astr/
│
├── code/
│   ├── 01_data_loading.py            # Spark setup, raw data cleaning & edge normalization
│   ├── 02_graph_construction.py      # Undirected graph construction & schema verification
│   ├── 03_degree_centrality.py       # Distributed degree centrality calculation
│   ├── 04_top_researchers.py         # Identification of most collaborative researchers
│   ├── 05_graphframes_test.py        # GraphFrames package validation & environment test
│   ├── 06_connected_components.py    # Connected components analysis & giant component detection
│   ├── 07_pagerank.py                # PageRank computation for researcher influence ranking
│   └── 08_network_visualization.py   # Subgraph extraction & NetworkX visualization
│
├── dataset/
│   ├── ca-AstroPh_edges.txt          # Cleaned edge list
│   └── ca-AstroPh.txt.gz             # Compressed raw dataset
│
├── outputs/
│   └── AstroPh_Top30_Network.png     # Network visualization of top 30 key researchers
│
├── .gitignore                        # Git ignore rules (checkpoints, caches, temp files)
├── requirements.txt                  # Python dependencies
└── README.md                         # Project documentation
```

---

## 🔬 Key Tasks & Methodologies

### 1. Data Cleaning & Undirected Graph Normalization (`01`, `02`)
- Ingested raw tab-delimited co-authorship pairs with Spark DataFrames.
- Filtered comments, metadata, and self-loops.
- Enforced edge symmetry (`(u, v) == (v, u)`) using `least()` and `greatest()` functions to eliminate duplicate collaboration entries in undirected network space.

### 2. Degree Centrality & Top Collaborators (`03`, `04`)
- Measured the direct collaboration connectivity for all 18,772 researchers.
- Ranked researchers to identify core collaborative hubs in the astrophysics community.

### 3. Connected Components Analysis (`06`)
- Utilized GraphFrames' distributed connected component algorithm to segment the network.
- Verified the presence of the **Giant Connected Component (GCC)** encompassing the vast majority of astronomers, demonstrating the "small-world" nature of scientific research.

### 4. PageRank Influence Ranking (`07`)
- Applied the **PageRank** algorithm (damping factor $0.85$, $10$ iterations) over the graph.
- Unlike pure degree centrality (which only counts direct co-authors), PageRank uncovered authors whose collaborators are themselves highly influential.

### 5. High-Density Network Visualization (`08`)
- Extracted the induced subgraph of the top 30 most central astronomers.
- Rendered using NetworkX and Matplotlib with spring layout, node-size scaled by degree, and edge alpha transparency.

---

## 🖼️ Network Visualization

Below is the induced collaboration network of the **Top 30 Most Connected Astronomers**:

<p align="center">
  <img src="outputs/AstroPh_Top30_Network.png" alt="Top 30 Astronomers Collaboration Network" width="850"/>
</p>

---

## 🚀 Getting Started & Execution

### 1. Clone the Repository
```bash
git clone https://github.com/<your-username>/<your-repo-name>.git
cd <your-repo-name>
```

### 2. Set Up Virtual Environment & Dependencies
```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Ensure Java 17 is Installed
Spark requires Java 17. Verify your installation:
```bash
java -version
```

### 4. Run the Pipeline
Run the scripts in numerical order:
```bash
python code/01_data_loading.py
python code/02_graph_construction.py
python code/03_degree_centrality.py
python code/04_top_researchers.py
python code/06_connected_components.py
python code/07_pagerank.py
python code/08_network_visualization.py
```

---

## 💡 Key Findings & Insights

1. **Small-World Phenomenon**: The astrophysics research network displays strong clustering where the vast majority of researchers belong to a single giant connected component.
2. **Degree vs. PageRank**: Prolific researchers with the highest number of co-authors do not always hold the highest PageRank score, highlighting the difference between *quantity* of collaborations and *strategic influence* in citation/co-author networks.
3. **Scalability**: Utilizing PySpark and GraphFrames allowed distributed graph execution that scales seamlessly from local testing to distributed Spark clusters.

---

## 👤 Author

**Devyansh Verma**
- **Course**: INT315 (Big Data / Graph Analytics)
- **Email**: devyansh770@gmail.com
---

## 📜 Acknowledgements
- Dataset provided by **Stanford SNAP (Stanford Network Analysis Platform)**: J. Leskovec, J. Kleinberg, and C. Faloutsos.
- Apache Spark & GraphFrames open-source communities.

