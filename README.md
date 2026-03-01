<h1>📊 Real-Time Crypto Analytics Platform</h1>

<h2>🚀 Overview</h2>
<p>
This project implements a <strong>production-style real-time data engineering pipeline</strong> 
that ingests cryptocurrency market data, processes it using 
<strong>Kafka and Spark Structured Streaming</strong>, and stores analytics-ready datasets 
using <strong>Delta Lake</strong> following a <strong>Medallion Architecture (Bronze → Silver → Gold)</strong>.
</p>

<p>
The goal of this project is to simulate a real-world streaming data platform with:
</p>

<ul>
  <li>Fault tolerance</li>
  <li>Exactly-once processing</li>
  <li>Schema evolution handling</li>
  <li>Watermarking for late data</li>
  <li>Aggregation pipelines</li>
  <li>Production-grade repository structure</li>
  <li>Version control discipline</li>
</ul>

<hr>

<h2>🏗 Architecture</h2>

<pre>
External Crypto API
        ↓
Kafka (Event Backbone)
        ↓
Spark Structured Streaming
        ↓
Delta Lake
   ├── Bronze (Raw Data)
   ├── Silver (Cleaned & Validated)
   └── Gold (Aggregated Analytics)
</pre>

<hr>

<h2>🧱 Medallion Architecture</h2>

<h3>🥉 Bronze Layer</h3>
<ul>
  <li>Raw immutable events</li>
  <li>Minimal transformation</li>
  <li>Partitioned by event date</li>
  <li>Supports replay & audit</li>
</ul>

<h3>🥈 Silver Layer</h3>
<ul>
  <li>Schema enforcement</li>
  <li>Type casting</li>
  <li>Deduplication</li>
  <li>Watermarking for late data</li>
  <li>Data quality validation</li>
</ul>

<h3>🥇 Gold Layer</h3>
<ul>
  <li>1-minute windowed price aggregations</li>
  <li>Moving averages</li>
  <li>Volatility calculations</li>
  <li>Market ranking metrics</li>
</ul>

<hr>

<h2>⚙️ Tech Stack</h2>

<ul>
  <li><strong>Apache Kafka (KRaft mode)</strong> – Event streaming backbone</li>
  <li><strong>Apache Spark 3.5 (Structured Streaming)</strong> – Stream processing</li>
  <li><strong>Delta Lake 3.x</strong> – ACID storage layer</li>
  <li><strong>Python 3.10</strong> – Producer & orchestration</li>
  <li><strong>Git + GitHub</strong> – Version control & project management</li>
</ul>

<hr>

<h2>📂 Project Structure</h2>

<pre>
crypto-streaming-project/
│
├── producer/              # Kafka producer (API ingestion)
├── spark/                 # Bronze, Silver, Gold streaming jobs
├── config/                # YAML configuration files
├── tests/                 # Unit tests for transformations
├── docs/                  # Documentation
├── delta/                 # Delta tables (gitignored)
├── checkpoints/           # Spark checkpoints (gitignored)
├── .gitignore
└── README.md
</pre>

<hr>

<h2>🔄 Data Flow Explanation</h2>

<ol>
  <li>A producer polls a public crypto API.</li>
  <li>Data is transformed into a canonical event schema.</li>
  <li>Events are published to a Kafka topic.</li>
  <li>Spark consumes from Kafka using Structured Streaming.</li>
  <li>Data is written to Delta Bronze.</li>
  <li>Silver layer applies cleaning, watermarking, deduplication.</li>
  <li>Gold layer performs window-based aggregations for analytics.</li>
</ol>

<hr>

<h2>🔐 Reliability & Production Considerations</h2>

<ul>
  <li>Exactly-once semantics via Spark checkpointing + Delta transactions</li>
  <li>Kafka offset tracking</li>
  <li>Partition strategy optimization</li>
  <li>Schema evolution handling</li>
  <li>Config-driven architecture</li>
  <li>Feature branch Git workflow</li>
</ul>

<hr>

<h2>📈 Future Enhancements</h2>

<ul>
  <li>Dockerized deployment (Kafka + Spark stack)</li>
  <li>CI/CD integration</li>
  <li>Dashboard integration (Superset / Power BI)</li>
  <li>Data quality framework integration</li>
  <li>Prometheus metrics monitoring</li>
  <li>Cloud deployment (Azure / AWS)</li>
</ul>

<hr>

<h2>🎯 Purpose of This Project</h2>

<p>
This project demonstrates the design and implementation of a real-time streaming data platform 
aligned with modern data engineering best practices.
</p>

<p>
It showcases:
</p>

<ul>
  <li>Streaming architecture design</li>
  <li>Fault tolerance mechanisms</li>
  <li>Distributed systems understanding</li>
  <li>Delta Lake optimization strategies</li>
  <li>Engineering discipline via Git workflows</li>
</ul>

<hr>

<h2>🔧 How to Run (To Be Updated as We Build)</h2>

<ol>
  <li>Start Kafka</li>
  <li>Run producer</li>
  <li>Launch Spark streaming jobs</li>
  <li>Query Delta tables</li>
</ol>

<hr>

<h2>👤 Author</h2>

<p>
Naman Agarwal<br>
</p>
