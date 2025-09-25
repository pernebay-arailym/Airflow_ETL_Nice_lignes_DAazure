# 🚍 Real-Time Public Transport ETL with Airflow, DuckDB & Tableau  

![Airflow DAG](https://img.shields.io/badge/Airflow-2.9-blue?logo=apacheairflow)  
![DuckDB](https://img.shields.io/badge/DuckDB-0.9-yellow?logo=duckdb)  
![Tableau](https://img.shields.io/badge/Tableau-Dashboard-orange?logo=tableau)  
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)  

---

## 📌 Project Overview  
This project is part of a **2-week Data Engineering sprint**.  
The goal: build an **end-to-end ETL pipeline** that extracts **real-time GTFS transport data** from Nice, orchestrates with **Apache Airflow**, processes with **DuckDB**, and visualizes with **Tableau**.  

It enables monitoring of public transport in **real-time**: vehicle positions, trip updates, late arrivals, busiest stops, accessibility, and more.  

---

## 🎯 Objectives  
- ✅ Learn & apply **Airflow orchestration**  
- ✅ Automate **real-time ETL** with APIs  
- ✅ Store data efficiently in **DuckDB**  
- ✅ Build a **dashboard** with actionable KPIs  
- ✅ Deliver a **production-like solution**  

---

---

## ⚙️ Tech Stack  

| Tool          | Purpose                          |
|---------------|----------------------------------|
| **Apache Airflow** | DAG orchestration & scheduling |
| **Docker**        | Containerized environment       |
| **DuckDB**        | In-process OLAP database        |
| **Python**        | Data extraction & processing    |
| **Tableau**       | Visualization dashboard         |
| **GTFS-RT API**   | Real-time transport data        |

---

## 🚦 Workflow Timeline  

📅 **Day 1–2** → Environment setup (Airflow + Docker)  
📅 **Day 3–4** → Extraction of GTFS feeds (vehicles & trips)  
📅 **Day 5–6** → Orchestration & DuckDB transformations  
📅 **Day 7** → Data cleaning & daily scheduling  
📅 **Day 8–9** → Dashboard design (Tableau)  
📅 **Day 10** → Finalization & presentation  

---
## 📊 Dashboard (Tableau) 
#### https://public.tableau.com/app/profile/arailym.pernebay/viz/Lignes_dAzure_visualization/Lignes_DAzure_analysis

Final dashboard includes:

- 🔹 KPIs (on top)
- 🚌 Total Trips Today
- ♿ Wheelchair Accessibility %
- 🕒 Peak Usage → e.g. 898 trips at 16h

🔹 Visualizations

- 🌍 Vehicle Movement Map
- 📍 Stop Hotspots Map
- 🥧 Top Stops Pie Chart

---
## ✅ Key Learnings

- How to **design and orchestrate DAGs** in Airflow
- Containerization with **Docker Compose**
- Querying and modeling with **DuckDB**
- Building **interactive dashboards** from APIs
- Managing **real-time ETL pipelines**

---

## 👩‍💻 Author

#### Arailym PERNEBAY
- 📅 Duration: 2 weeks
- 🏫 Simplon Data Engineering Bootcamp
