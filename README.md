# BA882_Project <br>
## Predictive Inventory Intelligence Platform<br>
-Aaryan Bammi, Achinthya Sreedhar, Amisha Kelkar, Chenhui Shen, Yucheng He<br>
### Introduction: 
  The Predictive Inventory Intelligence Platform automates the process of collecting, transforming, and analyzing retail inventory and availability data to support proactive, data-driven stock management. Built for Target’s retail ecosystem, the platform identifies potential stockout risks in advance and translates them into actionable insights for operations teams. It leverages a modern data architecture powered by MotherDuck, Google Cloud Functions, and Astronomer (Airflow) to orchestrate ingestion, transformation, and analytics across multiple datasets.

The platform’s foundation was established through a fully automated ingestion layer that connects to Target’s public APIs for product search, store information, and fulfillment details, focusing on the Boston metropolitan area for localized analysis. Data extracted from these endpoints is cleaned and structured within a Bronze → Silver → Gold architecture in MotherDuck. Subsequent enhancements introduced synthetic data generation to overcome API limits, using SDV (Synthetic Data Vault) and custom Python logic to simulate realistic product and pricing data. Additional enrichment came from integrating Google Trends via the PyTrends API, allowing the system to capture consumer demand signals tied to seasonal search terms such as Christmas decor and winter jackets.

The unified dataset powers machine learning models that predict potential stockouts by analyzing historical pricing, fulfillment, and demand relationships. The final stage extends this predictive capability through LLMOps integration, enabling natural-language alerts that communicate upcoming stock risks and insights directly to decision-makers. Together, these components form a scalable, intelligent platform capable of transforming raw retail data into actionable, interpretable predictions.

### Data Tables
The data that is used in the project comes from the following tables- <br>
● **STORE_DATA** - Defines store identifiers and attributes. <br>
● **TARGET_PRODUCTS_SEARCH** - Captures product metadata and availability from Target’s API. <br>
● **TARGET_PRODUCTS_FULFILLMENT** - Tracks delivery status and inventory availability.<br>
● **TARGET_PRODUCT_PRICE_DETAILS** - Stores synthetic store-level pricing and promotions. <br>
● **TARGET_PRODUCTS_SEARCH** - Captures product metadata and availability from Target’s API. <br>

# Predictive Inventory Intelligence Platform

## Introduction
The Predictive Inventory Intelligence Platform automates the ingestion, transformation, and analysis of Target retail data to enable proactive, data-driven stock management. Built for Target’s retail ecosystem, the platform identifies potential stockout risks and converts raw data into actionable analytical insights. It leverages a modern, scalable stack including MotherDuck, Google Cloud Functions, and Astronomer (Airflow) to coordinate ingestion pipelines, transformation workflows, machine-learning tasks, and LLM-based alerting.

The platform initially ingests Target’s public APIs, including product search, store metadata, and fulfillment availability, focusing on the Boston metropolitan region. Data is processed through a structured Bronze → Silver → Gold architecture within MotherDuck. To overcome API rate limits and enrich the dataset, synthetic data generation (SDV and custom Python logic) is incorporated to produce realistic store-level pricing and discount patterns. Additional demand context is introduced through Google Trends, using PyTrends to collect seasonal and category-specific search interest signals (e.g., holiday decor, winter apparel).

The unified dataset serves as the foundation for machine-learning models that estimate next-day stockout risk based on pricing, availability behaviors, fulfillment patterns, and demand signals. The final stage integrates LLMOps to translate predictions into natural-language operational insights. A Google Cloud Function formats model outputs and passes them to Gemini, which generates summary alerts posted directly to Slack for real-time decision support.

Together, these layers form a comprehensive inventory intelligence platform capable of transforming scattered retail data sources into automated predictions and insights.

---

## Data Tables
The project uses the following core tables within MotherDuck:

● **STORE_DATA** - Defines store identifiers and attributes. <br>
● **TARGET_PRODUCTS_SEARCH** - Captures product metadata and availability from Target’s API. <br>
● **TARGET_PRODUCTS_FULFILLMENT** - Tracks delivery status and inventory availability.<br>
● **TARGET_PRODUCT_PRICE_DETAILS** - Stores synthetic store-level pricing and promotions. <br>
● **TARGET_PRODUCTS_SEARCH** - Captures product metadata and availability from Target’s API. <br>


---

## Pipeline Architecture

### Ingestion
Ingestion is automated using a combination of Cloud Functions and scheduled Airflow DAGs. These components retrieve data from:
- Target Product Search API  
- Store Metadata API  
- Fulfillment Availability API  
- PyTrends (Google Trends)

Raw JSON payloads are written to Bronze tables with time-stamped partitions for reproducibility.

### Transformation (Bronze → Silver → Gold)
Data undergoes normalization, schema alignment, de-duplication, enrichment, and aggregation.
Key operations include:
- joining fulfillment and product attributes,
- integrating synthetic pricing data,
- merging Google Trends demand signals,
- creating ML-ready features (lag variables, price deltas, availability shifts).

Gold tables serve as the final analytical layer used for supervised modeling and alert generation.

### Synthetic Data Generation
Synthetic pricing and promotion data is built using:
- SDV SingleTableSynthesizer
- CTGAN or GaussianCopula models
- Custom logic to reflect realistic price movements and store-specific variations

This step addresses API limitations and enhances model robustness by providing sufficient variability.

### Machine Learning
Machine-learning models predict next-day stockout probability using:
- historical fulfillment trends  
- product and store features  
- pricing volatility  
- demand trends (Google search interest)

Outputs are written to MotherDuck Gold prediction tables.

### LLMOps Integration
A dedicated Google Cloud Function retrieves predictions and metadata, generates structured prompts, and calls the Gemini API. Gemini produces concise, human-readable summaries highlighting:
- which stores/products are at risk,
- predicted probability of stockout,
- contributing factors such as demand spikes or fulfillment delays.

These summaries are posted automatically to Slack through a bot integration.

---

## Technology Stack
- MotherDuck / DuckDB  
- Google Cloud Functions  
- Astronomer / Apache Airflow  
- Python  
- SDV (Synthetic Data Vault)  
- scikit-learn, XGBoost  
- Google Gemini API for LLM-based alerting  
- Slack Bot API  

---

## Future Improvements
- Incorporation of real-time ingestion streams  
- Store-level optimization and replenishment recommendations  
- Fine-tuned domain-specific LLMs for retail operations  
- Multi-region scaling and cross-store aggregation modeling 

