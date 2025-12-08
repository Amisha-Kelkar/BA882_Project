# BA882_Project <br>
## Predictive Inventory Intelligence Platform<br>
-Aaryan Bammi, Achinthya Sreedhar, Amisha Kelkar, Chenhui Shen, Yucheng He<br>

### Introduction
The Predictive Inventory Intelligence Platform automates the ingestion, transformation, and analysis of Target retail data to enable proactive, data-driven stock management. Built for Target’s retail ecosystem, the platform identifies potential stockout risks and converts raw data into actionable analytical insights. It leverages a modern, scalable stack including MotherDuck, Google Cloud Functions, and Astronomer (Airflow) to coordinate ingestion pipelines, transformation workflows, machine-learning tasks, and LLM-based alerting.

The platform initially ingests Target’s public APIs, including product search, store metadata, and fulfillment availability, focusing on the Boston metropolitan region. Data is processed through a structured Bronze → Silver → Gold architecture within MotherDuck. To overcome API rate limits and enrich the dataset, synthetic data generation (SDV and custom Python logic) is incorporated to produce realistic store-level pricing and discount patterns. Additional demand context is introduced through Google Trends, using PyTrends to collect seasonal and category-specific search interest signals (e.g., holiday decor, winter apparel).

The unified dataset serves as the foundation for machine-learning models that estimate next-day stockout risk based on pricing, availability behaviors, fulfillment patterns, and demand signals. The final stage integrates LLMOps to translate predictions into natural-language operational insights. A Google Cloud Function formats model outputs and passes them to Gemini, which generates summary alerts posted directly to Slack for real-time decision support. A Superset dashboard is built on top of the Gold data layer to enable interactive visualization of pricing trends, availability metrics, category-level performance, and stockout risk patterns across stores.

Together, these layers form a comprehensive inventory intelligence platform capable of transforming scattered retail data sources into automated predictions and insights.


### Data Tables
The project uses the following core tables within MotherDuck:

● **STORE_DATA** - Defines store identifiers and attributes. <br>
● **TARGET_PRODUCTS_SEARCH** - Captures product metadata and availability from Target’s API. <br>
● **TARGET_PRODUCTS_FULFILLMENT** - Tracks delivery status and inventory availability.<br>
● **TARGET_PRODUCT_PRICE_DETAILS** - Stores synthetic store-level pricing and promotions. <br>
● **TARGET_PRODUCTS_SEARCH** - Captures product metadata and availability from Target’s API. <br>
● **GOOGLE_TRENDS_DATA** – Search interest values for demand-related seasonal or category terms.  


### Pipeline Architecture

#### Ingestion
Ingestion is automated using a combination of Cloud Functions and scheduled Airflow DAGs. These components retrieve data from:
- Target Product Search API  
- Store Metadata API  
- Fulfillment Availability API  
- PyTrends (Google Trends)

Raw JSON payloads are written to Bronze tables with time-stamped partitions for reproducibility.

#### Transformation (Bronze → Silver → Gold)
Data undergoes normalization, schema alignment, de-duplication, enrichment, and aggregation.
Key operations include:
- joining fulfillment and product attributes,
- integrating synthetic pricing data,
- merging Google Trends demand signals,
- creating ML-ready features (lag variables, price deltas, availability shifts).

Gold tables serve as the final analytical layer used for supervised modeling and alert generation.

#### Synthetic Data Generation
Synthetic pricing and promotion data is built using:
- SDV SingleTableSynthesizer
- CTGAN or GaussianCopula models
- Custom logic to reflect realistic price movements and store-specific variations

This step addresses API limitations and enhances model robustness by providing sufficient variability.

#### Machine Learning
Machine-learning models predict next-day stockout probability using:
- historical fulfillment trends  
- product and store features  
- pricing volatility  
- demand trends (Google search interest)

Outputs are written to MotherDuck Gold prediction tables.

#### LLMOps Integration
A dedicated Google Cloud Function retrieves predictions and metadata, generates structured prompts, and calls the Gemini API. Gemini produces concise, human-readable summaries highlighting:
- which stores/products are at risk,
- predicted probability of stockout,
- contributing factors such as demand spikes or fulfillment delays.
- these summaries are posted automatically to Slack through a bot integration.

An additional LLM Agent layer enables conversational querying of the data pipeline and model outputs, allowing users to ask natural-language questions about stockout risks, trends, product performance, or store-level patterns. The agent retrieves relevant tables from MotherDuck, interprets query intent, and returns synthesized insights.


### Technology Stack
- MotherDuck / DuckDB  
- Google Cloud Functions  
- Astronomer / Apache Airflow  
- Python  
- SDV (Synthetic Data Vault)  
- scikit-learn, XGBoost  
- Apache Superset  
- Google Gemini API for LLM-based alerting and LLM agent functionality  
- Slack Bot API  


### Future Improvements
- Incorporation of real-time ingestion streams  
- Store-level optimization and replenishment recommendations  
- Fine-tuned domain-specific LLMs for retail operations  
- Multi-region scaling and cross-store aggregation modeling  
- Agent-enabled automated remediation workflows (e.g., creating replenishment tickets)

