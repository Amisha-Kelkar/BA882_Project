# BA882_Project <br>
## Predictive Inventory Intelligence Platform<br>
-Aaryan Bammi, Achinthya Sreedhar, Amisha Kelkar, Chenhui Shen, Yucheng He<br>
### Introduction: 
  The Predictive Inventory Intelligence Platform automates the process of collecting, transforming, and analyzing retail inventory and availability data to support proactive, data-driven stock management. Built for Target’s retail ecosystem, the platform identifies potential stockout risks in advance and translates them into actionable insights for operations teams. It leverages a modern data architecture powered by MotherDuck, Google Cloud Functions, and Astronomer (Airflow) to orchestrate ingestion, transformation, and analytics across multiple datasets.

The platform’s foundation was established through a fully automated ingestion layer that connects to Target’s public APIs for product search, store information, and fulfillment details, focusing on the Boston metropolitan area for localized analysis. Data extracted from these endpoints is cleaned and structured within a Bronze → Silver → Gold architecture in MotherDuck. Subsequent enhancements introduced synthetic data generation to overcome API limits, using SDV (Synthetic Data Vault) and custom Python logic to simulate realistic product and pricing data. Additional enrichment came from integrating Google Trends via the PyTrends API, allowing the system to capture consumer demand signals tied to seasonal search terms such as Christmas decor and winter jackets.

The unified dataset powers machine learning models that predict potential stockouts by analyzing historical pricing, fulfillment, and demand relationships. The final stage extends this predictive capability through LLMOps integration, enabling natural-language alerts that communicate upcoming stock risks and insights directly to decision-makers. Together, these components form a scalable, intelligent platform capable of transforming raw retail data into actionable, interpretable predictions.

### ER Diagram 
The data that is used in the project comes from the following tables- <br>
● **STORE_DATA** - Defines store identifiers and attributes. <br>
● **TARGET_PRODUCTS_SEARCH** - Captures product metadata and availability from Target’s API. <br>
● **TARGET_PRODUCTS_FULFILLMENT** - Tracks delivery status and inventory availability.<br>
● **TARGET_PRODUCT_PRICE_DETAILS** - Stores synthetic store-level pricing and promotions. <br>
● **TARGET_PRODUCTS_SEARCH** - Captures product metadata and availability from Target’s API. <br>
