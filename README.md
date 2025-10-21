# BA882_Project
### Introduction: 
  The Predictive Inventory Intelligence Platform is designed to automate the process of collecting, 
transforming, and analyzing retail inventory and availability data to support proactive decision-making for 
stock management. The platform aims to identify potential stock-out risks in advance and translate them 
into actionable insights for retail operations teams. 
  In the Phase 1 implementation, Our team developed a fully functional cloud-based data pipeline 
leveraging MotherDuck, Astronomer (Airflow), and Apache Superset. The data ingestion layer is powered 
by three RapidAPI endpoints—product_search, nearby_stores, and product_fulfillment—which retrieve 
real-time product and store-level information from retailers. To maintain focus and data consistency, the 
project scope was limited to Target stores within the Boston metropolitan area, allowing the team to 
analyze local product availability, pricing, and fulfillment trends in a controlled geographic context. Also, 
due to the restriction of API calls, the tables are currently being refreshed once a week but can be enabled 
for real-time updation in the future. 
  The ingested data is stored in MotherDuck, structured into three logical layers—raw_data, 
ML_data, and BI_data—to separate raw ingestion, processed datasets, and business intelligence outputs. 
Pipeline orchestration is managed through Astronomer (Airflow), where custom DAGs handle API 
requests, data cleaning, and warehouse updates on a scheduled basis. The resulting datasets are visualized 
through Apache Superset, which provides interactive dashboards for exploring store coverage, product 
stock levels, and potential fulfillment gaps. 

### ER Diagram 
This ERD models how Target store data, product search results, and fulfillment status are connected. 
● **STORE_DATA** defines each physical store, including identifiers and location details. 
● **TARGET_PRODUCTS_SEARCH** records products captured from store-level search pages, 
including product metadata and load timestamp. 
● **TARGET_PRODUCTS_FULFILLMENT** tracks the stock and delivery status of those products 
per store, including availability, shipping options, promise dates, etc.  

<img width="1600" height="1481" alt="image" src="https://github.com/user-attachments/assets/8113664c-f489-40dc-affc-ed2cac811f78" />
