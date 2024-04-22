# Comscore-api-dag

Comscore is a third party service that is responsible for verifying our measurements of things like page views, and provides us with marketing data such as market share ownership and whatnot. Essentially it is a way for us to be able to back up our claims of "We're the most popular real estate website" without making that claim based on our own data.

This process is used to extract the reports (local_market,mobile_apps,mobile_web,desktop,multi_platform) from the comscore api and store it to Datalake.

### Goals and Requirements
- Insights can be drawn based on comscore reports/datasets we generated from the DAG. We can visualize these reports using tools like Tableau, PowerBI.

- The goal of this project is to extract the reports monthly (local_market,mobile_apps,mobile_web,desktop,multi_platform) from comscore api and store it to Data Lake.
