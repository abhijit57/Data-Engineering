# Real Time Crypto Streaming/Analysis Using Kafka and AWS End to End Project

The architecture of the project is as shown below:

![alt text](https://github.com/abhijit57/Data-Engineering/blob/main/Stock%20Market%20RTA_Kafka_E2E%20Project/Architecture.jpg)

Here, I used Wazirx python API (https://docs.wazirx.com/#public-rest-api-for-wazirx) to extract cryptocurrency data from Wazirx (Indiana cryptocurrency exchange). Using Python, the batch data was simulated as real-time streaming data and was sent to Kafka cluster on AWS EC2 instance. Each instance of streaming data was stored in Amazon S3 and a crawler was run on this data stored in S3 bucket to create an AWS Glue Data Catalog. Then, AWS Athena was used to analyze the data from the catalog.
