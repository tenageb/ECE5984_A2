The project retrieves historical stock data for Microsoft(MSFT), Meta(META), and Netflix(NFLX) starting from January 1, 2005. 
It performs Exploratory Data Analysis(EDA) using Python and applies transformation to the dataset. 
The raw data is loaded into a data lake on an Amazon S3 bucket, while the transformed(clean) data is stored in a data warehouse, also on S3. 
The data ingestion process is managed in batches, with the script running inside Docker containers on EC2 instances,
orchestrated by Apache Airflow, and finally, the data is downloaded into the Amazon S3 bucket for future Data Analysis.
