# CDC to S3
<img width="913" alt="Databricks pipeline" src="https://github.com/joe-bryan/cdc-kafka-spark/assets/101160575/c4a19ef8-41f8-4de9-acb8-d44929f05e83">
<br>
<br>
Data pipeline that captures row-level changes to the data in a Postgres database. From there, Apache Kafka receives the feed in real time. Apache Spark runs daily and batches the changes over to an S3 data lake. The goal of the pipeline is to have a secure location for data analysts and data scientists to get the data for drawing insights and running algorithms. The data consists of DVD rental transactions.
<br>
<br>
The diagrams show two ways to achieve the pipeline. The Databricks version uses managed versions of Postgres, Kafka, and Spark. The Docker version uses local resources instead. With the Docker way, ensure you have more than 4GB of RAM available as the number of containers eats your computer resources.
<img width="913" alt="Docker pipeline" src="https://github.com/joe-bryan/cdc-kafka-spark/assets/101160575/e9c3ed1a-6e71-4c7e-8f3d-ac43e287033e">
