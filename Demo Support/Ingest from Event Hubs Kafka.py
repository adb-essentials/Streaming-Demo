# Databricks notebook source
kafka_topic = "taxidemo"
kafka_bootstrap_servers = "fieldengdeveastus2ehb.servicebus.windows.net:9093"
kafka_sasl_jaas_config = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{eh_connection_string}\";"

# COMMAND ----------

spark
    .readStream
    .format("kafka")
    .option("subscribe", kafka_topic)
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config)
    .option("kafka.session.timeout.ms", "60000")
    .option("kafka.request.timeout.ms", "30000")
    .option("kafka.group.id", "$Default")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 50000)
    .load()
