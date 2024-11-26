from kafka.admin import KafkaAdminClient

admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")

# List all topics
topics = admin_client.list_topics()
print("Topics:", topics)

# Close the client
admin_client.close()
