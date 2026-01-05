from kafka.admin import KafkaAdminClient, NewTopic

try:
    admin_client = KafkaAdminClient(
        bootstrap_servers="[::1]:9092",
        client_id='test',
        api_version=(2, 5, 0)
    )

    topic_list = [NewTopic(name="stock_market", num_partitions=1, replication_factor=1)]
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print(" Topic 'stock_market' created successfully!")
except Exception as e:
   print(f" Failed: {e}")
