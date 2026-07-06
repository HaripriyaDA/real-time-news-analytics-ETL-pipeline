from kafka import KafkaConsumer
import json
import csv

topic = "News-Topic"
brokers = "localhost:9092"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=brokers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Poll for 5 seconds and exit
records = consumer.poll(timeout_ms=5000, max_records=10)

with open("articles.csv", mode="w", encoding="utf-8", newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["title", "description", "publishedAt", "source", "url"])

    for tp, messages in records.items():
        for message in messages:
            article = message.value
            writer.writerow([
                article.get('title', ''),
                article.get('description', ''),
                article.get('publishedAt', ''),
                article.get('source', ''),
                article.get('url', '')
            ])

print("Fetched messages successfully and saved to articles.csv")
