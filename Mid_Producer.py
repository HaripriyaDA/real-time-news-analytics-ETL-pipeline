from newsapi import NewsApiClient
from kafka import KafkaProducer
import json

# Step 1: Set NewsAPI key
key = "218e4442d9474f748d48796081cade0f"  

# Step 2: Initialize NewsAPI client
newsapi = NewsApiClient(api_key=key)

# Step 3: Define news sources and keyword/topic
sources = 'bbc-news,cnn,fox-news,nbc-news,the-guardian-uk,the-new-york-times,the-washington-post,usa-today,independent,daily-mail'
keyword = 'Germany'  

# Step 4: Get articles from NewsAPI
all_articles = newsapi.get_everything(q=keyword,
                                      sources=sources,
                                      language='en')

# Step 5: Kafka producer configuration
topic = "News-Topic"
brokers = "localhost:9092"

# Create Kafka producer with JSON serialization
producer = KafkaProducer(
    bootstrap_servers=brokers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Step 6: Send articles to Kafka topic
for article in all_articles['articles']:
    # Optional: Only send selected fields
    cleaned_article = {
        'title': article.get('title'),
        'description': article.get('description'),
        'publishedAt': article.get('publishedAt'),
        'source': article['source']['name'],
        'url': article.get('url')
    }

    print(f"Sending article: {cleaned_article['title']}")
    producer.send(topic, value=cleaned_article)

print(f"All articles sent to Kafka topic: {topic}")
