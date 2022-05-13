# To test the messages publishing to a topic
from google.cloud import pubsub_v1
import json

#Credentials key path of the service account
SERVICE_ACCOUNT_JSON='/Users/divya/google-cloud-sdk/projects-329415-0c713edf94ce.json'
project_id = "projects-329415"
topic_id = "pubsub-to-biqquery-divy"

#publisher = pubsub_v1.PublisherClient()
publisher = pubsub_v1.PublisherClient.from_service_account_json(SERVICE_ACCOUNT_JSON)

topic_path = publisher.topic_path(project_id, topic_id)

try:
    publisher.create_topic(request={"name": topic_path})

except:
    print ('Topic already exists')

record = {
   "customer_id": "JXJY167254JK",
   "date": "11-11-2020",
   "order_id": "654S654",
   "amount": 197,
   "Status": "Delivered"
}

data = json.dumps(record).encode("utf-8")
future = publisher.publish(topic_path, data)
print(f'published message id {future.result()}')
print(f"Published messages to {topic_path}.")