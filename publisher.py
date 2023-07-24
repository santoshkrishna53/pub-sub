from concurrent import futures
from google.cloud import pubsub_v1
import os
import json
with open('students.json') as f:
  students_data = json.load(f)


project_id = "training-316807"
topic_id = "pub-sub-training"
credential_path = "newpub.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path



def pub(project_id, topic_id):
    client = pubsub_v1.PublisherClient()
    topic_path = client.topic_path(project_id, topic_id)
    data = json.dumps(students_data)
    print(type(data))
    message = bytes(data, 'utf-8')
    api_future = client.publish(topic_path, message)
    message_id = api_future.result()
    print(f"Published {data} to {topic_path}: {message_id}")


if __name__ == "__main__":
    pub(project_id, topic_id)
