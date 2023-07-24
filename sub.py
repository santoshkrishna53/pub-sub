from google.cloud import pubsub_v1
import os
import json
import codecs
project_id = "training-316807"
topic_id = "pub-sub-training"
sub_id = 'pub-sub-training-sub'
credential_path = "newpub.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

def sub(project_id, subscription_id, timeout=None):
    subscriber_client = pubsub_v1.SubscriberClient()
    subscription_path = subscriber_client.subscription_path(project_id, subscription_id)

    def callback(message):
        data = str(message.data)
        with open('output.json', 'wb') as f:
           json.dump(data, codecs.getwriter('utf-8')(f), ensure_ascii=False)
        print('message written to file output.json')
        print(f"Received {message}.")
       
        message.ack()
        print(f"Acknowledged {message.message_id}.")

    streaming_pull_future = subscriber_client.subscribe(
        subscription_path, callback=callback
    )
    print(f"Listening for messages on {subscription_path}..\n")

    try:
        streaming_pull_future.result(timeout=timeout)
    except:  
        streaming_pull_future.cancel()  
        streaming_pull_future.result() 

    subscriber_client.close()


if __name__ == "__main__":
    sub(project_id, sub_id)
