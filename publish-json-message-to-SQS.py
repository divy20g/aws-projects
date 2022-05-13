import boto3
import json

sqs_queue_name = "sqs-to-pubsub-table1"

class sqsQueue(object):

    def __init__(self, queueName=None):
        self.resource = boto3.resource('sqs')
        self.queue = self.resource.get_queue_by_name(QueueName=sqs_queue_name)
        self.QueueName = queueName

    def sendmessage(self, Message={}):
        data = json.dumps(Message)
        response = self.queue.send_message(MessageBody=data)
        return response

    def receivemessage(self):
        try:
            queue = self.resource.get_queue_by_name(QueueName=self.QueueName)
            for message in queue.receive_messages():
                data = message.body
                data = json.loads(data)
                message.delete()
        except Exception:
            print(e)
            return []
        return data


if __name__ == "__main__":
    q = sqsQueue(queueName=sqs_queue_name)
    Message = {
   "Customer_id": "NEERAJ",
   "date": "11-11-2020",
   "timestamp": "8:12:20",
   "order_id": "654S654",
   "items": "PiZza:Manch?uriaN:CHOW Mein:Crispy Onion Rings",
   "amount": 197,
   "mode": "Wallet",
   "restaurnt": "Emperial",
   "Status": "Delivered",
   "ratings": 2,
   "feedback": "Late delivery"
   }
    response = q.sendmessage(Message=Message)
    print(response)
    #data = q.receive()
    #print(data)