__author__ = "Akshay Joshi"

from time import sleep

# Install with PIP: 'pip install paho-mqtt'
from paho.mqtt import client as mqtt

class MQTTClient:
    def __init__(self):
        
        # Address of the MQTT Broker
        self.broker_name = 'inet-mqtt-broker.mpi-inf.mpg.de'
        self.broker_port = 1883

        # Topic name used for login
        self.login_topic = 'login'

        # Quality of Service Level
        self.qos = 2

        # Message sent while logging in (publish)
        self.client_id = '2547841'

        # MQTT Client login credentials
        self.username = 'student'
        self.password = 'i_make_mqtt_cool'

        # Dictionary to store the corresponding mappings between the commands sent 
        # by broker & the expected reply
        self.cmd_reply_dict = {
                                'CMD1': 'Apple',
                                'CMD2': 'Cat',
                                'CMD3': 'Dog',
                                'CMD4': 'Rat',
                                'CMD5': 'Boy',
                                'CMD6': 'Girl',
                                'CMD7': 'Toy',
                            }
        
        # If everything goes right, broker sends this at last
        self.final_reply = 'Well done my IoT!'

        self.broker_reply = []

    # Callback to receive connection status information 
    # back from the broker
    def on_connect_callback(self, client, 
                            userdata, 
                            flags, rc, 
                            properties=None):
        """
        The value of rc determines if connection setup 
        is success or not:
            0: Connection successful
            1: Connection refused - incorrect protocol version
            2: Connection refused - invalid client identifier
            3: Connection refused - server unavailable
            4: Connection refused - bad username or password
            5: Connection refused - not authorised
            6 - 255: Currently unused.
        """

        if rc > 0:
            print(f'\nConnection Failed with Status Code: {rc}')

        else:
            print(f"\nConnection to MQTT Broker at '{self.broker_name}' is successfull")

    
    def on_message_callback(self, client, userdata, message):
        self.broker_reply.append(str(message.payload.decode('utf-8')))
        print(f"\nReceived '{self.broker_reply[-1]}' from '{message.topic}' topic")

    
    def on_subscribe_callback(self, mosq, obj, mid, granted_qos):
        print("\nSubscribed: " + str(mid) + " " + str(granted_qos))


    # Setup/connect to MQTT Broker
    def connect_to_mqtt_broker(self):

        # Create an mqtt client object
        client = mqtt.Client(str(self.client_id))

        # Set username/password attributs in the client object
        client.username_pw_set(self.username, self.password)

        # Set the MQTT Client 'on_connect' callback
        client.on_connect = self.on_connect_callback

        # Finally connect to broker at specified address
        client.connect(self.broker_name, self.broker_port)
        return client

    
    # Publish messages to MQTT Broker on specific topics
    def publish_to_mqtt_broker(self, client, message='dogecoin', topic='dogecoin'):

        result = client.publish(topic, message, qos=self.qos)

        if result[0] == 0:
            print(f"\nSent '{message}' to topic '{topic}'")

        else:
            print(f"\nFailed to send message to topic '{topic}'")

        
    # Subscribe to MQTT Broker's messages o specific topics
    def subscribe_to_mqtt_broker(self, client, topic='login'):

        result = client.subscribe(topic, qos=self.qos)
        #client.on_subscribe = self.on_subscribe_callback
        client.on_message = self.on_message_callback


# Driver program
def simulate_mqtt_communication():
    client = MQTTClient()

    mqtt_client = client.connect_to_mqtt_broker()

    mqtt_client.loop_start()

    client.subscribe_to_mqtt_broker(mqtt_client, client.client_id + '/' + 'UUID')

    sleep(1)
    client.publish_to_mqtt_broker(mqtt_client, client.client_id, client.login_topic)

    sleep(1)
    
    client.subscribe_to_mqtt_broker(mqtt_client, client.broker_reply[0])
    sleep(2)
    
    patience = 0
    while True:
        if len(client.broker_reply) > 1:
            for cmd in client.broker_reply[1:]:
                client.publish_to_mqtt_broker(mqtt_client, client.cmd_reply_dict[cmd], \
                                        client.broker_reply[0] + '/' + cmd)
        
        else:
            patience += 1

            if patience >= 5:
                print("\nConnection Timed Out")
                break

        #client.subscribe_to_mqtt_broker(mqtt_client, client.broker_reply[0])
        sleep(2)

        if client.broker_reply[-1] == client.final_reply:
            break

    mqtt_client.loop_stop()

    print("\nDisconnecting from MQTT Broker...\n")
    mqtt_client.disconnect()


# Invoke driver function
if __name__ == "__main__":
    simulate_mqtt_communication()
