from kafka import KafkaProducer
from kafka.errors import KafkaError
import ssl
import logging
logging.basicConfig(level=logging.DEBUG)

sasl_mechanism = 'PLAIN'
security_protocol = 'SASL_PLAINTEXT'

# Create a new context using system defaults, disable all but TLS1.2
context = ssl.create_default_context()
context.options &= ssl.OP_NO_TLSv1
context.options &= ssl.OP_NO_TLSv1_1

producer = KafkaProducer(bootstrap_servers = app.config['KAFKA_BROKERS_SASL'],
                         sasl_plain_username = app.config['KAFKA_USERNAME'],
                         sasl_plain_password = app.config['KAFKA_PASSWORD'],
                         security_protocol = security_protocol,
                         ssl_context = context,
                         sasl_mechanism = sasl_mechanism,
                         api_version = (0,10),
                         retries=5)

def send_message(message):
    
    try:
        producer.send(app.config['KAFKA_TOPIC'], message.encode('utf-8')).get(1)
        print("message was sent to broker!")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise SystemExit

send_message("Say Hi from Python!")