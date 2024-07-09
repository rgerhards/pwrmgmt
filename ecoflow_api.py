import requests
import hmac
import hashlib
import binascii
import random
import json
import logging
import time
import paho.mqtt.client as mqtt

class EcoFlowAPI:
    def __init__(self, base_url, access_key, secret_key, serial_number, status_update_callback):
        self.base_url = base_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.serial_number = serial_number
        self.status_update_callback = status_update_callback
        self.mqtt_client = None

    def hmac_sha256(self, data, key):
        hashed = hmac.new(key.encode('utf-8'), data.encode('utf-8'), hashlib.sha256).digest()
        sign = binascii.hexlify(hashed).decode('utf-8')
        return sign

    def get_map(self, json_obj, prefix=""):
        def flatten(obj, pre=""):
            result = {}
            if isinstance(obj, dict):
                for k, v in obj.items():
                    result.update(flatten(v, f"{pre}.{k}" if pre else k))
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    result.update(flatten(item, f"{pre}[{i}]"))
            else:
                result[pre] = obj
            return result
        return flatten(json_obj, prefix)

    def get_qstr(self, params):
        return '&'.join([f"{key}={params[key]}" for key in sorted(params.keys())])

    def put_api(self, url, params=None):
        nonce = str(random.randint(100000, 999999))
        timestamp = str(int(time.time() * 1000))
        headers = {'accessKey': self.access_key, 'nonce': nonce, 'timestamp': timestamp}
        sign_str = (self.get_qstr(self.get_map(params)) + '&' if params else '') + self.get_qstr(headers)
        headers['sign'] = self.hmac_sha256(sign_str, self.secret_key)
        response = requests.put(url, headers=headers, json=params)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"put_api: {response.text}")

    def get_api_quota_all(self):
        url = self.base_url + 'iot-open/sign/device/quota/all'
        params = {'sn': self.serial_number}
        nonce = str(random.randint(100000, 999999))
        timestamp = str(int(time.time() * 1000))
        headers = {'accessKey': self.access_key, 'nonce': nonce, 'timestamp': timestamp}
        sign_str = (self.get_qstr(self.get_map(params)) + '&' if params else '') + self.get_qstr(headers)
        headers['sign'] = self.hmac_sha256(sign_str, self.secret_key)
        response = requests.get(url + f"?sn={self.serial_number}", headers=headers)
        if response.status_code == 200:
            data = response.json()
            with open('quota_response.json', 'w') as outfile:
                json.dump(data, outfile, indent=4)
            return data
        else:
            logging.error(f"get_api: {response.text}")

    def get_api(self, url, params=None):
        nonce = str(random.randint(100000, 999999))
        timestamp = str(int(time.time() * 1000))
        headers = {'accessKey': self.access_key, 'nonce': nonce, 'timestamp': timestamp}
        sign_str = (self.get_qstr(self.get_map(params)) + '&' if params else '') + self.get_qstr(headers)
        headers['sign'] = self.hmac_sha256(sign_str, self.secret_key)
        response = requests.get(url, headers=headers, json=params)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"get_api: {response.text}")

    def post_api(self, url, params=None):
        nonce = str(random.randint(100000, 999999))
        timestamp = str(int(time.time() * 1000))
        headers = {'accessKey': self.access_key, 'nonce': nonce, 'timestamp': timestamp}
        sign_str = (self.get_qstr(self.get_map(params)) + '&' if params else '') + self.get_qstr(headers)
        headers['sign'] = self.hmac_sha256(sign_str, self.secret_key)
        response = requests.post(url, headers=headers, json=params)
        if response.status_code == 200:
            return response
        else:
            logging.error(f"post_api: {response.text}")

    def check_if_device_is_online(self, sn=None, payload=None):
        parsed_data = payload
        desired_device_sn = sn

        device_found = False

        for device in parsed_data.get('data', []):
            if device.get('sn') == desired_device_sn:
                device_found = True
                online_status = device.get('online', 0)
                if online_status == 1:
                    return "online"
                else:
                    return "offline"
        if not device_found:
            logging.error(f"Device with SN '{desired_device_sn}' not found in the data.")
            sys.exit(1)
            return "devices not found"

    def set_ef_powerstream_custom_load_power(self, NewPower):
        logging.info(f"set_ef_powerstream_custom_load_power: NewPower {NewPower}")

        url = 'https://api.ecoflow.com/iot-open/sign/device/quota'
        url_device = 'https://api.ecoflow.com/iot-open/sign/device/list'

        cmdCode = 'WN511_SET_PERMANENT_WATTS_PACK'
        PWR_MAX = 800

        payload = self.get_api(url_device, {"sn": self.serial_number})
        check_ps_status = self.check_if_device_is_online(self.serial_number, payload)

        quotas = ["20_1.permanentWatts"]
        params = {"quotas": quotas}

        try:
            logging.info(f"setting new power output: {NewPower}")
            params = {"permanentWatts": NewPower * 10 }
            payload = self.put_api(url, {"sn": self.serial_number, "cmdCode": cmdCode, "params": params})
            return payload

        except Exception as e:
            logging.error(f"Error fetching Ecoflow data: {str(e)}")
            return None

    def get_mqtt_certification(self):
        url = self.base_url + 'iot-open/sign/certification'
        json_data = self.get_api(url)
        cert = json_data.get("data")
        json.dumps(cert)
        return cert

    # MQTT-related methods
    def connect_to_mqtt(self):
        # Get MQTT certification
        certification = self.get_mqtt_certification()
        logging.info(f"MQTT Certification: {certification}")

        broker_url = certification['url']
        broker_port = int(certification['port'])
        certificate_account = certification['certificateAccount']
        certificate_password = certification['certificatePassword']

        # Create MQTT client instance
        self.mqtt_client = mqtt.Client(client_id="rg_pwrMgmt")
        logging.debug("MQTT client created")

        # Set username and password
        self.mqtt_client.username_pw_set(certificate_account, certificate_password)

        # Enable TLS for secure connection
        self.mqtt_client.tls_set()

        # Enable detailed logging for the MQTT client
        self.mqtt_client.enable_logger(logging.getLogger(__name__))

        # Define callback functions
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logging.info("Connected to MQTT broker successfully")
                # Subscribe to the topic after successful connection
                topic = f'/open/{certificate_account}/{self.serial_number}/quota'
                print(f"TOPIC: {topic}")
                client.subscribe(topic)
                logging.info(f"Subscribed to topic: {topic}")
            else:
                logging.error(f"Failed to connect to MQTT broker. Return code {rc}")

        def on_message(client, userdata, message):
            payload = message.payload.decode("utf-8")
            try:
                data = json.loads(payload)
                params = data['param']
                # Call the user-provided callback if it exists
                if self.status_update_callback:
                    self.status_update_callback(params)
                #print(json.dumps(params, indent=4))
                #logging.info(f"Message received from topic {message.topic}")
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode JSON payload: {e}")
                logging.error(f"Payload: {payload}")

        def on_log(client, userdata, level, buf):
            logging.debug(f"MQTT Log: {buf}")

        # Assign callback functions
        self.mqtt_client.on_connect = on_connect
        self.mqtt_client.on_message = on_message
        self.mqtt_client.on_log = on_log

        # Connect to the broker
        logging.info(f"Connecting to {broker_url}:{broker_port} with client ID rg_pwrMgmt")
        self.mqtt_client.connect(broker_url, broker_port)
        logging.info("Connect command issued")

        # Start the network loop
        self.mqtt_client.loop_start()

    def disconnect_mqtt(self):
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            logging.info("Disconnected from MQTT broker")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    # Example usage
    config_handler = ConfigHandler()

    ecoflow_api = EcoFlowAPI(
        config_handler.get('ECOFLOW_API_HTTP_URL'), 
        config_handler.get('ECOFLOW_ACCESSKEY'), 
        config_handler.get('ECOFLOW_SECRETKEY'), 
        config_handler.get('ECOFLOW_SN')
    )

    # Connect to MQTT server
    ecoflow_api.connect_to_mqtt()

    # Keep the script running
    try:
        while True:
            time.sleep(1)  # Sleep to reduce CPU usage
    except KeyboardInterrupt:
        logging.info("Disconnecting from MQTT broker...")
        ecoflow_api.disconnect_mqtt()

