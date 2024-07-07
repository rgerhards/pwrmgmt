import yaml

class ConfigHandler:
    def __init__(self, file_path='config.yaml'):
        self.file_path = file_path
        self.config = self.read_config()

    def read_config(self):
        with open(self.file_path, 'r') as file:
            config = yaml.safe_load(file)

        required_keys = [
            'sleep_time', 'eps', 'min_change_interval', 'hysteresis_interval', 'url',
            'ECOFLOW_ACCESSKEY', 'ECOFLOW_SECRETKEY', 'ECOFLOW_SN', 'ECOFLOW_API_HTTP_URL',
            'MQTT_BROKER_ADDRESS', 'MQTT_PORT'
        ]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required configuration key: {key}")

        return config

    def get(self, key):
        return self.config.get(key)

