import time
import traceback
import json
from datetime import datetime
import paho.mqtt.client as mqtt
import logging
import threading
import requests

from ecoflow_api import EcoFlowAPI
from config_handler import ConfigHandler

# Set up logging without the header
logging.basicConfig(level=logging.INFO, format='%(message)s')

# Global variables
last_injection_value = 1  # Initialize the last injection value to 1
soc = None
soc_mqtt = -1
pv1_power = None
pv2_power = None
total_power_generation = None
soc_below_30 = False
injection_permitted = True  # New variable to store injection permission
last_soc_check_time = 0
car_charging = 0
current_power = 0  # Global variable to store the current power

def on_message(client, userdata, message):
    global injection_permitted, car_charging
    try:
        payload = json.loads(message.payload.decode("utf-8"))
        injection_permitted = payload.get("injection_permitted", True)
        car_charging = payload.get("car_charging", 0)
        logging.info(f"MQTT message received: injection_permitted = {injection_permitted}, car_charging {car_charging}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON from MQTT message: {e}")

def setup_mqtt_client(config):
    client = mqtt.Client(client_id='rg_pwrMgmt')
    client.on_message = on_message
    client.connect(config.get('MQTT_BROKER_ADDRESS'), config.get('MQTT_PORT'), 60)
    client.subscribe("HA-to-rg_PwrMgmt")
    return client

def publish_to_mqtt(client, topic, payload):
    client.publish(topic, json.dumps(payload))

def update_soc(new_soc):
    global soc
    soc = new_soc

def on_status_update(params):
    global soc_mqtt, soc, pv1_power, pv2_power, total_power_generation, mqtt_client
    #print(f'pv1: {params["pv1InputWatts"]}, pv2: {params["pv2InputWatts"]}')
    #print(f'number params: {len(params)}')
    soc = params.get("batSoc")
    if soc is not None:
        soc_mqtt = soc
    pv1_power = params["pv1InputWatts"]
    pv2_power = params["pv2InputWatts"]
    prev_total = total_power_generation
    if pv1_power is not None and pv2_power is not None:
        pv1_power /= 10
        pv2_power /= 10
        total_power_generation = int(pv1_power + pv2_power)
    else:
        total_power_generation = None
    print(f"EF MQTT CALLBACK: total PV: {total_power_generation}, soc: {soc_mqtt}")
    # Publish updated data to HA
    if prev_total != total_power_generation:
        payload = {
            "PV_total": int(total_power_generation)
        }
        publish_to_mqtt(mqtt_client, 'rg_PwrMgmt-to-HA', payload)



def update_and_get_soc(ecoflow_api, mqtt_client):
    global soc, pv1_power, pv2_power, total_power_generation
    try:
        soc_response = ecoflow_api.get_api_quota_all()
        if 'data' in soc_response:
            soc = soc_response['data'].get('20_1.batSoc', None)
            pv1_power = soc_response['data'].get('20_1.pv1InputWatts', None)
            pv2_power = soc_response['data'].get('20_1.pv2InputWatts', None)
            if pv1_power is not None and pv2_power is not None:
                pv1_power /= 10
                pv2_power /= 10
                total_power_generation = pv1_power + pv2_power
            else:
                total_power_generation = None
            logging.info(f"SoC: {soc}%, PV1: {pv1_power}W, PV2: {pv2_power}W, Total: {total_power_generation}W")
            
            # Publish SoC and PV data to MQTT
            payload = {
                "SoC": int(soc),
                "PV1": int(pv1_power),
                "PV2": int(pv2_power),
                "PV_total": int(total_power_generation)
            }
            publish_to_mqtt(mqtt_client, 'rg_PwrMgmt-to-HA', payload)
        else:
            logging.warning("SoC data not found in the response")
            soc = pv1_power = pv2_power = total_power_generation = None
    except Exception as e:
        logging.error(f"Failed to update SoC: {e}")
        soc = pv1_power = pv2_power = total_power_generation = None

def get_inject_power_range():
    global soc, soc_below_30, injection_permitted, total_power_generation, car_charging

    current_hour = datetime.now().hour
    if 19 <= current_hour or current_hour < 7:
        constant_max_power = 100
    elif 7 <= current_hour <= 14:
        constant_max_power = 800
    else:
        if soc is not None:
            if soc < 30:
                soc_below_30 = True
                constant_max_power = 0
            elif soc_below_30 and soc < 40:
                constant_max_power = 0
            else:
                soc_below_30 = False
                if soc > 50:
                    constant_max_power = 800
                elif 41 <= soc <= 50:
                    constant_max_power = 200
                else:
                    constant_max_power = 99
        else:
            constant_max_power = 800

    if total_power_generation is None:
        total_power_generation = 0

    max_power = constant_max_power

    if car_charging > 10:
        max_power = int(total_power_generation * 0.9)

    if soc is not None and soc <= 17:
        max_power = int(total_power_generation * 0.9)

    if soc is not None and soc <= 85:
        min_power = 0
    else: # "battery full" case
        # This is kind of a work-around for bugs in PowerStream firmware, which
        # sometimes behaves strange if it hits real bat max SoC. Sometimes it
        # does not inject all generation. We try to avoid this by taking action
        # before real max Soc.
        min_power = int(total_power_generation * 0.98)

    print(f"min/max power ({min_power},{max_power}), SoC: {soc}, PV: {total_power_generation}, car {car_charging}")
    return min_power, max_power

def set_battery_output(current_power, config, ecoflow_api, mqtt_client):
    global last_injection_value

    min_power, max_power = get_inject_power_range()
    logging.info(f"got inject power range {min_power}, {max_power}")
    injection_value = max(min(current_power + last_injection_value, max_power), 0)
    eps = config.get('eps')

    if current_power < -eps:
        new_injection_value = max(last_injection_value + current_power, 0)
        if last_injection_value > 0:
            injection_value = new_injection_value
            logging.info(f"Adjusting injection to {injection_value} watts due to negative power consumption.")
        else:
            logging.info("Current power is negative and injection is already 0. No change needed.")
            #return last_injection_value
    elif current_power > eps:
        if injection_value == last_injection_value:
            logging.info("No significant change in power consumption. No change needed.")
            #return last_injection_value
        logging.info(f"Increasing injection to {injection_value} watts due to positive power consumption.")
    else:
        logging.info(f"Power consumption within epsilon range ({current_power}). No change needed.")
        #return last_injection_value

    if injection_value < min_power: # battery full case
        injection_value = min_power

    if injection_value != last_injection_value:
        ecoflow_api.set_ef_powerstream_custom_load_power(injection_value)
        logging.info(f"Setting battery output to {injection_value} watts")
    
    # Publish power injection to MQTT
    payload = {"pwr_injection": injection_value}
    publish_to_mqtt(mqtt_client, 'rg_PwrMgmt-to-HA', payload)

    last_injection_value = injection_value

    return

def get_power_in(url, mqtt_client):
    global current_power
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if 'StatusSNS' in data and 'E320' in data['StatusSNS'] and 'Power_in' in data['StatusSNS']['E320']:
            power_in = data['StatusSNS']['E320']['Power_in']
            current_power = power_in
            # Publish power consumption to MQTT
            payload = {"SmartMeter_currentPowerIn": int(power_in)}
            publish_to_mqtt(mqtt_client, 'rg_PwrMgmt-to-HA', payload)
            return power_in
        else:
            raise ValueError("Required data not found in the response")
    except requests.RequestException as e:
        raise RuntimeError(f"HTTP request failed: {e}")
    except ValueError as e:
        raise RuntimeError(f"Data extraction error: {e}")

def power_in_loop(url, mqtt_client, config_handler):
    while True:
        get_power_in(url, mqtt_client)
        time.sleep(config_handler.get('sleep_time'))

def processing_loop(url, config_handler, ecoflow_api, mqtt_client):
    global last_injection_value, last_soc_check_time, current_power
    last_injection_value = 1  # Initialize the last injection value
    last_soc_check_time = time.time()

    try:
        # Get initial SoC immediately after startup
        update_and_get_soc(ecoflow_api, mqtt_client)

        # Initial setting to establish the state of the battery subsystem
        current_power = get_power_in(url, mqtt_client)
        logging.info(f"Initial Power_in: {current_power} watts, Current Injection: {last_injection_value} watts")
        set_battery_output(current_power, config_handler, ecoflow_api, mqtt_client)
        logging.info(f"Initial setting done. Sleeping for {config_handler.get('min_change_interval')} seconds.")
        time.sleep(config_handler.get('min_change_interval'))

        while True:
            mqtt_client.loop()  # Process MQTT messages

            current_time = time.time()
            if current_time - last_soc_check_time >= 240:  # Update SoC every 60 seconds
                update_and_get_soc(ecoflow_api, mqtt_client)
                last_soc_check_time = current_time

            set_battery_output(current_power, config_handler, ecoflow_api, mqtt_client)
            time.sleep(config_handler.get('hysteresis_interval') if last_injection_value != 0 else config_handler.get('sleep_time'))
    except Exception as e:
        logging.error("An exception occurred:")
        traceback.print_exc()

def main():
    global mqtt_client
    logging.info("EcoFlow PowerStream Management Script")
    config_handler = ConfigHandler()

    mqtt_client = setup_mqtt_client(config_handler)
    
    ecoflow_api = EcoFlowAPI(
        config_handler.get('ECOFLOW_API_HTTP_URL'), 
        config_handler.get('ECOFLOW_ACCESSKEY'), 
        config_handler.get('ECOFLOW_SECRETKEY'), 
        config_handler.get('ECOFLOW_SN'),
        on_status_update
    )

    url = config_handler.get('url')
    
    # Connect to MQTT server
    ecoflow_api.connect_to_mqtt()

    # initial queries
    update_and_get_soc(ecoflow_api, mqtt_client)
    get_power_in(url, mqtt_client)

    # Start the power_in_loop in a separate thread
    power_in_thread = threading.Thread(target=power_in_loop, args=(url, mqtt_client, config_handler))
    power_in_thread.daemon = True
    power_in_thread.start()

    processing_loop(url, config_handler, ecoflow_api, mqtt_client)

if __name__ == "__main__":
    main()

