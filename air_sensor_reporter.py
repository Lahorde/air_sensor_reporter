import opc
from time import sleep

import usbiss
import opc
import logging
import sys
from influxdb import InfluxDBClient
import paho.mqtt.client as mqtt
import collections
import serial
import time
import threading as thread
from os import path
import configparser
from datetime import datetime
from urllib import request

'''''''''''''''''''''
 VARIABLE DEFINITIONS
'''''''''''''''''''''
# Sampling period in s
SAMPLING_PERIOD = 1 

db_point_index = 0

db_data_mutex = thread.Lock()
wu_get_mutex = thread.Lock()

alphasense = None 
mqtt_client = None
db_client = None

json_body = []
pending_wu_get = []

PRIMARY_WINDOW_LENGTH = 15
SEC_WINDOW_LENGTH = 7
filter_windows = {}

publisher_th_stop = False

opc_config = {}

'''''''''''''''''''''
 METHODS
'''''''''''''''''''''
def read_config():
    global opc_config 
    
    # read configuration file
    CONFIG_FILE = 'air_sensor_reporter.cfg'
    config = configparser.ConfigParser()
    config_file = config.read(CONFIG_FILE)
    
    if len(config_file) == 0 :
        raise Exception("A config file {} must be present".format(CONFIG_FILE)) 
    else :
        try :
            opc_config['mqtt_server_ip']     = config.get('mqtt_config', 'broker_ip')
            opc_config['mqtt_port']          = int(config.get('mqtt_config', 'port'))
            opc_config['mqtt_pm1_topic']     = config.get('mqtt_config', 'pm1_topic')
            opc_config['mqtt_pm2dot5_topic'] = config.get('mqtt_config', 'pm2dot5_topic')
            opc_config['mqtt_pm10_topic']    = config.get('mqtt_config', 'pm10_topic')

            opc_config['db_ip']              = config.get('influxdb_config', 'ip')
            opc_config['db_port']            = int(config.get('influxdb_config', 'port'))
            opc_config['db_name']            = config.get('influxdb_config', 'db_name')
            opc_config['db_id']              = config.get('influxdb_config', 'db_id')
            opc_config['db_pass']            = config.get('influxdb_config', 'db_pass')

            opc_config['pws_id']             = config.get('wu_config', 'station_id')
            opc_config['pws_pass']           = config.get('wu_config', 'station_pass')
            
        except configparser.NoOptionError as e:
            logging.error('Some fields missing in configuration file {}'.format(e))
            raise e 


def configure_logger() :
    # Use root logger 
    hdlr = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s %(filename)s %(message)s')
    hdlr.setFormatter(formatter)
    logging.getLogger().addHandler(hdlr) 
    logging.getLogger().setLevel(logging.DEBUG) 

    # Disable requests logs
    logging.getLogger("requests").setLevel(logging.WARNING)


def add_window_filter(meas_name, threshold):
    filter_windows[meas_name] = []
    # Primary window 
    filter_windows[meas_name].append(collections.deque(maxlen=PRIMARY_WINDOW_LENGTH))
    # Secondary window 
    filter_windows[meas_name].append(collections.deque(maxlen=SEC_WINDOW_LENGTH))
    # Threshold on data 
    filter_windows[meas_name].append(threshold)
    # Current filtered value 
    filter_windows[meas_name].append(0.0)
    # TODO send measures in a separate thread 


def reset_window(meas_name):
    # Primary window 
    filter_windows[meas_name][0].clear()
    # Secondary window 
    filter_windows[meas_name][1].clear()
    
    
def wu_post(pm2dot5, pm10):
    BASEURL_WU="http://weatherstation.wunderground.com/weatherstation/updateweatherstation.php"
    WU_URL_DATA_SUFFIX="?ID=$WU_PWS_ID&PASSWORD=$WU_PWD&{}&AqPM2.5={}&AqPM10={}&action=updateraw"
    
    url_data_suffix = WU_URL_DATA_SUFFIX.format(opc_config['pws_id'], opc_config['pws_pass'], datetime.utcnow().strftime('%Y-%m-%d+%H%%3A%M%%3A%S'), pm2dot5, pm10)
    
    # do request in a separate thread 
    with wu_get_mutex : 
        pending_wu_get.append('{}{}'.format(BASEURL_WU, url_data_suffix)) 


def get_filtered_value(meas_name, new_val):
    # is window full?
    if len(filter_windows[meas_name][0]) < PRIMARY_WINDOW_LENGTH :
        filter_windows[meas_name][0].append(new_val)
    else : # window full
        # large variation in input 
        if abs(new_val - filter_windows[meas_name][3]) > filter_windows[meas_name][2] :
            logging.debug('{} - {} above threshold'.format(new_val, meas_name))
            # is secondary window full ?
            if len(filter_windows[meas_name][1]) == SEC_WINDOW_LENGTH :
               logging.debug('{} window full {}'.format(meas_name, filter_windows[meas_name][1])) 
               # variation is not a noise, include it
               for val in filter_windows[meas_name][1] :
                   filter_windows[meas_name][0].append(val)
                
               filter_windows[meas_name][0].append(new_val)
               # free secondary window
               filter_windows[meas_name][1].clear()  
            
            else : # not sure if it is noise
               logging.debug('{} - {} added to secondary window'.format(new_val, meas_name))
               filter_windows[meas_name][1].append(new_val)
               
        else: # Add current meas to primary window 
            filter_windows[meas_name][0].append(new_val)
            # free secondary window
            if len(filter_windows[meas_name][1]) > 0 :
                logging.debug('{} - {} filter : excluding noise : {}'.format(new_val, meas_name, filter_windows[meas_name][1]))
                filter_windows[meas_name][1].clear()  

    # compute moving average 
    total = 0
    for val in filter_windows[meas_name][0] :
        total += val
        
    filter_windows[meas_name][3] = total/len(filter_windows[meas_name][0])
    return filter_windows[meas_name][3] 
    

def publisher_loop():
    global db_point_index, json_body
    
    while not publisher_th_stop :
        # Send date to DB 
        if len(json_body) > 0 : 
            with db_data_mutex :
                json_body_copy = json_body[:] 
                db_point_index = 0 
                json_body.clear()
            db_client.write_points(json_body_copy) 
            
        # Send data to WU 
        if len(pending_wu_get):
            with wu_get_mutex :
                pending_wu_get_copy = pending_wu_get[:] 
                pending_wu_get.clear() 
                
            for req in pending_wu_get_copy :
                try : 
                    request.urlopen(req).read()
                except Exception as e :
                    logging.error('could not publish data to WU - {}'.format(e))
                    pass
                
        else :
            time.sleep(1)
    

def connect_mqtt() :
    global mqtt_client 
    
    try :
        mqtt_client = mqtt.Client()
        mqtt_client.connect_async(opc_config['mqtt_server_ip'], opc_config['mqtt_port'], 60) 
        
        mqtt_client.loop_start()
    
    except Exception as e :
        logging.debug("Cannot connect MQTT server - " + str(e))
        if mqtt_client : 
            mqtt_client.loop_stop()
            mqtt_client = None


def add_meas_to_db(meas_name, tc, pm, raw_meas):
    global db_point_index, json_body
    
    raw_suffix = ''
    if raw_meas :
        raw_suffix = '_raw'
    
    with db_data_mutex :
        json_body.append({}) 
        json_body[db_point_index]['fields'] = {}
        json_body[db_point_index]['measurement'] = 'opc_n2_{}{}'.format(meas_name, raw_suffix) 
        json_body[db_point_index]['fields']['value'] = pm
        json_body[db_point_index]['time'] = tc
        db_point_index += 1 
    

def main(argv = None) :
    global db_client, mqtt_client, alphasense 
    
    above_pm1_thresold = False
    above_pm2dot5_thresold = False
    above_pm10_thresold = False
    
    configure_logger()    
    read_config()
    logging.info('start air sensor reporter using alphasense opc n2')
    
    USB_SPI_PATH = '/dev/ttyACM0'    
    if not path.exists(USB_SPI_PATH):
        logging.error('{} does not exist'.format(USB_SPI_PATH))
        sys.exit(1) 
        
    try : 
        nb_serial_attempt = 10 
        while True :
            try :
                # Open a SPI connection
                spi = usbiss.USBISS(USB_SPI_PATH, 'spi', spi_mode = 2, freq = 500000)
                time.sleep(1.0) 
                break 
            except serial.serialutil.SerialException as e :
                nb_serial_attempt -= 1
                if nb_serial_attempt == 0 :
                    sys.exit(1) 
                else : 
                    logging.error('could not connect to {}'.format(USB_SPI_PATH)) 
                    time.sleep(1.0)

        connect_mqtt()
        
        db_client = InfluxDBClient(opc_config['db_ip'], opc_config['db_port'], opc_config['db_id'], opc_config['db_pass'], opc_config['db_name'])
        db_client.create_database(opc_config['db_name'])
        
        nb_opc_conn_attempt = 10    
        while True : 
            try:    
                alphasense = opc.OPCN2(spi)
                break 
            except Exception as e: 
                nb_opc_conn_attempt -= 1 
                if nb_opc_conn_attempt == 0 :
                    raise
                else:
                    logging.error('cannot connect opc sensor from spi - {}'.format(e)) 
                    time.sleep(2.0) 

        # Turn the opc ON
        alphasense.on()
        sleep(0.5)

        logging.info('alphasense actual config:\n{}'.format(alphasense.config2()))

        # Read the information string
        logging.info(alphasense.read_info_string())
        sleep(0.5)

        add_window_filter('PM1', 3) 
        add_window_filter('PM2.5', 3) 
        add_window_filter('PM10', 6) 
        
        # push data to db in a separate thread 
        db_th = thread.Thread(target=publisher_loop) 
        db_th.daemon = True 
        db_th.start()
        
        # Read the histogram
        while True :
            pms = alphasense.pm()
            
            pm1 = get_filtered_value('PM1', pms['PM1'])
            pm2dot5 = get_filtered_value('PM2.5', pms['PM2.5'])
            pm10 = get_filtered_value('PM10', pms['PM10'])
            
            logging.info('Raw meas      :  PM1 = {:.2f}µg/m3   PM2.5 = {:.2f}µg/m3  PM10 = {:.2f}µg/m3'.format(pms['PM1'], pms['PM2.5'], pms['PM10'])) 
            logging.info('Filtered meas :  PM1 = {:.2f}µg/m3   PM2.5 = {:.2f}µg/m3  PM10 = {:.2f}µg/m3'.format(pm1, pm2dot5, pm10)) 
            
            meas_tc = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ') 
             
            # Add filtered measures to DB 
            add_meas_to_db('pm1'   , meas_tc, pm1     , False)
            add_meas_to_db('pm2.5' , meas_tc, pm2dot5 , False)
            add_meas_to_db('pm10'  , meas_tc, pm10    , False)
            
            # Add raw measures to DB
            add_meas_to_db('pm1'   , meas_tc, pms['PM1']   , True)
            add_meas_to_db('pm2.5' , meas_tc, pms['PM2.5'] , True)
            add_meas_to_db('pm10'  , meas_tc, pms['PM10']  , True)
            
            # Add data to Weather Underground 
            wu_post(pm2dot5, pm10)

            # Publish data over MQTT
            mqtt_client.publish(opc_config['mqtt_pm1_topic']     , pm1)
            mqtt_client.publish(opc_config['mqtt_pm2dot5_topic'] , pm2dot5)
            mqtt_client.publish(opc_config['mqtt_pm10_topic']    , pm10)
            
            sleep(SAMPLING_PERIOD) 

    except KeyboardInterrupt :
        pass

    finally:
        logging.debug('closing air sensor reporter') 
        if alphasense is not None: 
            # Turn the opc OFF
            alphasense.off()
        if mqtt_client : 
            mqtt_client.disconnect() 


if __name__ == "__main__":
    sys.exit(main())

