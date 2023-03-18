import Adafruit_DHT
import time
import mariadb
from datetime import datetime


dht_sensor = Adafruit_DHT.DHT22
dht_pin = 4 # depends on the connected pins!

try:
    connection = mariadb.connect(
        user="user",
        password="Password123!",
        host="localhost",
        port=3306,
        database="sensor_data")
    
    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS dht22
                        (timestamp TIMESTAMP DEFAULT NOW(),
                         temperature FLOAT,
                         humidity FLOAT)''')
    while True:
        humidity, temperature = Adafruit_DHT.read_retry(dht_sensor, dht_pin)      
        if humidity is not None and temperature is not None:
            sql = "INSERT INTO dht22 (timestamp, temperature, humidity) VALUES (DEFAULT, %s, %s)"
            cursor.execute(sql, ("{:.1f}".format(temperature), "{:.1f}".format(humidity)))
            connection.commit()
        else:
            print("Failed to read values from sensor")            
        time.sleep(60)
except mariadb.Error as error:
    print("Database error", error)
except KeyboardInterrupt:
    print("Exiting script with keyboard interrupt")
except:
    print("Something unexpected has happened")    
finally:
    if connection:
        connection.close()
        print("The connection is closed")


