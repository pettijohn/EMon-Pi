import time
import connect
from decimal import Decimal
from datetime import datetime, timedelta
from lcd_i2c import LcdSerialDisplay
from ADC import ADC

arn = "arn:aws:iot:us-east-1:422446087002:thing/EMonPi"
client = connect.Client(arn)
rate = Decimal(str(0.1326))

try:
    with ADC(gain=2/3) as adc:

        #print('Reading ADS1x15 values, press Ctrl-C to quit...')
        # Print nice channel column headers.
        #print('| {0:>9} | {1:>9} | {2:>9} | {3:>9} |'.format(*range(4)))
        #print('-' * 49)

        
        with LcdSerialDisplay() as display:
            # Take a reading each second - at the next minute, average and send to cloud
            currentReadings = []
            prevMinute = ""
            
            # Main loop.
            while True:
                # Read voltage from the ADC
                rawVoltage = Decimal(str(adc.getLastVoltage_01diff()))

                # v0 is a percentage of 5V on a 0-50A current monitor
                current = (rawVoltage/Decimal('5.0'))*Decimal('50.0')
                currentReadings.append(current)
                voltage = Decimal('242.0')
                wattage = current*voltage

                now = datetime.utcnow()
                timeStr = now.strftime("%Y-%m-%dT%H:%M:%SZ") # floor of current second
                minuteStr = now.strftime("%Y-%m-%dT%H:%M:00Z") # floor of current minute
                nextSec = datetime.strptime(timeStr, "%Y-%m-%dT%H:%M:%SZ") + timedelta(seconds=1) #floor of next second
                
                # Print the ADC values.
                #print('| {0:>+9.6f} | {1:>+9.6f} | {2:>+9.6f} | {3:>+9.6f} |'.format(*voltages))
                
                #display.print(list(map(lambda f: '{0:>+9.6f}'.format(f), voltages)))
                display.print([timeStr,                    
                    '{0:>+9.6f} Amps'.format(current),
                    '{0:>+9.4f} Volts'.format(voltage),
                    '{0:>+9.6f} Watts'.format(wattage)])

                if prevMinute == "":
                    prevMinute = minuteStr
                if prevMinute != minuteStr:
                    # This reading started a new minute
                    # Average all of the current readings
                    prevMinute = minuteStr
                    avgCurrent = sum(currentReadings) / len(currentReadings)
                    # Reset the array
                    currentReadings = []
                    payload = { "device_id": arn,
                        "bucket_id": now.strftime("%Y-%m-%dT%H:%MZ"),
                        "current": (avgCurrent),
                        "volts": (voltage),
                        "watt_hours": current*voltage/60,
                        "cost_usd": current*voltage/60*rate
                    }
                    client.publish("EnergyReading/Minute", json.dumps(payload, cls=client.DecimalEncoder)
                    


                # Pause until the next second.
                pause = (nextSec - datetime.utcnow()).total_seconds()
                if pause > 0:
                    time.sleep(pause)
                # Else don't pause 

except KeyboardInterrupt:
    quit()
finally:
    client.disconnect()