import time
from datetime import datetime, timedelta
from lcd_i2c import LcdSerialDisplay
from ADC import ADC

try:
    with ADC(gain=16) as adc:

        print('Reading ADS1x15 values, press Ctrl-C to quit...')
        # Print nice channel column headers.
        #print('| {0:>9} | {1:>9} | {2:>9} | {3:>9} |'.format(*range(4)))
        #print('-' * 49)

        
        with LcdSerialDisplay() as display:

            # Main loop.
            while True:
                # Get a list of voltages
                #voltages = adc.readVoltages()
                #rawVoltage = adc.readDifferential_0_1()
                rawVoltage = adc.getLastVoltage_01diff()

                # v0 is a percentage of 5V on a 0-50A current monitor
                current = (rawVoltage/5.0)*50.0
                #current = (voltages[0]/5.0)*50.0
                voltage = 242.0
                wattage = current*voltage

                now = datetime.utcnow()
                timeStr = now.strftime("%Y-%m-%dT%H:%M:%SZ") # floor of now
                nextSec = datetime.strptime(timeStr, "%Y-%m-%dT%H:%M:%SZ") + timedelta(seconds=1) #floor of next second
                
                # Print the ADC values.
                #print('| {0:>+9.6f} | {1:>+9.6f} | {2:>+9.6f} | {3:>+9.6f} |'.format(*voltages))
                
                #display.print(list(map(lambda f: '{0:>+9.6f}'.format(f), voltages)))
                display.print([timeStr,
                    '{0:>+9.6f} Amps'.format(current),
                    '{0:>+9.4f} Volts'.format(voltage),
                    '{0:>+9.6f} Watts'.format(wattage)])
                # Pause until the next second.
                pause = (nextSec - datetime.utcnow()).total_seconds()
                if pause > 0:
                    time.sleep(pause)
                # Else don't pause 

except KeyboardInterrupt:
    quit()
