import time
from lcd_i2c import LcdSerialDisplay
from ADC import ADC

adc = ADC(gain=16)

print('Reading ADS1x15 values, press Ctrl-C to quit...')
# Print nice channel column headers.
print('| {0:>9} | {1:>9} | {2:>9} | {3:>9} |'.format(*range(4)))
print('-' * 49)

try:
    with LcdSerialDisplay() as display:

        # Main loop.
        while True:
            # Get a list of voltages
            #voltages = adc.readVoltages()
            rawVoltage = adc.readDifferential_0_1()

            # v0 is a percentage of 5V on a 0-50A current monitor
            current = (rawVoltage/5.0)*50.0
            #current = (voltages[0]/5.0)*50.0
            voltage = 123.0
            wattage = current*voltage
            
            # Print the ADC values.
            #print('| {0:>+9.6f} | {1:>+9.6f} | {2:>+9.6f} | {3:>+9.6f} |'.format(*voltages))
            
            #display.print(list(map(lambda f: '{0:>+9.6f}'.format(f), voltages)))
            display.print(['{0:>+9.6f} volts raw'.format(rawVoltage),
                '{0:>+9.6f} I'.format(current),
                '{0:>+9.4f} V'.format(voltage),
                '{0:>+9.6f} W'.format(wattage)])
            # Pause for one second.
            time.sleep(0.5)
except KeyboardInterrupt:
    quit()