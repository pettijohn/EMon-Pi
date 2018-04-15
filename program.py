import time
from lcd_i2c import LcdSerialDisplay
from ADC import ADC

adc = ADC(gain=2/3)

print('Reading ADS1x15 values, press Ctrl-C to quit...')
# Print nice channel column headers.
print('| {0:>9} | {1:>9} | {2:>9} | {3:>9} |'.format(*range(4)))
print('-' * 49)

try:
    with LcdSerialDisplay() as display:

        # Main loop.
        while True:
            # Get a list of voltages
            voltages = adc.readVoltages()
            
            # Print the ADC values.
            print('| {0:>+9.6f} | {1:>+9.6f} | {2:>+9.6f} | {3:>+9.6f} |'.format(*voltages))
            
            display.print(list(map(lambda f: '{0:>+9.6f}'.format(f), voltages)))
            # Pause for one second.
            time.sleep(1.0)
except KeyboardInterrupt:
    quit()