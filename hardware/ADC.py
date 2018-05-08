import Adafruit_ADS1x15

class ADC:

    def __init__(self, gain=1):
        # Choose a gain of 1 for reading voltages from 0 to 4.09V.
        # Or pick a different gain to change the range of voltages that are read:
        #  - 2/3 = +/-6.144V
        #  -   1 = +/-4.096V
        #  -   2 = +/-2.048V
        #  -   4 = +/-1.024V
        #  -   8 = +/-0.512V
        #  -  16 = +/-0.256V
        # See table 3 in the ADS1015/ADS1115 datasheet for more info on gain.

        gainVoltMap = { 
            2/3: 6.144,
            1:  4.096,
            2:  2.048,
            4:  1.024,
            8:  0.512,
            16: 0.256
        }

        assert gain in gainVoltMap

        self.gain = gain
        self.vMax = gainVoltMap[gain]

        # Create an ADS1115 ADC (16-bit) instance.
        self.adc = Adafruit_ADS1x15.ADS1115()
        self.adc.start_adc_difference(0, self.gain)

    def __enter__(self):
        # support with block
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # clean up from with block
        self.adc.stop_adc()

    def getLastVoltage_01diff(self):
        return self.adc.get_last_result() / 32767.0 * self.vMax

    # def readVoltagesSingleShot(self):
    #     # Note you can also pass in an optional data_rate parameter that controls
    #     # the ADC conversion time (in samples/second). Each chip has a different
    #     # set of allowed data rate values, see datasheet Table 9 config register
    #     # DR bit values.
    #     #values[i] = adc.read_adc(i, gain=GAIN, data_rate=128)
    #     # Each value will be a 12 or 16 bit signed integer value depending on the
    #     # ADC (ADS1015 = 12-bit, ADS1115 = 16-bit).
    #     values = map(lambda i: self.adc.read_adc(i, gain=self.gain), range(4))
    #     return list(map(lambda v: v / 32767.0 * self.vMax, values))

    # def readDifferential_0_1(self):
    #     return self.adc.read_adc_difference(0, self.gain) / 32767.0 * self.vMax

