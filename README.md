# Energy monitoring on a Raspberry Pi.

Goal is to read current and voltage on a houshold AC circuit
via an ADS1115 I2C ADC, rendering a simple UI on a textual multi-line
display, finally logging data to an AWS lambda function for historical analysis plus a web UI.

# References 
* ADS 1115 - I2C ADC    
  * Example https://learn.adafruit.com/raspberry-pi-analog-to-digital-converters/ads1015-slash-ads1115
  * Datasheet https://cdn-shop.adafruit.com/datasheets/ads1115.pdf
* I2C Multiline Textual Display 