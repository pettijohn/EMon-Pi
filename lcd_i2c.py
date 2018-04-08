#!/usr/bin/python
#--------------------------------------
#    ___  ___  _ ____
#   / _ \/ _ \(_) __/__  __ __
#  / , _/ ___/ /\ \/ _ \/ // /
# /_/|_/_/  /_/___/ .__/\_, /
#                /_/   /___/
#
#  lcd_i2c.py
#  LCD test script using I2C backpack.
#  Supports 16x2 and 20x4 screens.
#
# Author : Matt Hawkins
# Date   : 20/09/2015
#
# http://www.raspberrypi-spy.co.uk/
#
# Copyright 2015 Matt Hawkins
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#--------------------------------------
import smbus
import time

class LcdDisplay:

    def __init__(self):
        #Open I2C interface
        #bus = smbus.SMBus(0)  # Rev 1 Pi uses 0
        self.bus = smbus.SMBus(1) # Rev 2 Pi uses 1
        self.lcd_init()

        # Define some device parameters
        self.I2C_ADDR  = 0x27 # I2C device address
        self.LCD_WIDTH = 20   # Maximum characters per line

        # Define some device constants
        self.LCD_CHR = 1 # Mode - Sending data
        self.LCD_CMD = 0 # Mode - Sending command

        self.LCD_LINE_1 = 0x80 # LCD RAM address for the 1st line
        self.LCD_LINE_2 = 0xC0 # LCD RAM address for the 2nd line
        self.LCD_LINE_3 = 0x94 # LCD RAM address for the 3rd line
        self.LCD_LINE_4 = 0xD4 # LCD RAM address for the 4th 
        # Helper array
        self.LCD_LINES = [self.LCD_LINE_1, self.LCD_LINE_2, self.LCD_LINE_3, self.LCD_LINE_4]

        self.LCD_BACKLIGHT  = 0x08  # On
        #self.LCD_BACKLIGHT = 0x00  # Off

        self.ENABLE = 0b00000100 # self.ENABLE bit

        # Timing constants
        self.E_PULSE = 0.0005
        self.E_DELAY = 0.0005

    

    def lcd_init(self):
        # Initialise display
        self.lcd_byte(0x32,self.LCD_CMD) # 110010 Initialise
        self.lcd_byte(0x06,self.LCD_CMD) # 000110 Cursor move direction
        self.lcd_byte(0x0C,self.LCD_CMD) # 001100 Display On,Cursor Off, Blink Off 
        self.lcd_byte(0x28,self.LCD_CMD) # 101000 Data length, number of lines, font size
        self.lcd_byte(0x01,self.LCD_CMD) # 000001 Clear display
        self.lcd_byte(0x33,self.LCD_CMD) # 110011 Initialise
        time.sleep(self.E_DELAY)

    def lcd_byte(self, bits, mode):
        # Send byte to data pins
        # bits = the data
        # mode = 1 for data
        #        0 for command

        bits_high = mode | (bits & 0xF0) | self.LCD_BACKLIGHT
        bits_low = mode | ((bits<<4) & 0xF0) | self.LCD_BACKLIGHT

        # High bits
        self.bus.write_byte(self.I2C_ADDR, bits_high)
        self.lcd_toggle_enable(bits_high)

        # Low bits
        self.bus.write_byte(self.I2C_ADDR, bits_low)
        self.lcd_toggle_enable(bits_low)

    def lcd_toggle_enable(self, bits):
        # Toggle enable
        time.sleep(self.E_DELAY)
        self.bus.write_byte(self.I2C_ADDR, (bits | self.ENABLE))
        time.sleep(self.E_PULSE)
        self.bus.write_byte(self.I2C_ADDR,(bits & ~self.ENABLE))
        time.sleep(self.E_DELAY)

    def lcd_string(self, message, line):
        # Send string to display

        message = message.ljust(self.LCD_WIDTH," ")

        self.lcd_byte(line, self.LCD_CMD)

        for i in range(self.LCD_WIDTH):
            self.lcd_byte(ord(message[i]),self.LCD_CHR)

    def print(self, messages):
        if len(messages) != 4:
            raise "Array of length 4 required"
        for i in range(4):
            self.lcd_string(str(messages[i]), self.LCD_LINES[i])

#     def main():
#         # Main program block

#         # Initialise display
#         display = LcdDisplay()

#         while True:

#             # Send some test
#             lcd_string("RPiSpy         <",self.LCD_LINE_1)
#             lcd_string("I2C LCD        <",self.LCD_LINE_2)
#             lcd_string("Hello              <", self.LCD_LINE_3)
#             lcd_string("world!             !", self.LCD_LINE_4)

#             time.sleep(3)
    
#             # Send some more text
#             lcd_string(">         RPiSpy",self.LCD_LINE_1)
#             lcd_string(">        I2C LCD",self.LCD_LINE_2)

#             time.sleep(3)

# if __name__ == '__main__':

#     try:
#         main()
#     except KeyboardInterrupt:
#         pass
#     finally:
#         lcd_byte(0x01, self.self.LCD_CMD)

