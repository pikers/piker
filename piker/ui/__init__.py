"""
Stuff for you eyes.
"""
import os

# use the trio async loop
os.environ['KIVY_EVENTLOOP'] = 'trio'
import kivy
kivy.require('1.10.0')
