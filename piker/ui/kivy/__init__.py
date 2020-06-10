"""
Legacy kivy components.
"""
import os
import sys

# XXX clear all flags at import to avoid upsetting
# ol' kivy see: https://github.com/kivy/kivy/issues/4225
# though this is likely a ``click`` problem
sys.argv[1:] = []

# use the trio async loop
os.environ['KIVY_EVENTLOOP'] = 'trio'
import kivy
kivy.require('1.10.0')
