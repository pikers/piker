"""Hoverable Behaviour (changing when the mouse is on the widget by O. Poyen.
License: LGPL
"""
__author__ = 'Olivier Poyen'


from kivy.properties import BooleanProperty, ObjectProperty
from kivy.factory import Factory
from kivy.core.window import Window


class HoverBehavior(object):
    """Hover behavior.

    :Events:
        `on_enter`
            Fired when mouse enter the bbox of the widget.
        `on_leave`
            Fired when the mouse exit the widget.
    """
    hovered = BooleanProperty(False)
    # Contains the last relevant point received by the Hoverable. This can
    # be used in `on_enter` or `on_leave` in order to know where was dispatched
    # the event.
    border_point = ObjectProperty(None)

    def __init__(self, **kwargs):
        self.register_event_type('on_enter')
        self.register_event_type('on_leave')
        Window.bind(mouse_pos=self.on_mouse_pos)
        super(HoverBehavior, self).__init__(**kwargs)

    def on_mouse_pos(self, *args):
        # do proceed if I'm not displayed <=> If have no parent
        if not self.get_root_window():
            return
        pos = args[1]
        # Next line to_widget allow to compensate for relative layout
        inside = self.collide_point(*self.to_widget(*pos))
        if self.hovered == inside:
            # We have already done what was needed
            return
        self.border_point = pos
        self.hovered = inside
        if inside:
            self.dispatch('on_enter')
        else:
            self.dispatch('on_leave')

    # implement these in the widget impl

    def on_enter(self):
        pass

    def on_leave(self):
        pass


# register for global use via kivy.factory.Factory
Factory.register('HoverBehavior', HoverBehavior)
