"""Mouse over behaviour.

Based on initial LGPL work by O. Poyen. here:
https://gist.github.com/opqopq/15c707dc4cffc2b6455f
"""
import time
from functools import wraps
from collections import deque

from kivy.properties import BooleanProperty, ObjectProperty
from kivy.core.window import Window
from kivy.clock import Clock

from ...log import get_logger


log = get_logger('kivy')


# XXX: copied from 1.10.1 since the async branch isn't ported yet.
def triggered(timeout=0, interval=False):
    '''Decorator that will trigger the call of the function at the specified
    timeout, through the method :meth:`CyClockBase.create_trigger`. Subsequent
    calls to the decorated function (while the timeout is active) are ignored.
    It can be helpful when an expensive funcion (i.e. call to a server) can be
    triggered by different methods. Setting a proper timeout will delay the
    calling and only one of them wil be triggered.
        @triggered(timeout, interval=False)
        def callback(id):
            print('The callback has been called with id=%d' % id)
        >> callback(id=1)
        >> callback(id=2)
        The callback has been called with id=2
    The decorated callback can also be unscheduled using:
        >> callback.cancel()
    .. versionadded:: 1.10.1
    '''

    def wrapper_triggered(func):

        _args = []
        _kwargs = {}

        def cb_function(dt):
            func(*tuple(_args), **_kwargs)

        cb_trigger = Clock.create_trigger(
            cb_function,
            timeout=timeout,
            interval=interval)

        @wraps(func)
        def trigger_function(*args, **kwargs):
            _args[:] = []
            _args.extend(list(args))
            _kwargs.clear()
            _kwargs.update(kwargs)
            cb_trigger()

        def trigger_cancel():
            cb_trigger.cancel()

        setattr(trigger_function, 'cancel', trigger_cancel)

        return trigger_function

    return wrapper_triggered


class MouseOverBehavior(object):
    """Mouse over behavior.

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
    _widgets = deque()
    _last_hovered = None
    _last_time = time.time()

    def __init__(self, **kwargs):
        self.register_event_type('on_enter')
        self.register_event_type('on_leave')
        super().__init__(**kwargs)
        Window.bind(mouse_pos=self._on_mouse_pos)
        self._widgets.append(self)

    def __del__(self):
        MouseOverBehavior.remove(self)

    @classmethod
    # throttle at 10ms latency
    @triggered(timeout=0.01, interval=False)
    def _on_mouse_pos(cls, *args):
        log.debug(
            f"{cls} time since last call: {time.time() - cls._last_time}")
        cls._last_time = time.time()
        # XXX: how to still do this at the class level?
        # don't proceed if I'm not displayed <=> If have no parent
        # if not self.get_root_window():
        #     return

        pos = args[1]
        # Next line to_widget allow to compensate for relative layout
        for widget in cls._widgets:
            w_coords = widget.to_widget(*pos)
            inside = widget.collide_point(*w_coords)
            if inside and widget.hovered:
                log.debug('already hovered')
                return
            elif inside:
                # un-highlight the last highlighted
                last_hovered = cls._last_hovered
                if last_hovered:
                    last_hovered.dispatch('on_leave')
                    last_hovered.hovered = False

                # stick last highlighted at the front of the stack
                # resulting in LIFO iteration for efficiency
                cls._widgets.remove(widget)
                cls._widgets.appendleft(widget)
                cls._last_hovered = widget

                # highlight new widget
                widget.border_point = pos
                widget.hovered = True
                widget.dispatch('on_enter')
                return

    # implement these in the widget impl

    @classmethod
    def on_enter(cls):
        pass

    @classmethod
    def on_leave(cls):
        pass


def new_mouse_over_group():
    """Return a new *mouse over group*, a class that can be mixed
    in to a group of widgets which can be mutex highlighted based
    on the mouse position.
    """
    return type(
        'MouseOverBehavior',
        (MouseOverBehavior,),
        {},
    )
