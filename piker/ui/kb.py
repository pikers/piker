"""
Keyboard controls
"""
import inspect
from functools import partial

from kivy.core.window import Window
from kivy.uix.textinput import TextInput
from kivy.uix.scrollview import ScrollView

from ..log import get_logger
log = get_logger('keyboard')


async def handle_input(widget, patts2funcs: dict, patt_len_limit=3):
    """Handle keyboard input.

    For each character pattern-tuple in ``patts2funcs`` invoke the
    corresponding mapped function(s) or coro(s).
    """
    def _kb_closed():
        """Teardown handler.
        """
        log.debug('Closing keyboard controls')
        print('Closing keyboard controls')
        kb.unbind(on_key_down=_kb_closed)

    last_patt = []
    kb = Window.request_keyboard(_kb_closed, widget, 'text')
    keyq = kb.async_bind('on_key_down')

    while True:
        async for kb, keycode, text, modifiers in keyq:
            log.debug(
                f"Keyboard input received:\n"
                f"key {keycode}\ntext {text}\nmodifiers {modifiers}"
            )
            code, key = keycode
            if modifiers and key in modifiers:
                continue
            elif modifiers and key not in modifiers:
                key = '-'.join(modifiers + [key])

            # keep track of multi-key patterns
            last_patt.append(key)

            func = patts2funcs.get(tuple(last_patt))
            if not func:
                func = patts2funcs.get((key,))

            if inspect.iscoroutinefunction(func):
                # stop kb queue to avoid duplicate input processing
                keyq.stop()

                log.debug(f'invoking kb coro func {func}')
                await func()
                last_patt = []
                break  # trigger loop restart

            elif func:
                log.debug(f'invoking kb func {func}')
                func()
                last_patt = []

            if len(last_patt) > patt_len_limit:
                last_patt = []

            log.debug(f"last_patt {last_patt}")

        log.debug("Restarting keyboard handling loop")
        # rebind to avoid capturing keys processed by ^ coro
        keyq = kb.async_bind('on_key_down')

    log.debug("Exitting keyboard handling loop")


class SearchBar(TextInput):
    def __init__(self, kbctls: dict, parent, **kwargs):
        super(SearchBar, self).__init__(
            multiline=False,
            # text='/',
            **kwargs
        )
        self.kbctls = kbctls
        self._container = parent
        # indicate to ``handle_input`` that search is activated on '/'
        self.kbctls.update({
            ('/',): self.handle_input
        })

    def on_text(self, instance, value):
        # ``scroll.scroll_to()`` looks super awesome here for when we
        # our interproc-ticker protocol
        # async for item in self.async_bind('text'):
        print(value)

    async def handle_input(self):
        self._container.add_widget(self)  # display it
        self.focus = True  # focus immediately (doesn't work from __init__)
        # wait for <enter> to close search bar
        await self.async_bind('on_text_validate').__aiter__().__anext__()

        log.debug("Closing search bar")
        self._container.remove_widget(self)  # stop displaying
        return self.text


class PagerView(ScrollView):
    """Pager view that adds less-like scrolling and search.
    """
    def __init__(self, container, nursery, kbctls: dict = None, **kwargs):
        super(PagerView, self).__init__(**kwargs)
        self._container = container
        self.kbctls = kbctls or {}
        self.kbctls.update({
            ('g', 'g'): partial(self.move_y, 1),
            ('shift-g',): partial(self.move_y, 0),
            ('u',): partial(self.halfpage_y, 'u'),
            ('d',): partial(self.halfpage_y, 'd'),
            # ('?',):
        })
        self.search = SearchBar(self.kbctls, container)
        # spawn kb handler task
        nursery.start_soon(handle_input, self, self.kbctls)

    def move_y(self, val):
        '''Scroll in the y direction [0, 1].
        '''
        self.scroll_y = val

    def halfpage_y(self, direction):
        """Scroll a half-page up or down.
        """
        pxs = (self.height/2)
        _, yscale = self.convert_distance_to_scroll(0, pxs)
        new = self.scroll_y + (yscale * {'u': 1, 'd': -1}[direction])
        # bound to near [0, 1] to avoid "over-scrolling"
        limited = max(-0.03, min(new, 1.03))
        self.scroll_y = limited
