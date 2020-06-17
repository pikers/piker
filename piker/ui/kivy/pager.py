"""
Pager widget + kb controls
"""
import inspect
from functools import partial

from kivy.core.window import Window
from kivy.uix.widget import Widget
from kivy.uix.textinput import TextInput
from kivy.uix.scrollview import ScrollView

from ...log import get_logger
from .utils_async import async_bind

log = get_logger('keyboard')


async def handle_input(
    nursery,
    widget,
    patts2funcs: dict,
    patt_len_limit=3
) -> None:
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
    keyq = async_bind(kb, 'on_key_down')

    while True:
        async for kb, keycode, text, modifiers in keyq:
            log.debug(f"""
            kb: {kb}
            keycode: {keycode}
            text: {text}
            modifiers: {modifiers}
            patts2funcs: {patts2funcs}
            """)
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

                log.debug(f'spawning task for kb func {func}')
                nursery.start_soon(func)
                last_patt = []
                break  # trigger loop restart

            elif func:
                log.debug(f'invoking kb func {func}')
                func()
                last_patt = []
                break

            if len(last_patt) > patt_len_limit:
                last_patt = []

            log.debug(f"last_patt {last_patt}")

        log.debug("Restarting keyboard handling loop")
        # rebind to avoid capturing keys processed by ^ coro
        keyq = async_bind(kb, 'on_key_down')

    log.debug("Exitting keyboard handling loop")


class SearchBar(TextInput):
    def __init__(
        self,
        container: Widget,
        pager: 'PagerView',
        searcher,
        **kwargs
    ):
        super(SearchBar, self).__init__(
            multiline=False,
            hint_text='Ticker Search',
            **kwargs
        )
        self.cursor_blink = False
        self.foreground_color = self.hint_text_color  # actually readable
        self._container = container
        self._pager = pager
        self._searcher = searcher
        # indicate to ``handle_input`` that search is activated on '/'
        self._pager.kbctls.update({
            ('/',): self.handle_input
        })

        self.kbctls = {
            ('ctrl-c',): self.undisplay,
        }

        self._sugg_template = ' '*4 + '{} matches:  '
        self._matched = []

    def undisplay(self):
        "Stop displaying this text widget"
        self.dispatch('on_text_validate')  # same as pressing <enter>
        if self.text_validate_unfocus:
            self.focus = False

    def suggest(self, matches):
        self.suggestion_text = ''
        suffix = self._sugg_template.format(len(matches) or "No")
        self.suggestion_text = suffix + '  '.join(matches)

    def on_text(self, instance, value):

        def unhighlight(cells):
            for cell in cells:
                cell.outline_color = [0]*3
                cell.outline_width = 0
                self._matched.remove(cell)

        if not value:
            if self._matched:
                unhighlight(self._matched.copy())
            return

        if not value.isupper():
            self.text = value.upper()
            return

        text = self.text
        matches = self._searcher.search(text)
        self.suggest(matches.keys())

        if matches:
            # unhighlight old matches
            unmatched = set(self._matched) - set(matches.keys())
            unhighlight(unmatched)

            for key, widget in matches.items():
                cell = widget.get_cell('symbol')
                # cell.background_color = [0.4]*3 + [1]
                cell.outline_width = 2
                cell.outline_color = [0.6] * 3
                self._matched.append(cell)

            key, widget = list(matches.items())[0]
            # ensure first match widget is displayed
            self._pager.scroll_to(widget)

    async def handle_input(self):
        # TODO: wrap this in a cntx mng
        old_ctls = self._pager.kbctls.copy()
        self._pager.kbctls.clear()
        # makes a copy
        self._pager.kbctls.update(self.kbctls)

        self._container.add_widget(self)  # display it
        self.focus = True  # focus immediately (doesn't work from __init__)

        # select any existing text making the widget ready
        # to accept new input right away
        if self.text:
            self.select_all()

        # wait for <enter> to close search bar
        await async_bind(self, 'on_text_validate').__aiter__().__anext__()
        log.debug(f"Seach text is {self.text}")

        log.debug("Closing search bar")
        self._container.remove_widget(self)  # stop displaying

        # restore old keyboard bindings
        self._pager.kbctls.clear()
        self._pager.kbctls.update(old_ctls)

        return self.text


class PagerView(ScrollView):
    """Pager view that adds less-like scrolling and search.
    """
    def __init__(self, container, contained, nursery, kbctls: dict = None,
                 **kwargs):
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
        # add contained child widget (can only be one)
        self._contained = contained
        self.add_widget(contained)
        self.search = SearchBar(container, self, searcher=contained)
        # spawn kb handler task
        nursery.start_soon(handle_input, nursery, self, self.kbctls)

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
        limited = max(-0.001, min(new, 1.001))
        self.scroll_y = limited
