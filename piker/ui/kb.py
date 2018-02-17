"""
VI-like Keyboard controls
"""
from kivy.core.window import Window

from ..log import get_logger
log = get_logger('keyboard')


async def handle_input(widget, patts2funcs: dict, patt_len_limit=3):
    """Handle keyboard inputs.

    For each character pattern in ``patts2funcs`` invoke the corresponding
    mapped functions.
    """
    def _kb_closed():
        """Teardown handler.
        """
        log.debug('Closing keyboard controls')
        print('Closing keyboard controls')
        kb.unbind(on_key_down=_kb_closed)

    last_patt = []

    kb = Window.request_keyboard(_kb_closed, widget, 'text')
    async for kb, keycode, text, modifiers in kb.async_bind(
            'on_key_down'
    ):
        # log.debug(
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
            func = patts2funcs.get(key)

        if func:
            log.debug(f'invoking func kb {func}')
            func()
            last_patt = []
        elif key == 'q':
            kb.release()

        if len(last_patt) > patt_len_limit:
            last_patt = []

        log.debug(f"last_patt {last_patt}")
