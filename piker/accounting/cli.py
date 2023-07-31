# piker: trading gear for hackers
# Copyright (C) Tyler Goodlet (in stewardship for pikers)

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

'''
CLI front end for trades ledger and position tracking management.

'''
from __future__ import annotations
from pprint import pformat


from rich.console import Console
from rich.markdown import Markdown
import polars as pl
import tractor
import trio
import typer

from ..log import get_logger
from ..service import (
    open_piker_runtime,
)
from ..clearing._messages import BrokerdPosition
from ..calc import humanize
from ..brokers._daemon import broker_init
from ._ledger import (
    load_ledger,
    TransactionLedger,
    # open_trade_ledger,
)
from .calc import (
    open_ledger_dfs,
)


ledger = typer.Typer()


def unpack_fqan(
    fully_qualified_account_name: str,
    console: Console | None = None,
) -> tuple | bool:
    try:
        brokername, account = fully_qualified_account_name.split('.')
        return brokername, account
    except ValueError:
        if console is not None:
            md = Markdown(
                f'=> `{fully_qualified_account_name}` <=\n\n'
                'is not a valid '
                '__fully qualified account name?__\n\n'
                'Your account name needs to be of the form '
                '`<brokername>.<account_name>`\n'
            )
            console.print(md)
        return False


@ledger.command()
def sync(
    fully_qualified_account_name: str,
    pdb: bool = False,

    loglevel: str = typer.Option(
        'error',
        "-l",
    ),
):
    log = get_logger(loglevel)
    console = Console()

    pair: tuple[str, str]
    if not (pair := unpack_fqan(
        fully_qualified_account_name,
        console,
    )):
        return

    brokername, account = pair

    brokermod, start_kwargs, deamon_ep = broker_init(
        brokername,
        loglevel=loglevel,
    )
    brokername: str = brokermod.name

    async def main():

        async with (
            open_piker_runtime(
                name='ledger_cli',
                loglevel=loglevel,
                debug_mode=pdb,

            ) as (actor, sockaddr),

            tractor.open_nursery() as an,
        ):
            try:
                log.info(
                    f'Piker runtime up as {actor.uid}@{sockaddr}'
                )

                portal = await an.start_actor(
                    loglevel=loglevel,
                    debug_mode=pdb,
                    **start_kwargs,
                )

                from ..clearing import (
                    open_brokerd_dialog,
                )
                brokerd_stream: tractor.MsgStream

                async with (
                    # engage the brokerd daemon context
                    portal.open_context(
                        deamon_ep,
                        brokername=brokername,
                        loglevel=loglevel,
                    ),

                    # manually open the brokerd trade dialog EP
                    # (what the EMS normally does internall) B)
                    open_brokerd_dialog(
                        brokermod,
                        portal,
                        exec_mode=(
                            'paper'
                            if account == 'paper'
                            else 'live'
                        ),
                        loglevel=loglevel,
                    ) as (
                        brokerd_stream,
                        pp_msg_table,
                        accounts,
                    ),
                ):
                    try:
                        assert len(accounts) == 1
                        if not pp_msg_table:
                            ld, fpath = load_ledger(brokername, account)
                            assert not ld, f'WTF did we fail to parse ledger:\n{ld}'

                            console.print(
                                '[yellow]'
                                'No pps found for '
                                f'`{brokername}.{account}` '
                                'account!\n\n'
                                '[/][underline]'
                                'None of the following ledger files exist:\n\n[/]'
                                f'{fpath.as_uri()}\n'
                            )
                            return

                        pps_by_symbol: dict[str, BrokerdPosition] = pp_msg_table[
                            brokername,
                            account,
                        ]

                        summary: str = (
                            '[dim underline]Piker Position Summary[/] '
                            f'[dim blue underline]{brokername}[/]'
                            '[dim].[/]'
                            f'[blue underline]{account}[/]'
                            f'[dim underline] -> total pps: [/]'
                            f'[green]{len(pps_by_symbol)}[/]\n'
                        )
                        # for ppdict in positions:
                        for fqme, ppmsg in pps_by_symbol.items():
                            # ppmsg = BrokerdPosition(**ppdict)
                            size = ppmsg.size
                            if size:
                                ppu: float = round(
                                    ppmsg.avg_price,
                                    ndigits=2,
                                )
                                cost_basis: str = humanize(size * ppu)
                                h_size: str = humanize(size)

                                if size < 0:
                                    pcolor = 'red'
                                else:
                                    pcolor = 'green'

                                # sematic-highlight of fqme
                                fqme = ppmsg.symbol
                                tokens = fqme.split('.')
                                styled_fqme = f'[blue underline]{tokens[0]}[/]'
                                for tok in tokens[1:]:
                                    styled_fqme += '[dim].[/]'
                                    styled_fqme += f'[dim blue underline]{tok}[/]'

                                # TODO: instead display in a ``rich.Table``?
                                summary += (
                                    styled_fqme +
                                    '[dim]: [/]'
                                    f'[{pcolor}]{h_size}[/]'
                                    '[dim blue]u @[/]'
                                    f'[{pcolor}]{ppu}[/]'
                                    '[dim blue] = [/]'
                                    f'[{pcolor}]$ {cost_basis}\n[/]'
                                )

                        console.print(summary)

                    finally:
                        # exit via ctx cancellation.
                        brokerd_ctx: tractor.Context = brokerd_stream._ctx
                        await brokerd_ctx.cancel(timeout=1)

                    # TODO: once ported to newer tractor branch we should
                    # be able to do a loop like this:
                    # while brokerd_ctx.cancel_called_remote is None:
                    #     await trio.sleep(0.01)
                    #     await brokerd_ctx.cancel()

            finally:
                await portal.cancel_actor()

    trio.run(main)


@ledger.command()
def disect(
    # "fully_qualified_account_name"
    fqan: str,
    fqme: str,  # for ib

    # TODO: in tractor we should really have
    # a debug_mode ctx for wrapping any kind of code no?
    pdb: bool = False,
    bs_mktid: str = typer.Option(
        None,
        "-bid",
    ),
    loglevel: str = typer.Option(
        'error',
        "-l",
    ),
):
    from piker.log import get_console_log
    from piker.toolz import open_crash_handler
    get_console_log(loglevel)

    pair: tuple[str, str]
    if not (pair := unpack_fqan(fqan)):
        raise ValueError('{fqan} malformed!?')

    brokername, account = pair

    # ledger dfs groupby-partitioned by fqme
    dfs: dict[str, pl.DataFrame]
    # actual ledger instance
    ldgr: TransactionLedger

    pl.Config.set_tbl_cols(-1)
    pl.Config.set_tbl_rows(-1)
    with (
        open_crash_handler(),
        open_ledger_dfs(
            brokername,
            account,
        ) as (dfs, ldgr),
    ):

        # look up specific frame for fqme-selected asset
        if (df := dfs.get(fqme)) is None:
            mktids2fqmes: dict[str, list[str]] = {}
            for bs_mktid in dfs:
                df: pl.DataFrame = dfs[bs_mktid]
                fqmes: pl.Series[str] = df['fqme']
                uniques: list[str] = fqmes.unique()
                mktids2fqmes[bs_mktid] = set(uniques)
                if fqme in uniques:
                    break
            print(
                f'No specific ledger for fqme={fqme} could be found in\n'
                f'{pformat(mktids2fqmes)}?\n'
                f'Maybe the `{brokername}` backend uses something '
                'else for its `bs_mktid` then the `fqme`?\n'
                'Scanning for matches in unique fqmes per frame..\n'
            )

        # :pray:
        assert not df.is_empty()

        # TODO: we REALLY need a better console REPL for this
        # kinda thing..
        breakpoint()
