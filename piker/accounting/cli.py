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
from typing import (
    Any,
)

from rich.console import Console
from rich.markdown import Markdown
import tractor
import trio
import typer

from ..service import (
    open_piker_runtime,
)
from ..clearing._messages import BrokerdPosition
from ..calc import humanize


ledger = typer.Typer()


def broker_init(
    brokername: str,
    loglevel: str | None = None,

    **start_actor_kwargs,

) -> dict:
    '''
    Given an input broker name, load all named arguments
    which can be passed to a daemon + context spawn for
    the relevant `brokerd` service endpoint.

    '''
    from ..brokers import get_brokermod
    brokermod = get_brokermod(brokername)
    modpath = brokermod.__name__

    start_actor_kwargs['name'] = f'brokerd.{brokername}'
    start_actor_kwargs.update(
        getattr(
            brokermod,
            '_spawn_kwargs',
            {},
        )
    )

    # lookup actor-enabled modules declared by the backend offering the
    # `brokerd` endpoint(s).
    enabled = start_actor_kwargs['enable_modules'] = [modpath]
    for submodname in getattr(
        brokermod,
        '__enable_modules__',
        [],
    ):
        subpath = f'{modpath}.{submodname}'
        enabled.append(subpath)

        # TODO XXX: DO WE NEED THIS?
        # enabled.append('piker.data.feed')

    # non-blocking setup of brokerd service nursery
    from ..brokers._daemon import _setup_persistent_brokerd

    return (
        start_actor_kwargs,  # to `ActorNursery.start_actor()`
        _setup_persistent_brokerd,  # service task ep
        getattr(  # trades endpoint
            brokermod,
            'trades_dialogue',
            None,
        ),
    )


@ledger.command()
def sync(
    fully_qualified_account_name: str,
    pdb: bool = False,

    loglevel: str = typer.Option(
        'error',
        "-l",
    ),
):
    console = Console()

    try:
        brokername, account = fully_qualified_account_name.split('.')
    except ValueError:
        md = Markdown(
            f'=> `{fully_qualified_account_name}` <=\n\n'
            'is not a valid '
            '__fully qualified account name?__\n\n'
            'Your account name needs to be of the form '
            '`<brokername>.<account_name>`\n'
        )
        console.print(md)
        return

    start_kwargs, _, trades_ep = broker_init(
        brokername,
        loglevel=loglevel,
    )

    async def main():

        async with (
            open_piker_runtime(
                name='ledger_cli',
                loglevel=loglevel,
                debug_mode=pdb,

            ) as (actor, sockaddr),

            tractor.open_nursery() as an,
        ):
            portal = await an.start_actor(
                loglevel=loglevel,
                debug_mode=pdb,
                **start_kwargs,
            )

            if (
                brokername == 'paper'
                or trades_ep is None
            ):
                from ..clearing import _paper_engine as paper
                open_trades_endpoint = paper.open_paperboi(
                    fqme=None,  # tell paper to not start clearing loop
                    broker=brokername,
                    loglevel=loglevel,
                )
            else:
                # open live brokerd trades endpoint
                open_trades_endpoint = portal.open_context(
                    trades_ep,
                    loglevel=loglevel,
                )

            positions: dict[str, Any]
            accounts: list[str]
            async with (
                open_trades_endpoint as (
                    brokerd_ctx,
                    (positions, accounts,),
                ),
            ):
                summary: str = (
                    '[dim underline]Piker Position Summary[/] '
                    f'[dim blue underline]{brokername}[/]'
                    '[dim].[/]'
                    f'[blue underline]{account}[/]'
                    f'[dim underline] -> total pps: [/]'
                    f'[green]{len(positions)}[/]\n'
                )
                for ppdict in positions:
                    ppmsg = BrokerdPosition(**ppdict)
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

                # exit via ctx cancellation.
                await brokerd_ctx.cancel(timeout=1)
                # TODO: once ported to newer tractor branch we should
                # be able to do a loop like this:
                # while brokerd_ctx.cancel_called_remote is None:
                #     await trio.sleep(0.01)
                #     await brokerd_ctx.cancel()

            await portal.cancel_actor()

    trio.run(main)


if __name__ == "__main__":
    ledger()  # this is called from ``>> ledger <accountname>``
