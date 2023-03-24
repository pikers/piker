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
import typer

from ._pos import open_pps


ledger = typer.Typer()


@ledger.command()
def sync(
    brokername: str,
    account: str,
):
    with open_pps(
        brokername,
        account,
    ) as table:
        breakpoint()


if __name__ == "__main__":
    ledger()
