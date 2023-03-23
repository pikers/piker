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
TOML codec hacks to make position tables look decent.

(looking at you "`toml`-lib"..)

'''
import re

import toml


# TODO: instead see if we can hack tomli and tomli-w to do the same:
# - https://github.com/hukkin/tomli
# - https://github.com/hukkin/tomli-w
class PpsEncoder(toml.TomlEncoder):
    '''
    Special "styled" encoder that makes a ``pps.toml`` redable and
    compact by putting `.clears` tables inline and everything else
    flat-ish.

    '''
    separator = ','

    def dump_list(self, v):
        '''
        Dump an inline list with a newline after every element and
        with consideration for denoted inline table types.

        '''
        retval = "[\n"
        for u in v:
            if isinstance(u, toml.decoder.InlineTableDict):
                out = self.dump_inline_table(u)
            else:
                out = str(self.dump_value(u))

            retval += " " + out + "," + "\n"
        retval += "]"
        return retval

    def dump_inline_table(self, section):
        """Preserve inline table in its compact syntax instead of expanding
        into subsection.
        https://github.com/toml-lang/toml#user-content-inline-table
        """
        val_list = []
        for k, v in section.items():
            # if isinstance(v, toml.decoder.InlineTableDict):
            if isinstance(v, dict):
                val = self.dump_inline_table(v)
            else:
                val = str(self.dump_value(v))

            val_list.append(k + " = " + val)

        retval = "{ " + ", ".join(val_list) + " }"
        return retval

    def dump_sections(self, o, sup):
        retstr = ""
        if sup != "" and sup[-1] != ".":
            sup += '.'
        retdict = self._dict()
        arraystr = ""
        for section in o:
            qsection = str(section)
            value = o[section]

            if not re.match(r'^[A-Za-z0-9_-]+$', section):
                qsection = toml.encoder._dump_str(section)

            # arrayoftables = False
            if (
                self.preserve
                and isinstance(value, toml.decoder.InlineTableDict)
            ):
                retstr += (
                    qsection
                    +
                    " = "
                    +
                    self.dump_inline_table(o[section])
                    +
                    '\n'  # only on the final terminating left brace
                )

            # XXX: this code i'm pretty sure is just blatantly bad
            # and/or wrong..
            # if isinstance(o[section], list):
            #     for a in o[section]:
            #         if isinstance(a, dict):
            #             arrayoftables = True
            # if arrayoftables:
            #     for a in o[section]:
            #         arraytabstr = "\n"
            #         arraystr += "[[" + sup + qsection + "]]\n"
            #         s, d = self.dump_sections(a, sup + qsection)
            #         if s:
            #             if s[0] == "[":
            #                 arraytabstr += s
            #             else:
            #                 arraystr += s
            #         while d:
            #             newd = self._dict()
            #             for dsec in d:
            #                 s1, d1 = self.dump_sections(d[dsec], sup +
            #                                             qsection + "." +
            #                                             dsec)
            #                 if s1:
            #                     arraytabstr += ("[" + sup + qsection +
            #                                     "." + dsec + "]\n")
            #                     arraytabstr += s1
            #                 for s1 in d1:
            #                     newd[dsec + "." + s1] = d1[s1]
            #             d = newd
            #         arraystr += arraytabstr

            elif isinstance(value, dict):
                retdict[qsection] = o[section]

            elif o[section] is not None:
                retstr += (
                    qsection
                    +
                    " = "
                    +
                    str(self.dump_value(o[section]))
                )

                # if not isinstance(value, dict):
                if not isinstance(value, toml.decoder.InlineTableDict):
                    # inline tables should not contain newlines:
                    # https://toml.io/en/v1.0.0#inline-table
                    retstr += '\n'

            else:
                raise ValueError(value)

        retstr += arraystr
        return (retstr, retdict)
