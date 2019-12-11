#     test_alignment: Test code for sequence alignment calls in python using stretcher
#     Copyright (C) 2019  ja-pg
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.

from sparkseqreducer import stretcher
import os

SeqA = "TAATGAAATATTCACGAA"
SeqB = "TAATAAATATTCACAA"

SeqASmall = ""
SeqBSmall = ""
mat = os.path.abspath(os.path.join(os.path.dirname(__file__),"../data/EDNAFULL"))

print(stretcher.align(SeqA, SeqB, mat))