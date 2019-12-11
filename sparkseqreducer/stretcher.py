#     stretcher: Functions to call C stretcher code for sequence alignment in python using ctypes
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
import ctypes
import os

_lib_stretcher = os.path.abspath(os.path.join(os.path.dirname(__file__), "./stretcher/libstretcher.so"))
_stretcher = ctypes.CDLL(_lib_stretcher)
_stretcher.stretcher.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]
_stretcher.stretcher.restype = ctypes.c_char_p


def align(seq_a, seq_b, cmp_mat_file):
    global _stretcher
    if len(seq_a) < len(seq_b):
        seq_a, seq_b = seq_b, seq_a
    # if s' and t' (s and t with gaps) are the alignment output then
    # card(s') = card(t') = l, and max(m, n) <= l <= m + n
    res0 = ctypes.create_string_buffer(len(seq_a) + len(seq_b) + 1)
    res1 = ctypes.create_string_buffer(len(seq_a) + len(seq_b) + 1)
    result = _stretcher.stretcher(seq_a.encode("UTF-8"), seq_b.encode("UTF-8"), res0, res1, cmp_mat_file.encode("UTF-8"))
    return res0.value.decode("UTF-8"), res1.value.decode("UTF-8")
