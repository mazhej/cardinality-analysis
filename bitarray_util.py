"""
bitarray_util.py
===============
This module provides utility functions for converting between bitarrays and string representations.
Used by both K12 and wage processors to avoid code duplication.

Functions:
    convert_bitarray_to_string: Convert bitarray to string representation.
    convert_string_to_bitarray: Convert string representation to bitarray.
"""

from typing import List


def convert_bitarray_to_string(bitarray: List[int], enrolled_char: str = '#', not_enrolled_char: str = '_') -> str:
    """
    Convert a bitarray to a string representation.
    
    Args:
        bitarray (List[int]): List of integers representing binary values (0 or 1).
        enrolled_char (str): Character to represent enrolled/present (1) values.
        not_enrolled_char (str): Character to represent not enrolled/absent (0) values.
        
    Returns:
        str: String representation of the bitarray.
    """
    return ''.join(enrolled_char if bit == 1 else not_enrolled_char for bit in bitarray)


def convert_string_to_bitarray(enrollment_string: str, enrolled_char: str = '#') -> List[int]:
    """
    Convert a string representation to a bitarray.
    
    Args:
        enrollment_string (str): String with enrolled and not enrolled characters.
        enrolled_char (str): Character that represents enrolled/present (1) values.
        
    Returns:
        List[int]: List of integers representing binary values.
    """
    return [1 if char == enrolled_char else 0 for char in enrollment_string] 