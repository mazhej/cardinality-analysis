def create_smash_id(name: str) -> str:
    """
    Generate a SmashID from a name by converting it to uppercase, keeping only alphabetic characters,
    and removing duplicates while preserving the order of first occurrences.


    Args:
        name (str): The input name to generate a SmashID from (e.g., "JENNIFER DOOLITTLE").


    Returns:
        str: The generated SmashID (e.g., "JENIFRDOLT").


    Examples:
        >>> create_smash_id("JENNIFER DOOLITTLE")
        'JENIFRDOLT'
    """
    if not isinstance(name, str):
        raise TypeError("Input must be a string")
    seen = set()
    return "".join(
        c
        for c in name.upper()
        if c.isalpha() and c.isascii() and c not in seen and not seen.add(c)
    )




