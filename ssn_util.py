def clean_ssn(ssn):
    """
    Clean the SSN value to ensure it is a 9-digit string.
   
    Args:
        ssn: The SSN value from the worksheet.
   
    Returns:
        str: A cleaned 9-digit SSN string.
   
    Raises:
        ValueError: If the SSN cannot be cleaned to exactly 9 digits.
    """
    # Convert to string
    ssn_str = str(ssn)
    # Remove non-digit characters
    ssn_digits = ''.join(filter(str.isdigit, ssn_str))
    # Ensure exactly 9 digits
    if len(ssn_digits) != 9:
        raise ValueError(f"Invalid SSN length: {ssn_digits}")
    return ssn_digits



