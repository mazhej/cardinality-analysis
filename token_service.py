"""
token_service.py

This module provides functions to manage and retrieve TokenID and P20ID data from the database,
specifically for PersonToken entities. It handles database queries and error conditions efficiently.

Description:
    This module provides functions to retrieve TokenID and P20ID pairs from the PersonToken
    table based on a list of P20IDs. Since P20IDs are sourced from a database with integer constraints,
    redundant type validation is avoided. The module integrates with SQLAlchemy for database operations
    and includes logging for debugging and monitoring.

Functions:
    get_tokenids_by_p20ids: Function to retrieve TokenID and P20ID data.

Dependencies:
    - sqlalchemy.orm: For database session and query management.
    - ir_modernized.models.entity: For the PersonToken model.
    - logging: For logging operations and errors.
"""


import logging
from typing import List, Tuple, Type
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from ir_modernized.models.entity import Token


logger = logging.getLogger(__name__)


def get_tokenids_by_p20ids(session: Session, p20ids: List[int], token_model: Type[Token]) -> List[Tuple[int, int]]:
    """
    Retrieve a list of (TokenID, P20ID) tuples for the provided P20IDs and token model.

    This function queries the Token table to find all TokenID and P20ID pairs
    matching the given P20IDs and token model.

    Args:
        session (Session): The SQLAlchemy session for database interactions.
        p20ids (List[int]): A list of P20IDs to filter Token records.
        token_model (Type[Token]): The specific Token model class to query.

    Returns:
        List[Tuple[int, int]]: A list of (TokenID, P20ID) tuples.

    Raises:
        SQLAlchemyError: If there is an error executing the database query.
    """

    # Execute the database query with error handling
    if not p20ids:
        logger.info("Empty P20ID list provided; returning empty result.")
        return []
    
    try:
        rows = (
            session.query(token_model.TokenID, token_model.P20ID)
            .filter(token_model.P20ID.in_(p20ids))
            .all()
        )
        if not rows:
            logger.info("No rows found for the provided P20IDs.")
        return [tuple(row) for row in rows]
    except SQLAlchemyError as e:
        logger.error(f"Database query failed: {str(e)}")
        raise



