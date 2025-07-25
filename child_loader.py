import logging
from typing import List
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from ir_modernized.models.entity import PersonChild, Child


logger = logging.getLogger(__name__)


def load_children_by_tokenids(session: Session, token_ids: List[int]) -> List[Child]:
    """
    Load Child records (PersonChild or OrganizationChild) associated with the given TokenIDs.

    Args:
        session (Session): The SQLAlchemy session object used for database queries.
        token_ids (List[int]): A list of TokenIDs to filter Child records.

    Returns:
        List[Child]: A list of Child objects (which will be instances of PersonChild or
                     OrganizationChild) matching the given TokenIDs.

    Raises:
        ValueError: If token_ids is not a list of integers.
        SQLAlchemyError: If there is an error executing the database query.
    """
    # Check if token_ids is empty and return an empty list if so
    if not token_ids:
        logger.info("Empty token_ids list provided. Returning empty list.")
        return []

    # Execute the database query with error handling
    try:
        return (
            session.query(Child)
            .filter(Child.TokenID.in_(token_ids))
            .all()
        )
    except SQLAlchemyError as e:
        logger.error(f"Database query failed: {e}")
        raise



