import logging
from typing import List, Type
from sqlalchemy.orm import Session
from sqlalchemy import select, distinct
from sqlalchemy.exc import SQLAlchemyError
from ir_modernized.models.entity import UnmergeIn, PersonUnmergeIn


# Set up logging
logger = logging.getLogger(__name__)


def get_p20ids_from_unmerge_in(session: Session, unmerge_model: Type[UnmergeIn]) -> List[int]:
    """
    Retrieve a list of distinct P20IDs from the Unmerge_In table for a specific entity type.

    This function constructs a SQLAlchemy query to select distinct P20IDs
    based on the provided unmerge_model and returns them as a list of integers.

    Args:
        session (Session): An active SQLAlchemy session for database operations.
        unmerge_model (Type[UnmergeIn]): The specific UnmergeIn model class to query.

    Returns:
        List[int]: A list of unique P20IDs.

    Raises:
        SQLAlchemyError: If there is an error during the database query.
    """
    logger.debug(f"Starting to retrieve distinct P20IDs from Unmerge_In for model: {unmerge_model.__name__}")
   
    # Query the specific model directly - SQLAlchemy handles the EntityType filtering automatically
    stmt = select(distinct(unmerge_model.P20ID))
   
    try:
        # Execute the query and return all distinct P20IDs as a list
        p20ids = list(session.execute(stmt).scalars().all())
        logger.debug(f"Retrieved {len(p20ids)} distinct P20IDs")
        return p20ids
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving P20IDs: {str(e)}")
        raise



