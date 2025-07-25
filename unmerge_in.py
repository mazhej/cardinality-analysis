"""
PersonUnmerge_In model

This module defines the ORM mapping for the PersonUnmerge_In table.

Only three columns exist:

    P20ID      The person identifier that may need to be split/bridged.
    CreateDT   When the row was inserted.
    JobID      Batch-control ID used for audit / logging.

"""

from __future__ import annotations
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Integer
from sqlalchemy.orm import Mapped, mapped_column

from ir_modernized.models.base import Base


class PersonUnmergeIn(Base):
    """
    Represents a row in the **PersonUnmerge_In** table.

    Attributes
    ----------
    P20ID : int
        The primary-key column (bigint).  Acts as the unique row identifier
        for SQLAlchemy even though the physical table might not have a PK.
    CreateDT : datetime
        Timestamp when the row was inserted.
    JobID : int
        The job / batch id that loaded the row.
    """
    __tablename__ = "PersonUnmerge_In"


    P20ID: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True, 
        autoincrement=False
    )
    CreateDT: Mapped[DateTime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    JobID:    Mapped[int]      = mapped_column(Integer,   nullable=False)

