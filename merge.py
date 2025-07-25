from sqlalchemy import BigInteger, Integer, SmallInteger, String, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column, synonym
from ir_modernized.models.base import Base

class PersonMerge(Base):
    """
    Represents the PersonMerge table in the database.

    Attributes:
        RecordID (int): The primary key for the table.
        P20ID_1 (int): The first person ID being merged.
        P20ID_2 (int): The second person ID being merged.
        MergeIND (smallint): Indicator for the merge status.
        RuleNumber (smallint): The rule number used for the merge.
        RuleSet (str): The rule set used for the merge.
        CreateDT (datetime): The timestamp when the merge record was created.
        LastUpdateDT (datetime): The timestamp when the merge record was last updated.
        JobID (int): The ID of the job associated with the merge.
    """
    __tablename__ = 'PersonMerge'

    RecordID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    P20ID_1: Mapped[int] = mapped_column(BigInteger, nullable=False)
    P20ID_2: Mapped[int] = mapped_column(BigInteger, nullable=False)
    MergeIND: Mapped[int] = mapped_column(SmallInteger, nullable=True)
    RuleNumber: Mapped[int] = mapped_column(SmallInteger, nullable=True)
    RuleSet: Mapped[str] = mapped_column(String(50), nullable=True)
    CreateDT: Mapped[DateTime] = mapped_column(DateTime, nullable=False, default=func.now())
    LastUpdateDT: Mapped[DateTime] = mapped_column(DateTime, nullable=False, default=func.now())
    JobID: Mapped[int] = mapped_column(Integer, nullable=True)
    
    p20id_1_column: Mapped[int] = synonym('P20ID_1')
    p20id_2_column: Mapped[int] = synonym('P20ID_2')
    