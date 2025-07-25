from __future__ import annotations


# from typing import TYPE_CHECKING
from sqlalchemy import (
    BigInteger,
    Float,
    Integer,
    String,
    DateTime,
    ForeignKey,
    func,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship, Mapped, mapped_column
from ir_modernized import models
from ir_modernized.models.base import Base
from sqlalchemy import Sequence


# if TYPE_CHECKING:
#     from ir_modernized.models.delete import PersonDelete, OrganizationDelete


# Define the sequence for JobID (for SQL Server)
job_id_seq = Sequence("job_id_seq", start=1, increment=1)
job_id_seq.metadata = Base.metadata  # Bind to metadata for Alembic




class SourceSystem(Base):
    """
    Represents the SourceSystem table in the database.


    Attributes:
        SourceSystemID (int): The primary key for the table.
        SourceSystem (str): The unique name of the source system.
        SourceSystemTTL (str): The title of the source system.
        Description (str | None): An optional description of the source system.
        CreateDT (datetime): The timestamp when the record was created.
    """


    __tablename__ = "SourceSystem"


    # Define columns
    SourceSystemID: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    SourceSystem: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    Description: Mapped[str] = mapped_column(String(250), nullable=False)
    CreateDT: Mapped[DateTime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )




class Entity(Base):
    """
    Single Table Inheritance (STI) base class for Person and Organization entities.


    This abstract class defines common fields and behaviors shared by both Person and Organization
    models, including consolidation status, user comments, timestamps, job tracking, and entity type
    discrimination for polymorphic mapping.


    Attributes:
        P20ID (int): The primary key for the entity.
        ConsolidationIND (int): Indicator for consolidation status.
        UserComment (str | None): Optional user-provided comment.
        CreateDT (datetime): Timestamp when the record was created.
        LastUpdateDT (datetime): Timestamp when the record was last updated.
        JobID (int): The ID of the job associated with this record.
        EntityType (str): Discriminator column for polymorphic identity.
        tokens (list[Token]): Relationship to associated Token records.


    Notes:
        - This class should not be instantiated directly; use Person or Organization subclasses.
        - Uses SQLAlchemy's polymorphic mapping via the 'EntityType' column.
    """


    __tablename__ = "Entity"
    P20ID: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False
    )
    ConsolidationIND: Mapped[int] = mapped_column(Integer, nullable=False)
    UserComment: Mapped[str | None] = mapped_column(String(200), nullable=True)
    CreateDT: Mapped[DateTime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    LastUpdateDT: Mapped[DateTime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    JobID: Mapped[int] = mapped_column(Integer, nullable=False)
    EntityType: Mapped[str] = mapped_column(String(32), nullable=False)


    __mapper_args__ = {
        "polymorphic_on": "EntityType",  # Discriminator column
        "polymorphic_identity": "entity",  # Default identity for the base class
    }


    tokens: Mapped[list[Token]] = relationship(
        back_populates="entity", cascade="all, delete-orphan"
    )




class Person(Entity):
    """
    Represents a person


    Inherits common fields and behaviors from the Entity base class.
    """


    __mapper_args__ = {"polymorphic_identity": "person"}




class Organization(Entity):
    """
    Represents an organization.


    Inherits common fields and behaviors from the Entity base class.
    """


    __mapper_args__ = {"polymorphic_identity": "organization"}


    PrimaryOrgTokenID: Mapped[int] = mapped_column(BigInteger, nullable=True)




class Token(Base):
    """
    Single Table Inheritance (STI) base class for PersonToken and OrganizationToken.


    This abstract class defines common fields and behaviors shared by all token entities,
    such as token IDs, keys, source system references, user comments, timestamps, job tracking,
    and entity type discrimination for polymorphic mapping.


    Attributes:
        TokenID (int): The primary key for the token.
        P20ID (int): Foreign key linking to the Entity table (Entity.P20ID).
        PKey (str): The primary key for the token in the source system.
        P20ID_Orig (int): The original P20ID value from the source system.
        SourceSystem (str): The source system of the token.
        UserComment (str | None): Optional user-provided comment.
        CreateDT (datetime): Timestamp when the record was created.
        LastUpdateDT (datetime): Timestamp when the record was last updated.
        JobID (int): The ID of the job associated with this record.
        EntityType (str): Discriminator column for polymorphic identity.
        entity (Entity): Relationship to the parent Entity.
        childs (list[Child]): Relationship to associated Child records.
        deletes (list[Delete]): Relationship to associated Delete records.


    Notes:
        - This class should not be instantiated directly; use PersonToken or OrganizationToken subclasses.
        - Enforces a unique constraint on (PKey, SourceSystem).
        - Uses SQLAlchemy's polymorphic mapping via the 'EntityType' column.
    """


    __tablename__ = "Token"


    # Shared fields
    TokenID: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False
    )
    P20ID: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("Entity.P20ID", ondelete="CASCADE"), nullable=False
    )
    PKey: Mapped[str] = mapped_column(String(255), nullable=False)
    P20ID_Orig: Mapped[int] = mapped_column(BigInteger, nullable=False)
    SourceSystem: Mapped[str] = mapped_column(
        ForeignKey("SourceSystem.SourceSystem"), nullable=False
    )  # Foreign key to SourceSystem tabl
    UserComment: Mapped[str | None] = mapped_column(String(200), nullable=True)
    CreateDT: Mapped[DateTime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    LastUpdateDT: Mapped[DateTime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    JobID: Mapped[int] = mapped_column(Integer, nullable=False)
    EntityType: Mapped[str] = mapped_column(String(32), nullable=False)


    __table_args__ = (
        UniqueConstraint("PKey", "SourceSystem", name="uq_token_pkey_sourcesystem"),
    )


    __mapper_args__ = {
        "polymorphic_on": "EntityType",  # Discriminator column
        "polymorphic_identity": "token",  # Default identity for the base class
    }


    # Up the hierarchy, P20ID is the FK
    entity: Mapped[Entity] = relationship(back_populates="tokens")


    # Down the hierarchy
    childs: Mapped[list["Child"]] = relationship(
        back_populates="token", cascade="all, delete-orphan", lazy="selectin"
    )
    deletes: Mapped[list["Delete"]] = relationship(
        back_populates="token", cascade="all, delete-orphan"
    )




class PersonToken(Token):
    """
    Represents a PersonToken


    Inherits common fields and behaviors from the Token base class.
    """


    __mapper_args__ = {"polymorphic_identity": "person_token"}




class OrganizationToken(Token):
    """
    Represents a token associated with an organization.


    Inherits common fields and behaviors from the Token base class.


    Attributes:
        SubSourceSystem (str | None): Optional foreign key to a sub-source system.
    """


    __mapper_args__ = {"polymorphic_identity": "organization_token"}


    SubSourceSystem: Mapped[str] = mapped_column(
        ForeignKey("SourceSystem.SourceSystem"), nullable=True
    )




class Child(Base):
    """
    Joined Table Inheritance (JTI) base class for PersonChild and OrganizationChild.


    This class defines common fields and behaviors shared by all child entities,
    such as child IDs, token references, source system information, user comments, timestamps,
    job tracking, and entity type discrimination for polymorphic mapping.


    Attributes:
        ChildID (int): The primary key for the child.
        TokenID (int): Foreign key linking to the Token table (Token.TokenID).
        PKey (str): The primary key for the child in the source system.
        ChildHashCD (str): A hash code representing the child in the source system.
        SourceSystem (str): The source system of the child.
        FullName (str): The full name of the child.
        UserComment (str | None): Optional user-provided comment.
        CreateDT (datetime): Timestamp when the record was created.
        JobID (int): The ID of the job associated with this record.
        EntityType (str): Discriminator column for polymorphic identity.
        token (Token): Relationship to the parent Token.


    Notes:
        - This class should not be instantiated directly; use PersonChild or OrganizationChild subclasses.
        - Enforces a unique constraint on (TokenID, ChildHashCD).
        - Uses SQLAlchemy's polymorphic mapping via the 'EntityType' column.
    """


    __tablename__ = "Child"


    # Common fields
    ChildID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    TokenID: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("Token.TokenID", ondelete="CASCADE"), nullable=False
    )
    PKey: Mapped[str] = mapped_column(String(255), nullable=False)
    ChildHashCD: Mapped[str] = mapped_column(String(32), nullable=False)
    SourceSystem: Mapped[str] = mapped_column(
        String(20), ForeignKey("SourceSystem.SourceSystem"), nullable=False
    )
    FullName: Mapped[str] = mapped_column(String(150), nullable=False)
    UserComment: Mapped[str | None] = mapped_column(String(200), nullable=True)
    CreateDT: Mapped[DateTime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    JobID: Mapped[int] = mapped_column(Integer, nullable=False)
    EntityType: Mapped[str] = mapped_column(String(32), nullable=False)


    __table_args__ = (
        UniqueConstraint("TokenID", "ChildHashCD", name="uq_child_tokenid_childhashcd"),
    )


    __mapper_args__ = {
        "polymorphic_identity": "child",
        "polymorphic_on": "EntityType",
    }


    # Up the hierarchy
    token: Mapped["Token"] = relationship(back_populates="childs")


    def __repr__(self):
        return f"{self.__class__.__name__}({self.FullName!r})"




class PersonChild(Child):
    """
    Represents a set of identifiers associated with a person.


    Inherits common fields and behaviors from the Child base class.


    Attributes:
        ChildID (int): The primary key for the person child (also a foreign key to Child).
        FirstName (str | None): The first name of the person.
        MiddleName (str | None): The middle name of the person.
        LastName (str | None): The last name of the person.
        Suffix (str | None): Name suffix (e.g., Jr., Sr.).
        Nickname (str | None): Nickname of the person.
        Gender (str | None): Gender code.
        BirthDate (str | None): Birth date in string format.
        InvalidDob (int | None): Indicator if the date of birth is invalid.
        CollapsedDob (float | None): Collapsed date of birth value.
        SSN (str | None): Social Security Number.
        InvalidSSN (int | None): Indicator if the SSN is invalid.
        Last4Ssn (str | None): Last 4 digits of the SSN.
        OrgStudentID (str | None): Organization-specific student ID.
        SourcewideID (str | None): Source-wide identifier.
        LocalStudentID (str | None): Local student ID.
        ResidentDistrictCD (str | None): Resident district code.
        OrgStaffID (str | None): Organization staff ID.
        DriversLicenseNumber (str | None): Driver's license number.
        Organization (str | None): Organization name or code.
        CountyCD (str | None): County code.
        StateCD (str | None): State code.
        ZipCode (str | None): Zip code.


    Notes:
        - This class should not be instantiated directly; use through ORM queries or relationships.
        - Inherits relationships and constraints from the Child base class.
    """


    __tablename__ = "PersonChild"


    # Attributes
    ChildID: Mapped[int] = mapped_column(
        Integer, ForeignKey("Child.ChildID", ondelete="CASCADE"), primary_key=True
    )
    FirstName: Mapped[str] = mapped_column(String(50), nullable=True)
    MiddleName: Mapped[str] = mapped_column(String(50), nullable=True)
    LastName: Mapped[str] = mapped_column(String(50), nullable=True)
    Suffix: Mapped[str] = mapped_column(String(4), nullable=True)
    Nickname: Mapped[str] = mapped_column(String(50), nullable=True)
    # FullName: Mapped[str] = mapped_column(String(150), nullable=True)
    Gender: Mapped[str] = mapped_column(String(1), nullable=True)
    BirthDate: Mapped[str] = mapped_column(String(10), nullable=True)
    InvalidDob: Mapped[int] = mapped_column(Integer, nullable=True)
    CollapsedDob: Mapped[float] = mapped_column(Float, nullable=True)
    SSN: Mapped[str] = mapped_column(String(9), nullable=True)
    InvalidSSN: Mapped[int] = mapped_column(Integer, nullable=True)
    Last4Ssn: Mapped[str] = mapped_column(String(4), nullable=True)
    OrgStudentID: Mapped[str] = mapped_column(String(25), nullable=True)
    SourcewideID: Mapped[str] = mapped_column(String(20), nullable=True)
    LocalStudentID: Mapped[str] = mapped_column(String(16), nullable=True)
    ResidentDistrictCD: Mapped[str] = mapped_column(String(10), nullable=True)
    OrgStaffID: Mapped[str] = mapped_column(String(20), nullable=True)
    DriversLicenseNumber: Mapped[str] = mapped_column(String(12), nullable=True)
    Organization: Mapped[str] = mapped_column(String(20), nullable=True)
    CountyCD: Mapped[str] = mapped_column(String(3), nullable=True)
    StateCD: Mapped[str] = mapped_column(String(5), nullable=True)
    ZipCode: Mapped[str] = mapped_column(String(5), nullable=True)


    __mapper_args__ = {
        "polymorphic_identity": "person_child",
    }




class OrganizationChild(Child):
    """
    Represents a set of identifiers associated with an organization.


    Inherits common fields and behaviors from the Child base class.


    Attributes:
        SubSourceSystem (str | None): The sub-source system of the child.
        SectorType (str | None): The sector type of the organization.
        OwnershipType (str | None): The ownership type of the organization.
        DegreeYearType (str | None): The degree year type of the organization.
        DistrictCD (str | None): The district code of the organization.
        SourcewideID (str | None): The source-wide ID for the child.
        CEEB_CD (str | None): The CEEB code of the organization.
        AddressLine_1 (str | None): The first line of the address.
        AddressLine_2 (str | None): The second line of the address.
        City (str | None): The city of the organization.
        CountyCD (str | None): The county code.
        StateCD (str | None): The state code.
        ZipCode (str | None): The zip code.
    """


    __tablename__ = "OrganizationChild"


    # Attributes


    # Additional Fields
    ChildID: Mapped[int] = mapped_column(
        Integer, ForeignKey("Child.ChildID", ondelete="CASCADE"), primary_key=True
    )
    SubSourceSystem: Mapped[str] = mapped_column(
        String(20), ForeignKey("SourceSystem.SourceSystem"), nullable=False
    )
    SectorType: Mapped[str] = mapped_column(String(1), nullable=False)
    OwnershipType: Mapped[str] = mapped_column(String(8), nullable=True)
    DegreeYearType: Mapped[str] = mapped_column(String(1), nullable=True)
    DistrictCD: Mapped[str] = mapped_column(String(5), nullable=True)
    SourcewideID: Mapped[str] = mapped_column(String(20), nullable=True)
    CEEB_CD: Mapped[str] = mapped_column(String(6), nullable=True)
    AddressLine_1: Mapped[str] = mapped_column(String(50), nullable=True)
    AddressLine_2: Mapped[str] = mapped_column(String(50), nullable=True)
    City: Mapped[str] = mapped_column(String(30), nullable=True)
    CountyCD: Mapped[str] = mapped_column(String(3), nullable=True)
    StateCD: Mapped[str] = mapped_column(String(6), nullable=True)
    ZipCode: Mapped[str] = mapped_column(String(5), nullable=True)


    __mapper_args__ = {
        "polymorphic_identity": "organization_child",
    }




class Delete(Base):
    """ """


    __tablename__ = "Delete"


    # Common fields
    RecordID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    TokenID: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("Token.TokenID", ondelete="CASCADE"), nullable=False
    )
    CreateDT: Mapped[DateTime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    JobID: Mapped[int] = mapped_column(Integer, nullable=True)
    EntityType: Mapped[str] = mapped_column(String(32), nullable=False)


    __table_args__ = (UniqueConstraint("TokenID", name="uq_delete_tokenid"),)


    __mapper_args__ = {
        "polymorphic_identity": "delete",  # Default identity for the base class
        "polymorphic_on": "EntityType",
    }


    token: Mapped[Token] = relationship(back_populates="deletes")




class PersonDelete(Delete):
    """
    Represents the PersonDelete table.
    """


    __mapper_args__ = {"polymorphic_identity": "person_delete"}




class OrganizationDelete(Delete):
    """
    Represents the OrganizationDelete table.
    """


    __mapper_args__ = {"polymorphic_identity": "organization_delete"}




class Err_Delete(Base):
    """ """


    __tablename__ = "err_delete"


    # Common fields
    RecordID: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=False
    )
    TokenID: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("Token.TokenID"), nullable=False
    )
    CreateDT: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    JobID: Mapped[int] = mapped_column(Integer, nullable=True)
    EntityType: Mapped[str] = mapped_column(String(32), nullable=False)


    __mapper_args__ = {
        "polymorphic_identity": "err_delete",  # Default identity for the base class
        "polymorphic_on": "EntityType",
    }




class Err_PersonDelete(Err_Delete):
    """
    Represents the Err_PersonDelete table.
    """


    __mapper_args__ = {"polymorphic_identity": "err_person_delete"}




class Err_OrganizationDelete(Err_Delete):
    """
    Represents the Err_OrganizationDelete table.
    """


    __mapper_args__ = {"polymorphic_identity": "err_organization_delete"}




class UnmergeIn(Base):
    """
    Base class for unmerge input records, using Single Table Inheritance.
    Stores both person and organization unmerge inputs in a single table.
    """


    __tablename__ = "Unmerge_In"


    P20ID: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    CreateDT: Mapped[DateTime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    JobID: Mapped[int] = mapped_column(Integer, nullable=True)
    EntityType: Mapped[str] = mapped_column(String(32), nullable=False)


    __mapper_args__ = {
        "polymorphic_identity": "unmerge_in",  # Default identity for the base class
        "polymorphic_on": "EntityType",
    }




class PersonUnmergeIn(UnmergeIn):
    """
    Represents person unmerge inputs in the Unmerge_In table.
    """


    __mapper_args__ = {"polymorphic_identity": "person_unmerge_in"}




class OrganizationUnmergeIn(UnmergeIn):
    """
    Represents organization unmerge inputs in the Unmerge_In table.
    """


    __mapper_args__ = {"polymorphic_identity": "organization_unmerge_in"}




class BlackList(Base):
    __tablename__ = "BlackList"
    RecordID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    TokenID_1: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("Token.TokenID"), nullable=False
    )
    TokenID_2: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("Token.TokenID"), nullable=False
    )
    UserComment: Mapped[str | None] = mapped_column(String(200), nullable=True)
    CreateDT: Mapped[DateTime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    JobID: Mapped[int | None] = mapped_column(Integer, nullable=True)
    EntityType: Mapped[str] = mapped_column(String(32), nullable=False)


    __mapper_args__ = {
        "polymorphic_on": "EntityType",
        "polymorphic_identity": "blacklist",
    }




class PersonBlackList(BlackList):
    __mapper_args__ = {"polymorphic_identity": "person_blacklist"}




class OrganizationBlackList(BlackList):
    __mapper_args__ = {"polymorphic_identity": "organization_blacklist"}




class WhiteList(Base):
    __tablename__ = "WhiteList"
    P20ID: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("Entity.P20ID"), primary_key=True
    )
    UserComment: Mapped[str | None] = mapped_column(String(200), nullable=True)
    CreateDT: Mapped[DateTime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    JobID: Mapped[int | None] = mapped_column(Integer, nullable=True)
    EntityType: Mapped[str] = mapped_column(String(32), nullable=False)


    __mapper_args__ = {
        "polymorphic_on": "EntityType",
        "polymorphic_identity": "whitelist",
    }




class PersonWhiteList(WhiteList):
    __mapper_args__ = {"polymorphic_identity": "person_whitelist"}




class OrganizationWhiteList(WhiteList):
    __mapper_args__ = {"polymorphic_identity": "organization_whitelist"}




class InvalidSSN(Base):
    """
    Represents the InvalidSSN table, storing invalid SSNs associated with tokens.


    Attributes:
        RecordID (int): The primary key, auto-incrementing.
        TokenID (int): Foreign key to Token.TokenID.
        SSN (str): Social Security Number marked as invalid.
        Source (str | None): Source of the invalid SSN.
        Description (str | None): Description or reason for invalidity.
        CreateDT (datetime): Timestamp when the record was created.
        JobID (int | None): ID of the job associated with this record.
    """


    __tablename__ = "InvalidSSN"


    RecordID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    TokenID: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("Token.TokenID"), nullable=False
    )
    SSN: Mapped[str] = mapped_column(String(9), nullable=False)
    Source: Mapped[str | None] = mapped_column(String(100), nullable=True)
    Description: Mapped[str | None] = mapped_column(String(250), nullable=True)
    CreateDT: Mapped[DateTime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    JobID: Mapped[int | None] = mapped_column(Integer, nullable=True)




class Unmerge(Base):
    """
    Base class for unmerge records, using Single Table Inheritance (STI).
    This class maps to the 'PersonUnmerge' table in the database and serves as the base for both person and organization unmerge records.
    """


    __tablename__ = "Unmerge"


    RecordID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    TokenID: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True)
    GroupID: Mapped[str] = mapped_column(String(14), nullable=False)
    CreateDT: Mapped[DateTime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    JobID: Mapped[int] = mapped_column(Integer, nullable=False)
    entity_type: Mapped[str] = mapped_column(String(32), nullable=False)


    __table_args__ = ()


    __mapper_args__ = {
        "polymorphic_on": "entity_type",
    }




class PersonUnmerge(Unmerge):
    """
    Represents person unmerge records in the 'PersonUnmerge' table.
    """


    __mapper_args__ = {
        "polymorphic_identity": "person_unmerge",
    }




class OrganizationUnmerge(Unmerge):
    """
    Represents organization unmerge records in the 'PersonUnmerge' table.
    """


    __mapper_args__ = {
        "polymorphic_identity": "organization_unmerge",
    }





