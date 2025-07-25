from sqlalchemy import SmallInteger, String
from sqlalchemy.orm import Mapped, mapped_column, synonym, relationship, declared_attr
from ir_modernized.models.base import Base
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import Integer


class IR_ColumnOrder(Base):
    __tablename__ = 'IR_ColumnOrder'
    ColumnOrderID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ColumnOrder: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    ColumnName: Mapped[str] = mapped_column(String(32), nullable=False)
    ColumnType: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    EntityType: Mapped[str] = mapped_column(String(32), nullable=False)
    
    __mapper_args__ = {
        "polymorphic_on": "EntityType",
        "polymorphic_identity": "ir_column_order",
    }

class IR_PersonColumnOrder(IR_ColumnOrder):
    __mapper_args__ = {"polymorphic_identity": "ir_person_column_order"}

class IR_OrganizationColumnOrder(IR_ColumnOrder):
    __mapper_args__ = {"polymorphic_identity": "ir_organization_column_order"}







