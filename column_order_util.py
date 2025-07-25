from ir_modernized.models.metadata import IR_ColumnOrder
from sqlalchemy.orm import Session
from typing import List, Dict, Type

def get_column_order(session: Session, column_order_model: Type[IR_ColumnOrder]) -> List[Dict]:
    """
    Fetch column order configuration from the IR_ColumnOrder table for a specific model.
    
    Args:
        session (Session): SQLAlchemy session for querying the database.
        column_order_model (Type[IR_ColumnOrder]): The specific column order model class to query.
        
    Returns:
        List[Dict]: List of column configurations ordered by ColumnOrder.
    """
    query = (
        session.query(column_order_model)
        .order_by(column_order_model.ColumnOrder)
    )
    return [
        {
            'ColumnOrder': row.ColumnOrder,
            'ColumnName': row.ColumnName,
            'ColumnType': row.ColumnType,
            'EntityType': row.EntityType
        }
        for row in query.all()
    ]