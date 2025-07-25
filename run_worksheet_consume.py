"""
run_worksheet_consume.py

Runner script to consume an unmerge review worksheet using UnmergeWorksheetConsumer.
Uses SQLAlchemy's polymorphic capabilities to handle both person and organization entities uniformly.
Loads configuration from the config module, connects to a SQL Server database,
generates a JobID dynamically, and processes the worksheet specified in WORKBOOK_PATH.

Dependencies:
    - ir_modernized.database.SqlServerDatabase: For database connection, session management, and JobID generation.
    - ir_modernized.unmerge.worksheet_consume: For consuming the worksheet using polymorphic models.
    - config: For configuration constants (SERVER, IR_DATABASE, WORKBOOK_PATH).
    - pytds: For SQL Server connection.
"""


import sys
import os
import argparse
import logging
from sqlalchemy.orm import Session
from ir_modernized.database import SqlServerDatabase
from ir_modernized.unmerge.worksheet_consume import (
    create_person_consumer,
    create_organization_consumer,
    UnmergeWorksheetConsumer,
)
from config import WORKBOOK_PATH


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class WorksheetConsumerRunner:
    """
    Manages the execution of the UnmergeWorksheetConsumer to process an unmerge review worksheet.
    
    Uses SQLAlchemy's polymorphic capabilities to handle both person and organization entities uniformly.
    Provides parameterized output directory and filename support.

    Attributes:
        db (SqlServerDatabase): The database connection object.
    """


    def __init__(self):
        """
        Initialize the runner with a database connection.
        """
        self.db = SqlServerDatabase()
        self.db.connect()
        logger.info("Database connection established")


    def consume_worksheet(
        self, 
        workbook_name: str, 
        entity_type: str = "person",
        output_dir: str = "output",
        output_filename: str | None = None
    ) -> None:
        """
        Consume the unmerge worksheet specified by workbook_name using a dynamically generated JobID.

        Args:
            workbook_name (str): The name of the workbook file (e.g., "unmerge_workbook.xlsx").
            entity_type (str): The entity type to process ("person" or "organization").
            output_dir (str): Directory for output files (default: "output").
            output_filename (str): Name of the output file (if None, uses default based on entity type).

        Raises:
            FileNotFoundError: If the worksheet file does not exist.
            ValueError: If entity_type is not "person" or "organization".
        """
        # Construct the full path using WORKBOOK_PATH and workbook_name
        worksheet_path = os.path.join(WORKBOOK_PATH, workbook_name)
        if not os.path.exists(worksheet_path):
            raise FileNotFoundError(f"Worksheet file not found: {worksheet_path}")

        # Validate entity type
        if entity_type not in ["person", "organization"]:
            raise ValueError(
                f"Invalid entity_type: {entity_type}. Must be 'person' or 'organization'"
            )

        # Set default output filename if not provided
        if output_filename is None:
            output_filename = f"{entity_type}_unmerge_output.xlsx"

        # Generate a new JobID
        job_id = self.db.get_job_id()
        logger.info(f"Generated JobID: {job_id}")

        with self.db.get_database_session() as session:
            # Create and run the consumer using the factory method
            consumer = UnmergeWorksheetConsumer.create_for_entity(
                session=session,
                worksheet_path=worksheet_path,
                job_id=job_id,
                entity_type=entity_type,
                output_dir=output_dir,
                output_filename=output_filename
            )

            consumer.consume()
            logger.info(
                f"{entity_type.capitalize()} worksheet consumption completed for {worksheet_path}"
            )


    def close(self):
        """
        Close the database connection.
        """
        self.db.disconnect()
        logger.info("Database connection closed")


def main():
    """
    Parse command-line arguments and run the worksheet consumer.
    """
    parser = argparse.ArgumentParser(description="Consume an unmerge review worksheet using polymorphic models")
    parser.add_argument(
        "--workbook",
        default="unmerge_workbook.xlsx",
        help="Name of the workbook file (e.g., unmerge_workbook.xlsx)",
    )
    parser.add_argument(
        "--entity-type",
        default="person",
        choices=["person", "organization"],
        help="Entity type to process (person or organization)",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory for output files (default: output)",
    )
    parser.add_argument(
        "--output-filename",
        default=None,
        help="Name of the output file (default: {entity_type}_unmerge_output.xlsx)",
    )
    
    args = parser.parse_args()
    runner = WorksheetConsumerRunner()
    try:
        runner.consume_worksheet(
            args.workbook, 
            args.entity_type, 
            args.output_dir, 
            args.output_filename
        )
        print(
            f"{args.entity_type.capitalize()} worksheet consumption completed successfully"
        )
    except Exception as e:
        logger.error(f"Failed to consume worksheet: {e}")
        raise
    finally:
        runner.close()


if __name__ == "__main__":
    main()




