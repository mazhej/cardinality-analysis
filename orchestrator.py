"""
Module for orchestrating the generation of an unmerge review worksheet.


This module provides the UnmergeWorksheetOrchestrator class, which manages the process of
generating an unmerge worksheet by retrieving data from a SQL Server database and an ODS system,
processing it, and saving it to an Excel file.


Classes:
    UnmergeWorksheetOrchestrator: Orchestrates the generation of the unmerge worksheet.


    Dependencies:
    - ir_modernized.database.SqlServerDatabase: For database connection and session management.
    - ir_modernized.unmerge.unmerge_in_loader.get_p20ids_from_unmerge_in: For loading P20IDs.
    - token_service.get_tokenids_by_p20ids: For retrieving tokenIDs.
    - worksheet_builder.UnmergeWorksheetBuilder: For building the worksheet.
    - ods_fetcher.ODSFetcher: For fetching ODS data.
    - pytds: For SQL Server connection.
    - config: For configuration constants.
"""


import sys
import os
import logging
import pandas as pd
from typing import Type


# Add the project root (IR) to sys.path to ensure module imports work correctly
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, project_root)


# Import necessary classes and modules from the project
from ir_modernized.database import Database
from ir_modernized.unmerge.unmerge_in_loader import get_p20ids_from_unmerge_in
from ir_modernized.unmerge.token_service import get_tokenids_by_p20ids
from ir_modernized.unmerge.worksheet_builder import UnmergeWorksheetBuilder
from ir_modernized.unmerge.ods_fetcher import ODSFetcher
from config import WORKBOOK_PATH
from ir_modernized.models.entity import UnmergeIn, Token, PersonUnmergeIn, PersonToken, OrganizationUnmergeIn, OrganizationToken
from ir_modernized.models.metadata import IR_ColumnOrder, IR_PersonColumnOrder, IR_OrganizationColumnOrder


logger = logging.getLogger(__name__)


class UnmergeWorksheetOrchestrator:
    """
    Orchestrates the generation of an unmerge review worksheet.


       This class manages the database connection, data retrieval from various sources,
    and the construction of the worksheet using the UnmergeWorksheetBuilder.


    Attributes:
        db (Database): The database connection object.
        ods_fetcher (ODSFetcher): The ODS data fetcher.
        entity_type (str): The entity type to process ('person' or 'organization').
    """


    def __init__(
        self,
        database: Database,
        unmerge_model: Type[UnmergeIn],
        token_model: Type[Token],
        column_order_model: Type[IR_ColumnOrder]
    ):
        """
        Initialize the orchestrator with a database instance and entity type.
        
        Args:
            database: Database instance (SqlServerDatabase, SqliteDatabase, etc.)
            entity_type: The entity type to process ('person' or 'organization')
        """
        self.db = database
        self.db.connect()
        self.ods_fetcher = ODSFetcher(self.db) if issubclass(unmerge_model, PersonUnmergeIn) else None
        self.unmerge_model = unmerge_model
        self.token_model = token_model
        self.column_order_model = column_order_model

    @classmethod
    def org_or_person(cls, database: Database, org_person: str = "person") -> "UnmergeWorksheetOrchestrator":
        """
        Factory method to create an orchestrator for a specific entity type.
        
        Args:
            database: Database instance (SqlServerDatabase, SqliteDatabase, etc.)
            org_person: "org" for organization, "person" for person (default: "person")
        """
        if org_person == "person":
            unmerge_model = PersonUnmergeIn
            token_model = PersonToken
            column_order_model = IR_PersonColumnOrder
        elif org_person == "org":
            unmerge_model = OrganizationUnmergeIn
            token_model = OrganizationToken
            column_order_model = IR_OrganizationColumnOrder
        else:
            raise ValueError("org_person must be 'org' or 'person'")
        
        return cls(database, unmerge_model, token_model, column_order_model)

    def _determine_required_ods_data(self, column_order):
        """
        Determine which ODS data is required based on column order configuration.

        This method analyzes the column order to identify which optional data markers
        (ColumnType = 2) are present, which determines whether K12 or wage data needs
        to be fetched from ODS.

        Args:
            column_order (List[Dict]): Column order configuration from database.

        Returns:
            Dict[str, bool]: Dictionary indicating which data types are required.
        """
        # Check for ColumnType = 2 entries (optional column sets) and determine ODS requirements
        requires_k12 = False
        requires_wage = False

        for entry in column_order:
            if entry["ColumnType"] == 2:  # Optional column set
                column_name = entry["ColumnName"]
                # Determine ODS requirements based on column name patterns
                if column_name.startswith("K12_"):
                    requires_k12 = True
                elif column_name.startswith("Wage") or column_name.startswith("InWorkforce"):
                    requires_wage = True

        return {
            "requires_k12": requires_k12,
            "requires_wage": requires_wage
        }


    def generate_worksheet(self, output_path: str, worksheet_name: str):
        """
        Generate the unmerge worksheet and save it to the specified output path.




        This method retrieves P20IDs, token IDs, ODS data (only if needed), builds the worksheet,
        and saves it as an Excel file at the specified location.




        Args:
            output_path (str): The full path to save the workbook file (e.g., "C:/path/to/unmerge_workbook.xlsx").
            worksheet_name (str): The name of the worksheet within the workbook (e.g., "Unmerge Review").
        """
        with self.db.get_database_session() as session:
            # Retrieve P20IDs from the appropriate UnmergeIn table for unmerge processing
            p20ids = get_p20ids_from_unmerge_in(session, self.unmerge_model)
            # Fetch token IDs and their corresponding P20IDs for data mapping
            token_rows = get_tokenids_by_p20ids(session, p20ids, self.token_model)




            # Extract token IDs and create a mapping dictionary
            token_ids = [tid for tid, _ in token_rows]
            tokenid_to_p20id = {tid: pid for tid, pid in token_rows}




            # Create worksheet builder to get column order configuration
            worksheet_builder = UnmergeWorksheetBuilder(session, self.column_order_model)




            # Determine which ODS data is required based on column order
            required_data = self._determine_required_ods_data(worksheet_builder.column_order)




            # Initialize empty DataFrames for optional data
            k12_df = pd.DataFrame()
            wage_df = pd.DataFrame()




            # Only fetch K12 data if required
            if self.ods_fetcher:  # Only if person
                if required_data["requires_k12"]:
                    logger.info(f"K12 required by column configuration - fetching from ODS for {len(token_ids)} TokenIDs")
                    k12_df = self.ods_fetcher.get_k12_data(token_ids)
                else:
                    logger.info("K12 data not required by column configuration - skipping ODS call")




            # Only fetch wage data if required
            if self.ods_fetcher:  # Only if person
                if required_data["requires_wage"]:
                    logger.info(f"Wage data required by column configuration - fetching from ODS for {len(token_ids)} TokenIDs")
                    wage_df = self.ods_fetcher.get_wage_data(token_ids)
                else:
                    logger.info("Wage data not required by column configuration - skipping ODS call")




            # Build the worksheet by integrating database and ODS data
            final_df = worksheet_builder.build(token_ids, tokenid_to_p20id, k12_df, wage_df)




            # Save the final worksheet to an Excel file with the specified sheet name
            final_df.to_excel(output_path, sheet_name=worksheet_name, index=False)
            logger.info(f"Worksheet saved to {output_path} with sheet {worksheet_name}")




    def close(self):
        """
        Close the database connection.




        Ensures that database resources are released properly after use.
        """
        self.db.disconnect()




if __name__ == "__main__":
    import argparse
    from ir_modernized.database import SqlServerDatabase
    from config import WORKBOOK_PATH
    import os
    
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description="Generate unmerge worksheet for specified entity type")
    parser.add_argument(
        "--entity-type", 
        choices=["person", "org"], 
        default="person",
        help="Entity type to process: 'person' or 'org' (default: person)"
    )
    parser.add_argument(
        "--output-path",
        help="Output file path (default: unmerge_workbook_{entity_type}.xlsx)"
    )
    
    args = parser.parse_args()
    
    # Create database instance (tests should pass their own database instance)
    db = SqlServerDatabase()
    
    # Ensure output directory exists with proper error handling
    try:
        os.makedirs(WORKBOOK_PATH, exist_ok=True)
    except OSError as e:
        logger.warning(f"Warning: Could not create directory {WORKBOOK_PATH}: {e}")
        logger.warning("Using current directory instead.")
        WORKBOOK_PATH = "."
    
    # Generate worksheet for the specified entity type only
    orchestrator = UnmergeWorksheetOrchestrator.org_or_person(database=db, org_person=args.entity_type)
    
    # Determine output path
    if args.output_path:
        output_path = args.output_path
    else:
        output_path = os.path.join(WORKBOOK_PATH, f"unmerge_workbook_{args.entity_type}.xlsx")
    
    logger.info(f"Generating {args.entity_type} unmerge worksheet...")
    orchestrator.generate_worksheet(output_path, "Unmerge Review")
    orchestrator.close()
    logger.info(f"Worksheet saved to: {output_path}")



