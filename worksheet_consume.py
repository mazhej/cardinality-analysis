"""
worksheet_consumer.py
Worksheet consumer that uses SQLAlchemy's polymorphic capabilities to handle both person and organization unmerge worksheets uniformly.
Uses a factory method pattern similar to the delete service to create appropriate consumer instances.
"""


import logging
import os
import pandas as pd
from sqlalchemy.orm import Session
from typing import Optional, Type
from datetime import datetime
from ir_modernized.models.entity import (
    WhiteList,
    BlackList,
    InvalidSSN,
    Unmerge,
    Token,
    PersonWhiteList,
    PersonBlackList,
    PersonUnmerge,
    OrganizationWhiteList,
    OrganizationBlackList,
    OrganizationUnmerge,
)
from ir_modernized.utils.ssn_util import clean_ssn




logger = logging.getLogger(__name__)




class UnmergeWorksheetConsumer:
    """
    Worksheet consumer that uses SQLAlchemy's polymorphic capabilities.
    
    This consumer treats Person and Organization data uniformly by leveraging the polymorphic
    models (Unmerge, WhiteList, BlackList) and using a factory method pattern to create
    appropriate instances based on entity type.
    
    Key features:
    - Uses SQLAlchemy's polymorphic capabilities to handle both entity types uniformly
    - Factory method pattern for creating appropriate consumer instances
    - Parameterized output directory and filename
    - Single codebase for both person and organization processing
    - Uses externally provided JobID for provenance
    - Truncates the appropriate Unmerge table based on entity type
    - Loads worksheet and processes WhiteList, BlackList, InvalidSSN, DoNotMerge logic
    - Loads TokenIDs and GroupIDs into the appropriate Unmerge table where GroupID is not null
    - Uses separate transactions for truncation and processing
    """


    def __init__(
        self,
        session: Session,
        worksheet_path: str,
        job_id: int,
        entity_type: str,
        output_dir: str = "output",
        output_filename: str = "unmerge_output.xlsx",
    ):
        """
        Initialize the consumer with database session, worksheet path, JobID, and entity configuration.

        Args:
            session: The database session to use for all operations
            worksheet_path: Path to the Excel worksheet file
            job_id: The JobID to use for this operation
            entity_type: The entity type ("person" or "organization")
            output_dir: Directory for output files (default: "output")
            output_filename: Name of the output file (default: "unmerge_output.xlsx")
        """
        self.session = session
        self.worksheet_path = worksheet_path
        self.worksheet_df: Optional[pd.DataFrame] = None
        self.job_id = job_id
        self.entity_type = entity_type
        self.output_dir = output_dir
        self.output_filename = output_filename

        # Set up polymorphic model classes based on entity type
        self._setup_model_classes()

        # Validate inputs
        if not os.path.exists(worksheet_path):
            raise FileNotFoundError(f"Worksheet file not found: {worksheet_path}")

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        logger.info(
            f"Initialized UnmergeWorksheetConsumer for {entity_type} "
            f"with worksheet: {self.worksheet_path}, JobID: {self.job_id}, "
            f"output: {os.path.join(output_dir, output_filename)}"
        )


    def _setup_model_classes(self) -> None:
        """Set up the model classes based on entity type."""
        if self.entity_type == "person":
            self.whitelist_class: Type[WhiteList] = PersonWhiteList
            self.blacklist_class: Type[BlackList] = PersonBlackList
            self.unmerge_class: Type[Unmerge] = PersonUnmerge
            self.required_columns = [
                "P20ID", "TokenID", "GroupID", "DoNotMerge", "InvalidSSN", "SSN"
            ]
            self.uses_ssn = True
        elif self.entity_type == "organization":
            self.whitelist_class = OrganizationWhiteList
            self.blacklist_class = OrganizationBlackList
            self.unmerge_class = OrganizationUnmerge
            self.required_columns = [
                "P20ID", "TokenID", "GroupID", "DoNotMerge", "InvalidSSN", "SSN"
            ]
            self.uses_ssn = True
        else:
            raise ValueError(f"Unsupported entity type: {self.entity_type}")


    @classmethod
    def create_for_entity(
        cls,
        session: Session,
        worksheet_path: str,
        job_id: int,
        entity_type: str,
        output_dir: str = "output",
        output_filename: str = "unmerge_output.xlsx",
    ) -> "UnmergeWorksheetConsumer":
        """
        Factory method to create a consumer for the specified entity type.
        
        Args:
            session: The database session to use for all operations
            worksheet_path: Path to the Excel worksheet file
            job_id: The JobID to use for this operation
            entity_type: The entity type ("person" or "organization")
            output_dir: Directory for output files
            output_filename: Name of the output file
            
        Returns:
            UnmergeWorksheetConsumer: Configured consumer instance
            
        Raises:
            ValueError: If entity_type is not supported
        """
        if entity_type not in ["person", "organization"]:
            raise ValueError(f"Unsupported entity type: {entity_type}. Must be 'person' or 'organization'")
        
        return cls(session, worksheet_path, job_id, entity_type, output_dir, output_filename)


    def truncate_tables(self) -> None:
        """
        Truncate the appropriate Unmerge table based on entity type using polymorphic filtering.
        """
        logger.info(f"Truncating {self.entity_type} Unmerge table")
        
        # Use polymorphic filtering to delete only records of the specific entity type
        entity_type_filter = f"{self.entity_type}_unmerge"
        self.session.query(Unmerge).filter(
            Unmerge.entity_type == entity_type_filter
        ).delete(synchronize_session=False)


    def load_worksheet(self) -> pd.DataFrame:
        """
        Load the reviewed worksheet into a pandas DataFrame.
        
        Returns:
            pd.DataFrame: The loaded worksheet data
            
        Raises:
            ValueError: If required columns are missing
        """
        logger.info(f"Loading worksheet from: {self.worksheet_path}")
        self.worksheet_df = pd.read_excel(
            self.worksheet_path, dtype={"SSN": str, "GroupID": str}
        )

        # Validate required columns
        if self.worksheet_df is not None:
            missing_columns = [
                col
                for col in self.required_columns
                if col not in self.worksheet_df.columns
            ]
            if missing_columns:
                raise ValueError(
                    f"Missing required columns in worksheet: {missing_columns}"
                )

            logger.info(
                f"Loaded worksheet with {len(self.worksheet_df)} rows and {len(self.worksheet_df.columns)} columns"
            )
        else:
            raise ValueError("Failed to load worksheet - DataFrame is None")

        return self.worksheet_df


    def process_whitelist(self) -> None:
        """
        Insert P20IDs with no GroupIDs into WhiteList and remove related TokenIDs from BlackList.
        Uses polymorphic models to handle both entity types uniformly.
        """
        if self.worksheet_df is None:
            raise ValueError("Worksheet not loaded. Call load_worksheet() first.")

        df = self.worksheet_df
        no_group = df[df["GroupID"].isnull()]
        whitelist_p20ids = (
            pd.Series(no_group["P20ID"]).drop_duplicates().astype(int).tolist()
        )

        # Bulk insert into WhiteList using polymorphic class
        whitelist_objs = [
            self.whitelist_class(P20ID=p20id, JobID=self.job_id)
            for p20id in whitelist_p20ids
        ]
        if whitelist_objs:
            self.session.bulk_save_objects(whitelist_objs, return_defaults=False)

        # Remove related TokenIDs from BlackList
        tokenids = pd.Series(no_group["TokenID"]).drop_duplicates().astype(int).tolist()
        if tokenids:
            self.session.query(BlackList).filter(
                (BlackList.TokenID_1.in_(tokenids))
                | (BlackList.TokenID_2.in_(tokenids))
            ).delete(synchronize_session=False)


    def process_do_not_merge_pairs(self) -> None:
        """
        Process DoNotMerge data into BlackList using polymorphic models.
        For each P20ID with exactly one TokenID where DoNotMerge=1 and one or more where DoNotMerge=2,
        create pairs (TokenID_1, TokenID_2) and insert into BlackList.
        """
        if self.worksheet_df is None:
            raise ValueError("Worksheet not loaded. Call load_worksheet() first.")

        df = self.worksheet_df
        pairs = []
        for p20id, group in df.groupby("P20ID"):
            dnm1 = group[group["DoNotMerge"] == 1]
            dnm2 = group[group["DoNotMerge"] == 2]
            if len(dnm1) == 1 and len(dnm2) >= 1:
                tokenid_1 = int(dnm1.iloc[0]["TokenID"])
                for _, row2 in dnm2.iterrows():
                    tokenid_2 = int(row2["TokenID"])
                    pairs.append((tokenid_1, tokenid_2))

        # Remove duplicates and insert into BlackList using polymorphic class
        unique_pairs = list(set(pairs))
        blacklist_objs = [
            self.blacklist_class(
                TokenID_1=t1, TokenID_2=t2, JobID=self.job_id
            )
            for t1, t2 in unique_pairs
        ]
        if blacklist_objs:
            self.session.bulk_save_objects(blacklist_objs, return_defaults=False)


    def process_invalid_ssn(self) -> None:
        """
        Insert TokenIDs and SSNs where InvalidSSN == 1 into InvalidSSN table.
        Only processes SSN validation if the entity type uses SSN.
        """
        if self.worksheet_df is None:
            raise ValueError("Worksheet not loaded. Call load_worksheet() first.")

        if not self.uses_ssn:
            logger.info(
                f"Skipping InvalidSSN processing for {self.entity_type} entity type"
            )
            return

        df = self.worksheet_df
        invalid = df[df["InvalidSSN"] == 1]
        invalid_objs = []
        for _, row in invalid.iterrows():
            try:
                ssn_cleaned = clean_ssn(row["SSN"])
                invalid_objs.append(
                    InvalidSSN(
                        TokenID=int(row["TokenID"]), SSN=ssn_cleaned, JobID=self.job_id
                    )
                )
            except ValueError as e:
                logger.error(f"Skipping invalid SSN for TokenID {row['TokenID']}: {e}")
                continue

        if invalid_objs:
            self.session.bulk_save_objects(invalid_objs, return_defaults=False)


    def load_unmerge_records(self) -> None:
        """
        Load TokenIDs and GroupIDs into the appropriate Unmerge table where GroupID is not null.
        Uses polymorphic models to handle both entity types uniformly.
        """
        if self.worksheet_df is None:
            raise ValueError("Worksheet not loaded. Call load_worksheet() first.")

        df = self.worksheet_df
        not_null_group = df[df["GroupID"].notnull()]

        # Deduplicate by TokenID to avoid unique constraint violations
        unique_records = not_null_group.drop_duplicates(subset=["TokenID"])  # type: ignore

        unmerge_objs = [
            self.unmerge_class(
                TokenID=int(row["TokenID"]),
                GroupID=str(row["GroupID"]),
                JobID=self.job_id,
                CreateDT=datetime.now(),
            )
            for _, row in unique_records.iterrows()
        ]
        if unmerge_objs:
            self.session.bulk_save_objects(unmerge_objs, return_defaults=False)


    def save_processing_summary(self) -> None:
        """
        Save a summary of the processing results to the output directory.
        """
        if self.worksheet_df is None:
            logger.warning("No worksheet data available for summary")
            return

        summary_data = {
            "Entity Type": [self.entity_type],
            "Job ID": [self.job_id],
            "Total Records": [len(self.worksheet_df)],
            "Records with GroupID": [len(self.worksheet_df[self.worksheet_df["GroupID"].notnull()])],
            "Records without GroupID": [len(self.worksheet_df[self.worksheet_df["GroupID"].isnull()])],
            "Invalid SSN Records": [len(self.worksheet_df[self.worksheet_df["InvalidSSN"] == 1])],
            "Processing Date": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        }

        summary_df = pd.DataFrame(summary_data)
        output_path = os.path.join(self.output_dir, f"summary_{self.output_filename}")
        summary_df.to_excel(output_path, index=False)
        logger.info(f"Processing summary saved to: {output_path}")


    def consume(self) -> None:
        """
        Main entry point to run the full workflow with separate transactions.
        First truncates tables, then processes worksheet data in a separate transaction.
        """
        logger.info(
            f"Starting {self.entity_type} unmerge worksheet consumption workflow"
        )

        # Step 1: Truncate tables in separate transaction
        try:
            self.truncate_tables()
            self.session.commit()
            logger.info("Tables truncated successfully")
        except Exception as e:
            logger.error(f"Error during table truncation: {e}")
            self.session.rollback()
            raise

        # Step 2: Process worksheet data in separate transaction
        try:
            self.load_worksheet()

            # Process all steps in a single transaction
            self.process_whitelist()
            self.process_do_not_merge_pairs()
            self.process_invalid_ssn()
            self.load_unmerge_records()

            # Save processing summary
            self.save_processing_summary()

            # Commit all changes
            self.session.commit()
            logger.info("All worksheet processing operations committed successfully")
            logger.info(
                f"{self.entity_type} unmerge worksheet consumption completed successfully"
            )

        except Exception as e:
            logger.error(f"Error during worksheet processing: {e}")
            self.session.rollback()
            raise


# Convenience functions for creating consumers for specific entity types
def create_person_consumer(
    session: Session, 
    worksheet_path: str, 
    job_id: int,
    output_dir: str = "output",
    output_filename: str = "person_unmerge_output.xlsx"
) -> UnmergeWorksheetConsumer:
    """Create a consumer configured for person unmerge worksheets."""
    return UnmergeWorksheetConsumer.create_for_entity(
        session, worksheet_path, job_id, "person", output_dir, output_filename
    )


def create_organization_consumer(
    session: Session, 
    worksheet_path: str, 
    job_id: int,
    output_dir: str = "output",
    output_filename: str = "organization_unmerge_output.xlsx"
) -> UnmergeWorksheetConsumer:
    """Create a consumer configured for organization unmerge worksheets."""
    return UnmergeWorksheetConsumer.create_for_entity(
        session, worksheet_path, job_id, "organization", output_dir, output_filename
    )




