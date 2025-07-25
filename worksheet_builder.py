"""
worksheet_builder.py
==================
This module contains the UnmergeWorksheetBuilder class, which constructs a person-level
un-merge review worksheet by integrating PersonChild core columns, K12 attendance blocks,
wage blocks, and applying column ordering rules from the database.








Description:
    The UnmergeWorksheetBuilder class orchestrates the creation of a worksheet DataFrame
    by combining data from multiple sources: PersonChild records, K12 enrollment data
    (processed via K12Processor), and wage data (processed via WageProcessor). It applies
    column ordering based on the 'worksheet_column_order' configuration and sorts the
    final output for review purposes.








Classes:
    UnmergeWorksheetBuilder:
        A class to build an unmerge review worksheet for a set of TokenIDs.








Dependencies:
    - pandas: For DataFrame manipulation.
    - SQLAlchemy ORM: For database session management.
    - ir_modernized.unmerge.child_loader: For loading PersonChild data.
    - ir_modernized.utils.smashid_util: For generating SmashIDs.
    - ir_modernized.unmerge.k12_processor: For processing K12 enrollment data.
    - ir_modernized.unmerge.wage_processor: For processing wage data.
    - ir_modernized.utils.column_order_util: For retrieving column order from the database.
    - ir_modernized.utils.worksheet_utils: For utility functions like assigning group IDs
      and applying column order.
"""


from __future__ import annotations
from sqlalchemy import inspect
import logging
from typing import Dict, List, Type
import pandas as pd
from sqlalchemy.orm import Session
from ir_modernized.unmerge.child_loader import load_children_by_tokenids
from ir_modernized.unmerge.k12_processor import K12Processor
from ir_modernized.unmerge.wage_processor import WageProcessor
from ir_modernized.utils.smashid_util import create_smash_id
from ir_modernized.utils.column_order_util import get_column_order
from ir_modernized.utils.worksheet_utils import (
    process_worksheet_group_ids,
    apply_column_order,
)
from ir_modernized.utils.bitarray_util import convert_bitarray_to_string
from ir_modernized.models.metadata import IR_ColumnOrder




logger = logging.getLogger(__name__)




class UnmergeWorksheetBuilder:
    """
    Build an un-merge review worksheet for a set of TokenIDs.








    This class integrates data from PersonChild records, K12 enrollment data, and wage data
    to create a comprehensive worksheet. It computes SmashIDs, assigns group IDs, merges
    additional data blocks, and applies column ordering rules fetched from the database.








    Attributes:
        session (Session): The SQLAlchemy database session.
        k12_processor (K12Processor): Processor for K12 enrollment data.
        wage_processor (WageProcessor): Processor for wage data.
        column_order (list): List of column names defining the desired order from the database.
    """


    def __init__(self, session: Session, column_order_model: Type[IR_ColumnOrder]) -> None:
        """
        Initialize the UnmergeWorksheetBuilder with a database session.

        Args:
            session (Session): The SQLAlchemy database session used for data queries.
            column_order_model (Type[IR_ColumnOrder]): The specific column order model class to use.
        """
        self.session = session
        self.k12_processor = K12Processor()
        self.wage_processor = WageProcessor()
        self.column_order = get_column_order(session, column_order_model)


    # ----------------------- Helper: Compute SmashIDs ----------------------
    def _compute_smash_ids(self, children: List) -> Dict[int, str]:
        """
        Compute SmashIDs for the given list of child records.
        SmashIDs are unique identifiers generated from the FullName of child records.
        For each TokenID, this method selects the longest SmashID(s). If there are ties
        for the longest SmashID, all tied SmashIDs are joined with a '|' delimiter.








        Args:
            children (List): List of PersonChild objects.








        Returns:
            Dict[int, str]: Mapping of TokenID to its corresponding SmashID(s), with tied SmashIDs joined by '|'.
        """
        logger.info("Computing SmashIDs for %d child rows", len(children))
        child_df = pd.DataFrame(
            {
                "TokenID": [c.TokenID for c in children],
                "FullName": [
                    c.FullName or f"{c.FirstName or ''} {c.LastName or ''}".strip()
                    for c in children
                ],
            }
        )
        child_df["SmashID"] = child_df["FullName"].apply(create_smash_id)
        # Group by TokenID and join all SmashIDs with max length
        return (
            child_df.groupby("TokenID")["SmashID"]
            .agg(
                lambda col: (
                    "|".join(
                        sorted(
                            set(
                                col.dropna().loc[
                                    lambda s: s.str.len() == col.str.len().max()
                                ]
                            )
                        )
                    )
                    if col.dropna().any()
                    else ""
                )
            )
            .to_dict()
        )


    @staticmethod
    def _overwrite_smashid_with_group_canonical(sheet: pd.DataFrame) -> pd.DataFrame:
        """
        For each GroupID choose one canonical SmashID and overwrite the column
        so every row in that group displays the same value.
















        * Longest length wins
        * If tie on length, lowest lexical sort order
        * If a SmashID has multiple variants separated by '|', each variant
          is considered individually for the comparison.
        """


        def select_main(smash_series: pd.Series) -> str:
            variants: List[str] = []
            for s in smash_series.dropna().unique():
                variants.extend(s.split("|"))
            # sort: longest first, then lexicographic
            variants.sort(key=lambda v: (-len(v), v))
            return variants[0] if variants else ""

        sheet["SmashID"] = sheet.groupby("GroupID")["SmashID"].transform(select_main)
        return sheet


    # --------------------------- Helper: Prepare K12 Data ------------------
    def _prep_k12(
        self, k12_df: pd.DataFrame, optional: Dict[str, List[str]]
    ) -> pd.DataFrame:
        """
        Prepare K12 enrollment data for integration into the worksheet.
        Processes the K12 DataFrame by preprocessing, generating enrollment bitarrays,
        pivoting the data by school year, and trimming non-enrollment years. The
        resulting columns are prefixed with 'k12_' and stored as strings for display.








        Args:
            k12_df (pd.DataFrame): DataFrame containing raw K12 enrollment data.
            optional (Dict[str, List[str]]): Dictionary to store optional column lists.








        Returns:
            pd.DataFrame: Processed and pivoted K12 DataFrame, or empty if input is empty.
        """
        if k12_df.empty:
            return pd.DataFrame()


        logger.info("Processing %d K-12 rows", len(k12_df))
        k12_df = self.k12_processor.preprocess_data(k12_df.copy())


        # Generate enrollment bitarrays instead of strings
        bitarrays = self.k12_processor.generate_enrollment_bitarrays(k12_df)
        pivot = bitarrays.pivot(
            index="TokenID", columns="SchoolYear", values="EnrollmentBitarray"
        )
        pivot.columns = [f"k12_{c}" for c in pivot.columns]
        pivot = self.k12_processor.trim_non_enrollment_years(pivot)


        # Convert bitarrays to strings for human-readable display
        for col in pivot.columns:
            pivot[col] = pivot[col].apply(
                lambda x: convert_bitarray_to_string(x) if isinstance(x, list) else ""
            )


        optional["K12_District"] = list(pivot.columns)
        return pivot


    # --------------------------- Public: Build Worksheet -------------------
    def build(
        self,
        token_ids: List[int],
        tokenid_to_p20id: Dict[int, int],
        k12_df: pd.DataFrame,
        wage_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Build the unmerge review worksheet DataFrame.
        Orchestrates the worksheet creation by:
        - Loading PersonChild records for the given token IDs.
        - Computing SmashIDs and assigning group IDs.
        - Building the core worksheet with child data.
        - Merging K12 and wage data blocks if available.
        - Applying column ordering and sorting the final output.








        Args:
            token_ids (List[int]): List of token IDs to include in the worksheet.
            tokenid_to_p20id (Dict[int, int]): Mapping of token IDs to P20IDs.
            k12_df (pd.DataFrame): DataFrame containing K12 enrollment data.
            wage_df (pd.DataFrame): DataFrame containing wage data.








        Returns:
            pd.DataFrame: The final worksheet DataFrame, sorted and ordered.
        """
        optional_cols: Dict[str, List[str]] = {}


        # Load child records for the given token_ids
        children = load_children_by_tokenids(self.session, token_ids)
        if not children:
            logger.warning("No PersonChild rows found – returning empty sheet")
            return pd.DataFrame()


        # Compute SmashIDs and assign group IDs
        token_to_smash = self._compute_smash_ids(children)
        group_map = process_worksheet_group_ids(token_to_smash)


        # Build the initial worksheet DataFrame
        sheet = self._build_core(children, tokenid_to_p20id, token_to_smash, group_map)


        # Merge K12 data if available
        if not k12_df.empty:
            k12_block = self._prep_k12(k12_df, optional_cols)
            sheet = sheet.merge(
                k12_block, left_on="TokenID", right_index=True, how="left"
            )


        # Merge wage data if available using WageProcessor
        if not wage_df.empty:
            wage_block = self.wage_processor.prepare(
                wage_df, optional_cols
            )  # NEW: Use WageProcessor
            sheet = sheet.merge(wage_block, on="TokenID", how="left")


            # OVERWRITE SmashID with canonical per-group value
        sheet = self._overwrite_smashid_with_group_canonical(sheet)


        # Apply column ordering and sort the final DataFrame
        sheet = apply_column_order(sheet, self.column_order, optional_cols)
        return sheet.sort_values(["P20ID", "TokenID", "FullName"], na_position="last")


    # ----------------------- Helper: Build Core Worksheet ------------------
    @staticmethod
    def _build_core(
        children: List,
        tokenid_to_p20id: Dict[int, int],
        smash_map: Dict[int, str],
        group_map: Dict[int, int],
    ) -> pd.DataFrame:
        """
        Build the core worksheet DataFrame from PersonChild records.
        This method processes a list of PersonChild objects, extracts their attributes
        (excluding 'PKey' and 'ChildHashCD'), and adds computed fields like P20ID,
        SmashID, GroupID, and placeholder columns for review purposes.








        Args:
            children (list of PersonChild): A list of PersonChild ORM objects.
            tokenid_to_p20id (dict[int, int]): A mapping from TokenID to P20ID.
            smash_map (dict[int, str]): A mapping from TokenID to SmashID.
            group_map (dict[int, int]): A mapping from TokenID to GroupID.








        Returns:
            pd.DataFrame: A DataFrame containing the extracted and computed data for each child.








        Notes:
            - Only children with TokenID present in tokenid_to_p20id are included.
            - Uses SQLAlchemy's inspect to access all mapped column attributes, including inherited ones.
            - The attributes 'PKey' and 'ChildHashCD' are excluded from the worksheet.
        """
        records = []
        for child in children:
            if child.TokenID not in tokenid_to_p20id:
                continue
            # Use mapper.column_attrs to get all columns, including inherited ones
            mapper = inspect(child).mapper
            row = {
                attr.key: getattr(child, attr.key)
                for attr in mapper.column_attrs
                if attr.key not in {"PKey", "ChildHashCD"}
            }
            # Add computed and placeholder fields
            row.update(
                {
                    "P20ID": tokenid_to_p20id[child.TokenID],
                    "SmashID": smash_map.get(child.TokenID, ""),
                    "GroupID": group_map.get(child.TokenID),
                    "InvalidSSN": "",
                    "InvalidDob": "",
                    "DoNotMerge": "",
                }
            )
            records.append(row)
        return pd.DataFrame(records)





