import pandas as pd
from typing import Dict, List
from ir_modernized.unmerge.groupid_assigner import GroupIdAssigner
import logging


logger = logging.getLogger(__name__)




def process_worksheet_group_ids(token_smash_final: Dict[int, str]) -> Dict[int, int]:
    """
    Process worksheet TokenID to SmashID mappings and assign GroupIDs.
    
    This function handles worksheet-specific processing before delegating to the core
    GroupIdAssigner algorithm. It processes tied SmashIDs (pipe-separated) and
    filters out empty SmashIDs.

    Args:
        token_smash_final (Dict[int, str]): Mapping of TokenID to final SmashID.
            SmashIDs can be pipe-separated for tied values (e.g., "ABC|DEF").

    Returns:
        Dict[int, int]: Mapping of TokenID to assigned GroupID.
    """
    logger.info(f"Processing GroupIDs for {len(token_smash_final)} TokenIDs")
    token_smash_pairs = []
    for tid, smash in token_smash_final.items():
        if smash:
            smash_ids = smash.split("|")  # Split tied SmashIDs
            for smash_id in smash_ids:
                token_smash_pairs.append((tid, smash_id))
    return GroupIdAssigner().assign_group_ids(token_smash_pairs)




def apply_column_order(
    worksheet_df: pd.DataFrame,
    column_order: List[Dict],
    optional_column_lists: Dict[str, List[str]],
) -> pd.DataFrame:
    """
    Apply column ordering based on the provided column order configuration.


    Args:
        worksheet_df (pd.DataFrame): The worksheet DataFrame to reorder.
        column_order (List[Dict]): The column order configuration.
        optional_column_lists (Dict[str, List[str]]): Optional columns to include.


    Returns:
        pd.DataFrame: The reordered worksheet DataFrame.


    Raises:
        ValueError: If required columns are missing from the DataFrame.
    """
    logger.info(f"Applying column order to DataFrame with {len(worksheet_df)} rows")
    final_columns = []
    for entry in column_order:
        if entry["ColumnType"] == 0:  # Required columns
            if entry["ColumnName"] not in worksheet_df.columns:
                logger.warning(
                    f"Required column '{entry['ColumnName']}' not found, adding as empty"
                )
                worksheet_df[entry["ColumnName"]] = ""
            final_columns.append(entry["ColumnName"])
        elif entry["ColumnType"] == 1:  # Optional columns
            if entry["ColumnName"] in worksheet_df.columns:
                final_columns.append(entry["ColumnName"])
            else:
                logger.warning(
                    f"Optional column '{entry['ColumnName']}' not found, skipping"
                )
        elif entry["ColumnType"] == 2:  # Dynamic optional columns
            optional_columns = optional_column_lists.get(entry["ColumnName"], [])
            # Filter to include only columns present in worksheet_df
            available_columns = [
                col for col in optional_columns if col in worksheet_df.columns
            ]
            if available_columns:
                final_columns.extend(available_columns)
            else:
                logger.warning(
                    f"No available optional columns found for '{entry['ColumnName']}'"
                )


    required_columns = [
        entry["ColumnName"] for entry in column_order if entry["ColumnType"] == 0
    ]
    missing_columns = [
        col for col in required_columns if col not in worksheet_df.columns
    ]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


    # Ensure we always return a DataFrame by using loc accessor
    result_df = worksheet_df.loc[:, final_columns]
    return result_df




