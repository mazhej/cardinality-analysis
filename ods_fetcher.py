import pandas as pd
import logging
from typing import List
from pytds.tds_types import TableValuedParam, BigIntType
from pytds.tds_base import Column


logger = logging.getLogger(__name__)


class ODSFetcher:
    """
    Fetches wage and K12 enrollment data from ODS stored procedures using the IR API.
    
    This class uses the existing IR API's call_stored_procedure_with_tvp method
    to properly call stored procedures with Table-Valued Parameters (TVP).
    
    Attributes:
        database: The SqlServerDatabase instance from the IR API.
    """


    def __init__(self, database):
        """
        Initialize the ODSFetcher with a database instance.
        
        Args:
            database: SqlServerDatabase instance from the IR API.
        """
        self.database = database


    def get_wage_data(self, token_ids: List[int]) -> pd.DataFrame:
        """
        Fetch wage data by TokenIDs using the IR API's stored procedure method.
        
        Args:
            token_ids: List of TokenIDs to retrieve wage data for.
            
        Returns:
            pd.DataFrame: DataFrame containing wage data with columns such as
                          P20ID, TokenID, WageAMT, etc., as defined by sp_get_wage_data_tvp.
        """
        logger.info("Fetching wage data for %d TokenIDs", len(token_ids))
        
        # Return empty DataFrame if no token IDs provided
        if not token_ids:
            logger.info("No TokenIDs provided; returning empty DataFrame")
            return pd.DataFrame()
        
        try:
            # Create the TVP directly
            tvp_param = TableValuedParam(
                type_name="dbo.udt_TokenID",
                columns=[Column(name="TokenID", type=BigIntType())],
                rows=[(token_id,) for token_id in token_ids]
            )
            df = self.database.call_stored_procedure_with_tvp('sp_get_wage_data_tvp', tvp_param)
            logger.info("Wage data fetched successfully: %d rows", len(df))
            return df
        except Exception as e:
            logger.error(f"Error fetching wage data: {str(e)}")
            raise


    def get_k12_data(self, token_ids: List[int]) -> pd.DataFrame:
        """
        Fetch K12 district enrollment history by TokenIDs using the IR API's stored procedure method.
        
        Args:
            token_ids: List of TokenIDs to retrieve K12 enrollment data for.
            
        Returns:
            pd.DataFrame: DataFrame containing K12 enrollment data with columns such as
                          P20ID, TokenID, EnrollmentBeginDT, etc., as defined by sp_get_k12_history_tvp.
        """
        logger.info("Fetching K12 enrollment data for %d TokenIDs", len(token_ids))
        
        try:
            # Create the TVP directly
            tvp_param = TableValuedParam(
                type_name="dbo.udt_TokenID",
                columns=[Column(name="TokenID", type=BigIntType())],
                rows=[(token_id,) for token_id in token_ids]
            )
            df = self.database.call_stored_procedure_with_tvp('sp_get_k12_history_tvp', tvp_param)
            logger.info("K12 enrollment data fetched successfully: %d rows", len(df))
            return df
        except Exception as e:
            logger.error(f"Error fetching K12 data: {str(e)}")
            raise



