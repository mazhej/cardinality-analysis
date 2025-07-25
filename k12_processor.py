import pandas as pd
from typing import List, Tuple
from datetime import datetime
import array  # For bitarray implementation
from ir_modernized.utils.bitarray_util import convert_bitarray_to_string


class K12Processor:
    """
    A class to process K12 enrollment data for unmerge and merge review worksheets.
    
    This class handles preprocessing, generating enrollment bitarrays, and trimming
    non-enrollment years from K12 data using binary representation instead of strings.
    
    Methods:
        preprocess_data: Filter and adjust K12 enrollment data.
        generate_enrollment_bitarrays: Create bitarrays for enrollment status per TokenID and school year.
        compute_enrollment_bitarray: Compute a bitarray for enrollment status.
        trim_non_enrollment_years: Remove school years with no enrollments.
    """


    def preprocess_data(self, k12_df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess K12 enrollment data by filtering invalid records and adjusting dates.
        
        This method:
        - Discards records where EnrollmentBeginDT is after the school year's end date (August 31).
        - Sets EnrollmentEndDT to September 1 of the school year if null.
        - Assigns a SchoolYear column based on OrganizationYear.
        
        Args:
            k12_df (pd.DataFrame): Raw K12 data with columns TokenID, OrganizationYear, EnrollmentBeginDT, EnrollmentEndDT.
            
        Returns:
            pd.DataFrame: Preprocessed K12 data with an additional SchoolYear column.
        """
        # Set the school year end date as August 31 of the organization year
        k12_df['SchoolYearEnd'] = pd.to_datetime(k12_df['OrganizationYear'].apply(lambda y: f"{y}-08-31"))
        # Filter out records where the enrollment begins after the school year ends
        k12_df = k12_df[k12_df['EnrollmentBeginDT'] <= k12_df['SchoolYearEnd']]
        # Fill null EnrollmentEndDT with September 1 of the previous year (start of school year)
        k12_df['EnrollmentEndDT'] = k12_df['EnrollmentEndDT'].fillna(
            (k12_df['OrganizationYear'] - 1).apply(lambda y: pd.to_datetime(f"{y}-09-01"))
        )
        # Assign the school year based on the organization year
        k12_df['SchoolYear'] = k12_df['OrganizationYear']
        return k12_df


    def generate_enrollment_bitarrays(self, k12_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate bitarrays for enrollment status for each TokenID and SchoolYear.
        
        This method groups K12 data by TokenID and SchoolYear, then computes a bitarray
        indicating enrollment status for each month (September to August).
        
        Args:
            k12_df (pd.DataFrame): Preprocessed K12 data with columns TokenID, SchoolYear, EnrollmentBeginDT, EnrollmentEndDT.
            
        Returns:
            pd.DataFrame: DataFrame with columns TokenID, SchoolYear, and EnrollmentBitarray (list of integers).
        """
        # Group K12 data by TokenID and SchoolYear
        grouped = k12_df.groupby(['TokenID', 'SchoolYear'])
        enrollment_data = []
        
        # Iterate over each group to compute the enrollment bitarray
        for (token_id, school_year), group in grouped:
            # Extract enrollment periods (begin and end dates) for the group
            periods = [
                (b.date() if hasattr(b, "date") else b, e.date() if hasattr(e, "date") else e)
                for b, e in zip(group['EnrollmentBeginDT'], group['EnrollmentEndDT'])
            ]
            # Compute the 12-bit enrollment bitarray for this TokenID and school year
            enrollment_bitarray = self.compute_enrollment_bitarray(school_year, periods)
            # Append the result to the enrollment data list
            enrollment_data.append({
                'TokenID': token_id,
                'SchoolYear': school_year,
                'EnrollmentBitarray': enrollment_bitarray
            })
        
        # Create a DataFrame from the enrollment data
        return pd.DataFrame(enrollment_data)


    def compute_enrollment_bitarray(self, school_year: int, periods: List[Tuple]) -> List[int]:
        """
        Compute a 12-bit enrollment bitarray for a given school year and enrollment periods.
        
        The bitarray represents monthly enrollment from September to August, where 1 indicates
        the student was enrolled on the 15th of the month, and 0 indicates they were not.
        
        Args:
            school_year (int): The school year (e.g., 2005 for the 2004-2005 school year).
            periods (List[Tuple]): List of (begin, end) enrollment periods as datetime tuples.
            
        Returns:
            List[int]: A 12-element list with 1 for enrolled months and 0 for non-enrolled months.
        """
        # Define the start year (previous year, since school year starts in September)
        year_start = school_year - 1
        # Create a list of dates for the 15th of each month from September to August
        months = [datetime(year_start, month, 15).date() for month in range(9, 13)] + \
                 [datetime(school_year, month, 15).date() for month in range(1, 9)]
        
        # Initialize the enrollment bitarray
        enrollment_bitarray = []
        
        # Check each month for enrollment status
        for month in months:
            # Determine if the student was enrolled on the 15th of the month
            enrolled = any(
                (begin.date() if hasattr(begin, "date") else begin) <= month <= (end.date() if hasattr(end, "date") else end)
                for begin, end in periods
            )
            # Append 1 if enrolled, 0 if not
            enrollment_bitarray.append(1 if enrolled else 0)
        
        return enrollment_bitarray


    def trim_non_enrollment_years(self, k12_pivot: pd.DataFrame) -> pd.DataFrame:
        """
        Trim leading and trailing school years with no enrollments (all 0s) from K12 pivot data.
        
        This method removes school year columns (e.g., '_2005_k12') where all TokenIDs have
        no enrollment (i.e., the bitarray is all zeros).
        
        Args:
            k12_pivot (pd.DataFrame): Pivoted K12 data with school year columns (e.g., '_2005_k12').
            
        Returns:
            pd.DataFrame: Trimmed DataFrame with only school years containing enrollments.
        """
        cols = k12_pivot.columns
        first_enrolled = 0
        last_enrolled = len(cols) - 1
        
        # Find the first school year with at least one enrollment (non-zero bitarray)
        for i, col in enumerate(cols):
            # Check if any TokenID has at least one enrollment (1) in this school year
            def has_enrollment(bitarray) -> int:
                if isinstance(bitarray, list):
                    return sum(bitarray)
                return 0
            
            # Handle NaN values properly - convert to empty list for processing
            col_data = k12_pivot[col].apply(lambda x: x if isinstance(x, list) else [])
            if not col_data.apply(has_enrollment).eq(0).all():
                first_enrolled = i
                break
        
        # Find the last school year with at least one enrollment
        for i, col in enumerate(cols[::-1]):
            # Check if any TokenID has at least one enrollment (1) in this school year
            def has_enrollment(bitarray) -> int:
                if isinstance(bitarray, list):
                    return sum(bitarray)
                return 0
            
            # Handle NaN values properly - convert to empty list for processing
            col_data = k12_pivot[col].apply(lambda x: x if isinstance(x, list) else [])
            if not col_data.apply(has_enrollment).eq(0).all():
                last_enrolled = len(cols) - 1 - i
                break
        
        # Return the DataFrame with only the columns between first and last enrolled years
        return k12_pivot.iloc[:, first_enrolled:last_enrolled + 1]






