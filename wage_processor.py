# ── wage_processor.py ───────────────────────────────────────────────────────────
from __future__ import annotations




import logging
from typing import Dict, List, Tuple




import pandas as pd
from ir_modernized.utils.bitarray_util import convert_bitarray_to_string




logger = logging.getLogger(__name__)




class WageProcessor:
    """
    Transform raw ODS wage rows into the five blocks of columns the review
    worksheet needs:








    • presence-by-quarter     (# / _)          →  wq_<year>_P
    • hours   by quarter                         wq_<y>_<q>_HR
    • amount  by quarter                         wq_<y>_<q>_AMT
    • hours   by calendar year                  wq_<year>_HR
    • amount  by calendar year                  wq_<year>_AMT
    • plus OrgID & Organization name.








    The main entry point is `prepare()`.
    Uses bitarrays internally for efficiency, converts to strings for display.
    """


    QUARTERS = ["1", "2", "3", "4"]


    # ─────────────── public API ────────────────────────────────────────────────
    def prepare(
        self,
        wage_df: pd.DataFrame,
        optional_column_lists: Dict[str, List[str]],
    ) -> pd.DataFrame:
        """
        Parameters
        ----------
        wage_df : pd.DataFrame
            Raw rows returned by `sp_get_wage_data`.
        optional_column_lists : dict (mutable)
            Will be mutated so `apply_column_order()` knows which optional
            bundles were produced.








        Returns
        -------
        pd.DataFrame
            One row per TokenID with all wage columns.  `TokenID` is a column
            (not the index) so the caller can merge on it directly.
        """
        if wage_df.empty:
            return pd.DataFrame()


        df = self._housekeep(wage_df)
        years = sorted(df["OrganizationYear"].unique())


        presence = self._presence_matrix(df, years)
        qtr_hr, qtr_amt = self._quarter_tables(df, years)
        yr_hr, yr_amt = self._annual_tables(df, years)
        org_meta = self._org_meta(df)


        # register optional bundles
        optional_column_lists["InWorkforceByQuarter"] = list(presence.columns)
        optional_column_lists["WageHoursByQuarter"] = list(qtr_hr.columns)
        optional_column_lists["WagesByQuarter"] = list(qtr_amt.columns)
        optional_column_lists["WageHoursByYear"] = list(yr_hr.columns)
        optional_column_lists["WagesByYear"] = list(yr_amt.columns)


        combined = (
            pd.concat(
                [presence, qtr_hr, qtr_amt, yr_hr, yr_amt],
                axis=1,
                copy=False,
            )
            .join(org_meta, how="left")
            .reset_index()  # bring TokenID out of the index
        )
        return combined


    # ───────── internal helpers ────────────────────────────────────────────────
    @staticmethod
    def _housekeep(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.loc[df["WageHR"] == -9999, "WageHR"] = 0
        df.loc[df["WageAMT"] == -9999.99, "WageAMT"] = 0
        df["TermTypeCD"] = df["TermTypeCD"].astype(str)
        return df.drop_duplicates(
            subset=["TokenID", "OrganizationYear", "TermTypeCD", "WageAMT", "WageHR"]
        )


    def _presence_matrix(self, df: pd.DataFrame, years: List[int]) -> pd.DataFrame:
        """
        Create presence matrix using bitarrays internally, then convert to strings for display.


        Returns:
            pd.DataFrame: Presence data with string representation (# for wages, _ for no wages)
        """
        # Create quarter-level presence data
        present = (
            df[df["TermTypeCD"].isin(self.QUARTERS)]
            .assign(HasWages=lambda d: d["WageAMT"].gt(0))
            .pivot_table(
                index="TokenID",
                columns=["OrganizationYear", "TermTypeCD"],
                values="HasWages",
                aggfunc="first",
                fill_value=False,
            )
            .reindex(
                columns=pd.MultiIndex.from_product([years, self.QUARTERS]),
                fill_value=False,
            )
        )


        # Convert to bitarrays and then to strings
        presence_compact = pd.DataFrame(index=present.index)
        for year in years:
            cols = present.loc[:, year]  # Select all quarter columns for this year
            # Convert to bitarray (True=1, False=0) then to string
            presence_compact[f"wq_{year}_P"] = cols.apply(
                lambda r: convert_bitarray_to_string([1 if x else 0 for x in r], enrolled_char='#', not_enrolled_char='_'), axis=1
            )


        presence_compact = presence_compact.astype("string")


        return presence_compact


    def _quarter_tables(
        self, df: pd.DataFrame, years: List[int]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        idx = ["TokenID"]
        col_idx = pd.MultiIndex.from_product([years, self.QUARTERS])


        qtr_hr = (
            df[df["TermTypeCD"].isin(self.QUARTERS)]
            .pivot_table(
                index="TokenID",
                columns=["OrganizationYear", "TermTypeCD"],
                values="WageHR",
                aggfunc="first",
                fill_value=0,
            )
            .reindex(columns=col_idx, fill_value=0)
        )
        qtr_hr.columns = [f"wq_{y}_{q}_HR" for y, q in qtr_hr.columns]


        qtr_amt = (
            df[df["TermTypeCD"].isin(self.QUARTERS)]
            .pivot_table(
                index="TokenID",
                columns=["OrganizationYear", "TermTypeCD"],
                values="WageAMT",
                aggfunc="first",
                fill_value=0,
            )
            .reindex(columns=col_idx, fill_value=0)
        )
        qtr_amt.columns = [f"wq_{y}_{q}_AMT" for y, q in qtr_amt.columns]


        return qtr_hr, qtr_amt


    def _annual_tables(
        self, df: pd.DataFrame, years: List[int]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        yr_hr = (
            df[df["TermTypeCD"] == "ANNUAL"]
            .pivot_table(
                index="TokenID",
                columns="OrganizationYear",
                values="WageHR",
                aggfunc="first",
                fill_value=0,
            )
            .reindex(columns=years, fill_value=0)
        )
        yr_hr.columns = [f"wq_{y}_HR" for y in yr_hr.columns]


        yr_amt = (
            df[df["TermTypeCD"] == "ANNUAL"]
            .pivot_table(
                index="TokenID",
                columns="OrganizationYear",
                values="WageAMT",
                aggfunc="first",
                fill_value=0,
            )
            .reindex(columns=years, fill_value=0)
        )
        yr_amt.columns = [f"wq_{y}_AMT" for y in yr_amt.columns]


        return yr_hr, yr_amt


    @staticmethod
    def _org_meta(df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.sort_values(
                ["TokenID", "OrganizationYear", "TermTypeCD"],
                ascending=[True, False, False],
            )
            .groupby("TokenID")
            .first()[["OrganizationID", "OrganizationTTL"]]
            .rename(
                columns={
                    "OrganizationID": "OrgID",
                    "OrganizationTTL": "Organization",
                }
            )
        )



