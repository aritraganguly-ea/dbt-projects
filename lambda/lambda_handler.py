import random

import pandas as pd
import requests

from base_class import DataExtractor


class CPIU(DataExtractor):
    URL = "https://data.bls.gov/timeseries/CUUR0000SA0?years_option=all_years"

    METADATA = dict(
        country="US",
        description="CPI-U",
        frequency="monthly",
        region="NA",
        source="USBLS",
        unit="1982-84=100",
    )

    def extract(self) -> None:
        self.logger.info("Initiating the Data Extraction Method.")
        self.df = pd.read_html(self.URL)[1]

    def transform(self) -> None:
        self.logger.info("Initiating the Data Transformation Method.")
        self.df = (
            self.df.iloc[:, :-2]
            .melt(id_vars=["Year"], var_name="Month")
            .assign(
                date=lambda df: pd.to_datetime(
                    df["Year"].astype(str) + "-" + df["Month"] + "-01",
                    format="%Y-%b-%d",
                )
            )
            .drop(columns=["Year", "Month"])
            .assign(**self.METADATA)
        )


class Transtrend(DataExtractor):
    URL = "https://transtrend.com/api/indexes/?category=dtp"

    METADATA = dict(
        country="Netherlands",
        description="Transtrend DTP",
        frequency="monthly",
        region="EU",
        source="Transtrend",
        unit="per_month",
    )

    def extract(self):
        self.logger.info("Initiating the Data Extraction Method.")

        response = requests.get(self.URL)
        response.raise_for_status()

        data = response.json()
        self.df = pd.json_normalize(
            data,
            record_path="returns",
            meta=["pk", "category", "name", "is_main_index"],
        )

    def transform(self):
        self.logger.info("Initiating the Data Transformation Method.")

        self.df = (
            self.df[self.df["pk"] == 1]
            .copy()
            .assign(
                date=lambda x: pd.to_datetime(x["timestamp"], unit="ms"),
                value=lambda x: x["monthly_return"] * 100,
            )
            .loc[:, ["date", "value"]]
            .assign(**self.METADATA)
        )


def run_cpiu_etl():
    CPIU().etl()


def run_transtrend_etl():
    Transtrend().etl()


def lambda_handler(event, context):
    """
    Lambda handler function to run the ETL process.
    """
    funcs = [(run_cpiu_etl, "CPIU ETL"), (run_transtrend_etl, "Transtrend ETL")]

    chosen_func, func_name = random.choice(funcs)
    chosen_func()

    return {
        "statusCode": 200,
        "body": f"ETL process completed successfully. Invoked function: {func_name}",
    }
