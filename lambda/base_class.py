import io
import logging
import os
from datetime import datetime

import boto3


class DataExtractor:
    # Configure logging.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.df = None
        self.S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "etl-bucket-ag")
        self.S3_FOLDER = f"{self.__class__.__name__.lower()}_data/"

    def extract(self) -> None:
        raise NotImplementedError("Extract Method Not Implemented.")

    def transform(self) -> None:
        raise NotImplementedError("Transform Method Not Implemented.")

    def load(self) -> None:
        self.logger.info("Initiating the Data Loading Method.")

        s3 = boto3.client("s3")
        csv_buffer = io.StringIO()
        output_filename = f'{self.__class__.__name__.lower()}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        self.df.to_csv(csv_buffer, index=False)

        s3.put_object(
            Bucket=self.S3_BUCKET_NAME,
            Key=os.path.join(self.S3_FOLDER, output_filename),
            Body=csv_buffer.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )

        self.logger.info("DataFrame Uploaded to S3 Successfully.")

    def etl(self) -> None:
        try:
            self.extract()
        except Exception as err:
            raise RuntimeError(
                f"Scraper failed at Extraction. Error was {err}"
            ) from err
        try:
            self.transform()
        except Exception as err:
            raise RuntimeError(
                f"Scraper failed at Transformation. Error was {err}"
            ) from err
        try:
            self.load()
        except Exception as err:
            raise RuntimeError(f"Scraper failed at Upload. Error was {err}") from err
