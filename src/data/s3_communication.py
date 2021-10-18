"""S3 communication tools."""
import boto3
import pandas as pd
from enum import Enum
from io import BytesIO


class S3FileType(Enum):
    """Enum to describe possible file type upload/downloads that S3Communication can handle."""

    CSV = 0
    JSON = 1
    PARQUET = 2


class S3Communication(object):
    """
    Class to establish communication with a ceph s3 bucket.
    It connects with the bucket and provides methods to read and write data in the parquet format.
    """

    def __init__(
        self, s3_endpoint_url, aws_access_key_id, aws_secret_access_key, s3_bucket
    ):
        """Initialize communicator."""
        self.s3_endpoint_url = s3_endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_resource = boto3.resource(
            "s3",
            endpoint_url=self.s3_endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )
        self.bucket = s3_bucket

    def _upload_bytes(self, buffer, prefix, key):
        """Upload byte content in buffer to bucket."""
        s3_object = self.s3_resource.Object(self.bucket, f"{prefix}/{key}")
        status = s3_object.put(Body=buffer.getvalue())
        return status

    def _download_bytes(self, prefix, key):
        """Download byte content in bucket/prefix/key to buffer."""
        buffer = BytesIO()
        s3_object = self.s3_resource.Object(self.bucket, f"{prefix}/{key}")
        s3_object.download_fileobj(buffer)
        return buffer

    def upload_file_to_s3(self, filepath, s3_prefix, s3_key):
        """Read file from disk and upload to s3 bucket under prefix/key."""
        with open(filepath, "rb") as f:
            status = self._upload_bytes(f, s3_prefix, s3_key)
        return status

    def upload_df_to_s3(self, df, s3_prefix, s3_key, filetype=S3FileType.PARQUET):
        """
        This helper function takes as input the data frame to be uploaded, and the output s3_key.
        It then saves the data frame in the defined s3 bucket.
        """
        buffer = BytesIO()
        if filetype == S3FileType.CSV:
            df.to_csv(buffer)
        elif filetype == S3FileType.JSON:
            df.to_json(buffer)
        elif filetype == S3FileType.PARQUET:
            df.to_parquet(buffer)
        else:
            raise ValueError(
                f"Received unexpected file type arg {filetype}. Can only be one of: {list(S3FileType)})"
            )

        status = self._upload_bytes(buffer, s3_prefix, s3_key)
        return status

    def download_file_from_s3(self, filepath, s3_prefix, s3_key):
        """Download file from s3 bucket/prefix/key and save it to filepath on disk."""
        buffer = self._download_bytes(s3_prefix, s3_key)
        with open(filepath, "wb") as f:
            f.write(buffer)

    def download_df_from_s3(self, s3_prefix, s3_key, filetype=S3FileType.PARQUET):
        """
        Helper function to read from s3 and see if the saved data is correct.
        """
        buffer = self._download_bytes(s3_prefix, s3_key)

        if filetype == S3FileType.CSV:
            df = pd.read_csv(buffer)
        elif filetype == S3FileType.JSON:
            df = pd.read_json(buffer)
        elif filetype == S3FileType.PARQUET:
            df = pd.read_parquet(buffer)
        else:
            raise ValueError(
                f"Received unexpected file type arg {filetype}. Can only be one of: {list(S3FileType)})"
            )
        return df
