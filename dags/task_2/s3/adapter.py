from io import BytesIO, BufferedWriter

import boto3

from ..config import settings


class S3Adapter:
    """
    Адаптер для S3 хранилища.

    Attributes:
        self.bucket_name: название бакета в S3 хранилище.
        self.endpoint: адрес S3 хранилища.
        self.access_key: логин для S3 хранилища.
        self.secret_key: пароль для S3 хранилища.
        self.s3_client: клиент для S3 хранилища.
    """

    def __init__(self) -> None:
        self.bucket_name = settings.S3_BUCKET_NAME
        self.endpoint = settings.S3_ENDPOINT
        self.access_key = settings.S3_ACCESS_KEY
        self.secret_key = settings.S3_SECRET_KEY
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def upload_file(
        self,
        file_name: str,
        content: str | bytes,
    ) -> None:
        """
        Загружает файл в S3 хранлище.

        Attributes:
            file_name: название файла в хранилище.
            content: содержимое файла.
        """
        if isinstance(content, bytes):
            buffer = BytesIO(content)
        else:
            buffer = BytesIO(content.encode("utf-8"))
        self.s3_client.upload_fileobj(buffer, self.bucket_name, file_name)

    def download_file(
        self,
        file_name: str,
        file: BufferedWriter,
    ) -> None:
        """
        Скачивает файл из S3 хранилища.

        Attributes:
            file_name: название файла в хранилище.
            file: контейнер для содержимого файла.
        """
        self.s3_client.download_fileobj(
            Fileobj=file,
            Bucket=self.bucket_name,
            Key=file_name
        )
