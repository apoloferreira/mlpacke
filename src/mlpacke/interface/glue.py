import boto3
import logging
from boto3.session import Session


logger = logging.getLogger("mlpacke")


class CatalogInterface:
    """
    Classe singleton de interface para operações no catálogo de Glue.

    Atributos:
    _instance : CatalogInterface
        Instância única da classe.
    _client_glue : boto3.client
        Cliente boto3 para o serviço AWS Glue.
    _cache : dict[str, list]
        Dicionário de cache para armazenar partições recuperadas.
    """

    _instance = None
    _client_glue = None
    _cache: dict[str, list] = {}

    def __new__(cls):
        """Inicializa a classe e garante que apenas uma instância seja criada."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._client_glue = cls.create_client("glue")
        return cls._instance

    @classmethod
    def create_client(cls, service: str) -> Session.client:
        """
        Cria um cliente boto3 para o serviço AWS especificado.

        :param service: Nome do serviço AWS.
        :return: Um cliente boto3 para o serviço especificado.
        """
        return boto3.client(service_name=service)

    @classmethod
    def get_client(cls) -> Session.client:
        """
        Retorna o cliente boto3 do Glue se já existente ou criando-o se necessário.

        :return: Cliente boto3 para o AWS Glue.
        """
        if cls._client_glue is None:
            cls._client_glue = cls.create_client("glue")
        return cls._client_glue

    @classmethod
    def get_partition(cls, database: str, table: str, partition: list) -> dict | None:
        """
        Recupera uma partição específica do Catálogo do Glue.

        :param database: Nome do banco de dados.
        :param table: Nome da tabela.
        :param partition: Os valores da partição.
        :return: Os detalhes da partição se encontrada, caso contrário, None.
        """
        client_glue = cls.get_client()
        partition = list(map(str, partition))

        try:
            return client_glue.get_partition(
                DatabaseName=database,
                TableName=table,
                PartitionValues=partition
            )
        except client_glue.exceptions.EntityNotFoundException:
            logger.warning(f"Partição {partition} não existe na tabela: {database}.{table}")
            return None

    @classmethod
    def get_all_partitions(cls, database: str, table: str, only_first_value: bool = True) -> list:
        """
        Recupera todas as partições de uma tabela específica no Catálogo do Glue.

        :param database: Nome do banco de dados.
        :param table: Nome da tabela.
        :param only_first_value: Se True, apenas o primeiro valor de cada partição é retornado.
        :return: Uma lista das partições.
        """
        client_glue = cls.get_client()

        if f"{database}.{table}" in cls._cache:
            if only_first_value:
                return [item[0] for item in cls._cache[f"{database}.{table}"]]
            return cls._cache[f"{database}.{table}"]

        next_token = ""
        partitions = []
        try:
            while True:
                response = client_glue.get_partitions(
                    DatabaseName=database,
                    TableName=table,
                    MaxResults=500,
                    ExcludeColumnSchema=True,
                    NextToken=next_token
                )
                partitions += [item["Values"] for item in sorted(
                    response["Partitions"],
                    key=lambda x: x["Values"][0],
                    reverse=True)
                ]
                if "NextToken" not in response:
                    break
                next_token = response["NextToken"]
        except client_glue.exceptions.EntityNotFoundException:
            logger.warning(f"Erro ao buscar partições da tabela: {database}.{table}")

        cls._cache[f"{database}.{table}"] = partitions
        if only_first_value:
            return [item[0] for item in partitions]
        return partitions

    @classmethod
    def create_partition(cls, database: str, table: str, partition: list, location: str) -> None:
        """
        Cria uma nova partição no Catálogo do Glue.

        :param database: Nome do banco de dados.
        :param table: Nome da tabela.
        :param partition: Os valores da partição.
        :param location: O local dos dados da partição no S3.
        """
        client_glue = cls.get_client()

        if cls.get_partition(database, table, partition):
            logger.warning("Partição já existe!")
            return

        partition = list(map(str, partition))
        try:
            logger.debug(f"Criando partição {partition} em {location}...")
            response = client_glue.create_partition(
                DatabaseName=database,
                TableName=table,
                PartitionInput={
                    "Values": partition,
                    "StorageDescriptor": {
                        "Location": location,
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                            "Parameters": {"serialization.format": "1"}
                        }
                    }
                }
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                logger.debug("Criado partição com sucesso!")
        except Exception as e:
            logger.error(f"Erro ao criar partição {partition} para tabela {database}.{table}")
            logger.error(e)


    @classmethod
    def update_partition(cls, database: str, table: str, partition: list, location: str) -> None:
        """
        Atualiza uma partição existente no Catálogo do Glue.

        :param database: Nome do banco de dados.
        :param table: Nome da tabela.
        :param partition: Os valores da partição.
        :param location: Nova localização no S3 dos dados da partição.
        """
        client_glue = cls.get_client()

        partition = list(map(str, partition))
        try:
            response = client_glue.update_partition(
                DatabaseName=database,
                TableName=table,
                PartitionValueList=partition,
                PartitionInput={
                    "Values": partition,
                    "StorageDescriptor": {
                        "Location": location
                    }
                }
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                logger.debug("Atualizado partição com sucesso!")
            else:
                status_code = response["ResponseMetadata"]["HTTPStatusCode"]
                logger.error(f"Erro ao atualizar partição! Status Code: {status_code}")
        except Exception as e:
            logger.error(f"Erro ao atualizar partição {partition} para tabela {database}.{table}")
            logger.error(e)

    @classmethod
    def get_last_partition(cls, database: str, table: str, only_first_value: bool = True) -> str:
        """
        Recupera a última partição de uma tabela específica no Catálogo do Glue.

        :param database: Nome do banco de dados.
        :param table: Nome da tabela.
        :param only_first_value: Se True, apenas o primeiro valor da última partição é retornado.
        :return: O valor da última partição.
        """
        partitions = cls.get_all_partitions(database, table, only_first_value)
        return partitions[0]
