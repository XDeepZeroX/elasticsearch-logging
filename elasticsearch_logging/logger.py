import datetime
import json
import logging
import os
import socket
import typing
from typing import Any

from elastic_transport import ConnectionTimeout
from elasticsearch import Elasticsearch, helpers


class ElasticSearchLogger(logging.Handler):
    """Синхронный логер для ElasticSearch, внедряемый в базовый модуль Python - logging."""

    DEFAULT_BUFFER_SIZE = 100  # Размер буфера по умолчанию, 100 записей
    DEFAULT_BUFFER_TIME_S = 10  # Время буфера по умолчанию, 10 секунд
    DEFAULT_RETRY_SEND_LOGS_S = 30  # Интервал повторной отправки логов, сек

    __time_last_attempt_request: datetime.datetime = None  # Время последней попытки отправить логи
    __available_unsent_logs: bool = False  # Флаг имеющихся неотправленных логов
    __elasticksearch_unable: bool = True  # Флаг, доступен ли ElasticSearch

    def __init__(
            self,
            hosts: str | typing.List[str],
            elastic_index: str,
            buffer_size: int = DEFAULT_BUFFER_SIZE,
            buffer_time_s: int = DEFAULT_BUFFER_TIME_S,
            environment: str = 'development',
            token: str = '',
            basic_auth: typing.Union[any, str, typing.Tuple[str, str]] = None,
    ) -> None:
        """Инициализация логера.

        Args:
            hosts: Список адресов ElasticSearch
            buffer_size: Размер буфера логов
            buffer_time_s: Время хранения буфера, после истечения которого происходит сброс в ElasticSearch
            environment: Название окружения
            token: Токен доступа к ElasticSearch
            elastic_index: Индекс в ElasticSearch
        """
        super(ElasticSearchLogger, self).__init__()

        self.buffer_time_s = self.DEFAULT_BUFFER_TIME_S
        if isinstance(buffer_time_s, int) and buffer_time_s >= 0:
            self.buffer_time_s = buffer_time_s

        self.buffer_size = self.DEFAULT_BUFFER_SIZE
        if isinstance(buffer_size, int) and buffer_size >= 0:
            self.buffer_size = buffer_size

        self.hosts = hosts
        self.environment = environment
        self.token = token

        self.elastic_index = elastic_index.lower()
        for ch in '.#_+$@&*!()=|, ':
            self.elastic_index = self.elastic_index.replace(ch, '-')

        self.__es_client = Elasticsearch(
            [hosts] if isinstance(hosts, str) else hosts,
            basic_auth=basic_auth,
        )
        self.__buffer = []
        self.__source_host = socket.gethostbyname(socket.gethostname())

        self.__check_available_unsent_logs()
        self.__time_last_send_logs = datetime.datetime.utcnow()

    @staticmethod
    def store_msgs(es_client: Elasticsearch, index: str, log: dict) -> typing.Dict[str, typing.Any]:
        """Отправка одной записи лога в ElasticSearch.

        Args:
            index: Индекс в ElasticSearch
            es_client: Клиент ElasticSearch
            log: Запись лога
        """
        print('Отправляются логи')
        return es_client.index(index=index, body=log)

    @staticmethod
    def store_msgs_bulk(es_client: Elasticsearch,
                        index: str,
                        buffer: typing.List[dict]) -> tuple[int, int | list[dict[str, Any]]]:
        """Отправка нескольких записей лога в ElasticSearch.

        Args:
            es_client: Клиент ElasticSearch
            index: Индекс в ElasticSearch
            buffer: Список записей лога
        """
        print('Отправляются логи')
        return helpers.bulk(client=es_client, actions=buffer, index=index)

    @staticmethod
    def clear_backup() -> None:
        """Чтение логов из backup.

        Returns: Список логов
        """
        with open(get_path_file_backup(), 'w', encoding='utf-8') as fp:
            fp.write('')

    def emit(self, record: logging.LogRecord) -> None:
        """Фиксация записи в лог.

        Args:
            record: Запись лога
        """
        self._add_log_to_buffer(record)

        try:
            # ElasticSearch доступен или прошло N секунд после последней попытки
            if self.__elasticksearch_unable or self.__diff_time_retry() >= self.DEFAULT_RETRY_SEND_LOGS_S:
                return self.send_logs()
            self._backup()

        except Exception as ex:  # noqa: PIE786
            print('Unable to connect elastic host. Logstash will restore when available.')
            self._backup()
            self.__elasticksearch_unable = False
            self.__time_last_attempt_request = datetime.datetime.utcnow()
            print(f'Произошла ошибка: {ex}')

    def send_logs(self) -> typing.Union[typing.Dict[str, typing.Any], ConnectionTimeout, None]:
        """Отправка логов в ElasticSearch.

        Returns:
            Результат POST запроса в ElasticSearch или Exception->ConnectionTimeout, если ElasticSearch не доступен
        """
        if len(self.__buffer) < self.buffer_size and (
                datetime.datetime.utcnow() - self.__time_last_send_logs).total_seconds() < self.buffer_time_s:
            return

        # region Отправка текущих логов
        if len(self.__buffer) == 1:
            ElasticSearchLogger.store_msgs(self.__es_client, self.elastic_index, self.__buffer[0])
        else:
            ElasticSearchLogger.store_msgs_bulk(self.__es_client, self.elastic_index, self.__buffer)
        # endregion

        # region Отправка бэкапа
        if self.__available_unsent_logs:
            self.try_send_backup()

        # endregion

        self.__buffer.clear()
        self.__time_last_send_logs = datetime.datetime.utcnow()

    def try_send_backup(self) -> typing.Union[None, ConnectionTimeout]:
        """Попытка отправить логи из backup."""
        log_entities = self.read_backup()
        if len(log_entities) != 0:
            ElasticSearchLogger.store_msgs_bulk(self.__es_client, self.elastic_index, log_entities)
            ElasticSearchLogger.clear_backup()

    def read_backup(self) -> typing.List[dict]:
        """Чтение логов из backup.

        Returns: Список логов
        """
        if not self.__check_available_unsent_logs():
            return []
        with open(get_path_file_backup(), 'r', encoding='utf-8') as fp:
            return list(map(json.loads, fp.readlines()))

    def _backup(self) -> None:
        """Сохранение логов в файл, если произошла ошибка или ElasticSearch не доступен."""
        if len(self.__buffer) >= self.buffer_size:
            backup_logs_path = get_path_file_backup()

            with open(backup_logs_path, 'a+', encoding='utf-8') as fp:
                for log_entry in self.__buffer:
                    fp.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
                self.__buffer.clear()
                self.__available_unsent_logs = True

    def _add_log_to_buffer(self, record: logging.LogRecord) -> None:
        """Добавление лога в буфер.

        Args:
            record: Запись логирования
        """
        log = self._formating_log_entry(record)
        self.__buffer.append(log)

    def _formating_log_entry(self, record: logging.LogRecord) -> dict:
        """Формирование объекта, отправляемого в ElasticSearch.

        Args:
            record:

        Returns:
            Сформированный объект для отправки
        """
        entry = self.format(record)

        if isinstance(entry, str):
            return {
                'message': entry,
                'logger_name': self.elastic_index,
                'source_host': self.__source_host,
                'source_environment': self.environment,
                'timestamp': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            }

        return {
            **entry,
            'source_host': self.__source_host,
            'source_environment': self.environment,
            'timestamp': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        }

    def __check_available_unsent_logs(self) -> bool:
        """Проверка на имеющиеся неотправленные логи."""
        backup_logs_path = get_path_file_backup()
        if os.path.exists(backup_logs_path) and os.path.getsize(backup_logs_path) != 0:
            self.__available_unsent_logs = True
            return True
        return False

    def __diff_time_retry(self) -> float:
        """Сколько прошло секунд после последней попытки установки соединения с ElasticSearch.

        Returns: float - секунды
        """
        if not self.__time_last_attempt_request:
            return float('inf')
        return (datetime.datetime.utcnow() - self.__time_last_attempt_request).total_seconds()


class ElasticSearchLoggerFormatter(logging.Formatter):
    """Форматер логов для ElasticSearch."""

    def __init__(self) -> None:
        super(ElasticSearchLoggerFormatter, self).__init__()

    def format(self, record: logging.LogRecord) -> dict:  # noqa: A003
        """Форматирование записи лога перед отправкой в ElasticSearch.

        Args:
            record: Запись лога

        Returns: Отредактированная запись лога
        """
        data = {
            'message': record.msg,
            'logger_name': record.name,
            'levelname': record.levelname,
            'funcName': record.funcName,
            'filename': record.filename,
            'pathname': record.pathname,
            'levelno': record.levelno,
            'lineno': record.lineno,
            'process': record.process,
            'processName': record.processName,
            'thread': record.thread,
            'threadName': record.threadName,
            'fullFuncName': f'[{record.filename}.{record.funcName}:{record.lineno}]',
        }
        extra = record.__dict__.get('elastic_fields')
        if extra:
            for key, value in extra.items():
                data[key] = value

        return data


def get_path_file_backup() -> str:
    """Получение пути к файлу бэкапа логов.

    Returns: Путь к бэкапу логов
    """
    return os.path.join(os.getcwd(), '.elastic_logs_backup')
