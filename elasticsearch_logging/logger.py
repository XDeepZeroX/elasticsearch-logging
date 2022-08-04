import logging
import datetime
import json
import typing

import socket
import os
from typing import Any

from elasticsearch import Elasticsearch, helpers


def get_path_file_backup():
    """
    Получение пути к файлу бэкапа логов
    Returns: Путь к бэкапу логов
    """
    return os.path.join(os.getcwd(), '.elastic_logs_backup')


class ElasticSearchLogger(logging.Handler):
    DEFAULT_BUFFER_SIZE = 100  # Размер буфера по умолчанию, 100 записей
    DEFAULT_BUFFER_TIME_S = 10  # Размер буфера по умолчанию, 100 записей
    DEFAULT_RETRY_SEND_LOGS_S = 30  # Интервал повторной отправки логов, сек

    __time_last_attempt_request: datetime.datetime = None  # Время последней попытки отправить логи
    __available_unsent_logs: bool = False  # Флаг имеющихся неотправленных логов
    __elasticksearch_unable: bool = True  # Флаг, доступен ли ElasticSearch

    def __init__(self,
                 hosts: str | typing.List[str],
                 buffer_size: int = DEFAULT_BUFFER_SIZE,
                 buffer_time_s: int = DEFAULT_BUFFER_TIME_S,
                 environment: str = 'development',
                 token: str = '',
                 basic_auth: typing.Union[any, str, typing.Tuple[str, str]] = None,
                 elastic_index: str = ''):
        """
        Инициализация логера
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
            basic_auth=basic_auth
        )
        self.__buffer = list()
        self.__source_host = socket.gethostbyname(socket.gethostname())

        self.__check_available_unsent_logs()
        self.__time_last_send_logs = datetime.datetime.utcnow()

    def __check_available_unsent_logs(self) -> bool:
        """
        Проверка на имеющиеся неотправленные логи
        """
        backup_logs_path = get_path_file_backup()

        if os.path.exists(backup_logs_path) and os.path.getsize(backup_logs_path) != 0:
            self.__available_unsent_logs = True
            return True
        return False

    def _formating_log_entry(self, record) -> object:
        """
        Формирование объекта, отправляемого в ElasticSearch
        Args:
            record:

        Returns:
            Сформированный объект для отправки
        """
        log_entry = self.format(record)
        if isinstance(log_entry, str):
            log_entry = {
                "message": log_entry,
                "logger_name": self.elastic_index,
                "source_host": self.__source_host,
                "source_environment": self.environment,
                "timestamp": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            }
        else:
            log_entry['source_host'] = self.__source_host
            log_entry['source_environment'] = self.environment
            log_entry['timestamp'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        return log_entry

    def _backup(self):
        """
        Сохранение логов в файл, если произошла ошибка или ElasticSearch не доступен
        """
        if len(self.__buffer) >= self.buffer_size:
            backup_logs_path = get_path_file_backup()

            with open(backup_logs_path, 'a+', encoding='utf-8') as fp:
                for log_entry in self.__buffer:
                    fp.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
                self.__buffer.clear()
                self.__available_unsent_logs = True

    def _add_log_to_buffer(self, record) -> None:
        """
        Добавление лога в буфер
        Args:
            record: Запись логирования
        """
        log = self._formating_log_entry(record)
        self.__buffer.append(log)

    def __diff_time_retry(self) -> float:
        """
        Сколько прошло секунд после последней попытки установки соединения с ElasticSearch
        Returns: float - секунды
        """
        if not self.__time_last_attempt_request:
            return float("inf")
        return (datetime.datetime.utcnow() - self.__time_last_attempt_request).total_seconds()

    def emit(self, record):
        """
        Фиксация записи в лог
        Args:
            record: Запись лога
        """
        self._add_log_to_buffer(record)

        try:
            # ElasticSearch доступен или прошло N секунд после последней попытки
            if self.__elasticksearch_unable or self.__diff_time_retry() >= self.DEFAULT_RETRY_SEND_LOGS_S:
                response = self.send_logs()
                return response
            self._backup()

        except Exception as ex:
            print('Unable to connect elastic host. Logstash will restore when available.')
            self._backup()
            self.__elasticksearch_unable = False
            self.__time_last_attempt_request = datetime.datetime.utcnow()
            print(f'Произошла ошибка: {ex}')

    def read_backup(self) -> typing.List[dict]:
        """
        Чтение логов из backup
        Returns: Список логов
        """
        if not self.__check_available_unsent_logs(): return []
        with open(get_path_file_backup(), 'r', encoding='utf-8') as fp:
            return list(map(json.loads, fp.readlines()))

    @staticmethod
    def clear_backup() -> None:
        """
        Чтение логов из backup
        Returns: Список логов
        """
        with open(get_path_file_backup(), 'w', encoding='utf-8') as fp:
            fp.write('')

    def try_send_backup(self):
        """
        Попытка отправить логи из backup
        """
        log_entities = self.read_backup()
        if len(log_entities) != 0:
            result = ElasticSearchLogger.store_msgs_bulk(self.__es_client, self.elastic_index, log_entities)
            ElasticSearchLogger.clear_backup()
            return result

    def send_logs(self) -> typing.Union[typing.Dict[str, typing.Any], Exception, None]:
        """
        Отправка логов в ElasticSearch
        Returns:
            Результат POST запроса в ElasticSearch или Exception, если ElasticSearch не доступен
        """
        if len(self.__buffer) < self.buffer_size and (
                datetime.datetime.utcnow() - self.__time_last_send_logs).total_seconds() < self.buffer_time_s: return

        # region Отправка текущих логов
        if len(self.__buffer) == 1:
            result = ElasticSearchLogger.store_msgs(self.__es_client, self.elastic_index, self.__buffer[0])
        else:
            result = ElasticSearchLogger.store_msgs_bulk(self.__es_client, self.elastic_index, self.__buffer)
        # endregion

        # region Отправка бэкапа
        if self.__available_unsent_logs:
            self.try_send_backup()

        # endregion

        self.__buffer.clear()
        self.__time_last_send_logs = datetime.datetime.utcnow()
        return result

    @staticmethod
    def store_msgs_bulk(es_client: Elasticsearch,
                        index: str,
                        buffer: typing.List[dict]) -> tuple[int, int | list[dict[str, Any]]]:
        """
        Отправка нескольких записей лога в ElasticSearch
        Args:
            es_client: Клиент ElasticSearch
            index: Индекс в ElasticSearch
            buffer: Список записей лога
        """
        print('Отправляются логи')
        return helpers.bulk(client=es_client, actions=buffer, index=index)

    @staticmethod
    def store_msgs(es_client, index: str, log: dict) -> typing.Dict[str, typing.Any]:
        """
        Отправка одной записи лога в ElasticSearch
        Args:
            index: Индекс в ElasticSearch
            es_client: Клиент ElasticSearch
            log: Запись лога
        """
        print('Отправляются логи')
        return es_client.index(index=index, body=log)


class ElasticSearchLoggerFormatter(logging.Formatter):
    def __init__(self):
        super(ElasticSearchLoggerFormatter, self).__init__()

    def format(self, record):
        """
        Форматирование записи лога перед отправкой в ElasticSearch
        Args:
            record: Запись лога

        Returns: Отредактированная запись лога

        """
        data = dict(message=record.msg,
                    logger_name=record.name,
                    levelname=record.levelname,
                    funcName=record.funcName,
                    filename=record.filename,
                    pathname=record.pathname,
                    levelno=record.levelno,
                    lineno=record.lineno,
                    process=record.process,
                    processName=record.processName,
                    thread=record.thread,
                    threadName=record.threadName,
                    fullFuncName=f'[{record.filename}.{record.funcName}:{record.lineno}]')
        extra = record.__dict__.get('elastic_fields')
        if extra:
            for key, value in extra.items():
                data[key] = value

        return data
