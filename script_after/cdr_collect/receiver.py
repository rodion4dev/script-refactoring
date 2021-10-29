"""Получатель данных по socket-серверу."""
import sys
from asyncio import IncompleteReadError, start_server
from datetime import datetime
from typing import TYPE_CHECKING

from cdr_collect.service import save_data

if TYPE_CHECKING:
    from argparse import Namespace
    from asyncio import StreamReader, StreamWriter


class CDRReceiver:
    """Сервер получения и экспорта данных о передаваемых звонках."""

    encoding = "ascii"
    chunks_delimiter = "\x00\x00\x00"
    encoded_chunks_delimiter = chunks_delimiter.encode()

    def __init__(self, parameters: "Namespace") -> None:
        """Конструктор сервера."""
        self.parameters = parameters
        self.__server = None

    async def start_listen(self) -> None:
        """Запуск сервера."""
        self.__server = await start_server(
            self.__handle_connection,
            host=str(self.parameters.listen_address),
            port=self.parameters.listen_port,
        )
        async with self.__server:
            try:
                await self.__server.serve_forever()
            except KeyboardInterrupt:
                pass

    async def __handle_connection(
        self, reader: "StreamReader", writer: "StreamWriter"
    ) -> None:
        sys.stdout.write(f"{datetime.now()} Создание нового соединения\n")

        chunks = []
        sys.stdout.write(f"{datetime.now()}   Чтение данных...\n")
        while not reader.at_eof():
            try:
                data = await reader.readuntil(separator=self.encoded_chunks_delimiter)
                data = data.decode(self.encoding).replace(self.chunks_delimiter, "")
                if data:
                    chunks.append(data)
            except IncompleteReadError:
                continue

        if chunks:
            sys.stdout.write(f"{datetime.now()}   Сохранение данных...\n")

        for chunk in chunks:
            await save_data(
                chunk,
                self.parameters.storage,
                self.parameters.utm5_sender,
                self.parameters.utm5_configuration,
            )

        sys.stdout.write(f"{datetime.now()} Закрытие соединения...\n")
        writer.close()
