"""Интерфейс командной строки."""
import asyncio
import sys
from argparse import ArgumentParser
from datetime import datetime
from ipaddress import ip_address
from pathlib import Path
from typing import TYPE_CHECKING

from cdr_collect import __description__ as _description
from cdr_collect.receiver import CDRReceiver

if TYPE_CHECKING:
    from argparse import Namespace


def _get_parameters() -> "Namespace":
    argument_parser = ArgumentParser(prog=_description)

    default_address = "10.1.2.4"
    argument_parser.add_argument(
        "--listen-address",
        nargs="?",
        default=ip_address(default_address),
        type=ip_address,
        help=f"Адрес, по которому будут обрабатываться данные. По-умолчанию: {default_address}",
    )

    default_port = 5005
    argument_parser.add_argument(
        "--listen-port",
        nargs="?",
        default=default_port,
        type=int,
        help=f"Порт, по которому будут обрабатываться данные. По-умолчанию: {default_port}",
    )

    default_storage = "/var/log/cdr/"
    argument_parser.add_argument(
        "--storage",
        nargs="?",
        default=Path(default_storage),
        type=Path,
        help=f"Хранилище передаваемых и экспортируемых данных. По-умолчанию: {default_storage}",
    )

    default_sender = "/netup/utm5/bin/utm5_send_cdr"
    argument_parser.add_argument(
        "--utm5-sender",
        nargs="?",
        default=Path(default_sender),
        type=Path,
        help=f"Исполняемый файл для отправки записи в формате utm5. По-умолчанию: {default_sender}",
    )

    default_configuration = "/netup/utm5/utm5_send_cdr.cfg"
    argument_parser.add_argument(
        "--utm5-configuration",
        nargs="?",
        default=Path(default_configuration),
        type=Path,
        help=f"Передаваемая для --utm5-sender конфигурация. По-умолчанию: {default_configuration}",
    )

    return argument_parser.parse_args()


def handle_command_line() -> None:
    """Получение параметров из командной строки и запуск сервера."""
    try:
        parameters = _get_parameters()

        sys.stdout.write(f"{datetime.now()} Инициализация программы...\n")
        receiver = CDRReceiver(parameters)

        host, port = parameters.listen_address, parameters.listen_port
        sys.stdout.write(f"{datetime.now()} Запуск сервера на {host}:{port}...\n")

        asyncio.run(receiver.start_listen())
    except KeyboardInterrupt:
        sys.stdout.write(f"\n{datetime.now()} Завершение работы...\n")
        sys.exit()
