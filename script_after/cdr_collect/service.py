"""Основная бизнес-логика и набор дополнительных инструментов."""
from asyncio import create_subprocess_shell
from asyncio.subprocess import PIPE
from dataclasses import dataclass
from pathlib import Path
from random import randint
from time import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Tuple


class CommandError(Exception):
    """Ошибка выполнения команды."""


@dataclass
class _Record:
    date: str
    time: str
    sec_dur: str
    dialed_num: str
    calling_num: str
    code_used: str
    code_dial: str
    in_trk_code: str
    frl: str
    out_crt_id: str

    _utm5_format = (
        "{calling_num};{dialed_num};{sec_dur};{day}-{month}-{year} "
        "{hours}:{minutes}:00"
    )
    _csv_format = (
        "{day};{month};{year};{hours};{minutes};{sec_dur};{dialed_num};"
        "{calling_num};{code_used};{code_dial};{in_trk_code};{frl};"
        "{out_crt_id}"
    )

    def _get_sec_dur(self) -> str:
        return self.sec_dur.strip("0")

    def _split_date(self) -> "Tuple[str, str, str]":
        return self.date[:2], self.date[2:4], self.date[4:6]

    def _split_time(self) -> "Tuple[str, str]":
        return self.time[:2], self.time[2:]

    @property
    def as_utm5(self) -> str:
        """Представление записи в utm5 формате."""
        day, month, year = self._split_date()
        hours, minutes = self._split_time()
        return self._utm5_format.format(
            calling_num=self.calling_num,
            dialed_num=self.dialed_num,
            sec_dur=self._get_sec_dur(),
            day=day,
            month=month,
            year=year,
            hours=hours,
            minutes=minutes,
        )

    @property
    def as_csv(self) -> str:
        """Представление записи в csv формате."""
        day, month, year = self._split_date()
        hours, minutes = self._split_time()
        return self._csv_format.format(
            day=day,
            month=month,
            year=year,
            hours=hours,
            minutes=minutes,
            sec_dur=self._get_sec_dur(),
            dialed_num=self.dialed_num,
            calling_num=self.calling_num,
            code_used=self.code_used,
            code_dial=self.code_dial,
            in_trk_code=self.in_trk_code,
            frl=self.frl,
            out_crt_id=self.out_crt_id,
        )


async def _send_cdr(log: Path, sender: Path, configuration: Path) -> None:
    command = [str(sender), "-c", str(configuration), "-s", str(log)]
    process = await create_subprocess_shell(" ".join(command), stdout=PIPE, stderr=PIPE)
    output, error = await process.communicate()

    if process.returncode == 0:
        return

    raise CommandError(output or error)


async def save_data(
    data: str, storage: Path, utm5_sender: Path, utm5_configuration: Path
) -> None:
    """Экспорт переданных данных в файлы и средствами утилиты."""
    storage = storage.resolve()

    with (storage / "cdr.raw").open(mode="a") as file:
        file.write(data + "\n")

    for record in data.splitlines():
        if len(record) != 77:
            continue

        record = record.split()
        if len(record) == 9:
            # Возвращаем отсутствующий code_dial параметр, удалённый методом split()
            record.insert(6, "")

        record = _Record(*record)

        with (storage / "cdr.csv").open(mode="a") as file:
            file.write(record.as_csv + "\n")

        timestamp = int(time())
        log = storage / (str(timestamp) + str(randint(0, timestamp)))
        with log.open(mode="w") as file:
            file.write(record.as_utm5 + "\n")

        await _send_cdr(
            log,
            utm5_sender.resolve(),
            utm5_configuration.resolve(),
        )
