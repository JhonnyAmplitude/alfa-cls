from __future__ import annotations
from pathlib import Path
from typing import  Optional

import datetime

from src.utils import logger


class XmlParserBase:
    """Базовый класс для XML парсеров"""

    @staticmethod
    def prepare_xml_source(path_or_bytes: str | bytes) -> tuple[str, bool]:
        """Подготавливает источник XML для iterparse"""
        try:
            if isinstance(path_or_bytes, (bytes, bytearray)):
                tmp = Path(f"__xml_tmp_{id(path_or_bytes)}__.xml")
                tmp.write_bytes(path_or_bytes)
                return str(tmp), True
            return str(path_or_bytes), False
        except Exception as e:
            logger.exception("Failed to prepare XML source: %s", e)
            raise

    @staticmethod
    def cleanup_tmp_file(source: str, cleanup_needed: bool) -> None:
        """Очищает временные файлы"""
        if cleanup_needed:
            try:
                Path(source).unlink(missing_ok=True)
            except Exception:
                pass

    @staticmethod
    def parse_datetime_from_text(s: Optional[str]) -> Optional[datetime]:
        """Парсит datetime из текста"""
        from datetime import datetime
        import re

        if not s:
            return None

        s = str(s)
        # Попробуем полный формат
        m = re.search(r"\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}:\d{2}", s)
        if m:
            try:
                return datetime.strptime(m.group(0), "%d.%m.%Y %H:%M:%S")
            except Exception:
                pass

        # Попробуем короткий формат
        m = re.search(r"\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}", s)
        if m:
            try:
                return datetime.strptime(m.group(0), "%d.%m.%Y %H:%M")
            except Exception:
                pass

        # Другие форматы
        for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(s.strip(), fmt)
            except Exception:
                continue
        return None