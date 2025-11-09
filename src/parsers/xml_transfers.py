from __future__ import annotations
from typing import List, Tuple, Dict, Any, Optional
import xml.etree.ElementTree as ET

from src.OperationDTO import OperationDTO
from src.utils import logger, to_float_safe, _local_name, _normalize_attrib, extract_isin_from_attr
from .xml_parser_base import XmlParserBase


class TransfersParser(XmlParserBase):
    """Парсер конвертаций из раздела Transfers"""

    def __init__(self):
        self.stats = {
            "total_rows": 0,
            "parsed": 0,
            "skipped_not_conversion": 0,
            "skipped_no_date": 0,
            "skipped_no_qty": 0,
            "skipped_invalid": 0,
        }

    def parse(self, path_or_bytes: str | bytes) -> Tuple[List[OperationDTO], Dict[str, Any]]:
        """Основной метод парсинга конвертаций"""
        ops: List[OperationDTO] = []
        source, cleanup_needed = self.prepare_xml_source(path_or_bytes)

        try:
            for event, elem in ET.iterparse(source, events=("end",)):
                if _local_name(elem.tag).lower() != "details":
                    elem.clear()
                    continue

                operation = self._process_details_element(elem)
                if operation:
                    ops.append(operation)

                elem.clear()

            logger.info("Parsed %s conversions (checked %s rows, skipped %s non-conversions)",
                        self.stats["parsed"], self.stats["total_rows"], self.stats["skipped_not_conversion"])
            return ops, self.stats.copy()

        except Exception as e:
            logger.exception("XML transfers parsing failed: %s", e)
            return ops, {"error": str(e), **self.stats}
        finally:
            self.cleanup_tmp_file(source, cleanup_needed)

    def _process_details_element(self, elem: ET.Element) -> Optional[OperationDTO]:
        """Обрабатывает элемент details"""
        self.stats["total_rows"] += 1
        attrib = _normalize_attrib(dict(elem.attrib))

        # Проверка типа операции
        if not self._is_conversion_operation(attrib):
            self.stats["skipped_not_conversion"] += 1
            return None

        # Извлечение количества
        qty = to_float_safe(attrib.get("qty") or "0")
        if qty == 0:
            self.stats["skipped_no_qty"] += 1
            return None

        # Извлечение даты
        date_val = self._extract_date(attrib)
        if not date_val:
            self.stats["skipped_no_date"] += 1
            return None

        # Создание DTO
        try:
            operation = OperationDTO(
                date=date_val,
                operation_type="asset_receive" if qty > 0 else "asset_withdrawal",
                payment_sum=0.0,
                currency="",
                ticker="",
                isin=self._extract_isin(attrib),
                reg_number="",
                price=0.0,
                quantity=abs(qty),
                aci=0.0,
                comment=attrib.get("comment_new") or "",
                operation_id="",
                commission=0.0,
            )
            self.stats["parsed"] += 1
            return operation
        except Exception as e:
            logger.exception("Failed to build OperationDTO for conversion: %s", e)
            self.stats["skipped_invalid"] += 1
            return None

    def _is_conversion_operation(self, attrib: Dict[str, str]) -> bool:
        """Проверяет, является ли операция конвертацией"""
        oper_type = (attrib.get("oper_type") or "").strip().lower()
        comment_new = (attrib.get("comment_new") or "").strip().lower()

        return oper_type == "перевод" and "конвертация" in comment_new

    def _extract_date(self, attrib: Dict[str, str]) -> Optional[datetime]:
        """Извлекает дату операции"""
        from src.utils import parse_datetime_from_components

        settlement_date = attrib.get("settlement_date")
        settlement_time = attrib.get("settlement_time")
        return parse_datetime_from_components(settlement_date, settlement_time)

    def _extract_isin(self, attrib: Dict[str, str]) -> str:
        """Извлекает ISIN из названия инструмента"""
        p_name = attrib.get("p_name") or ""
        return extract_isin_from_attr(p_name)


# Public API
def parse_transfers_from_xml(path_or_bytes: str | bytes) -> Tuple[List[OperationDTO], Dict[str, Any]]:
    """Публичный интерфейс для парсинга конвертаций"""
    parser = TransfersParser()
    return parser.parse(path_or_bytes)