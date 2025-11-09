from __future__ import annotations
from typing import List, Tuple, Dict, Any, Optional
import xml.etree.ElementTree as ET
import re
from datetime import datetime

from src.OperationDTO import OperationDTO
from src.utils import logger, to_float_safe, to_int_safe, _local_name, _normalize_attrib, extract_isin_from_attr
from .xml_parser_base import XmlParserBase


class TradesParser(XmlParserBase):
    """Парсер сделок из раздела Trades"""

    def __init__(self):
        self.stats = {
            "total_rows": 0,
            "parsed": 0,
            "skipped_no_date": 0,
            "skipped_no_qty": 0,
            "skipped_invalid": 0,
            "total_commission": 0.0,
        }

    def parse(self, path_or_bytes: str | bytes) -> Tuple[List[OperationDTO], Dict[str, Any]]:
        """Основной метод парсинга сделок"""
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

            logger.info("Parsed %s trades (checked %s rows). total_commission=%s",
                        self.stats["parsed"], self.stats["total_rows"], self.stats["total_commission"])
            return ops, self.stats.copy()

        except Exception as e:
            logger.exception("XML trades parsing failed: %s", e)
            return ops, {"error": str(e), **self.stats}
        finally:
            self.cleanup_tmp_file(source, cleanup_needed)

    def _process_details_element(self, elem: ET.Element) -> Optional[OperationDTO]:
        """Обрабатывает элемент details"""
        self.stats["total_rows"] += 1
        attrib = _normalize_attrib(dict(elem.attrib))

        # Проверка количества
        qty = self._extract_quantity(attrib)
        if qty == 0:
            self.stats["skipped_no_qty"] += 1
            return None

        # Извлечение даты
        date_val = self._extract_date(attrib)
        if not date_val:
            self.stats["skipped_no_date"] += 1
            return None

        # Извлечение основных данных
        price, total, nkd, currency = self._extract_trade_data(attrib)
        isin, ticker, trade_no = self._extract_instrument_data(attrib)
        commission = self._extract_commission(attrib)

        # Создание DTO
        try:
            operation = OperationDTO(
                date=date_val,
                operation_type="buy" if qty > 0 else "sale",
                payment_sum=total,
                currency=currency,
                ticker=ticker,
                isin=isin,
                reg_number="",
                price=price,
                quantity=abs(qty),
                aci=nkd,
                comment=self._extract_comment(attrib),
                operation_id=trade_no,
                commission=commission,
            )
            self.stats["parsed"] += 1
            self.stats["total_commission"] += float(commission or 0.0)
            return operation
        except Exception as e:
            logger.exception("Failed to build OperationDTO: %s", e)
            self.stats["skipped_invalid"] += 1
            return None

    def _extract_quantity(self, attrib: Dict[str, str]) -> int:
        """Извлекает количество"""
        return to_int_safe(
            attrib.get("qty") or attrib.get("quantity") or
            attrib.get("textbox14") or attrib.get("qty ") or "0"
        )

    def _extract_date(self, attrib: Dict[str, str]) -> Optional[datetime]:
        """Извлекает дату сделки"""
        date_keys = ("db_time", "dbtime", "settlement_time", "save_settlement_date", "save_depo_settlement_date")
        for key in date_keys:
            if attrib.get(key):
                date_val = self.parse_datetime_from_text(attrib.get(key))
                if date_val:
                    return date_val
        return None

    def _extract_trade_data(self, attrib: Dict[str, str]) -> Tuple[float, float, float, str]:
        """Извлекает данные о сделке: цена, сумма, НКД, валюта"""
        price = to_float_safe(attrib.get("price") or attrib.get("textbox25") or attrib.get("price "))
        total = to_float_safe(
            attrib.get("summ_trade") or attrib.get("summtrade") or
            attrib.get("summ_trade".lower()) or attrib.get("summ_trade")
        )
        nkd = to_float_safe(attrib.get("summ_nkd") or attrib.get("summnkd") or attrib.get("summ_nkd".lower()))

        currency = (attrib.get("curr_calc") or attrib.get("curr") or attrib.get("textbox14") or "").strip()
        if currency.upper() in ("RUR", "РУБ", "РУБЛЬ"):
            currency = "RUB"

        return price, total, nkd, currency

    def _extract_instrument_data(self, attrib: Dict[str, str]) -> Tuple[str, str, str]:
        """Извлекает данные об инструменте: ISIN, тикер, номер сделки"""
        isin = extract_isin_from_attr(attrib.get("isin_reg") or attrib.get("isin1") or attrib.get("isin"))
        p_name = attrib.get("p_name") or attrib.get("pname") or attrib.get("active_name") or ""
        ticker = self._extract_ticker_from_name(p_name)

        trade_no_raw = attrib.get("trade_no") or attrib.get("tradeno") or attrib.get("trade") or ""
        trade_no = self._extract_first_trade_no(trade_no_raw)

        return isin, ticker, trade_no

    def _extract_ticker_from_name(self, name: Optional[str]) -> str:
        """Извлекает тикер из названия инструмента"""
        if not name:
            return ""
        tok = str(name).strip().split()[0]
        return tok if re.fullmatch(r"[A-Za-z0-9\-\.]{1,8}", tok) else ""

    def _extract_first_trade_no(self, trade_no_raw: str) -> str:
        """Извлекает первый номер сделки"""
        from src.utils import extract_first_value
        return extract_first_value(trade_no_raw)

    def _extract_commission(self, attrib: Dict[str, str]) -> float:
        """Извлекает комиссию"""
        return to_float_safe(attrib.get("bank_tax") or attrib.get("banktax") or attrib.get("bank_tax"))

    def _extract_comment(self, attrib: Dict[str, str]) -> str:
        """Извлекает комментарий"""
        return str(attrib.get("place_name") or attrib.get("place") or "")


# Public API
def parse_trades_from_xml(path_or_bytes: str | bytes) -> Tuple[List[OperationDTO], Dict[str, Any]]:
    """Публичный интерфейс для парсинга сделок"""
    parser = TradesParser()
    return parser.parse(path_or_bytes)