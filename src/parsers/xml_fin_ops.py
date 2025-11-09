from __future__ import annotations
from typing import Optional, List, Dict, Any, Tuple, Union
from datetime import datetime
import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation
import re

from src.OperationDTO import OperationDTO
from src.utils import logger, _local_name
from .xml_parser_base import XmlParserBase
from .operation_classifier import OperationClassifier


class FinancialOperationsParser(XmlParserBase):
    """Парсер финансовых операций"""

    RE_ISIN = re.compile(r"\b[A-Z]{2}[A-Z0-9]{10}\b")
    RE_REG_NUMBER = re.compile(r"\b[0-9][0-9A-ZА-Я]{0,7}[-/][0-9A-ZА-Я\-\/]*\d[0-9A-ZА-Я\-\/]*\b", re.IGNORECASE)

    def __init__(self):
        self.stats = {
            "total_rows": 0,
            "parsed": 0,
            "skipped": 0,
            "example_comments": [],
            "amounts_by_mapped_type": {},
            "amounts_by_label": {},
            "total_income": Decimal("0"),
            "total_expense": Decimal("0"),
        }

    def parse(self, path_or_bytes: Union[str, bytes]) -> Tuple[List[OperationDTO], Dict[str, Any]]:
        """Основной метод парсинга"""
        try:
            if isinstance(path_or_bytes, (bytes, bytearray)):
                root = self._parse_bytes(path_or_bytes)
            else:
                root = self._parse_file(path_or_bytes)
            return self._parse_root(root)
        except Exception as e:
            logger.exception("Failed to parse financial operations: %s", e)
            return [], {"error": str(e), **self.stats}

    def _parse_bytes(self, data: bytes) -> ET.Element:
        """Парсит XML из bytes"""
        if data.startswith(b'\xef\xbb\xbf'):
            data = data.lstrip(b'\xef\xbb\xbf')
        return ET.fromstring(data)

    def _parse_file(self, file_path: str) -> ET.Element:
        """Парсит XML из файла"""
        tree = ET.parse(file_path)
        return tree.getroot()

    def _parse_root(self, root: ET.Element) -> Tuple[List[OperationDTO], Dict[str, Any]]:
        """Парсит корневой элемент"""
        ops: List[OperationDTO] = []

        report_elem = self._find_report_element(root)
        settlement_nodes = self._collect_elements_by_local_name(report_elem, "settlement_date")

        logger.info("Found %d settlement_date nodes", len(settlement_nodes))

        for settlement_node in settlement_nodes:
            settlement_date = self._parse_settlement_date(settlement_node)
            rn_nodes = self._collect_rn_nodes(settlement_node)

            for rn_node in rn_nodes:
                operation = self._process_rn_node(rn_node, settlement_date)
                if operation:
                    ops.append(operation)

        return ops, self._finalize_stats()

    def _find_report_element(self, root: ET.Element) -> ET.Element:
        """Находит элемент Report"""
        for rep in root.iter():
            if _local_name(rep.tag) == "Report":
                name = rep.attrib.get("Name") or rep.attrib.get("name")
                if name and "BrokerMoneyMove" in name:
                    return rep
                return rep  # возвращаем первый найденный Report
        return root

    def _collect_elements_by_local_name(self, root: ET.Element, local: str) -> List[ET.Element]:
        """Собирает все элементы с указанным локальным именем"""
        return [el for el in root.iter() if _local_name(el.tag) == local]

    def _collect_rn_nodes(self, settlement_node: ET.Element) -> List[ET.Element]:
        """Собирает узлы rn"""
        rn_nodes = [el for el in settlement_node.iter() if _local_name(el.tag) == "rn"]
        if not rn_nodes:
            rn_nodes = [c for c in list(settlement_node) if _local_name(c.tag) == "rn"]
        return rn_nodes

    def _parse_settlement_date(self, settlement_node: ET.Element) -> Optional[datetime]:
        """Парсит дату из settlement_date"""
        settlement_date_attr = self._safe_attr(settlement_node, "settlement_date")
        return self._parse_iso_datetime(settlement_date_attr)

    def _process_rn_node(self, rn_node: ET.Element, settlement_date: Optional[datetime]) -> Optional[OperationDTO]:
        """Обрабатывает узел rn"""
        self.stats["total_rows"] += 1

        # Извлечение данных
        oper_type, comment = self._extract_oper_type_and_comment(rn_node)
        currency, amount = self._extract_currency_and_amount(rn_node)
        reg_number = self._extract_reg_number(comment)
        isin = self._extract_isin(comment)

        # Проверка на пропуск
        if self._should_skip(oper_type, comment, amount):
            self.stats["skipped"] += 1
            return None

        # Определение типа операции
        payment_sum = self._decimal_to_float(amount)
        op_type = OperationClassifier.determine_operation_type(oper_type, comment, payment_sum)
        if op_type == "_skip_":
            self.stats["skipped"] += 1
            return None

        # Создание DTO
        operation = self._create_operation_dto(
            settlement_date, rn_node, op_type, currency, payment_sum, comment, reg_number, isin
        )

        self._update_stats(oper_type, comment, amount, op_type)
        self._collect_example_comment(comment)

        self.stats["parsed"] += 1
        return operation

    def _extract_oper_type_and_comment(self, rn_node: ET.Element) -> Tuple[str, str]:
        """Извлекает тип операции и комментарий"""
        # Поиск oper_type
        oper_type_elem = None
        for el in rn_node.iter():
            if _local_name(el.tag) == "oper_type":
                oper_type_elem = el
                break

        oper_type_val = self._safe_attr(oper_type_elem, "oper_type") if oper_type_elem else ""

        # Поиск comment
        comment_elem = None
        if oper_type_elem is not None:
            comment_elem = self._find_first_descendant_by_local_name(oper_type_elem, "comment")
        if comment_elem is None:
            comment_elem = self._find_first_descendant_by_local_name(rn_node, "comment")

        comment_text = self._safe_attr(comment_elem, "comment") or ""

        return oper_type_val, comment_text

    def _find_first_descendant_by_local_name(self, root: ET.Element, local: str) -> Optional[ET.Element]:
        """Находит первый потомок с указанным локальным именем"""
        for el in root.iter():
            if _local_name(el.tag) == local:
                return el
        return None

    def _extract_currency_and_amount(self, rn_node: ET.Element) -> Tuple[str, Optional[Decimal]]:
        """Извлекает валюту и сумму"""
        comment_elem = self._find_first_descendant_by_local_name(rn_node, "comment")
        if comment_elem is None:
            return "", None

        # Поиск в p_code элементах
        candidates = self._collect_p_code_candidates(comment_elem)
        currency = ""
        amount = None

        for c in candidates:
            cur = c.get("p_code") or c.get("currency")
            vol = c.get("volume") or c.get("volume1") or c.get("amount")
            if cur and not currency:
                currency = cur.strip()
            if vol and amount is None:
                amount = self._parse_decimal(vol.strip())

        # Fallback к textbox значениям
        if amount is None:
            textbox_values = self._extract_textbox_values(comment_elem)
            for key in ["money_volume", "all_volume", "debet_volume"]:
                if textbox_values.get(key):
                    amount = self._parse_decimal(textbox_values[key])
                    if amount is not None:
                        break

        return currency or "", amount or Decimal("0")

    def _collect_p_code_candidates(self, elem: Optional[ET.Element]) -> List[Dict[str, str]]:
        """Собирает кандидаты p_code"""
        res = []
        if elem is None:
            return res
        for p in elem.iter():
            if _local_name(p.tag) == "p_code":
                res.append(dict(p.attrib))
        return res

    def _extract_textbox_values(self, comment_elem: Optional[ET.Element]) -> Dict[str, Optional[str]]:
        """Извлекает значения из textbox элементов"""
        res = {"money_volume": None, "all_volume": None, "debet_volume": None, "acc_code": None}
        if comment_elem is None:
            return res

        textbox_mapping = {
            "Textbox83": "money_volume",
            "Textbox84": "all_volume",
            "Textbox93": "debet_volume",
            "Textbox11": "acc_code"
        }

        for node in comment_elem.iter():
            ln = _local_name(node.tag)
            if ln in textbox_mapping and res[textbox_mapping[ln]] is None:
                value = node.attrib.get(textbox_mapping[ln])
                if value and value.strip():
                    res[textbox_mapping[ln]] = value.strip()

        return res

    def _extract_reg_number(self, comment_text: Optional[str]) -> str:
        """Извлекает регистрационный номер"""
        if not comment_text:
            return ""
        m = self.RE_REG_NUMBER.search(comment_text)
        return m.group(0) if m else ""

    def _extract_isin(self, comment_text: Optional[str]) -> str:
        """Извлекает ISIN"""
        if not comment_text:
            return ""
        m = self.RE_ISIN.search(comment_text)
        return m.group(0) if m else ""

    def _should_skip(self, oper_type: str, comment: str, amount: Optional[Decimal]) -> bool:
        """Проверяет, нужно ли пропустить операцию"""
        label_source = (oper_type or "").strip() or (comment or "").strip()
        if not label_source and (amount is None or float(amount or 0) == 0.0):
            return True
        return OperationClassifier.should_skip_operation(oper_type, comment, label_source)

    def _create_operation_dto(self, settlement_date: Optional[datetime], rn_node: ET.Element,
                              op_type: str, currency: str, payment_sum: float, comment: str,
                              reg_number: str, isin: str) -> OperationDTO:
        """Создает DTO операции"""
        # Получаем last_update для даты
        last_update_attr = self._safe_attr(rn_node, "last_update")
        rn_last_update_dt = self._parse_iso_datetime(last_update_attr)

        date_field = settlement_date or rn_last_update_dt

        return OperationDTO(
            date=date_field,
            operation_type=op_type,
            payment_sum=payment_sum,
            currency=currency or "",
            ticker="",  # Не используется для финансовых операций
            isin=isin,
            reg_number=reg_number,
            price=0.0,
            quantity=0,
            aci=0.0,
            comment=comment,
            operation_id="",  # Не используется для финансовых операций
            commission=0.0,
        )

    def _update_stats(self, oper_type: str, comment: str, amount: Optional[Decimal], mapped_type: str):
        """Обновляет статистику"""
        if amount is None:
            return

        # Обновляем суммы по типам
        self.stats["amounts_by_mapped_type"][mapped_type] = (
                self.stats["amounts_by_mapped_type"].get(mapped_type, Decimal("0")) + amount
        )

        # Обновляем суммы по лейблам
        label_key = (oper_type or "").strip() or (comment.splitlines()[0].strip() if comment else "")
        if label_key:
            self.stats["amounts_by_label"][label_key] = (
                    self.stats["amounts_by_label"].get(label_key, Decimal("0")) + amount
            )

        # Обновляем доходы/расходы
        if amount > 0:
            self.stats["total_income"] += amount
        elif amount < 0:
            self.stats["total_expense"] += amount

    def _collect_example_comment(self, comment: str):
        """Собирает примеры комментариев"""
        if comment and len(self.stats["example_comments"]) < 5:
            self.stats["example_comments"].append(comment)

    def _safe_attr(self, elem: Optional[ET.Element], name: str) -> Optional[str]:
        """Безопасно извлекает атрибут"""
        if elem is None:
            return None
        v = elem.attrib.get(name) or elem.attrib.get(name.lower())
        return v.strip() if v and v.strip() else None

    def _parse_decimal(self, v: Optional[str]) -> Optional[Decimal]:
        """Парсит Decimal"""
        if v is None:
            return None
        try:
            return Decimal(v.replace(",", ".") if isinstance(v, str) else v)
        except (InvalidOperation, ValueError):
            return None

    def _parse_iso_datetime(self, v: Optional[str]) -> Optional[datetime]:
        """Парсит datetime из ISO формата"""
        if not v:
            return None
        try:
            return datetime.fromisoformat(v)
        except Exception:
            try:
                return datetime.strptime(v.split("T")[0], "%Y-%m-%d")
            except Exception:
                return None

    def _decimal_to_float(self, d: Optional[Decimal]) -> float:
        """Конвертирует Decimal в float"""
        if d is None:
            return 0.0
        try:
            return float(d)
        except Exception:
            return 0.0

    def _finalize_stats(self) -> Dict[str, Any]:
        """Форматирует итоговую статистику"""
        stats = self.stats.copy()
        stats["amounts_by_mapped_type"] = {k: self._format_decimal(v)
                                           for k, v in stats["amounts_by_mapped_type"].items()}
        stats["amounts_by_label"] = {k: self._format_decimal(v)
                                     for k, v in stats["amounts_by_label"].items()}
        stats["total_income"] = self._format_decimal(stats["total_income"])
        stats["total_expense"] = self._format_decimal(stats["total_expense"])
        return stats

    def _format_decimal(self, d: Decimal) -> str:
        """Форматирует Decimal в строку"""
        try:
            return format(d.quantize(Decimal("0.0001")), "f")
        except Exception:
            return str(d)


def parse_fin_operations_from_xml(path_or_bytes: Union[str, bytes]) -> Tuple[List[OperationDTO], Dict[str, Any]]:
    """Публичный интерфейс для парсинга финансовых операций"""
    parser = FinancialOperationsParser()
    return parser.parse(path_or_bytes)