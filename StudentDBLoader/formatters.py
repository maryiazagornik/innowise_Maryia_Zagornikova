# formatters.py
import json
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod


class Formatter(ABC):
    """Абстрактный базовый класс (интерфейс) для форматировщиков."""

    @abstractmethod
    def format(self, data: dict) -> str:
        pass


class JsonFormatter(Formatter):
    """Конкретная стратегия для вывода в JSON."""

    def format(self, data: dict) -> str:
        return json.dumps(data, indent=4, default=str)  # default=str для обработки дат/возраста


class XmlFormatter(Formatter):
    """Конкретная стратегия для вывода в XML."""

    def _dict_to_xml(self, parent_element, data):
        """Рекурсивный помощник для преобразования dict в XML."""
        if isinstance(data, dict):
            for key, value in data.items():
                element = ET.Element(parent_element, key)
                self._dict_to_xml(element, value)
        elif isinstance(data, list):
            for item in data:
                # Для списков обычно создаем элемент 'item' или по имени родителя
                item_element_name = parent_element.tag
                if item_element_name.endswith('s'):  # убираем 's' (e.g. rooms -> room)
                    item_element_name = item_element_name[:-1]
                else:
                    item_element_name = 'item'

                element = ET.Element(parent_element, item_element_name)
                self._dict_to_xml(element, item)
        else:
            parent_element.text = str(data)

    def format(self, data: dict) -> str:
        root = ET.Element("results")
        self._dict_to_xml(root, data)
        # tostring возвращает bytes, декодируем в str
        return ET.tostring(root, encoding='unicode', short_empty_elements=False)