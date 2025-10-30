import re
from decimal import Decimal, InvalidOperation
from typing import Optional, Union

from loguru import logger


def extract_number(text: str, *, default: Optional[Union[int, float]] = None) -> Optional[Union[int, float]]:
    """
    Извлекает первое число из текста с поддержкой целых, дробных и отрицательных чисел.
    
    Args:
        text: Текст для поиска числа
        default: Значение по умолчанию при отсутствии числа
        
    Returns:
        Извлеченное число (int или float) или default если число не найдено
        
    Examples:
        >>> extract_number("Цена: 1,500.50 руб")
        1500.5
        >>> extract_number("Товар №-15")
        -15
        >>> extract_number("Нет числа", default=0)
        0
    """
    if not text or not isinstance(text, str):
        logger.warning(f"Некорректный входной текст: {text}")
        return default
    
    # Паттерн для чисел: целые, дробные, отрицательные, с разделителями
    patterns = [
        # Отрицательные дробные числа
        r'-?\d{1,3}(?:[ ,]\d{3})*\.\d+',
        # Отрицательные целые числа с разделителями
        r'-?\d{1,3}(?:[ ,]\d{3})+',
        # Отрицательные целые числа
        r'-?\d+',
        # Положительные дробные числа
        r'\d{1,3}(?:[ ,]\d{3})*\.\d+',
        # Положительные целые числа с разделителями
        r'\d{1,3}(?:[ ,]\d{3})+',
        # Положительные целые числа
        r'\d+'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            number_str = match.group().replace(' ', '').replace(',', '')
            try:
                # Пробуем преобразовать в int если нет дробной части
                if '.' in number_str:
                    number = float(number_str)
                    # Если число целое, возвращаем как int
                    return int(number) if number.is_integer() else number
                else:
                    return int(number_str)
            except (ValueError, TypeError) as e:
                logger.warning(f"Не удалось преобразовать '{number_str}' в число: {e}")
                continue
    
    logger.debug(f"Числа не найдены в тексте: '{text[:50]}...'")
    return default


def get_integer(text: str, *, default: int = 0) -> int:
    """
    Извлекает первое целое число из текста.
    
    Args:
        text: Текст для поиска числа
        default: Значение по умолчанию при отсутствии числа
        
    Returns:
        Целое число или default
        
    Raises:
        ValueError: Если default=None и число не найдено
        
    Examples:
        >>> get_integer("Цена: 1,500 руб")
        1500
        >>> get_integer("Скидка -15%")
        -15
    """
    result = extract_number(text, default=default)
    
    if result is None:
        raise ValueError(f"Целое число не найдено в тексте: '{text[:50]}...'")
    
    if isinstance(result, float):
        result = int(result)
    
    return result


def get_float(text: str, *, default: Optional[float] = None) -> float:
    """
    Извлекает число с плавающей точкой из текста.
    
    Args:
        text: Текст для поиска числа
        default: Значение по умолчанию при отсутствии числа
        
    Returns:
        Число float или default
        
    Raises:
        ValueError: Если default=None и число не найдено
    """
    result = extract_number(text, default=default)
    
    if result is None:
        raise ValueError(f"Число не найдено в тексте: '{text[:50]}...'")
    
    return float(result)


def is_english_text(text: str, *, allow_spaces: bool = True, allow_punctuation: bool = False) -> bool:
    """
    Проверяет, состоит ли текст только из английских символов.
    
    Args:
        text: Текст для проверки
        allow_spaces: Разрешить пробелы
        allow_punctuation: Разрешить пунктуацию (. , ! ? и т.д.)
        
    Returns:
        True если текст состоит только из разрешенных английских символов
        
    Examples:
        >>> is_english_text("Hello")
        True
        >>> is_english_text("Hello World", allow_spaces=True)
        True
        >>> is_english_text("Hello, World!", allow_punctuation=True)
        True
    """
    if not text or not isinstance(text, str):
        return False
    
    # Базовый паттерн для английских букв
    base_pattern = r'[A-Za-z]'
    
    # Добавляем разрешенные символы
    allowed_chars = ''
    if allow_spaces:
        allowed_chars += r' '
    if allow_punctuation:
        allowed_chars += r'.,!?;:-'
    
    if allowed_chars:
        pattern = f'^[{re.escape(allowed_chars)}A-Za-z]+$'
    else:
        pattern = '^[A-Za-z]+$'
    
    return bool(re.match(pattern, text))


def contains_english_words(text: str, min_word_length: int = 2) -> bool:
    """
    Проверяет, содержит ли текст английские слова.
    
    Args:
        text: Текст для проверки
        min_word_length: Минимальная длина слова для учета
        
    Returns:
        True если найден хотя бы один английский слово
        
    Examples:
        >>> contains_english_words("Product iPhone 15")
        True
        >>> contains_english_words("Товар №123")
        False
    """
    if not text:
        return False
    
    # Ищем слова, состоящие только из английских букв
    english_words = re.findall(r'\b[A-Za-z]{%d,}\b' % min_word_length, text)
    return len(english_words) > 0


def extract_english_words(text: str, min_word_length: int = 2) -> list[str]:
    """
    Извлекает все английские слова из текста.
    
    Args:
        text: Текст для анализа
        min_word_length: Минимальная длина слова
        
    Returns:
        Список найденных английских слов
    """
    if not text:
        return []
    
    return re.findall(r'\b[A-Za-z]{%d,}\b' % min_word_length, text)


# Aliases для обратной совместимости
def get_num(text: str) -> int:
    """
    Устаревшая функция. Используйте get_integer вместо этого.
    
    Args:
        text: Текст для поиска числа
        
    Returns:
        Первое найденное целое число или 0 если не найдено
    """
    return get_integer(text, default=0)


def is_english(text: str) -> bool:
    """
    Устаревшая функция. Используйте is_english_text вместо этого.
    
    Args:
        text: Текст для проверки
        
    Returns:
        True если текст состоит только из английских букв
    """
    return is_english_text(text, allow_spaces=False, allow_punctuation=False)