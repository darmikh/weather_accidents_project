import re

def normalize_city_name(name):
    """
    Нормализует название города для поиска в API ГИБДД и других системах
    """
    if not name:
        return ""
    
    name = str(name)
    
    # Убираем примечания в квадратных скобках (из Wiki)
    name = re.sub(r'\[.*?\]', '', name)
    
    # Убираем примечание "не призн." (для городов Крыма)
    name = re.sub(r'не призн\.', '', name)
    
    # Убираем префикс "г." 
    name = re.sub(r"^г\.?\s*", "", name)
    
    # Убираем скобки с содержимым
    name = re.sub(r"\s*\([^)]*\)", "", name)
    
    # Оставляем только буквы, цифры, пробелы и дефисы
    name = re.sub(r"[^\w\s-]", "", name)
    
    return name.strip()