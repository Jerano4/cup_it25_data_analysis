# cup_it25_data_analysis
Алгоритм подбора маршрута для сервиса поиска рейсов между городами

Для работы алгоритма необходимы библиотеки Python:
  asyncio
  aiohttp
  pandas
  numpy
  math
  json
  datetime (datetime, timedelta)
  nest_asyncio

Обязательные параметры для ввода:
  departure_date - дата отправления (строка 379)
  departure_city - город отправления (строка 380)
  arrival_city1 - город прибытия, если не задан параметр arrival_city2, и город остановки, если задан параметр arrival_city2 (строка 381)

Дополнительные параметры:
  arrival_city2 - конечный город прибытия, если необходимо построить сложный маршрут (строка 382)
