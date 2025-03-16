import asyncio
import aiohttp
import pandas as pd
import numpy as np
import math
import json
from datetime import datetime, timedelta
from aiohttp import ClientSession
import nest_asyncio
nest_asyncio.apply()


# Функция минимального времени на пересадку (рассчитывается в зависимости от типов транспорта)
def get_required_wait(transport1, transport2):
    if transport1 == "train" and transport2 == "train":
        return timedelta(hours=2)
    elif transport1 == "train" and transport2 == "plane":
        return timedelta(hours=3)
    elif transport1 == "train" and transport2 == "bus":
        return timedelta(hours=2)
    elif transport1 == "plane" and transport2 == "train":
        return timedelta(hours=4)
    elif transport1 == "plane" and transport2 == "plane":
        return timedelta(hours=4)
    elif transport1 == "plane" and transport2 == "bus":
        return timedelta(hours=2)
    elif transport1 == "bus" and transport2 == "train":
        return timedelta(hours=2)
    elif transport1 == "bus" and transport2 == "plane":
        return timedelta(hours=4)
    elif transport1 == "bus" and transport2 == "bus":
        return timedelta(0)
    else:
        return timedelta(hours=3)

# Конфигурация API и URL
API_KEY = "08daef05-7d15-43e0-8ad6-4b39d42e1ac0"
SEARCH_URL = "https://api.rasp.yandex.net/v3.0/search/"
STATIONS_URL = "https://api.rasp.yandex.net/v3.0/stations_list/"

# Глобальные объекты для кэширования
_CACHED_CITY_CODES = None
routes_cache = {}

# Список городов для пересадки (если прямой рейс не найден)
candidate_transfer_list = [
    "Москва", "Санкт-Петербург", "Нижний Новгород", "Воронеж", "Петрозаводск", "Мурманск",
    "Архангельск", "Казань", "Самара", "Уфа", "Саратов", "Ростов-на-Дону", "Краснодар",
    "Минеральные Воды", "Волгоград", "Екатеринбург", "Челябинск", "Пермь", "Тюмень",
    "Новосибирск", "Омск", "Красноярск", "Томск", "Иркутск", "Улан-Удэ", "Хабаровск",
    "Владивосток", "Якутск", "Чита", "Магадан"
]

# ================= Асинхронные функции для получения данных =================

async def fetch_json(session: ClientSession, url: str, params: dict):
    try:
        async with session.get(url, params=params, timeout=20) as response:
            if response.status == 404:
                return {}
            response.raise_for_status()
            return await response.json(content_type=None)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        return {}
    except Exception:
        return {}

async def get_city_codes_async(session: ClientSession):
    global _CACHED_CITY_CODES
    if _CACHED_CITY_CODES is not None:
        return _CACHED_CITY_CODES
    params = {"apikey": API_KEY, "format": "json", "lang": "ru_RU"}
    data = await fetch_json(session, STATIONS_URL, params)
    city_codes = []
    for country in data.get("countries", []):
        for region in country.get("regions", []):
            for settlement in region.get("settlements", []):
                city_name = settlement.get("title", "Неизвестно")
                city_code = settlement.get("codes", {}).get("yandex_code", "Нет данных")
                coords_data = settlement.get("coords")
                coords = None
                if coords_data and isinstance(coords_data, dict):
                    try:
                        lat = float(coords_data.get("lat"))
                        lon = float(coords_data.get("lon"))
                        coords = (lat, lon)
                    except Exception:
                        coords = None
                if isinstance(city_code, str) and city_code.startswith("c"):
                    city_codes.append({
                        "Город": city_name,
                        "Yandex-код": city_code,
                        "coords": coords
                    })
    _CACHED_CITY_CODES = city_codes
    return city_codes

# ================= Функции для работы с городами =================

def city_codes_to_dataframe(city_codes):
    if not city_codes:
        return None
    return pd.DataFrame(city_codes)

def find_city_by_name(city_codes_df, city_name):
    filtered = city_codes_df[city_codes_df["Город"].str.lower() == city_name.lower()]
    if filtered.empty:
        return None
    return filtered

def get_city_code_by_name(city_codes_df, city_name):
    df_filtered = find_city_by_name(city_codes_df, city_name)
    if df_filtered is None or df_filtered.empty:
        return None
    return df_filtered.iloc[0]["Yandex-код"]

def get_all_city_codes_dict(city_codes):
    return {city["Город"]: {"code": city["Yandex-код"], "coords": city["coords"]}
            for city in city_codes if city["Yandex-код"].startswith("c")}


# ================= Функция получения маршрутов между станциями =================

async def async_get_routes(session, from_code, to_code, date, min_departure_time=None):
    params = {
        "apikey": API_KEY,
        "format": "json",
        "from": from_code,
        "to": to_code,
        "date": date,
        "lang": "ru_RU"
    }
    if min_departure_time:
        params["min_dep_time"] = min_departure_time.strftime("%Y-%m-%dT%H:%M")
    try:
        async with session.get(SEARCH_URL, params=params, timeout=20) as response:
            if response.status == 404:
                data = {}
            else:
                response.raise_for_status()
                data = await response.json(content_type=None)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        data = {}
    except Exception:
        data = {}
    routes = []
    for seg in data.get("segments", []):
        dep_str = seg.get("departure")
        arr_str = seg.get("arrival")
        if dep_str and arr_str:
            try:
                dep_time = datetime.fromisoformat(dep_str).replace(tzinfo=None)
                arr_time = datetime.fromisoformat(arr_str).replace(tzinfo=None)
            except Exception:
                continue
            if (arr_time - dep_time).total_seconds() <= 0:
                continue
            duration = (arr_time - dep_time).total_seconds() / 60.0
            routes.append({
                "departure": dep_time,
                "arrival": arr_time,
                "total_duration": duration,
                "raw": seg
            })
    return routes

# ================= Функция поиска маршрутов для одного этапа =================

async def async_find_best_routes(session, city_codes_df, candidate_transfer_list,
                                 departure_city, arrival_city, departure_date,
                                 top_n=1, min_dep_time=None):
    dep_code = get_city_code_by_name(city_codes_df, departure_city)
    arr_code = get_city_code_by_name(city_codes_df, arrival_city)
    if not dep_code or not arr_code:
        return []
    
    params = {
        "apikey": API_KEY,
        "format": "json",
        "from": dep_code,
        "to": arr_code,
        "date": departure_date,
        "lang": "ru_RU"
    }
    if min_dep_time:
        params["min_dep_time"] = min_dep_time.strftime("%Y-%m-%dT%H:%M")
    try:
        async with session.get(SEARCH_URL, params=params, timeout=20) as response:
            if response.status == 404:
                direct_data = {}
            else:
                response.raise_for_status()
                direct_data = await response.json(content_type=None)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        direct_data = {}
    except Exception:
        direct_data = {}
    direct_routes = []
    for seg in direct_data.get("segments", []):
        dep_str = seg.get("departure")
        arr_str = seg.get("arrival")
        if dep_str and arr_str:
            try:
                dep_time = datetime.fromisoformat(dep_str).replace(tzinfo=None)
                arr_time = datetime.fromisoformat(arr_str).replace(tzinfo=None)
            except Exception:
                continue
            if (arr_time - dep_time).total_seconds() <= 0:
                continue
            if min_dep_time and dep_time < min_dep_time:
                continue
            duration = (arr_time - dep_time).total_seconds() / 60.0
            direct_routes.append({
                "route_type": "direct",
                "total_duration": duration,
                "departure": dep_time,
                "arrival": arr_time,
                "raw": seg
            })
    if direct_routes:
        direct_routes.sort(key=lambda x: x["total_duration"])
        return direct_routes[:top_n]
    
    # Если прямого рейса нет, ищем варианты с пересадкой.
    candidate_transfer_dict = get_all_city_codes_dict(city_codes_df.to_dict(orient="records"))
    candidate_transfer_dict = {city: info for city, info in candidate_transfer_dict.items() if city in candidate_transfer_list}
    connecting_routes = []
    
    async def process_transfer_city(transfer_city):
        transfer_info = candidate_transfer_dict.get(transfer_city)
        if not transfer_info:
            return []
        transfer_code = transfer_info["code"]
        if transfer_code in [dep_code, arr_code]:
            return []
        results = []
        seg1_list = await async_get_routes(session, dep_code, transfer_code, departure_date, min_dep_time)
        async def process_seg1(route1):
            seg_results = []
            if "arrival" not in route1:
                return seg_results
            arrival_time_transfer = route1["arrival"]
            second_date = arrival_time_transfer.strftime("%Y-%m-%d")
            seg2_list = await async_get_routes(session, transfer_code, arr_code, second_date, min_departure_time=arrival_time_transfer)
            for route2 in seg2_list:
                if "arrival" not in route2:
                    continue
                transport1 = route1["raw"].get("thread", {}).get("transport_type", "").lower()
                transport2 = route2["raw"].get("thread", {}).get("transport_type", "").lower()
                wait_time = route2["departure"] - arrival_time_transfer
                required_wait = get_required_wait(transport1, transport2)
                if wait_time < required_wait:
                    continue
                total_duration = (route2["arrival"] - route1["departure"]).total_seconds() / 60.0
                seg_results.append({
                    "route_type": "connecting",
                    "total_duration": total_duration,
                    "departure": route1["departure"],
                    "arrival": route2["arrival"],
                    "raw": {"seg1": route1, "seg2": route2},
                    "first_leg": route1,
                    "second_leg": route2,
                    "transfer_city": transfer_city
                })
            return seg_results
        
        tasks_seg1 = [process_seg1(route1) for route1 in seg1_list]
        seg1_results = await asyncio.gather(*tasks_seg1)
        for sublist in seg1_results:
            results.extend(sublist)
        return results

    tasks_transfer = [process_transfer_city(city) for city in candidate_transfer_list]
    transfer_results = await asyncio.gather(*tasks_transfer)
    for sublist in transfer_results:
        connecting_routes.extend(sublist)
    
    if connecting_routes:
        connecting_routes.sort(key=lambda x: x["total_duration"])
        return connecting_routes[:top_n]
    return []


# ================= Функция поиска комбинированного маршрута =================
# Если задан arrival_city2, arrival_city1 рассматривается как обязательная точка пересадки.
# Для каждого кандидата первого этапа (departure_city → arrival_city1) ищутся варианты второго этапа (arrival_city1 → arrival_city2).
# После получения кандидатов проверяется, что время отправления второго этапа не меньше, чем время прибытия первого этапа плюс требуемый интервал,
# рассчитываемый функцией get_required_wait с учетом типов транспорта.
async def async_find_combined_routes(session, city_codes_df, candidate_transfer_list,
                                     departure_city, arrival_city1, arrival_city2,
                                     departure_date, top_n=1):
    leg1_routes = await async_find_best_routes(session, city_codes_df, candidate_transfer_list,
                                               departure_city, arrival_city1, departure_date, top_n=3)
    if not leg1_routes:
        return []
    combined_routes = []
    for leg1 in leg1_routes:
        candidate_leg2 = []
        # Сначала ищем варианты второго этапа на ту же дату, что и прибытие первого этапа
        departure_date_leg2 = leg1["arrival"].strftime("%Y-%m-%d")
        leg2_candidates = await async_find_best_routes(session, city_codes_df, candidate_transfer_list,
                                                       arrival_city1, arrival_city2, departure_date_leg2, top_n=3)
        for candidate in leg2_candidates:
            transport1 = leg1["raw"].get("thread", {}).get("transport_type", "").lower()
            transport2 = candidate["raw"].get("thread", {}).get("transport_type", "").lower()
            required_wait = get_required_wait(transport1, transport2)
            if candidate["departure"] >= leg1["arrival"] + required_wait:
                candidate_leg2.append(candidate)
        # Если на ту же дату нет подходящих вариантов – пробуем следующий день
        if not candidate_leg2:
            next_day = leg1["arrival"] + timedelta(days=1)
            departure_date_leg2 = next_day.strftime("%Y-%m-%d")
            leg2_candidates = await async_find_best_routes(session, city_codes_df, candidate_transfer_list,
                                                           arrival_city1, arrival_city2, departure_date_leg2, top_n=3)
            for candidate in leg2_candidates:
                transport1 = leg1["raw"].get("thread", {}).get("transport_type", "").lower()
                transport2 = candidate["raw"].get("thread", {}).get("transport_type", "").lower()
                required_wait = get_required_wait(transport1, transport2)
                if candidate["departure"] >= leg1["arrival"] + required_wait:
                    candidate_leg2.append(candidate)
        # Для всех удовлетворяющих условиям вариантов второго этапа объединяем этапы
        for leg2 in candidate_leg2:
            combined_total_duration = (leg2["arrival"] - leg1["departure"]).total_seconds() / 60.0
            combined_routes.append({
                "route_type": "combined",
                "total_duration": combined_total_duration,
                "first_leg": leg1,
                "second_leg": leg2,
                "departure": leg1["departure"],
                "arrival": leg2["arrival"]
            })
    combined_routes.sort(key=lambda x: x["total_duration"])
    return combined_routes[:top_n]

# ================= Функции форматирования маршрутов =================

def format_route(candidate, departure_city, arrival_city):
    if candidate["route_type"] == "direct":
        thread = candidate["raw"].get("thread", {}) if isinstance(candidate.get("raw"), dict) else {}
        number = thread.get("number", "N/A")
        transport = thread.get("transport_type", "N/A")
        dep_station = candidate["raw"].get("from", {}).get("title", departure_city) if isinstance(candidate.get("raw"), dict) else departure_city
        arr_station = candidate["raw"].get("to", {}).get("title", arrival_city) if isinstance(candidate.get("raw"), dict) else arrival_city
        route_str = f"{dep_station} -> {arr_station}"
        duration = candidate["total_duration"]
        dep_time = candidate["departure"].strftime("%Y-%m-%d %H:%M")
        arr_time = candidate["arrival"].strftime("%Y-%m-%d %H:%M")
        return (f"Номер рейса: {number}\nТранспорт: {transport}\nМаршрут: {route_str}\n"
                f"Время в пути: {duration:.0f} мин\nОтправление: {dep_time}\nПрибытие: {arr_time}\n")
    elif candidate["route_type"] in ("connecting", "combined"):
        seg1 = candidate.get("first_leg", {})
        seg2 = candidate.get("second_leg", {})
        thread1 = seg1.get("raw", {}).get("thread", {}) if isinstance(seg1.get("raw"), dict) else {}
        thread2 = seg2.get("raw", {}).get("thread", {}) if isinstance(seg2.get("raw"), dict) else {}
        number = f"{thread1.get('number', 'N/A')} / {thread2.get('number', 'N/A')}"
        transport = f"{thread1.get('transport_type', 'N/A')} / {thread2.get('transport_type', 'N/A')}"
        dep_station = seg1.get("raw", {}).get("from", {}).get("title", departure_city) if isinstance(seg1.get("raw"), dict) else departure_city
        transfer_station = candidate.get("transfer_city", "N/A")
        arr_station = seg2.get("raw", {}).get("to", {}).get("title", arrival_city) if isinstance(seg2.get("raw"), dict) else arrival_city
        route_str = f"{dep_station} -> {transfer_station} -> {arr_station}"
        duration = candidate["total_duration"]
        dep_time = seg1["departure"].strftime("%Y-%m-%d %H:%M")
        arr_time = seg2["arrival"].strftime("%Y-%m-%d %H:%M")
        return (f"Номер рейса: {number}\nТранспорт: {transport}\nМаршрут: {route_str}\n"
                f"Место пересадки: {transfer_station}\nВремя в пути: {duration:.0f} мин\n"
                f"Отправление: {dep_time}\nПрибытие: {arr_time}\n")

def format_complex_route(candidate, departure_city, transfer_city, arrival_city):
    leg1_info = format_route(candidate["first_leg"], departure_city, transfer_city)
    leg2_info = format_route(candidate["second_leg"], transfer_city, arrival_city)
    total = candidate.get("total_duration", 0)
    return (f"Этап 1 (от {departure_city} до {transfer_city}):\n{leg1_info}\n"
            f"Этап 2 (от {transfer_city} до {arrival_city}):\n{leg2_info}\n"
            f"Общее время в пути: {total:.0f} мин\n")

# ================= Основная асинхронная функция =================

async def main_async():
    departure_date = "2025-04-01"
    departure_city = "Ростов-на-Дону"   # Город отправления (обязательный параметр)
    arrival_city1 = "Выборг"   # Первый город прибытия (обязательная точка пересадки)
    arrival_city2 = "Тверь"       # Второй город прибытия
    async with ClientSession() as session:
        if arrival_city2.strip():
            combined_routes = await async_find_combined_routes(
                session, city_codes_df, candidate_transfer_list,
                departure_city, arrival_city1, arrival_city2, departure_date, top_n=1
            )
            if not combined_routes:
                print(f"Скомбинированный маршрут из {departure_city} через {arrival_city1} в {arrival_city2} не найден.")
                return
            best_route = combined_routes[0]
            print("Найден комбинированный маршрут:")
            print(format_complex_route(best_route, departure_city, arrival_city1, arrival_city2))
        else:
            best_routes = await async_find_best_routes(
                session, city_codes_df, candidate_transfer_list,
                departure_city, arrival_city1, departure_date, top_n=3
            )
            if not best_routes:
                print("Маршруты не найдены.")
                return
            for i, candidate in enumerate(best_routes, start=1):
                print(f"Рейс {i}:")
                print(format_route(candidate, departure_city, arrival_city1))
                print("-" * 40)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    async def prepare():
        global city_codes, city_codes_df
        async with ClientSession() as session:
            city_codes = await get_city_codes_async(session)
        city_codes_df = city_codes_to_dataframe(city_codes)
    loop.run_until_complete(prepare())
    loop.run_until_complete(main_async())
