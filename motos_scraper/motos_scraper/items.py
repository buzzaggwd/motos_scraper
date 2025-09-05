# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class MotosScraperItem(scrapy.Item):
    api_id = scrapy.Field()
    source = scrapy.Field() # сайт
    source_url = scrapy.Field() # ссылка на мотоцикл
    brand = scrapy.Field() # марка
    model = scrapy.Field() # модель
    year = scrapy.Field() # год выпуска
    category = scrapy.Field() # категория мотоцикла
    origin_country = scrapy.Field() # страна
    engine_type = scrapy.Field() # тип двигателя
    engine_displacement_cc = scrapy.Field() # объем двигателя, см3
    engine_power_hp = scrapy.Field() # мощность, лошадиных сил
    engine_power_rpm = scrapy.Field() # обороты макс мощности
    engine_torque_nm = scrapy.Field() # крутящий момент, Нм
    engine_torque_rpm = scrapy.Field() # обороты макс крутящего момента
    # engine_fuel_system = scrapy.Field() # система подачи топлива (Injection, Carburetor)
    # engine_cooling = scrapy.Field() # тип охлаждения
    gearbox = scrapy.Field() # тип коробки передач
    transmission_clutch = scrapy.Field() # тип сцепления
    transmission_type = scrapy.Field() # тип привода (Chain, Belt, Shaft)
    # frame_type = scrapy.Field() # тип рамы
    # front_suspension = scrapy.Field() # передняя подвеска
    # rear_suspension = scrapy.Field() # задняя подвеска
    # # front_brake_type = scrapy.Field() # передний тормоз (Single disc, Double disc)
    # rear_brake_type = scrapy.Field() # задний тормоз
    abs_type = scrapy.Field() # тип АБС
    # front_tire_size = scrapy.Field() # размер передней шины
    # rear_tire_size = scrapy.Field() # размер задней шины
    dry_weight_kg = scrapy.Field() # сухой вес, кг
    wet_weight_kg = scrapy.Field() # снаряженный вес, кг
    # seat_height_mm = scrapy.Field() # высота седла, мм
    # wheelbase_mm = scrapy.Field() # колесная база, мм
    fuel_capacity_l = scrapy.Field() # емкость бака, литры
    top_speed_kph = scrapy.Field() # макс. скорость, км/ч
    # acceleration_0_to_100_kph = scrapy.Field() # разгон 0-100 км/ч, сек
    fuel_consumption_l_per_100km = scrapy.Field() # расход топлива, л/100км