# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3

class MotosScraperPipelineNew:
    def open_spider(self, spider):
        
        # КАЖДЫЙ ПАУК ПИШЕТ В СВОЮ БАЗУ
        # db_name = f"motos_{spider.name}.db"
        # self.connection = sqlite3.connect(db_name)

        # ВСЕ ПАУКИ ПИШУТ В ОДНУ БАЗУ
        self.connection = sqlite3.connect("motos_all_spiders_plus_fastestlaps.db")

        self.cursor = self.connection.cursor()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS motos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_id INTEGER,
                source VARCHAR(46),
                source_url VARCHAR(256),
                brand VARCHAR(46),
                model VARCHAR(46),
                year VARCHAR(4),
                category VARCHAR(46),
                origin_country VARCHAR(46),
                engine_type VARCHAR(46),
                engine_displacement_cc VARCHAR(46),
                engine_power_hp VARCHAR(46),
                engine_power_rpm VARCHAR(46),
                engine_torque_nm VARCHAR(46),
                engine_torque_rpm VARCHAR(46),
                gearbox VARCHAR(46),
                transmission_clutch VARCHAR(46),
                transmission_type VARCHAR(46),
                abs_type VARCHAR(46),
                dry_weight_kg VARCHAR(46),
                wet_weight_kg VARCHAR(46),
                fuel_capacity_l VARCHAR(46),
                top_speed_kph VARCHAR(46),
                fuel_consumption_l_per_100km VARCHAR(46)
            )
        """)
        self.connection.commit()

    def process_item(self, item, spider):
        api_id = item.get("api_id")

        self.cursor.execute("SELECT api_id FROM motos WHERE api_id = ?", (api_id,))
        exists = self.cursor.fetchone()

        if not exists:
            self.cursor.execute("""
                INSERT INTO motos (api_id, source, source_url, brand, model, year, category, origin_country, engine_type, engine_displacement_cc, engine_power_hp, engine_power_rpm, engine_torque_nm, engine_torque_rpm, gearbox, transmission_clutch, transmission_type, abs_type, dry_weight_kg, wet_weight_kg, fuel_capacity_l, top_speed_kph, fuel_consumption_l_per_100km)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                item.get("api_id"),
                item.get("source"),
                item.get("source_url"),
                item.get("brand"),
                item.get("model"),
                item.get("year"),
                item.get("category"),
                item.get("origin_country"),
                item.get("engine_type"),
                item.get("engine_displacement_cc"),
                item.get("engine_power_hp"),
                item.get("engine_power_rpm"),
                item.get("engine_torque_nm"),
                item.get("engine_torque_rpm"),
                item.get("gearbox"),
                item.get("transmission_clutch"),
                item.get("transmission_type"),
                item.get("abs_type"),
                item.get("dry_weight_kg"),
                item.get("wet_weight_kg"),
                item.get("fuel_capacity_l"),
                item.get("top_speed_kph"),
                item.get("fuel_consumption_l_per_100km"),
            ))
            self.connection.commit()
        return item

    def close_spider(self, spider):
        self.connection.commit()
        self.connection.close()