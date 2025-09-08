# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
)
from motos_scraper.items import MotosScraperItem
from sqlalchemy.orm import Session
import logging
import sqlite3

logger = logging.getLogger(__name__)


Base = declarative_base()


class MotosScraperTable(Base):
    __tablename__ = "models"

    id = Column(Integer, primary_key=True, autoincrement=True)
    api_id = Column(Integer)
    source = Column(String)
    source_url = Column(String)
    brand = Column(String)
    model = Column(String)
    year = Column(String)
    category = Column(String)
    origin_country = Column(String)
    engine_type = Column(String)
    engine_displacement_cc = Column(String)
    engine_power_hp = Column(String)
    engine_power_rpm = Column(String)
    engine_torque_nm = Column(String)
    engine_torque_rpm = Column(String)
    # engine_fuel_system = Column(String)
    # engine_cooling = Column(String)
    gearbox = Column(String)
    transmission_clutch = Column(String)
    transmission_type = Column(String)
    # frame_type = Column(String)
    # front_suspension = Column(String)
    # rear_suspension = Column(String)
    # # front_brake_type = Column(String)
    # rear_brake_type = Column(String)
    abs_type = Column(String)
    # front_tire_size = Column(String)
    # rear_tire_size = Column(String)
    dry_weight_kg = Column(String)
    wet_weight_kg = Column(String)
    # seat_height_mm = Column(String)
    # wheelbase_mm = Column(String)
    fuel_capacity_l = Column(String)
    top_speed_kph = Column(String)
    # acceleration_0_to_100_kph = Column(String)
    fuel_consumption_l_per_100km = Column(String)


class MotosScraperPipeline:
    def __init__(self):
        basename = "motos_fastestlaps1.sqlite"
        self.engine = create_engine(f"sqlite:///{basename}", echo=True)
        Base.metadata.create_all(self.engine)
        self.counter = 0

    def open_spider(self, spider):
        self.session = Session(bind=self.engine)

    def process_item(self, item, spider):
        if isinstance(item, MotosScraperItem):
            try:
                data_dict = dict(item)

                existing = (
                    self.session.query(MotosScraperTable)
                    .filter(
                        MotosScraperTable.api_id == data_dict.get("api_id"),
                        # MotosScraperTable.brand == data_dict.get("brand"),
                        # MotosScraperTable.model == data_dict.get("model"),
                        # MotosScraperTable.year == data_dict.get("year"),
                    )
                    .first()
                )

                if not existing:
                    db_item = MotosScraperTable(
                        api_id=data_dict.get("api_id"),
                        source=data_dict.get("source"),
                        source_url=data_dict.get("source_url"),
                        brand=data_dict.get("brand"),
                        model=data_dict.get("model"),
                        year=data_dict.get("year"),
                        category=data_dict.get("category"),
                        origin_country=data_dict.get("origin_country"),
                        engine_type=data_dict.get("engine_type"),
                        engine_displacement_cc=data_dict.get("engine_displacement_cc"),
                        engine_power_hp=data_dict.get("engine_power_hp"),
                        engine_power_rpm=data_dict.get("engine_power_rpm"),
                        engine_torque_nm=data_dict.get("engine_torque_nm"),
                        engine_torque_rpm=data_dict.get("engine_torque_rpm"),
                        gearbox=data_dict.get("gearbox"),
                        transmission_clutch=data_dict.get("transmission_clutch"),
                        transmission_type=data_dict.get("transmission_type"),
                        abs_type=data_dict.get("abs_type"),
                        dry_weight_kg=data_dict.get("dry_weight_kg"),
                        wet_weight_kg=data_dict.get("wet_weight_kg"),
                        fuel_capacity_l=data_dict.get("fuel_capacity_l"),
                        top_speed_kph=data_dict.get("top_speed_kph"),
                        fuel_consumption_l_per_100km=data_dict.get(
                            "fuel_consumption_l_per_100km"
                        ),
                    )

                    self.session.add(db_item)
                    self.counter += 1
                    # self.session.flush()
                    if self.counter % 1000 == 0:
                        self.session.commit()
                        self.session.begin()

                    spider.logger.info(
                        f"[PIPELINE ДОБАВЛЕН]: {data_dict.get('brand')} {data_dict.get('model')}"
                    )
                else:
                    spider.logger.info(
                        f"[PIPELINE ПРОПУЩЕН] (уже есть): {data_dict.get('brand')} {data_dict.get('model')}"
                    )

            except Exception as e:
                spider.logger.error(
                    f"[PIPELINE ОШИБКА] при обработке {data_dict.get('model')}: {e}"
                )

        return item

    def close_spider(self, spider):
        try:
            self.session.commit()
            spider.logger.info("[PIPELINE] Коммит в БД выполнен")
        except Exception as e:
            spider.logger.error(f"[PIPELINE] Ошибка при коммите: {e}")
            self.session.rollback()
        finally:
            self.session.close()




class MotosScraperPipelineNew:
    def open_spider(self, spider):
        self.connection = sqlite3.connect("motos_fastestlaps3.db")
        self.cursor = self.connection.cursor()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS motos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_id INTEGER,
                source VARCHAR(46),
                source_url VARCHAR(46),
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

    def close_spider(self, spider):
        self.connection.commit()
        self.connection.close()

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
