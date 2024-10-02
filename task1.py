from typing import OrderedDict
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy import String, Integer
import csv


Base = declarative_base()
    
class Player(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    nickname: Mapped[str] = mapped_column(String(20))
    first_visit: Mapped[datetime] = mapped_column(DateTime(timezone=True)
    last_visit: Mapped[datetime] = mapped_column(DateTime(timezone=True) # для подсчета только одного посещения в день
    days: Mapped[int] = mapped_column(Integer) # количество дней посещения
    days_in_row: Mapped[int] = mapped_column(Integer) # количество дней посещения подряд


class Boost(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    description: Mapped[str] # описания бонусов и условия получения
    condition_day: Mapped[int] # условие получения - всего дней (необязательно)
    condition_day_in_row: Mapped[int] # условие получения - дней подряд (необязательно)

# так как у игроков могут быть несколько бонусов и бонусы могут пренадлежать разным игрокам, то необходима будет 3я модель промежуточная Player_Boost

    #2.1

    """
    Допустим мы присваеваем игроку приз прямо после записи в таблицу PlayerLevel
    тогда у нас есть данные: player_id, level_id, is_completed, score
    """
    db_resulst = db.execute(select(Level).where(Level.id == level_id))
    level = db_result.scalars().first()
    if score < level.order:
        return
    db_result = db.execute(select(LevelPrize).where(LevelPrize.level == level_id))
    levelPrize = db_result.scalars().first()
    # Для присвоения приза требуется модель Player_Prize, допустим она есть 
    player_prize = Player_price(player_id=player_id, prize_id=levelPrize.prize_id)
    db.add(player_prize)
    db.commit()
    

    #2.2

def get_data_in_batches(db: Session, batch_size: int = 1000):
    offset = 0
    while True:
        # Запрашиваем порциями данные
        results = db.query(
            Player.id.label("player_id"),
            Level.title.label("level_title"),
            PlayerLevel.is_completed,
            Prize.title.label("prize_title")
        ).join(PlayerLevel, Player.id == PlayerLevel.player_id) \
         .join(Level, PlayerLevel.level_id == Level.id) \
         .outerjoin(LevelPrize, Level.id == LevelPrize.level_id) \
         .outerjoin(Prize, LevelPrize.prize_id == Prize.id) \
         .offset(offset).limit(batch_size).all()
        
        if not results:
            break  # Если больше нет данных, останавливаем цикл

        yield result
        offset += batch_size  # следующая пачка данных

def export_csv():
   # Проверяем, существует ли директория для сохранения
    CSV_FILE_PATH = "./result.csv"

    db = get_db()

    # Открываем файл для записи
    with open(CSV_FILE_PATH, mode="w", newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # заголовки
        writer.writerow(["Player ID", "Level title", "Is completed", "Prize title"])
        
        # Запрашиваем данные батчами и записываем их в файл
        for batch in get_data_in_batches(db):
            for row in batch:
                writer.writerow([row.player_id, row.level_title, row.is_completed, row.prize_title])
