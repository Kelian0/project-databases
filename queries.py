from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
from sqlalchemy.exc import IntegrityError
import os
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.


class DBTM():
    def __init__(self):
        super().__init__()
        password = os.getenv("PASSWORD")
        user = os.getenv("USERNAME_DB")
        db = os.getenv("NAME_DB")
        self.engine = create_engine(f"postgresql+psycopg2://{user}:{password}@postgresql-edu.in.centralelille.fr:5432/{db}")


    def execute(self, query):
        with self.engine.connect() as conn:
            try:
                conn.execute(text(query))
                conn.commit()
            except IntegrityError:
                print("Alredy there")

    def read_sql_df(self, query):
        with self.engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn)

        return df

    def check_table_exist(self, table_name):
        with self.engine.connect() as conn:
            res = conn.execute(text(f"SELECT table_name FROM information_schema.tables WHERE table_name='{table_name}'"))
        return res.fetchone()

def main():
    db = DBTM()
    db.execute("""set search_path = 'proj_db_kelian_clement'""")


    print("Free Games ")
    df = db.read_sql_df(
    """SELECT name
    FROM games
    WHERE price = 0
    LIMIT 10;
    """)
    print(df.to_markdown(index=False))
    print()

    print("Games sorted by price ")
    df = db.read_sql_df(
    """SELECT name, price
    FROM games
    ORDER BY price DESC
    LIMIT 10;
    """)
    print(df.to_markdown(index=False))
    print()

    print("number of games by genre ")
    df = db.read_sql_df(
    """SELECT ge.name, COUNT(*) AS "nb of game"
    FROM genres ge
    JOIN game_genres gg ON ge.genreID = gg.genreID
    GROUP BY ge.genreID
    ORDER BY "nb of game" DESC
    LIMIT 10;
    """)
    print(df.to_markdown(index=False))
    print()
    

    print("Game ")
    df = db.read_sql_df(
    """SELECT name, release_date, metacritic_score
    FROM games
    WHERE release_date >= '2020-01-01'
    AND metacritic_score > 0
    ORDER BY metacritic_score DESC
    LIMIT 10;
    """)
    print(df.to_markdown(index=False))
    print()

    print("Number of studios for each country")
    df = db.read_sql_df(
    """SELECT country, COUNT(*) AS nb_stud
    FROM developers
    GROUP BY country
    ORDER BY nb_stud DESC
    LIMIT 10;
    """)
    print(df.to_markdown(index=False))
    print()

    print("The most played game and the least played game for each categories")
    df = db.read_sql_df(
    """SELECT c.name , ga.name as "the most played game", MAX(ga.average_playtime_forever) as "time played"
    FROM games ga
    JOIN game_categories as gc ON gc.appid = ga.appid
    JOIN categories as c ON c.categoryid = gc.categoryid
    GROUP BY c.name

    UNION 

    SELECT c.name as category, ga.name as "the least played game", MIN(ga.average_playtime_forever) as "time played"
    FROM games ga
    JOIN game_categories as gc ON gc.appid = ga.appid
    JOIN categories as c ON c.categoryid = gc.categoryid
    GROUP BY c.name
    """)
    print(df.to_markdown(index=False))
    print()


if __name__ == '__main__':
    main()