from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
from sqlalchemy.exc import IntegrityError
import os
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

def df_to_latex(df, file_paht='Report/queries.txt'):
    with open(file_paht, 'w') as f:
        f.write(df.to_latex(index=False))
    return df.to_latex(index=False)


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
    df_to_latex(df, file_paht='Report/queries/0FreeGames.txt')
    print(df.to_markdown(index=False))
    print()

    print("Games sorted by price ")
    df = db.read_sql_df(
    """SELECT name, price
    FROM games
    ORDER BY price DESC
    LIMIT 10;
    """)
    df_to_latex(df, file_paht='Report/queries/1GamesByPrice.txt')
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
    df_to_latex(df, file_paht='Report/queries/2GamesByGenre.txt')
    print(df.to_markdown(index=False))
    print()
    

    print("All game with a metacritic score after 2020")
    df = db.read_sql_df(
    """SELECT name, release_date, metacritic_score
    FROM games
    WHERE release_date >= '2020-01-01'
    AND metacritic_score > 0
    ORDER BY metacritic_score DESC
    LIMIT 10;
    """)
    df_to_latex(df, file_paht='Report/queries/3GamesAfter2020.txt')
    print(df.to_markdown(index=False))
    print()

    print("Number of studios for each country")
    df = db.read_sql_df(
    """SELECT c.country, COUNT(*) AS nb_stud
    FROM studios s JOIN countries c ON s.countryid = c.countryid
    GROUP BY c.country
    ORDER BY nb_stud DESC
    LIMIT 10;
    """)
    df_to_latex(df, file_paht='Report/queries/4NBStudiosBYCountry.txt')
    print(df.to_markdown(index=False))
    print()

    print("The most played game for each genres")
    df = db.read_sql_df(
    """
    SELECT ge.name as "Game Genres", MAX(g.name) as "The most played game", MAX(g.average_playtime_forever) as "Time played"
    FROM games g
    JOIN game_genres as gg ON gg.appid = g.appid
    JOIN genres as ge ON ge.genreid = gg.genreid
    JOIN (SELECT gg.genreid AS genreid, MAX(g.average_playtime_forever) as max_time_played
            FROM games g
            JOIN game_genres as gg ON gg.appid = g.appid
            GROUP BY gg.genreid) as table_genre_maxtime
            ON table_genre_maxtime.genreid = gg.genreid AND table_genre_maxtime.max_time_played = g.average_playtime_forever
    GROUP BY ge.name
    ORDER BY "Time played" DESC;
    """)
    df_to_latex(df, file_paht='Report/queries/5MostPlayedByGenre.txt')
    print(df.to_markdown(index=False))
    print()

    print("Top 3 games in 2020 which has at least 3 supported languages")
    df = db.read_sql_df(
    """SELECT ga.name as "Game name", ga.metacritic_score as "Metacritic score", COUNT(gl.languageID) AS "language count"
    FROM games ga
    JOIN game_languages AS gl USING (appid)
    WHERE ga.release_date >= '2020-01-01' AND ga.release_date < '2021-01-01'
    GROUP BY ga.appid
    HAVING COUNT(gl.languageID) >= 3
    ORDER BY ga.metacritic_score DESC
    LIMIT 3;
    """)
    df_to_latex(df, file_paht='Report/queries/6Top3Games.txt')
    print(df.to_markdown(index=False))
    print()

    print("Average games prices and notable games for each studio in Tokyo or New York or Montréal with more than 3 games")
    df = db.read_sql_df(
    """SELECT s.name as "studio name", AVG(ga.price) as "average price", COUNT(ga.name) as "nb of game",ci.city as city , ci.iso3 as country
    FROM games ga
    JOIN studios AS s USING(studioid)
    JOIN cities AS ci ON ci.id = s.cityid
    GROUP BY s.studioid, s.name, ci.iso3, ci.city
    HAVING (ci.city = 'Tokyo' OR ci.city = 'New York' OR ci.city = 'Montréal') AND COUNT(ga.name) >= 3
    ORDER BY "average price"
    LIMIT 20;
    """)
    df_to_latex(df, file_paht='Report/queries/7AveragePrices.txt')
    print(df.to_markdown(index=False))
    print()


if __name__ == '__main__':
    main()