from sqlalchemy import create_engine, text, DDL
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.exc import IntegrityError
import os
from dotenv import load_dotenv
import pandas as pd
import csv

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

    def insert_df(self, df, table_name, if_exists='append'):
            with self.engine.connect() as conn:
                try: 
                    df.to_sql(table_name, con=conn, if_exists=if_exists, index=False)
                    conn.commit()
                except IntegrityError as e:
                    print(e.orig)
                    



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
    # db.execute("""CREATE SCHEMA proj_db_kelian_clement;""")
    db.execute("""set search_path = 'proj_db_kelian_clement'""")

    # db.execute("""DROP TABLE cities CASCADE;""")
    # db.execute("""DROP TABLE games CASCADE;""")

    if db.check_table_exist(table_name="countries") is None:
        db.execute("""CREATE TABLE countries (
                   countryid SERIAL,
                   country VARCHAR(255) NOT NULL,
                   region VARCHAR(255),
                   population INT,
                   area INT,
                   pop_density NUMERIC(10,2),
                   coastline NUMERIC(10,2),
                   infant_mortality NUMERIC(10,2),
                   phones NUMERIC(10,2),
                   arable NUMERIC(10,2),
                   crops NUMERIC(10,2),
                   other NUMERIC(10,2),
                   birthrate NUMERIC(10,2),
                   deathrate NUMERIC(10,2),
                   agriculture NUMERIC(10,2),
                   industry NUMERIC(10,2),
                   service NUMERIC(10,2),
                   PRIMARY KEY(countryid)
        );""")

    if db.check_table_exist(table_name="cities") is None:
        # db.execute("""CREATE TYPE admin_cat AS ENUM ('primary', 'admin', 'minor'); """)
        db.execute("""CREATE TABLE cities (
                   id INT,
                   city VARCHAR(255) NOT NULL,
                   city_ascii VARCHAR(255),
                   lat NUMERIC(10,7),
                   lng NUMERIC(10,7),
                   countryid INT NOT NULL,
                   iso2 VARCHAR(2),
                   iso3 VARCHAR(3),
                   admin_name VARCHAR(255),
                   capital admin_cat,
                   population INT,
                   PRIMARY KEY (id),
                   FOREIGN KEY (countryid) REFERENCES countries(countryid) ON DELETE CASCADE

        );""")


    if db.check_table_exist(table_name="studios") is None:
        db.execute("""CREATE TABLE studios (
                studioid SERIAL,
                name VARCHAR(255) NOT NULL,
                notable_games TEXT,
                notes TEXT,
                cityid INT NOT NULL,
                countryid INT NOT NULL,
                year INT,
                PRIMARY KEY (studioid),
                FOREIGN KEY (cityid) REFERENCES cities(id) ON DELETE CASCADE,
                FOREIGN KEY (countryid) REFERENCES countries(countryid) ON DELETE CASCADE
        );""")
    
    if db.check_table_exist(table_name="games") is None:
        db.execute("""CREATE TABLE games (
                appid INT,
                name VARCHAR(255) NOT NULL,
                studioid INT,
                release_date DATE NOT NULL,
                required_age INT,
                price NUMERIC(6,2) NOT NULL,
                DLCcount INT,
                windows BOOLEAN,
                mac BOOLEAN,
                linux BOOLEAN, 
                achievements INT,
                estimated_owners VARCHAR(50),
                metacritic_score INT,
                positive INT,
                negative INT,
                average_playtime_forever INT,
                PRIMARY KEY (appid),
                FOREIGN KEY (studioid) REFERENCES studios(studioid) ON DELETE CASCADE,
                CHECK (price >= 0)
        );""")
    

    if db.check_table_exist(table_name="categories") is None:
        db.execute("""CREATE TABLE categories (
                    categoryid SERIAL,
                    name VARCHAR(255),
                    PRIMARY KEY (categoryid)
        );""")

    if db.check_table_exist(table_name="languages") is None:
        db.execute("""CREATE TABLE languages (
                   languageid SERIAL,
                   name VARCHAR(255),
                   PRIMARY KEY(languageid)

        );""")

    if db.check_table_exist(table_name="genres") is None:
        db.execute("""CREATE TABLE genres (
                   genreid SERIAL,
                   name VARCHAR(255),
                   PRIMARY KEY(genreid)
        );""")

    if db.check_table_exist(table_name="game_categories") is None:
        db.execute("""CREATE TABLE game_categories (
                   appid INT,
                   categoryid INT,
                   PRIMARY KEY(appid,categoryid),
                   FOREIGN KEY (appid) REFERENCES games(appid) ON DELETE CASCADE,
                   FOREIGN KEY (categoryid) REFERENCES categories(categoryid) ON DELETE CASCADE
        );""")

    if db.check_table_exist(table_name="game_languages") is None:
        db.execute("""CREATE TABLE game_languages (
                   appid INT,
                   languageid INT,
                   PRIMARY KEY(appid,languageid),
                   FOREIGN KEY (appid) REFERENCES games(appid) ON DELETE CASCADE,
                   FOREIGN KEY (languageid) REFERENCES languages(languageid) ON DELETE CASCADE
        );""")

    if db.check_table_exist(table_name="game_genres") is None:
        db.execute("""CREATE TABLE game_genres (
                   appid INT,
                   genreid INT,
                   PRIMARY KEY(appid,genreid),
                   FOREIGN KEY (appid) REFERENCES games(appid) ON DELETE CASCADE,
                   FOREIGN KEY(genreid) REFERENCES genres(genreid) ON DELETE CASCADE

        );""")

    
    
    db.execute("""ALTER TABLE studios ALTER COLUMN cityid DROP NOT NULL;""")
    db.execute("""ALTER TABLE studios ALTER COLUMN countryid DROP NOT NULL;""")

    db.execute("""ALTER TABLE games ALTER COLUMN release_date DROP NOT NULL;""")

#======================COMMENTS ON TABLES=====================
    db.execute("""COMMENT ON COLUMN games.appid IS 'AppID, unique identifier for each app';""")
    db.execute("""COMMENT ON COLUMN games.name IS 'Game name';""")
    db.execute("""COMMENT ON COLUMN games.studioid IS 'unique identifier for the studio';""")
    db.execute("""COMMENT ON COLUMN games.release_date IS 'Release date';""")
    db.execute("""COMMENT ON COLUMN games.required_age IS 'Age required to play, 0 if it is for all audiences ';""")
    db.execute("""COMMENT ON COLUMN games.price IS 'Price in USD, 0.0 if its free';""")
    db.execute("""COMMENT ON COLUMN games.DLCcount IS 'Number of DLCs, 0 if you have none';""")
    db.execute("""COMMENT ON COLUMN games.windows IS 'Does it support Windows? ';""")
    db.execute("""COMMENT ON COLUMN games.mac IS 'Does it support Mac? ';""")
    db.execute("""COMMENT ON COLUMN games.linux IS 'Does it support Linux?';""")
    db.execute("""COMMENT ON COLUMN games.achievements IS 'Number of achievements, 0 if it has none';""")
    db.execute("""COMMENT ON COLUMN games.estimated_owners IS 'Estimated owners (string, e.g.: 0 - 20000)';""")
    db.execute("""COMMENT ON COLUMN games.metacritic_score IS 'Metacritic score, 0 if it has none';""")
    db.execute("""COMMENT ON COLUMN games.positive IS 'Positive votes';""")
    db.execute("""COMMENT ON COLUMN games.negative IS 'Negative votes';""")
    db.execute("""COMMENT ON COLUMN games.average_playtime_forever IS 'Average playtime since March 2009, in minutes';""")

    db.execute("""COMMENT ON COLUMN countries.countryid IS 'Unique identifier for each country';""")
    db.execute("""COMMENT ON COLUMN countries.country IS 'Name of the country';""")
    db.execute("""COMMENT ON COLUMN countries.region IS 'Geographical region';""")
    db.execute("""COMMENT ON COLUMN countries.area IS 'Area (sq. mi.)';""")
    db.execute("""COMMENT ON COLUMN countries.pop_density IS 'Pop. Density (per sq. mi.)';""")
    db.execute("""COMMENT ON COLUMN countries.coastline IS 'Coastline (coast/area ratio)';""")
    db.execute("""COMMENT ON COLUMN countries.infant_mortality IS 'Infant mortality (per 1000 births)';""")
    db.execute("""COMMENT ON COLUMN countries.phones IS 'Number of Phones (per 1000)';""")
    db.execute("""COMMENT ON COLUMN countries.arable IS 'Percentage of land that is Arable (%)';""")
    db.execute("""COMMENT ON COLUMN countries.crops IS 'Percentage of land reserved for Crops (%)';""")
    db.execute("""COMMENT ON COLUMN countries.other IS 'Percentage of Other Land (forest, pasture, etc.)';""")
    db.execute("""COMMENT ON COLUMN countries.agriculture IS 'Ratio of Agriculture sector';""")
    db.execute("""COMMENT ON COLUMN countries.industry IS 'Ratio of Industry sector';""")
    db.execute("""COMMENT ON COLUMN countries.service IS 'Ratio of Service sector';""")


    db.execute("""COMMENT ON COLUMN cities.id IS 'Unique identifier for each city';""")
    db.execute("""COMMENT ON COLUMN cities.city IS 'City name';""")
    db.execute("""COMMENT ON COLUMN cities.city_ascii IS 'City name in ASCII';""")
    db.execute("""COMMENT ON COLUMN cities.lat IS 'Latitude';""")
    db.execute("""COMMENT ON COLUMN cities.lng IS 'Longitude';""")
    db.execute("""COMMENT ON COLUMN cities.iso2 IS 'Country code ISO2';""")
    db.execute("""COMMENT ON COLUMN cities.iso3 IS 'Coubtry code ISO3';""")
    db.execute("""COMMENT ON COLUMN cities.admin_name IS 'name of the adminstrative area';""")
    db.execute("""COMMENT ON COLUMN cities.capital IS 'primary, admin, minor or none';""")

    db.execute("""COMMENT ON COLUMN studios.studioid IS 'Unique identifier for each studio';""")
    db.execute("""COMMENT ON COLUMN studios.name IS 'Studio name';""")
    db.execute("""COMMENT ON COLUMN studios.notable_games IS 'Noteable games and franchises';""")
    db.execute("""COMMENT ON COLUMN studios.notes IS 'Additional information on the studio';""")
    db.execute("""COMMENT ON COLUMN studios.year IS 'Year of establishment';""")


#============================================================================
    df_countries = pd.read_csv("Data/Clean_Data/countries.csv",sep=",")
    db.insert_df(df_countries, 'countries', if_exists='append')

    print("Test countries ")
    df = db.read_sql_df(
    """SELECT *
    FROM countries
    LIMIT 5;
    """)
    print(df.to_markdown(index=False))
    print()


#============================================================================
    df_cities = pd.read_csv("Data/Clean_Data/cities.csv",sep=",")
    db.insert_df(df_cities, 'cities', if_exists='append')

    print("Test cities ")
    df = db.read_sql_df(
    """SELECT *
    FROM cities
    LIMIT 5;
    """)
    print(df.to_markdown(index=False))
    print()

#============================================================================
    df_studios = pd.read_csv("Data/Clean_Data/studios.csv",sep=",")
    db.insert_df(df_studios, 'studios', if_exists='append')

    print("Test studios ")
    df = db.read_sql_df(
    """SELECT *
    FROM studios
    LIMIT 5;
    """)
    print(df.to_markdown(index=False))
    print()


#============================================================================
    df_games = pd.read_csv("Data/Clean_Data/games.csv",sep=",")
    db.insert_df(df_games, 'games', if_exists='append')

    print("Test games ")
    df = db.read_sql_df(
    """SELECT *
    FROM games
    LIMIT 5;
    """)
    print(df.to_markdown(index=False))
    print()

#============================================================================
    df_categories = pd.read_csv("Data/Clean_Data/categories.csv",sep=",")
    db.insert_df(df_categories, 'categories', if_exists='append')

    print("Test categories ")
    df = db.read_sql_df(
    """SELECT *
    FROM categories
    LIMIT 5;
    """)
    print(df.to_markdown(index=False))
    print()

#============================================================================
    df_languages = pd.read_csv("Data/Clean_Data/languages.csv",sep=",")
    db.insert_df(df_languages, 'languages', if_exists='append')

    print("Test languages ")
    df = db.read_sql_df(
    """SELECT *
    FROM languages
    LIMIT 5;
    """)
    print(df.to_markdown(index=False))
    print()


#============================================================================
    df_genres = pd.read_csv("Data/Clean_Data/genres.csv",sep=",")
    db.insert_df(df_genres, 'genres', if_exists='append')

    print("Test genres ")
    df = db.read_sql_df(
    """SELECT *
    FROM genres
    LIMIT 5;
    """)
    print(df.to_markdown(index=False))
    print()

#============================================================================
    df_game_categories = pd.read_csv("Data/Clean_Data/game_categories.csv",sep=",")
    db.insert_df(df_game_categories, 'game_categories', if_exists='append')

    print("Test game_categories ")
    df = db.read_sql_df(
    """SELECT *
    FROM game_categories
    LIMIT 5;
    """)
    print(df.to_markdown(index=False))
    print()


#============================================================================
    df_game_languages = pd.read_csv("Data/Clean_Data/game_languages.csv",sep=",")
    db.insert_df(df_game_languages, 'game_languages', if_exists='append')

    print("Test game_languages ")
    df = db.read_sql_df(
    """SELECT *
    FROM game_languages
    LIMIT 5;
    """)
    print(df.to_markdown(index=False))
    print()

#============================================================================
    df_game_genres = pd.read_csv("Data/Clean_Data/game_genres.csv",sep=",")
    db.insert_df(df_game_genres, 'game_genres', if_exists='append')

    print("Test game_genres ")
    df = db.read_sql_df(
    """SELECT *
    FROM game_genres
    LIMIT 5;
    """)
    print(df.to_markdown(index=False))
    print()


if __name__ == '__main__':
    main()