from sqlalchemy import create_engine, text, DDL
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.exc import IntegrityError
import os
from dotenv import load_dotenv
import pandas as pd

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
    # db.execute("""CREATE SCHEMA proj_db_kelian_clement;""")
    db.execute("""set search_path = 'proj_db_kelian_clement'""")
    
    if db.check_table_exist(table_name="games") is None:
        db.execute("""CREATE TABLE games (
                appID INT,
                name VARCHAR(255) NOT NULL,
                developerID INT NOT NULL,
                release_date DATE NOT NULL,
                required_age INT,
                price NUMERIC(6,2),
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
                PRIMARY KEY (appID),
                FOREIGN KEY (developerID) REFERENCES developers(developerID),
                CHECK (price >= 0),
        );""")

    if db.check_table_exist(table_name="developers") is None:
        db.execute("""CREATE TABLE developers (
                developerID INT,
                name VARCHAR(255) NOT NULL,
                notable_games VARCHAR(255),
                Notes VARCHAR(255),
                cityID INT NOT NULL,
                country VARCHAR(255) NOT NULL,
                year INT,
                PRIMARY KEY (developerID),
                FOREIGN KEY (cityID) REFERENCES cities(id),
                FOREIGN KEY (country) REFERENCES countries(country)
        );""")
    
    if db.check_table_exist(table_name="cities") is None:
        db.execute("""CREATE TYPE admin_cat AS ENUM ('primary', 'admin', 'minor'); """)
        db.execute("""CREATE TABLE cities (
                   id INT,
                   city VARCHAR(255) NOT NULL,
                   city_ascii VARCHAR(255) NOT NULL,
                   lat NUMERIC(10,7),
                   lng NUMERIC(10,7),
                   country VARCHAR(255) NOT NULL,
                   iso2 VARCHAR(2) NOT NULL,
                   iso3 VARCHAR(3) NOT NULL,
                   admin_name VARCHAR(255),
                   capital admin_cat,
                   population INT,
                   PRIMARY KEY (id),
                   FOREIGN KEY (country) REFERENCES countries(...)
        );""")
    
    if db.check_table_exist(table_name="countries") is None:
        db.execute("""CREATE TABLE countries (
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
                   PRIMARY KEY(country)
        );""")

    if db.check_table_exist(table_name="categories") is None:
        db.execute("""CREATE TABLE categories (
                    categoryID SERIAL,
                    name VARCHAR(255),
                    PRIMARY KEY (categoryID)
        );""")

    if db.check_table_exist(table_name="languages") is None:
        db.execute("""CREATE TABLE  (
                   languageID SERIAL,
                   name VARCHAR(255),
                   PRIMARY KEY(languageID)

        );""")

    if db.check_table_exist(table_name="genres") is None:
        db.execute("""CREATE TABLE genres (
                   genreID SERIAL,
                   name VARCHAR(255),
                   PRIMARY KEY(genreID)
        );""")

    if db.check_table_exist(table_name="game_categories") is None:
        db.execute("""CREATE TABLE game_categories (
                   appID INT,
                   categoryID INT,
                   PRIMARY KEY(appID,categoryID),
                   FOREIGN KEY (appID) REFERENCES games(appID),
                   FOREIGN KEY (categoryID) REFERENCES categories(categoryID)
        );""")

    if db.check_table_exist(table_name="game_languages") is None:
        db.execute("""CREATE TABLE game_languages (
                   appID INT,
                   languageID INT,
                   PRIMARY KEY(appID,languageID),
                   FOREIGN KEY (appID) REFERENCES games(appID),
                   FOREIGN KEY (languageID) REFERENCES languages(languageID)
        );""")

    if db.check_table_exist(table_name="game_genres") is None:
        db.execute("""CREATE TABLE game_genres (
                   appID INT,
                   genreID INT,
                   PRIMARY KEY(appID,genreID)
                   FOREIGN KEY (appID) REFERENCES games(appID),
                   FOREIGN KEY(genreID) REFERENCES genres(genreID)

        );""")
    

    # db.execute("""INSERT INTO mel.municipalities VALUES ('009', 'Villeneuve-d''Ascq', '59009', '245900410', 27.56);""")
    # db.execute("""INSERT INTO mel.municipalities VALUES ('013', 'Anstaing', '59013', '245900410', 2.30);""")

    # if db.check_table_exist(table_name="proj_db.games") is not None:
    #     db.execute("""DROP TABLE proj_db.games;""")

    #     db.execute("""COMMENT ON COLUMN mel.municipalities.municipality IS 'code municipality sur 3 caractères';""")
    #     db.execute("""COMMENT ON COLUMN mel.municipalities.name IS 'name de la municipality';""")
    #     db.execute("""COMMENT ON COLUMN mel.municipalities.insee IS 'code INSEE sur 5 caractères';""")
    #     db.execute("""COMMENT ON COLUMN mel.municipalities.epci IS 'code de l''EPCI (MEL) sur 9 caractères';""")
    #     db.execute("""COMMENT ON COLUMN mel.municipalities.area IS 'area, en km2';""")

    # db.execute("""ALTER TABLE ONLY mel.municipalities
    #             ADD CONSTRAINT municipalities_pkey PRIMARY KEY (insee);""")



if __name__ == '__main__':
    main()