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
                appid INT,
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
                PRIMARY KEY (appid),
                FOREIGN KEY (developerID) REFERENCES developers(developerID),
                CHECK (price >= 0),
        );""")

    if db.check_table_exist(table_name="developers") is None:
        db.execute("""CREATE TABLE developers (
                developerID INT,
                name VARCHAR(255) NOT NULL,
                notable_games VARCHAR(255),
                Notes VARCHAR(255),
                CityID INT NOT NULL,
                Country VARCHAR(255) NOT NULL,
                year INT,
                PRIMARY KEY (developerID),
                FOREIGN KEY (CityID) REFERENCES cities(...),
                FOREIGN KEY (Country) REFERENCES countries(...)
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
                   population FLOAT,
                   PRIMARY KEY (id),
                   FOREIGN KEY (country) REFERENCES countries(...)
        );""")
    
    if db.check_table_exist(table_name="") is None:
        db.execute("""CREATE TABLE  (

        );""")

    if db.check_table_exist(table_name="") is None:
        db.execute("""CREATE TABLE  (

        );""")

    if db.check_table_exist(table_name="") is None:
        db.execute("""CREATE TABLE  (

        );""")

    if db.check_table_exist(table_name="") is None:
        db.execute("""CREATE TABLE  (

        );""")

    if db.check_table_exist(table_name="") is None:
        db.execute("""CREATE TABLE  (

        );""")

    if db.check_table_exist(table_name="") is None:
        db.execute("""CREATE TABLE  (

        );""")

    if db.check_table_exist(table_name="") is None:
        db.execute("""CREATE TABLE  (

        );""")
    

    # if db.check_table_exist(table_name="proj_db.games") is not None:
    #     db.execute("""DROP TABLE proj_db.games;""")

    # if db.check_table_exist(table_name="mel.municipalities") is None:
    #     db.execute("""CREATE TABLE mel.municipalities (
    #         municipality character(3) NOT NULL,
    #         name text NOT NULL,
    #         insee character(5) NOT NULL,
    #         epci character(9),
    #         area numeric(4,2)
    #     );""")

    #     db.execute("""COMMENT ON COLUMN mel.municipalities.municipality IS 'code municipality sur 3 caractères';""")
    #     db.execute("""COMMENT ON COLUMN mel.municipalities.name IS 'name de la municipality';""")
    #     db.execute("""COMMENT ON COLUMN mel.municipalities.insee IS 'code INSEE sur 5 caractères';""")
    #     db.execute("""COMMENT ON COLUMN mel.municipalities.epci IS 'code de l''EPCI (MEL) sur 9 caractères';""")
    #     db.execute("""COMMENT ON COLUMN mel.municipalities.area IS 'area, en km2';""")

    # db.execute("""INSERT INTO mel.municipalities VALUES ('009', 'Villeneuve-d''Ascq', '59009', '245900410', 27.56);""")
    # db.execute("""INSERT INTO mel.municipalities VALUES ('013', 'Anstaing', '59013', '245900410', 2.30);""")

    # db.execute("""ALTER TABLE ONLY mel.municipalities
    #             ADD CONSTRAINT municipalities_pkey PRIMARY KEY (insee);""")
    
    # db.execute("""ALTER TABLE ONLY mel.pollutants
    #             ADD CONSTRAINT pollutants_pkey PRIMARY KEY (code_pollutant);""")

    # db.execute("""ALTER TABLE ONLY mel.population
    #             ADD CONSTRAINT population_pkey PRIMARY KEY (insee, census);""")

    # db.execute("""ALTER TABLE ONLY mel.population
    #             ADD CONSTRAINT population_insee_fkey FOREIGN KEY (insee) REFERENCES mel.municipalities(insee);""")




if __name__ == '__main__':
    main()