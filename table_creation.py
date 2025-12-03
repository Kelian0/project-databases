from sqlalchemy import create_engine, text, DDL
from sqlalchemy_utils import database_exists, create_database
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

    def check_table_exist(self, table_name):
        with self.engine.connect() as conn:
            res = conn.execute(text(f"SELECT table_name FROM information_schema.tables WHERE table_name='{table_name}'"))
        return res.fetchone()

def main():
    db = DBTM()

    db.execute("""CREATE SCHEMA proj_db;""")

    if db.check_table_exist(table_name="proj_db.games") is None:
        db.execute("""CREATE TABLE proj_db.games (
            appID,
            name,
            release_date, 
            estimated_owners,
            required_age,
            price,
            DLCcount,
            about_the_game,
            supported_languages,
            windows,
            mac,
            linux,
            metacritic_score,
            user_score,
            positive,
            negativ,
            achievements,
            average_playtime_forever,
            developers,
            publisher,
            categories,
            genres,
            tags,
        );""")

    db.execute("""COMMENT ON COLUMN proj_db.games.appID IS 'AppID, unique identifier for each app';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.name IS 'Game name';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.release_date IS 'Release date';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.estimated_owners IS 'Estimated owners (string, e.g.: 0 - 20000)';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.required_age IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.price IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.DLCcount IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.about_the_game IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.supported_languages IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.windows IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.mac IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.linux IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.linux IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.metacritic_score IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.user_score IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.positive IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.negativ IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.achievements IS '';""")    
    db.execute("""COMMENT ON COLUMN proj_db.games.average_playtime_forever IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.developers IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.publisher IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.categories IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.genres IS '';""")
    db.execute("""COMMENT ON COLUMN proj_db.games.tags IS '';""")


    if db.check_table_exist(table_name="proj_db.countries") is None:
        db.execute("""CREATE TABLE proj_db.games (
            #TO DO
        );""")

    if db.check_table_exist(table_name="proj_db.indies") is None:
        db.execute("""CREATE TABLE proj_db.games (
            #TO DO
        );""")

    if db.check_table_exist(table_name="proj_db.studios") is None:
        db.execute("""CREATE TABLE proj_db.games (
            #TO DO
        );""")

    if db.check_table_exist(table_name="proj_db.cities") is None:
        db.execute("""CREATE TABLE proj_db.games (
            #TO DO
        );""")

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