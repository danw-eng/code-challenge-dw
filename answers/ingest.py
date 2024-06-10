import os
import sqlite3
from datetime import datetime, UTC
from glob import glob


def get_table_len(cur, table_name):
    """Return the number of rows in a table.
    :param table_name: table name
    :param cur: sqlite3 cursor
    :return: int
    """
    cur.execute(
        f"SELECT COUNT(*) FROM {table_name}",
    )
    return cur.fetchone()[0]


def reformat_num(num, null_val="-9999"):
    """Replace missing values with None or convert num str to int.
    :param num: Number string
    :param null_val: Format of missing values
    :return: int or None
    """
    return None if num == null_val else int(num)


def to_iso8601(date_str):
    """Convert a YYYYMMDD date string to ISO 8601 format YYYY-MM-DD."""
    return "-".join([date_str[:4], date_str[4:6], date_str[6:]])


def create_tables(con, cur):
    """
    Create the weather and ingest log tables if they don't already exist.
    :param con: sqlite3 connection
    :param cur: sqlite3 cursor
    :return: None
    """
    # Create the weather table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS WxData (
            station_id TEXT NOT NULL,
            date TEXT NOT NULL,
            temp_max INTEGER,
            temp_min INTEGER,
            precip INTEGER,
            PRIMARY KEY (station_id, date)
        );
    """
    )

    # Create the log table
    # Note: Using a composite primary key allows a text file to be re-ingested if it has been modified
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS WxIngestLog (
            file_name TEXT NOT NULL,
            file_dir TEXT NOT NULL,
            station_id TEXT NOT NULL,
            start_time INTEGER NOT NULL,
            end_time INTEGER NOT NULL,
            file_record_count INTEGER NOT NULL,
            inserted_record_count INTEGER NOT NULL,
            PRIMARY KEY (file_name, start_time)
        );
    """
    )

    # Create the stats table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS WxStats (
            year INTEGER NOT NULL,
            station_id TEXT NOT NULL,
            avg_max_temp REAL,
            avg_min_temp REAL,
            precip_total_accumulated REAL,
            PRIMARY KEY (year, station_id)
        );
    """
    )

    con.commit()


def ingest_txt_file(txt, cur, con):
    """Ingest a text file into the WxData table and log the action in the WxIngestLog table.
    :param txt: Path to the text file
    :param cur: sqlite3 cursor
    :param con: sqlite3 connection
    :return: None
    """
    # Get start time as UTC timestamp in iso8601 format
    # Note: I'm including microseconds in case duration needs to be calculated, and it's less than a second
    start_time = datetime.now(UTC).isoformat()
    station_id = os.path.splitext(os.path.basename(txt))[0]

    # Get the table length before inserting the data
    wx_record_count = get_table_len(cur, "WxData")

    # Open the text file and iterate through the lines
    with open(txt, "r") as f:
        lines = f.readlines()
        file_record_count = len(lines)

        for line in lines:
            date, temp_max, temp_min, precip = line.split()
            # Reformat the data to match the WxData schema
            date = to_iso8601(date)
            temp_max = reformat_num(temp_max)
            temp_min = reformat_num(temp_min)
            precip = reformat_num(precip)

            # Insert the weather data into the WxData table
            cur.execute(
                "INSERT OR IGNORE INTO WxData VALUES (?, ?, ?, ?, ?)",
                (station_id, date, temp_max, temp_min, precip),
            )

    # Calculate how many rows were inserted
    inserted_record_count = get_table_len(cur, "WxData") - wx_record_count

    con.commit()

    # Get the end time as UTC timestamp in iso8601 format
    end_time = datetime.now(UTC).isoformat()

    # Insert the log data into the WxIngestLog table
    cur.execute(
        "INSERT OR IGNORE INTO WxIngestLog VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            os.path.basename(txt),
            os.path.dirname(txt),
            station_id,
            start_time,
            end_time,
            file_record_count,
            inserted_record_count,
        ),
    )
    con.commit()


def calculate_stats(cur, con):
    """Calculate the average min and max temperatures and accumulated precipitation for each year and station.
    Total precip is divided by 100 to convert from tenths of a millimeter to centimeters. Temps are divided by 10 to
    convert from tenths of a degree to degrees Celsius.
    :param cur: sqlite3 cursor
    :param con: sqlite3 connection
    :return: None
    """
    # Note: Total precip is divided by 100 to convert from tenths of a millimeter to centimeters
    cur.execute(
        """
        INSERT OR REPLACE INTO WxStats(station_id, year, avg_max_temp, avg_min_temp, precip_total_accumulated)
        SELECT
            station_id,
            CAST(strftime('%Y', date) as INTEGER) as year,
            ROUND(AVG(temp_max)/10, 2) as avg_max_temp, 
            ROUND(AVG(temp_min)/10, 2) as avg_min_temp, 
            CAST(SUM(precip) AS REAL)/100 as precip_total_accumulated
        FROM WxData
        GROUP BY station_id, strftime('%Y', date)
        """
    )

    con.commit()


def main():
    # Create SQLite database
    con = sqlite3.connect("Weather.db")
    cur = con.cursor()

    # Create tables
    create_tables(con=con, cur=cur)

    # Define the path to the directory containing the weather data and get a list of all the files in the directory
    # Note: For test data use "../test_data/*.txt"
    wx_dir = "../wx_data/*.txt"
    for txt in glob(wx_dir):
        # Ingest the weather data for the given text file and update the log file
        ingest_txt_file(txt=txt, cur=cur, con=con)

    # Calculate the weather statistics
    calculate_stats(cur=cur, con=con)

    # Close the connection
    cur.close()
    con.close()


if __name__ == "__main__":
    main()
