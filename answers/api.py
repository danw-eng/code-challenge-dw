import sqlite3

from flask import Flask, jsonify, request
from flask_swagger import swagger
from flask_swagger_ui import get_swaggerui_blueprint


# Create the Flask app
app = Flask(__name__, static_url_path="/answers/static")
app.config["API_TITLE"] = "Weather API"
app.config["API_VERSION"] = "v1"

# Create the swagger docs
swagger = swagger(app)
swag_url = "/api/docs"
api_url = "/answers/static/swagger.yaml"
blueprint = get_swaggerui_blueprint(
    swag_url, api_url, config={"app_name": "Weather API"}
)
app.register_blueprint(blueprint, url_prefix=swag_url)


@app.get("/api/weather")
def get_weather():
    # Get args from the request
    date = request.args.get("date", None, str)
    station_id = request.args.get("station-id", None, str)
    page = request.args.get("page", 1, int)

    results_per_page = 100

    # Connect to the database
    con = sqlite3.connect("weather.db")
    cur = con.cursor()

    # Build the query
    query = "SELECT * FROM WxData"
    if date and station_id:
        query += f" WHERE date = '{date}' AND station_id = '{station_id}'"
    elif date:
        query += f" WHERE date = '{date}'"
    elif station_id:
        query += f" WHERE station_id = '{station_id}'"

    # Implement pagination
    query += f" LIMIT {results_per_page} OFFSET {results_per_page * (page - 1)}"

    # Execute the query and return the results as json
    cur.execute(query)
    data = cur.fetchall()
    con.close()
    return jsonify(data=data, page=page, status=200, mimetype="application/json")


@app.get("/api/weather/stats")
def get_weather_stats():
    # Get args from the request
    year = request.args.get("year", None, int)
    station_id = request.args.get("station-id", None, str)
    page = request.args.get("page", 1, int)

    results_per_page = 100

    # Connect to the database
    con = sqlite3.connect("weather.db")
    cur = con.cursor()

    # Build the query
    query = "SELECT * FROM WxStats"
    if year and station_id:
        query += f" WHERE year = '{year}' AND station_id = '{station_id}'"
    elif year:
        query += f" WHERE year = '{year}'"
    elif station_id:
        query += f" WHERE station_id = '{station_id}'"

    # Implement pagination
    query += f" LIMIT {results_per_page} OFFSET {results_per_page * (page - 1)}"

    # Execute the query and return the results as json
    cur.execute(query)
    data = cur.fetchall()
    con.close()
    return jsonify(data=data, page=page, status=200, mimetype="application/json")

    # Test url: http://127.0.0.1:5000/api/weather/stats?year=1990&station-id=USC00112140


if __name__ == "__main__":
    app.run(debug=True)
