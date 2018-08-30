import mysql.connector
from flask import Flask, request, jsonify
from datetime import datetime, time
from flask_caching import Cache

class Db:
    def __init__(self):
        self.mydb = mysql.connector.connect(
          host="localhost",
          user="root",
          database="simplecab"
        )

    @property
    def cursor(self):
        if not hasattr(self, '_cursor'):
            self._cursor = self.mydb.cursor(dictionary=True)
        return self._cursor


def buildSqlList(drivers, params):
    params.update({'m{}'.format(i): medallion for i, medallion in enumerate(drivers)})
    return "(" + ','.join(["%(m{})s".format(i) for i in range(len(drivers))]) + ")"


app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})


def get_lifts(drivers, rawdate):

    try:
        date = datetime.strptime(rawdate, "%Y-%m-%d").date()
    except:
        result = {'error': "date missing or not in Linux format"}
        return jsonify(result), 500

    if not drivers:
        result = {'error': "comma seperated list of medallions missing"}
        return jsonify(result), 500


    db = Db()
    params = {'startdate': datetime.combine(date, time.min),
              'enddate': datetime.combine(date, time.max)}

    sql = \
        """
        SELECT medallion AS driver, COUNT(*) AS lifts
        FROM cab_trip_data
        WHERE medallion IN
        """ \
        + buildSqlList(drivers, params) + \
        """
        AND pickup_datetime BETWEEN %(startdate)s AND %(enddate)s
        GROUP BY 1
        """

    db.cursor.execute(sql, params)

    result = db.cursor.fetchall()
    return jsonify(result), 200


@cache.cached()
@app.route('/api/lifts', methods = ['GET','POST'])
def lifts():
    '''
    'http://localhost:5000/api/lifts?medallions=D7D598CD99978BD012A87A76A7C891B7,801C69A08B51470871A8110F8B0505EE&date=2013-12-01'

    arg medallions: comma seperated list or medallions (eg: D7D598CD99978BD012A87A76A7C891B7,801C69A08B51470871A8110F8B0505EE)
    arg date: Linux formatted date (eg: 2013-12-01)

    :return: a list of the number of lifts given a list of drivers and date
    '''

    medallions = request.args.get('medallions')
    if medallions:
        medallions = medallions.split(',')

    rawdate = request.args.get('date')
    result = get_lifts(medallions, rawdate)
    return result


@app.route('/api/rawlifts', methods = ['GET','POST'])
def rawlifts():
    '''
    'http://localhost:5000/api/rawlifts?medallions=D7D598CD99978BD012A87A76A7C891B7,801C69A08B51470871A8110F8B0505EE&date=2013-12-01'

    arg medallions: comma seperated list or medallions (eg: D7D598CD99978BD012A87A76A7C891B7,801C69A08B51470871A8110F8B0505EE)
    arg date: Linux formatted date (eg: 2013-12-01)

    :return: a list of the number of lifts (ignoring cache) given a list of drivers and date
    '''

    medallions = request.args.get('medallions')
    if medallions:
        medallions = medallions.split(',')

    rawdate = request.args.get('date')
    result = get_lifts(medallions, rawdate)
    return result


@app.route('/api/flush', methods = ['GET','POST'])
def clear():
    '''
    http://localhost:5000/api/flush

    Clears cache

    '''
    cache.clear()
    return jsonify({'ok': True}), 200


if __name__ == "__main__":
    app.run()