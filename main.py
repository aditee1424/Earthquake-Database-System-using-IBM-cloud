import os
from flask import Flask,redirect,render_template,request
import urllib
import datetime
import json
import ibm_db
import geopy
import geocoder
from config import *
import geopy.distance



app = Flask(__name__)

if 'VCAP_SERVICES' in os.environ:
    db2info = json.loads(os.environ['VCAP_SERVICES'])['dashDB For Transactions'][0]
    db2cred = db2info["credentials"]
    appenv = json.loads(os.environ['VCAP_APPLICATION'])
else:
    raise ValueError('Expected cloud environment')


@app.route("/")
def Index():
    return render_template('index.html')

# main page to dump some environment information
@app.route('/retrievedata')
def retrievedata():
    db2conn = ibm_db.connect(db2cred['ssldsn'], "", "")
    if db2conn:
        sql = "SELECT * FROM EARTHQUAKE LIMIT 20"
        stmt = ibm_db.exec_immediate(db2conn, sql)
        rows = []
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            rows.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)
        ibm_db.close(db2conn)
        print(result)
    return render_template('retrievedata.html',rows=rows)

@app.route("/givendistance")
def givendistance():
    return render_template('quakes_within_dist.html')

@app.route('/quakes_within_dist',methods=['GET'])
def quakes_within_dist():
    distance = request.args.get('distance', 500, type=float)
    city = request.args.get('city', 'arlington')
    g = geocoder.osm(city).json
    target_coordinates = (g['lat'], g['lng'])

    db2conn = ibm_db.connect(db2cred['ssldsn'], "", "")
    if db2conn:
        sql = "SELECT * FROM EARTHQUAKE"
        stmt = ibm_db.exec_immediate(db2conn, sql)
        rows = []
        count = 0
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            current_coordinates = (result['LATITUDE'], result['LONGITUDE'])
            if geopy.distance.geodesic(current_coordinates, target_coordinates).km < distance:
                count += 1
                rows.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)
        ibm_db.close(db2conn)
        print(result)
    return render_template('quakes_within_dist.html',rows=rows, count=count)

@app.route("/lq")
def lg():
    return render_template('largest_quake.html')

@app.route('/largest_quake',methods=['GET'])
def largest_quake():
    distance = request.args.get('distance', 500, type=float)
    city = request.args.get('city')
    city = 'Dallas' if city == '' else city

    number = request.args.get('number', type=int)
    today = datetime.date.today()
    ago = today - datetime.timedelta(days=number)
    print(ago)

    g = geocoder.osm(city).json
    target_coordinates = (g['lat'], g['lng'])
    print(target_coordinates)
    # connect to DB2
    db2conn = ibm_db.connect(db2cred['ssldsn'], "", "")
    if db2conn:
        sql = "SELECT * FROM EARTHQUAKE"
        stmt = ibm_db.exec_immediate(db2conn, sql)
        rows, largest = [], 0
        # fetch the result
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            curr_date = result['TIME'][:10]
            curr_date = datetime.datetime.strptime(curr_date, "%Y-%m-%d")
            current_coordinates = (result['LATITUDE'], result['LONGITUDE'])
            if geopy.distance.geodesic(current_coordinates, target_coordinates).km < distance and float(result['MAG']) > largest and curr_date.date() >= ago:
                print(curr_date)
                largest = float(result['MAG'])
                rows = [result.copy()]
            result = ibm_db.fetch_assoc(stmt) #fetching new row like i++
        # close database connection
        ibm_db.close(db2conn)
    return render_template('largest_quake.html',rows=rows)

@app.route("/cq")
def cq():
    return render_template('closestquake.html')

@app.route('/closestquake',methods=['GET'])
def closestquake():

    city = request.args.get('city')
    city = 'Dallas' if city == '' else city

    magnitude = request.args.get('number', type=float)
    print(magnitude)
    g = geocoder.osm(city).json
    target_coordinates = (g['lat'], g['lng'])

    # connect to DB2
    db2conn = ibm_db.connect(db2cred['ssldsn'], "", "")
    if db2conn:
        sql = "SELECT * FROM EARTHQUAKE"
        stmt = ibm_db.exec_immediate(db2conn, sql)
        rows = []
        closest_distance = float("inf")
        print(closest_distance)
        # fetch the result
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            current_coordinates = (result['LATITUDE'], result['LONGITUDE'])
            if geopy.distance.geodesic(current_coordinates, target_coordinates).km < closest_distance and float(result['MAG']) >= magnitude :
                print(float(result['MAG']))
                closest_distance = geopy.distance.geodesic(current_coordinates, target_coordinates).km
                print(closest_distance)
                rows = [result.copy()]
            result = ibm_db.fetch_assoc(stmt) #fetching new row like i++
        # close database connection
        ibm_db.close(db2conn)
    return render_template('closestquake.html',rows=rows)

@app.route("/cp")
def cp():
    return render_template('compare_places.html')


@app.route('/compare', methods=['GET'])
def compare():
    distance = request.args.get('distance', 1000, type=int)

    placeA, placeB = request.args.get('placeA'), request.args.get('placeB')
    placeA = 'Anchorage' if placeA == '' else placeA
    placeB = 'Dallas' if placeB == '' else placeB

    pA_json, pB_json = geocoder.osm(placeA).json, geocoder.osm(placeB).json
    trgtA_coords, trgtB_coords = (pA_json['lat'], pA_json['lng']), (pB_json['lat'], pB_json['lng'])

    # connect to DB2
    db2conn = ibm_db.connect(db2cred['ssldsn'], "", "")
    if db2conn:
        sql = "SELECT * FROM EARTHQUAKE"
        stmt = ibm_db.exec_immediate(db2conn, sql)
        ansA, ansB = [], []
        # fetch the result
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            curr_coords = (result['LATITUDE'], result['LONGITUDE'])
            if geopy.distance.geodesic(curr_coords, trgtA_coords).km < distance:
                ansA.append(result.copy())
            if geopy.distance.geodesic(curr_coords, trgtB_coords).km < distance:
                ansB.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)
        # close database connection
        ibm_db.close(db2conn)
    print(len(ansA), len(ansB))
    return render_template('compare_places.html', ciA=ansA, ciB=ansB, pA=placeA, pB=placeB)

@app.route("/ms")
def ms():
    return render_template('magnitude_slots.html')

@app.route('/magnitude_slots', methods=['GET'])
def magnitude_slots():
    number = request.args.get('number', 1000, type=int)
    today = datetime.date.today()
    ago = today - datetime.timedelta(days=number)

    slot12, slot23, slot34, slot45, slot56, slot67 = 0, 0, 0, 0, 0, 0
    # connect to DB2
    db2conn = ibm_db.connect(db2cred['ssldsn'], "", "")
    if db2conn:
        sql = "SELECT * FROM EARTHQUAKE WHERE MAGTYPE=\'ml\'"
        stmt = ibm_db.exec_immediate(db2conn, sql)
        # fetch the result
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            curr_date = result['TIME'][:10]
            curr_date = datetime.datetime.strptime(curr_date, "%Y-%m-%d")
            if curr_date.date() >= ago:
                #print(curr_date)
                mag_scale = float(result['MAG'])
                if 1 <= mag_scale <= 2:
                    slot12 += 1
                if 2 <= mag_scale <= 3:
                    slot23 += 1
                if 3 <= mag_scale <= 4:
                    slot34 += 1
                if 4 <= mag_scale <= 5:
                    slot45 += 1
                if 5 <= mag_scale <= 6:
                    slot56 += 1
                if 6 <= mag_scale <= 7:
                    slot67 += 1
            result = ibm_db.fetch_assoc(stmt)
        # close database connection
        ibm_db.close(db2conn)
    return render_template('magnitude_slots.html', ci=[slot12, slot23, slot34, slot45, slot56, slot67])

@app.route("/lr")
def lr():
    return render_template('location_in_range.html')


@app.route('/location_in_range', methods=['GET'])
def location_in_range():
    magX = request.args.get('magX', 5, type=float)
    magY = request.args.get('magY', 6, type=float)

    db2conn = ibm_db.connect(db2cred['ssldsn'], "", "")
    if db2conn:
        sql = "SELECT * FROM EARTHQUAKE WHERE MAGTYPE=\'ml\'"
        stmt = ibm_db.exec_immediate(db2conn, sql)
        rows = []
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            mag = float(result['MAG'])
            if mag >= magX and mag <= magY:
                rows.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)

        ibm_db.close(db2conn)
    return render_template('location_in_range.html', rows=rows)



port = os.getenv('PORT', '5000')
if __name__ == "__main__":
	app.run(host='0.0.0.0', port=int(port))