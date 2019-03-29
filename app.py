from flask import Flask, render_template, request, jsonify
import requests
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()
app = Flask(__name__)


@app.route('/', methods=['GET'])
def get():
    crime_url_template = 'https://data.police.uk/api/crimes-street/all-crime?lat={lat}&lng={lng}&date={data}'
    my_latitude = request.args.get('lat', '51.52369')
    my_longitude = request.args.get('lng', '-0.0395857')
    my_date = request.args.get('date', '2019-01')

    crime_url = crime_url_template.format(lat=my_latitude,
                                          lng=my_longitude,
                                          data=my_date)

    resp = requests.get(crime_url)
    crimes = resp.json()

    for crime in crimes:
        session.execute(
            """ INSERT INTO test.crime(id, cate) VALUES (%s, %s) """,
            (crime['id'], crime['category'])
        )

    return 'Hello!'


@app.route('/crime/', methods=['GET'])
def crime():
    rows = session.execute("""Select * From test.crime""")

    return ('<h2>This is all crimes: {} </h2>'.format(rows.current_rows))


@app.route('/create/', methods=['POST'])
def create():
    session.execute(
            """ INSERT INTO test.crime(id, cate) VALUES (%s, %s) """,
            (request.json['id'], request.json['cate'])
                    )
    return jsonify({'message': 'created: /create/{}'.format(request.json['id'])}), 201


@app.route('/delete/<id>', methods=['DELETE'])
def delete(id):
    session.execute('Delete From test.crime where id={}'.format(id))
    return jsonify({'success': True})


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080, debug=True)
