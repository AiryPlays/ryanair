import os
import json
import sqlite3
from urllib.parse import urlparse, parse_qs
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

DB_PATH = os.path.join(os.path.dirname(__file__), 'flights.db')

def db_conn():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        'CREATE TABLE IF NOT EXISTS flights (id INTEGER PRIMARY KEY AUTOINCREMENT, day TEXT, flightNumber TEXT, time TEXT, departure TEXT, arrival TEXT, departureTime TEXT, arrivalTime TEXT, duration TEXT, operator TEXT)'
    )
    cur.execute('SELECT COUNT(1) FROM flights')
    count = cur.fetchone()[0]
    if count == 0:
        seed = [
            {"day":"Monday","flightNumber":"FR2489","time":"17:35 BST","departure":"London Stansted","arrival":"Asturias","departureTime":"17:35","arrivalTime":"18:45","duration":"1hr 10min","operator":"ryanairdac"},
            {"day":"Monday","flightNumber":"FR3871","time":"20:35 BST","departure":"Malta","arrival":"Venice Treviso","departureTime":"20:35","arrivalTime":"21:45","duration":"1hr 10min","operator":"ryanairdac"},
            {"day":"Tuesday","flightNumber":"FR7933","time":"16:35 BST","departure":"Kraków","arrival":"Podgorica","departureTime":"16:35","arrivalTime":"17:45","duration":"1hr 10min","operator":"buzz"},
            {"day":"Tuesday","flightNumber":"FR8681","time":"19:35 BST","departure":"Bristol","arrival":"Kaunas","departureTime":"19:35","arrivalTime":"20:45","duration":"1hr 10min","operator":"ryanairdac"},
            {"day":"Wednesday","flightNumber":"FR2325","time":"17:35 BST","departure":"Leeds Bradford","arrival":"Barcelona Girona","departureTime":"17:35","arrivalTime":"18:45","duration":"1hr 10min","operator":"ryanairdac"},
            {"day":"Wednesday","flightNumber":"FR2489","time":"20:35 BST","departure":"London Stansted","arrival":"Asturias","departureTime":"20:35","arrivalTime":"21:45","duration":"1hr 10min","operator":"ryanairdac"},
            {"day":"Thursday","flightNumber":"FR3871","time":"20:35 BST","departure":"Malta","arrival":"Venice Treviso","departureTime":"20:35","arrivalTime":"21:45","duration":"1hr 10min","operator":"buzz"},
            {"day":"Friday","flightNumber":"FR2489","time":"17:35 BST","departure":"London Stansted","arrival":"Asturias","departureTime":"17:35","arrivalTime":"18:45","duration":"1hr 10min","operator":"ryanairdac"},
            {"day":"Friday","flightNumber":"FR8681","time":"20:35 BST","departure":"Bristol","arrival":"Kaunas","departureTime":"20:35","arrivalTime":"21:45","duration":"1hr 10min","operator":"malta"},
            {"day":"Saturday","flightNumber":"FR3871","time":"00:35 BST","departure":"Malta","arrival":"Venice Treviso","departureTime":"00:35","arrivalTime":"01:45","duration":"1hr 10min","operator":"ryanairdac"},
            {"day":"Saturday","flightNumber":"FR7933","time":"15:35 BST","departure":"Kraków","arrival":"Podgorica","departureTime":"15:35","arrivalTime":"16:45","duration":"1hr 10min","operator":"malta"},
            {"day":"Saturday","flightNumber":"FR8681","time":"20:35 BST","departure":"Malta","arrival":"Venice Treviso","departureTime":"20:35","arrivalTime":"21:45","duration":"1hr 10min","operator":"buzz"},
            {"day":"Sunday","flightNumber":"FR2325","time":"00:35 BST","departure":"London Stansted","arrival":"Asturias","departureTime":"00:35","arrivalTime":"01:45","duration":"1hr 10min","operator":"ryanairdac"},
            {"day":"Sunday","flightNumber":"FR8681","time":"14:35 BST","departure":"Bristol","arrival":"Kaunas","departureTime":"14:35","arrivalTime":"15:45","duration":"1hr 10min","operator":"malta"},
            {"day":"Sunday","flightNumber":"FR7933","time":"19:35 BST","departure":"Kraków","arrival":"Podgorica","departureTime":"19:35","arrivalTime":"20:45","duration":"1hr 10min","operator":"buzz"},
            {"day":"Sunday","flightNumber":"FR2489","time":"23:35 BST","departure":"Leeds Bradford","arrival":"Barcelona Girona","departureTime":"23:35","arrivalTime":"00:45","duration":"1hr 10min","operator":"ryanairdac"}
        ]
        for f in seed:
            cur.execute(
                'INSERT INTO flights(day, flightNumber, time, departure, arrival, departureTime, arrivalTime, duration, operator) VALUES (?,?,?,?,?,?,?,?,?)',
                (f["day"], f["flightNumber"], f["time"], f["departure"], f["arrival"], f["departureTime"], f["arrivalTime"], f["duration"], f["operator"])
            )
        conn.commit()
    conn.close()

class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == '/api/flights':
            qs = parse_qs(parsed.query)
            day = qs.get('day', [None])[0]
            fid = qs.get('id', [None])[0]
            conn = db_conn()
            cur = conn.cursor()
            if fid:
                cur.execute('SELECT id, day, flightNumber, time, departure, arrival, departureTime, arrivalTime, duration, operator FROM flights WHERE id=?', (fid,))
            elif day:
                cur.execute('SELECT id, day, flightNumber, time, departure, arrival, departureTime, arrivalTime, duration, operator FROM flights WHERE day=?', (day,))
            else:
                cur.execute('SELECT id, day, flightNumber, time, departure, arrival, departureTime, arrivalTime, duration, operator FROM flights')
            rows = cur.fetchall()
            conn.close()
            data = [
                {
                    "id": r[0],
                    "day": r[1],
                    "flightNumber": r[2],
                    "time": r[3],
                    "departure": r[4],
                    "arrival": r[5],
                    "departureTime": r[6],
                    "arrivalTime": r[7],
                    "duration": r[8],
                    "operator": r[9],
                }
                for r in rows
            ]
            body = json.dumps(data).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        return SimpleHTTPRequestHandler.do_GET(self)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == '/api/flights':
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode('utf-8'))
            conn = db_conn()
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO flights(day, flightNumber, time, departure, arrival, departureTime, arrivalTime, duration, operator) VALUES (?,?,?,?,?,?,?,?,?)',
                (payload.get('day'), payload.get('flightNumber'), payload.get('time'), payload.get('departure'), payload.get('arrival'), payload.get('departureTime'), payload.get('arrivalTime'), payload.get('duration'), payload.get('operator'))
            )
            conn.commit()
            fid = cur.lastrowid
            conn.close()
            body = json.dumps({"ok": True, "id": fid}).encode('utf-8')
            self.send_response(201)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

    def do_PUT(self):
        parsed = urlparse(self.path)
        if parsed.path == '/api/flights':
            qs = parse_qs(parsed.query)
            fid = qs.get('id', [None])[0]
            if not fid:
                self.send_response(400)
                self.end_headers()
                return
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode('utf-8'))
            fields = ['day','flightNumber','time','departure','arrival','departureTime','arrivalTime','duration','operator']
            sets = []
            values = []
            for k in fields:
                if k in payload and payload[k] is not None:
                    sets.append(f"{k}=?")
                    values.append(payload[k])
            if not sets:
                self.send_response(400)
                self.end_headers()
                return
            values.append(fid)
            conn = db_conn()
            cur = conn.cursor()
            cur.execute(f"UPDATE flights SET {', '.join(sets)} WHERE id=?", values)
            conn.commit()
            conn.close()
            body = json.dumps({"ok": True}).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

    def do_DELETE(self):
        parsed = urlparse(self.path)
        if parsed.path == '/api/flights':
            qs = parse_qs(parsed.query)
            fid = qs.get('id', [None])[0]
            if not fid:
                self.send_response(400)
                self.end_headers()
                return
            conn = db_conn()
            cur = conn.cursor()
            cur.execute('DELETE FROM flights WHERE id=?', (fid,))
            conn.commit()
            conn.close()
            body = json.dumps({"ok": True}).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

def main():
    init_db()
    port = int(os.environ.get('PORT', '8000'))
    server = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    server.serve_forever()

if __name__ == '__main__':
    main()