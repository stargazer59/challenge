import sqlite3 as lite
import scraper


class PhoneDataLayer:
    def __init__(self, db='numbers.db'):
        con = None
        try:
            con = lite.connect(db)
            with con:
                cur = con.cursor()
                cur.execute("DROP TABLE IF EXISTS Numbers")
                cur.execute("CREATE TABLE Numbers(number TEXT primary key not null, count INT, comment TEXT, date TEXT)")
        except Exception as e:
            print('Failed to connect to numbers.db', e)
            raise

        if con:
            con.close()

    def insert_entries(self, entries):
        try:
            con = lite.connect('numbers.db')
            with con:
                cur = con.cursor()
                for entry in entries:
                    cur.execute('INSERT OR REPLACE INTO Numbers(number, count, comment, date) VALUES (?, ?, ?, CURRENT_TIMESTAMP);', [entry.phone_number, entry.report_count, entry.comment])
        except Exception as e:
            print('Failed to connect to numbers.db', e)
            raise

        if con:
            con.close()

    def get_all_entries(self):
        parser = scraper.Parser(scraper.ValidUAOpener().open(scraper.PHONE_SITE).read())
        entries = parser.parse()
        self.insert_entries(entries)

        try:
            con = lite.connect('numbers.db')
            rows = None
            with con:
                cur = con.cursor()
                cur.execute('SELECT * FROM Numbers ORDER BY date;')
                rows = cur.fetchall()

            entries = []
            for row in rows:
                entries[len(entries):] = [scraper.PhoneNumberEntry(row[0], row[1], row[2])]

        except Exception as e:
            print('Failed to connect to numbers.db', e)
            raise

        if con:
            con.close()

        return entries

    def get_db_entries(self, n=60):
        try:
            con = lite.connect('numbers.db')
            # rows = None
            with con:
                cur = con.cursor()
                cur.execute('SELECT * FROM Numbers ORDER BY date LIMIT {}'.format(n))
                rows = cur.fetchall()

            entries = []
            for row in rows:
                entries[len(entries):] = [scraper.PhoneNumberEntry(row[0], row[1], row[2])]

        except Exception as e:
            print('Failed to connect to numbers.db', e)
            raise

        if con:
            con.close()

        return entries

    def get_entries(self, n=None):
        parser = scraper.Parser(scraper.ValidUAOpener().open(scraper.PHONE_SITE).read())
        entries = parser.parse()
        self.insert_entries(entries)

        if n is None:
            return entries
        elif n < len(entries):
            return entries[:n]
        else:
            return self.get_db_entries(n)

