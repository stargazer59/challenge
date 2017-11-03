from flask import Flask
import data


app = Flask(__name__)
data = data.PhoneDataLayer()


def get_results(n=None):
    return u'[' + u', '.join(map(unicode, data.get_entries(n))) + u']'


def get_results_by_area(area_code, n=None):
    d = filter(lambda entry: entry.area_code == area_code, data.get_all_entries())
    if n:
        return u'[' + u', '.join(map(unicode, d[:n])) + u']'
    else:
        return u'[' + u', '.join(map(unicode, d)) + u']'


@app.route('/interview/api/v1.0/results', methods=['GET'])
def results():
    try:
        return get_results()
    except Exception as e:
        print('Bad Request: ', e)


@app.route('/interview/api/v1.0/results/<int:number>', methods=['GET'])
def results_with_limit(number):
    return get_results(number)


@app.route('/interview/api/v1.0/resultsForArea/<string:area_code>', methods=['GET'])
def results_by_area(area_code):
    return get_results_by_area(area_code)


@app.route('/interview/api/v1.0/resultsForArea/<string:area_code>/<int:number>', methods=['GET'])
def results_by_area_with_limit(area_code, number):
    return get_results_by_area(area_code, number)


if __name__ == '__main__':
    app.run(debug=True)
