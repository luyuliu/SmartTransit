import run_smart_transit
from pymongo import MongoClient

from eve import Eve

if __name__ == '__main__':
    run_smart_transit.generateSettingFile("cota_smart_transit")
    app = Eve(settings='setting.py')
    app.run(port=50031)
