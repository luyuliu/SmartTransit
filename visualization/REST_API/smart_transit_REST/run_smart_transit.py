from pymongo import MongoClient

codeString = '''DOMAIN = {
    '''

client = MongoClient("localhost", 27017, maxPoolSize=50)

cols = client.cota_smart_transit.list_collection_names()
for todayDate in cols:
    codeString = codeString + "'" + todayDate + "': {'datasource': {" + "'source': '" + todayDate + "'}},"+'''
    '''

codeString=codeString +"}"





codeString = codeString + '''
MONGO_HOST = 'localhost'
MONGO_PORT = 27017

MONGO_DBNAME = 'cota_smart_transit'

ALLOW_UNKNOWN=True

PAGINATION_LIMIT = 10000

PAGINATION_DEFAULT = 10000'''

try:
    F = open("C:\\Users\\liu.6544\\Documents\\GitHub\\SmartTransit\\visualization\\REST_API\\smart_transit_REST\\setting.py","x")
except:
    F = open("C:\\Users\\liu.6544\\Documents\\GitHub\\SmartTransit\\visualization\\REST_API\\smart_transit_REST\\setting.py","w")
F.write(codeString)

F.close()