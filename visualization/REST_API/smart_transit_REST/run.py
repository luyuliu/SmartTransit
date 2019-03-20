import run_smart_transit

from eve import Eve
app = Eve(settings='setting.py')

if __name__ == '__main__':
    app.run(port=50032)
