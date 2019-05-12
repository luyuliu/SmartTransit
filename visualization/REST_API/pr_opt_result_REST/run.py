import run_pr_opt_result
from pymongo import MongoClient

from eve import Eve

if __name__ == '__main__':
    run_pr_opt_result.generateSettingFile("cota_pr_optimization_result")
    app = Eve(settings='setting.py')
    app.run(port=50032)
