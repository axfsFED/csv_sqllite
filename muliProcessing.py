'''
Created on 2017年12月21日

@author: 3xtrees
'''

from pathos.multiprocessing import ProcessingPool as Pool
pool = Pool(10)
results = []
try:
    partial_csv2sqllite = partial(csv2sqllite, engine=engine) #偏函数，固定engine
    results = pool.map(partial_csv2sqllite, file_name_list, stock_code_list)
except Exception:
    print("Error!")
    traceback.print_exc()
pool.close()
pool.join()