from multiprocessing import Pool, TimeoutError
import time
import os




def f(x):
    return x*x

# with Pool(processes=4) as pool:
pool = Pool(processes=2)
res = pool.apply_async(f, (20,))  # runs in *only* one process
print(res.get(timeout=1000))
