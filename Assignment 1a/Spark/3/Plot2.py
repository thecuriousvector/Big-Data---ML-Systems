import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

times = pd.read_csv('TimesTaken.txt', sep=",", header=None)
times.columns = ["map", "reduce", "time"]


plt.rcParams.update({'font.size': 15})

f, axes = plt.subplots(1, 10, figsize=(100, 10))
f.subplots_adjust(hspace=0)
i = 0
c = (np.random.rand(), np.random.rand(), np.random.rand())
for map in range(1,99,10):
    for reduce in range(1,99,10):
        axes[i].plot(reduce, times.loc[((times['map'] == map) & (times['reduce'] == reduce)), 'time'].iloc[0], 'ro--', label = reduce)
        axes[i].set_xlim([1,100])
        axes[i].set_ylim([20,30])
    axes[i].set_xlabel("Mappers = "+str(map))   
    i = i+1  
	
#plt.legend(loc="best")
plt.show()