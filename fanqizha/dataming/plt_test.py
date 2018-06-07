import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# fig = plt.figure()
# ax1 = fig.add_subplot(2,2,1)
# ax2 = fig.add_subplot(2,2,2)
# ax3 = fig.add_subplot(2,2,3)
# plt.plot(np.random.randn(50).cumsum(),'k--')
# _ = ax1.hist(np.random.randn(100),bins = 20,color = 'k',alpha = 0.3)
# ax2.scatter(np.arange(30),np.arange(30) + 3 * np.random.randn(30))

subplots_adjust(left = None,bottom = None,right = None,top = None,wspace = None,hspace = None)
#wspace和space用于控制宽度和高度的百分比，可以用做subplot之间的间距，下面是个例子：
fig,ax = plt.subplots(2,2,sharex = True,sharey = True)
for i in range(2):
     for j in range(2):
          ax[i,j].hist(np.random.randn(500),bins = 50,color = 'k',alpha = 0.5)
plt.subplots_adjust(wspace = 0.5,hspace = 0.5)

plt.show()