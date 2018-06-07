# -*- cocoding: utf-8 -*-
import pandas as pd

import numpy as np

import matplotlib.pyplot as plt

import seaborn as sns

import os

# In[2]:

os.chdir(r'/Users/ufenqi/Downloads/课件和脚本')

# In[3]:

accepts = pd.read_csv('accepts.csv')

rejects = pd.read_csv('rejects.csv')
accepts_x = accepts[["tot_derog","age_oldest_tr","rev_util","fico_score","ltv"]]

# In[ ]:

accepts_x.head()

# In[5]:

accepts_y = accepts['bad_ind']

# In[ ]:

rejects.head()

# In[6]:

rejects_x = rejects[["tot_derog","age_oldest_tr","rev_util","fico_score","ltv"]]


# print rejects_x.head()
rejects_x.info()

# In[ ]:

accepts_x.info()
import fancyimpute as fimp

accepts_x_filled = pd.DataFrame(fimp.KNN(3).complete(accepts_x.as_matrix()))

accepts_x_filled.columns = accepts_x.columns

rejects_x_filled = pd.DataFrame(fimp.KNN(3).complete(rejects_x.as_matrix()))

rejects_x_filled.columns = rejects_x.columns