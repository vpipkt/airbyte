# "No format in ['%Y-%m-%dT%H:%M:%S.%f'] matching 2024-12-02T09:46:17.000000+01:00",

#%%
import datetime

#%% 
s = '2024-11-30T23:22:57.000000+01:00'
fmt = '%Y-%m-%dT%H:%M:%S.%f%z'

_d = datetime.datetime.strptime(s, fmt)
print(_d)

# %%
