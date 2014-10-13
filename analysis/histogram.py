import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import dataset

db = dataset.connect('sqlite:///./../database/data.sqlite')
db_attendance = db["attendance"]

# in_hrs_array = []

# in_hrs = db.query('select in_time_hrs from attendance where at_type="P" and in_time_hrs is not NULL')

# for input_time in in_hrs:
#     in_hrs_array.append(float(input_time['in_time_hrs']))

# plt.hist(in_hrs_array)


out_hrs = db.query('select out_time_hrs from attendance where at_type="P" and out_time_hrs is not NULL')

out_hrs_array = []
for input_time in out_hrs:
    out_hrs_array.append(float(input_time['out_time_hrs']))


plt.hist(out_hrs_array)
plt.show()

