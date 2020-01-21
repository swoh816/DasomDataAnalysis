import sys
from read_sql_server import change2df
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
import matplotlib

import concurrent.futures as cf


period = sys.argv[1] # regDate, week, day
analysis_content = sys.argv[2] # room, heartRate, normalizedActivity, sleep, all
phone_number = sys.argv[3] # Specific phone number or None (no input)
eventType_8 = ['room', 'heartRate', 'normalizedActivity']
eventType_73 = ['sleep', 'all']

# Get eventType using analysis_content
## room, heartrate, normalized activity are only available with eventType 8
if analysis_content in eventType_8:
    eventType = 8
# sleep, all are only available with eventType 73
elif analysis_content in eventType_73:
    eventType = 73
else:
    raise ValueError('Unknown eventType')

# Get period
if period == 'regDate':
    target_user = change2df(eventType, phone_number)
elif period == 'week':
    target_user = change2df(eventType, phone_number)
elif period == 'day':
    target_user = change2df(eventType, phone_number)
else:
    raise ValueError('Unknown value for period')


fontTicks = {'family' : 'normal',
        'size': 15}
fontLabel = {'family' : 'normal',
        'weight' : 'bold',
        'size': 22}
matplotlib.rc('font', **fontTicks)
plt.figure(figsize=(16,8))



## 1. Analysis of `eventType=8`
### 1.1 Room status
if analysis_content == 'room':
    # ### 2.1 Room state
    # Note that different locations in a house is represented by different numbers: `0: '거실', 1: '방', 2: '현관'`
    target_user.contentsRoom = target_user.contentsRoom.astype('category').cat.codes
    plt.scatter(target_user.regDate, target_user.contentsRoom)
    plt.xlabel("Date", fontdict=fontLabel)
    plt.ylabel("Location", fontdict=fontLabel)
    plt.yticks(range(3), ['LivingRoom', 'Room', 'FrontDoor'])


### 1.2 Heart Rate
elif analysis_content == 'heartRate':
    plt.bar(target_user.createdAt, target_user.contentsHrm)
    plt.title("Heart rate by time (user=01220461177, eventType=73)", fontdict=fontLabel)
    plt.xlabel("Date", fontdict=fontLabel)
    plt.ylabel("Activity", fontdict=fontLabel)


### 1.3 Normalized activity 
elif analysis_content == 'normalizedActivity':
    # ### 2.1.4 Normalized activity by time and location in a room
    # We normalize activity in a 0-1 scale, and we mark the location with red dot.
    for i in target_user:
        target_data = target_user[i].values
        if 'contents' in i:
            if 'Room' not in i:
                target_data = target_user[i].values
                plt.plot(target_user.createdAt, target_data/max(target_data), label=i.split('contents')[1])
    plt.xlabel("Time", fontdict=fontLabel)
    plt.ylabel("Normalized activity", fontdict=fontLabel)
    plt.title("Normalized activity by time", fontdict=fontLabel)



## 2. Analysis of `eventType=73`
### 2.1 Sleep pattern
elif analysis_content == 'sleep':
    for i in target_user:
        if 'contents' in i and 'Sleep' in i:
            target_data = target_user[i].values
            plt.plot(target_user.createdAt, target_data/max(target_data), label=i.split('contents')[1])
    plt.legend()


### 2.2 Other than sleep
elif analysis_content == 'all':
    for i in target_user:
        if 'contents' in i:
            target_data = target_user[i].values
            plt.plot(target_user.createdAt, target_data/max(target_data), label=i.split('contents')[1])
    plt.legend(loc=1)



## 3. Weekly tendency
### 3.1 EventType 8
#### One user
user_01220461177_8['weekday_hour'] = user_01220461177_8.createdAt.map(lambda x: str(x.weekday()) + datetime.strftime(x, '%H'))
user_01220461177_8_groupby_weekday_hour = user_01220461177_8.groupby('weekday_hour')

mean = user_01220461177_8_groupby_weekday_hour.mean().contentsCurrAct
std = user_01220461177_8_groupby_weekday_hour.std().contentsCurrAct
plt.subplot(2,1,1)
plt.plot(mean.index, mean)
plt.xticks([mean.index[0], mean.index[int(len(mean.index)/2)], mean.index[-1]])
plt.subplot(2,1,2)
plt.plot(mean.index, mean)
plt.fill_between(mean.index, mean-std, mean+std, alpha=0.2)
plt.xticks([mean.index[0], mean.index[int(len(mean.index)/2)], mean.index[-1]])
plt.vlines(x=[str(i) + '00' for i in range(7)], ymin=0, ymax=15, color='red')


#### All users
sensor_df_eventType8 = change2df(8)
sensor_df_eventType8['weekday_hour'] = sensor_df_eventType8.createdAt.map(lambda x: str(x.weekday()) + datetime.strftime(x, '%H'))
sensor_df_eventType8_groupby_weekday_hour = sensor_df_eventType8.groupby('weekday_hour')

mean = sensor_df_eventType8_groupby_weekday_hour.mean().contentsCurrAct
std = sensor_df_eventType8_groupby_weekday_hour.std().contentsCurrAct
plt.subplot(2,1,1)
plt.plot(mean.index, mean)
plt.xticks([str(i) + '00' for i in range(7)], ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
plt.title("Activity (mean)")
plt.ylabel("Activity")
plt.subplot(2,1,2)
plt.plot(mean.index, mean)
plt.fill_between(mean.index, mean-std, mean+std, alpha=0.2)
plt.vlines(x=[str(i) + '00' for i in range(7)], ymin=-200, ymax=200, color='red')
plt.title("Activity (with standard error)")
plt.ylabel("Activity")
plt.xlabel("Day")
plt.xticks([str(i) + '00' for i in range(7)], ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
# plt.savefig("./FigureCompilation/weekly_act_day_eT8")


### Daily tendency of a user
user_01220461177_8['hour'] = user_01220461177_8.createdAt.map(lambda x: datetime.strftime(x, '%H'))
user_01220461177_8_groupby_hour = user_01220461177_8.groupby('hour')

mean = user_01220461177_8_groupby_hour.mean().contentsPrevAct
std = user_01220461177_8_groupby_hour.std().contentsPrevAct
plt.subplot(2,1,1)
plt.plot(range(24), mean)

plt.subplot(2,1,2)
plt.plot(range(24), mean)
plt.fill_between(range(24), mean-std, mean+std, alpha=0.2)


### Daily tendency of the entire users
sensor_df_eventType8['hour'] = sensor_df_eventType8.createdAt.map(lambda x: datetime.strftime(x, '%H'))
sensor_df_eventType8_groupby_hour = sensor_df_eventType8.groupby('hour')

mean = sensor_df_eventType8_groupby_hour.mean().contentsPrevAct
std = sensor_df_eventType8_groupby_hour.std().contentsPrevAct
plt.subplot(2,1,1)
plt.plot(range(24), mean)
plt.title("Activity (mean)")
plt.ylabel("Activity")

plt.subplot(2,1,2)
plt.plot(range(24), mean)
plt.title("Activity (with standard error)")
plt.ylabel("Activity")
plt.xlabel("Hour")
plt.fill_between(range(24), mean-std, mean+std, alpha=0.2)
plt.savefig("./FigureCompilation/daily_act_day_eT8")



# ## 4.3 `eventType=73`
# Unfortunately, the number of data for `eventType=73` is quite small, so that there is no much meaningful analysis to conduct. However, here I present some data analysis that could be useful later when the data size gets larger.

target_user = change2df(73, '01220461177')
contents_list_73 = [i for i in target_user.columns if 'contents' in i]

target_user['hour'] = target_user.createdAt.map(lambda x: datetime.strftime(x, '%H'))
target_user['dayHour'] = target_user.createdAt.map(lambda x: str(x.weekday()) + datetime.strftime(x, '%H'))

def getNormalizedResult73(content, period):
    content_time_data = target_user[[period, content]]
    mean_content_time_data = content_time_data.groupby(period).mean()
    std_content_time_data = content_time_data.groupby(period).std()
    normalized_mean_content_time_data = mean_content_time_data/max(mean_content_time_data.values)
    std_content_time_data = std_content_time_data/max(std_content_time_data.values)
    
    # Insert empty time slots with `np.nan`
    idx_list = normalized_mean_content_time_data.index
    if period == 'hour':
        time_list = range(24)
    else:
        time_list = []
        for j in range(7):
            time_list.extend([str(j) + str(i).zfill(2) for i in range(24)])
        
    for time in time_list:
        zfill_time = str(time).zfill(2)
        if zfill_time not in idx_list:
            normalized_mean_content_time_data.loc[zfill_time] = np.nan
            std_content_time_data.loc[zfill_time] = np.nan
    normalized_mean_content_time_data = normalized_mean_content_time_data.sort_index()
    std_content_time_data = std_content_time_data.sort_index()
    return normalized_mean_content_time_data, std_content_time_data


def plotContent(content, period):
    mean_content_time_data, std_content_time_data = getNormalizedResult73(content, period)
    plt.plot(mean_content_time_data.index, mean_content_time_data, 'o-', label=content.split('contents')[1])
    plt.fill_between(mean_content_time_data.index,
                     (mean_content_time_data-std_content_time_data)[content],
                     (mean_content_time_data+std_content_time_data)[content], alpha=0.5)

    plt.xlabel("Hour")
    plt.ylabel("Normalized Activity")
    plt.legend


### Daily normalized activity
for content in contents_list_73:
    mean_content_time_data, std_content_time_data = getNormalizedResult73(content, 'hour')
    plt.plot(mean_content_time_data, 'o-', label=content.split('contents')[1])
#     plt.fill_between(normalized_mean)
plt.legend(loc=4)
plt.xlabel('Hour')
plt.ylabel('Normalized activity')


### Weekly normalized activity
for content in contents_list_73:
    mean_content_time_data, std_content_time_data = getNormalizedResult73(content, 'dayHour')
    plt.plot(mean_content_time_data, 'o-', label=content.split('contents')[1])
    plt.xticks([str(i) + '00' for i in range(7)])
#     plt.fill_between(normalized_mean)
plt.legend(loc=4)
plt.xlabel('Hour')
plt.ylabel('Normalized activity')





if phone_number == None:
    phone_number = "total"
plt.savefig('./Figures/{}_{}_{}.png'.format(eventType, analysis_content, phone_number))
