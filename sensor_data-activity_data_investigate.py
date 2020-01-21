import pymysql
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
import matplotlib

from sklearn.preprocessing import OneHotEncoder
import concurrent.futures as cf

mysql = pymysql.connect(
    host='104.199.189.13',
    user='root',
    password='1thefull!',
    db='silverCare'
)

sensor_df = pd.read_sql_query('select * from SensorData', mysql)

''' Module to recreate the dataframe
The current dataframe contains important information (`contents`) in `list` format. We split the list to different columns, and the column names are different depending on the `eventType`.
1. Change the type of `regDate` from string to datetime.
2. Split three elements of `contents` entry, save them separately, and drop `contents` column.
3. Convert the type of contents from string to integer.
'''

def convertContents2Float(contents):
    try:
        return float(contents)
    except:
        if contents == 'null':
            return np.nan
        else:
            return contents

def change2df(eventType, phone_number=None):
    if phone_number:
        target_df = sensor_df[(sensor_df.eventType == eventType) & (sensor_df.phone == phone_number)]
    else:
        target_df = sensor_df[sensor_df.eventType == eventType]
    target_df.reset_index(inplace=True)
    target_df.regDate = target_df.regDate.map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))

    if eventType == 8:
        converted_columns = ['contentsRoom', 'contentsCurrAct', 'contentsPrevAct']
    elif eventType == 73:
        converted_columns = ['contentsStep', 'contentsDistance', 'contentsCalory',
                               'contentsHrm', 'contentsBattery',
                               'contentsCondition', 'contentsTotalSleep',
                               'contentsTotalDeepSleep', 'contentsTotalLightSleep']
    else:
        raise ValueError("Eventtype {} is not available".format(eventType))
        
    temp_df = pd.DataFrame(target_df.contents.map(lambda x: x.split(';')).values.tolist(), columns=converted_columns)
    for column in temp_df:
        temp_df[column] = temp_df[column].map(lambda x: convertContents2Float(x))
    target_df = pd.concat([target_df, temp_df], axis=1)
    target_df.drop(columns=['contents'], inplace=True)
    target_df = target_df[['id', 'phone', 'devType', 'macAddr', 'eventType'] + converted_columns +                         ['commState', 'powerState', 'batState', 'regDate', 'createdAt', 'updatedAt']]
   
    return target_df




# ### 2.1 Room state
# Note that different locations in a house is represented by different numbers: `0: '거실', 1: '방', 2: '현관'`
user_01223386203 = change2df(8, '01223386203')
user_01223386203.contentsRoom = user_01223386203.contentsRoom.astype('category').cat.codes

fontTicks = {'family' : 'normal',
        'size': 15}
fontLabel = {'family' : 'normal',
        'weight' : 'bold',
        'size': 22}
matplotlib.rc('font', **fontTicks)

plt.figure(figsize=(16,8))
plt.scatter(user_01223386203.regDate, user_01223386203.contentsRoom)
plt.xlabel("Date", fontdict=fontLabel)
plt.ylabel("Location", fontdict=fontLabel)
plt.yticks(range(3), ['LivingRoom', 'Room', 'FrontDoor'])
plt.savefig('./FigureCompilation/user_01223386203_location_per_time.png')



# ### 2.1.4 Heart rate
user_01220461177_73 = change2df(73, '01220461177')
plt.figure(figsize=(16,8))
plt.bar(user_01220461177_73.createdAt, user_01220461177_73.contentsHrm)
plt.title("Heart rate per time (user=01220461177, eventType=73)", fontdict=fontLabel)
plt.xlabel("Date", fontdict=fontLabel)
plt.ylabel("Activity", fontdict=fontLabel)
plt.savefig('./FigureCompilation/user_01220461177_activity_time_first_100_eT73.png')


# ### 2.1.4 Normalized activity per time and location in a room
# We normalize activity in a 0-1 scale, and we mark the location with red dot.

user_01220461177_8 = change2df(8, '01220461177')
user_01220461177_8.contentsRoom = user_01220461177_8.contentsRoom.astype('category').cat.codes
data_of_interest = user_01220461177_8.iloc[:40]

plt.figure(figsize=(16,8))
for i in data_of_interest:
    if 'contents' in i:
        if 'Room' in i:
            target_data = data_of_interest[i].values
            plt.scatter(data_of_interest.createdAt, target_data/max(target_data), label=i.split('contents')[1], c='red')
        else:
            target_data = data_of_interest[i].values
            plt.bar(data_of_interest.createdAt, target_data/max(target_data), label=i.split('contents')[1], width=0.0001, alpha=0.5)
plt.xlim(data_of_interest.createdAt.iloc[0], data_of_interest.createdAt.iloc[-1])
# plt.legend()
plt.xlabel("Time", fontdict=fontLabel)
plt.ylabel("Normalized activity", fontdict=fontLabel)
plt.title("Normalized activity per time", fontdict=fontLabel)
plt.savefig('./FigureCompilation/user_01220461177_norm_act_time_first_40_eT8.png')



## 3. Analysis of `eventType=73`
### 3.1 Sleep pattern
plt.figure(figsize=(16,8))
for i in user_01220461177_73:
    if 'contents' in i and 'Sleep' in i:
        target_data = user_01220461177_73[i].values
        plt.plot(user_01220461177_73.createdAt, target_data/max(target_data), label=i.split('contents')[1])
plt.legend()


### 3.2 Other than sleep
plt.figure(figsize=(16,8))
for i in user_01220461177_73:
    if 'contents' in i:
        target_data = user_01220461177_73[i].values
        plt.plot(user_01220461177_73.createdAt, target_data/max(target_data), label=i.split('contents')[1])
plt.legend(loc=1)


## 4.2 `eventType=8`
### Weekly tendency of a user
user_01220461177_8['weekday_hour'] = user_01220461177_8.createdAt.map(lambda x: str(x.weekday()) + datetime.strftime(x, '%H'))
user_01220461177_8_groupby_weekday_hour = user_01220461177_8.groupby('weekday_hour')

mean = user_01220461177_8_groupby_weekday_hour.mean().contentsCurrAct
std = user_01220461177_8_groupby_weekday_hour.std().contentsCurrAct
plt.figure(figsize=(16,8))
plt.subplot(2,1,1)
plt.plot(mean.index, mean)
plt.xticks([mean.index[0], mean.index[int(len(mean.index)/2)], mean.index[-1]])
plt.subplot(2,1,2)
plt.plot(mean.index, mean)
plt.fill_between(mean.index, mean-std, mean+std, alpha=0.2)
plt.xticks([mean.index[0], mean.index[int(len(mean.index)/2)], mean.index[-1]])
plt.vlines(x=[str(i) + '00' for i in range(7)], ymin=0, ymax=15, color='red')


### Weekly tendency of the entire users
sensor_df_eventType8 = change2df(8)
sensor_df_eventType8['weekday_hour'] = sensor_df_eventType8.createdAt.map(lambda x: str(x.weekday()) + datetime.strftime(x, '%H'))
sensor_df_eventType8_groupby_weekday_hour = sensor_df_eventType8.groupby('weekday_hour')

mean = sensor_df_eventType8_groupby_weekday_hour.mean().contentsCurrAct
std = sensor_df_eventType8_groupby_weekday_hour.std().contentsCurrAct
plt.figure(figsize=(16,8))
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
plt.figure(figsize=(16,4))
plt.plot(range(24), mean)
plt.figure(figsize=(16,4))
plt.plot(range(24), mean)
plt.fill_between(range(24), mean-std, mean+std, alpha=0.2)


### Daily tendency of the entire users
sensor_df_eventType8['hour'] = sensor_df_eventType8.createdAt.map(lambda x: datetime.strftime(x, '%H'))
sensor_df_eventType8_groupby_hour = sensor_df_eventType8.groupby('hour')

mean = sensor_df_eventType8_groupby_hour.mean().contentsPrevAct
std = sensor_df_eventType8_groupby_hour.std().contentsPrevAct
plt.figure(figsize=(16,8))
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
# plt.savefig("./FigureCompilation/daily_act_day_eT8")



# ## 4.3 `eventType=73`
# Unfortunately, the number of data for `eventType=73` is quite small, so that there is no much meaningful analysis to conduct. However, here I present some data analysis that could be useful later when the data size gets larger.

user_01220461177_73 = change2df(73, '01220461177')
contents_list_73 = [i for i in user_01220461177_73.columns if 'contents' in i]

user_01220461177_73['hour'] = user_01220461177_73.createdAt.map(lambda x: datetime.strftime(x, '%H'))
user_01220461177_73['dayHour'] = user_01220461177_73.createdAt.map(lambda x: str(x.weekday()) + datetime.strftime(x, '%H'))

def getNormalizedResult73(content, period):
    content_time_data = user_01220461177_73[[period, content]]
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
    plt.figure(figsize=(16,8))
    mean_content_time_data, std_content_time_data = getNormalizedResult73(content, period)
    plt.plot(mean_content_time_data.index, mean_content_time_data, 'o-', label=content.split('contents')[1])
    plt.fill_between(mean_content_time_data.index,
                     (mean_content_time_data-std_content_time_data)[content],
                     (mean_content_time_data+std_content_time_data)[content], alpha=0.5)

    plt.xlabel("Hour")
    plt.ylabel("Normalized Activity")
    plt.legend


### Daily normalized activity
plt.figure(figsize=(16,8))
for content in contents_list_73:
    mean_content_time_data, std_content_time_data = getNormalizedResult73(content, 'hour')
    plt.plot(mean_content_time_data, 'o-', label=content.split('contents')[1])
#     plt.fill_between(normalized_mean)
plt.legend(loc=4)
plt.xlabel('Hour')
plt.ylabel('Normalized activity')


### Weekly normalized activity
plt.figure(figsize=(16,8))
for content in contents_list_73:
    mean_content_time_data, std_content_time_data = getNormalizedResult73(content, 'dayHour')
    plt.plot(mean_content_time_data, 'o-', label=content.split('contents')[1])
    plt.xticks([str(i) + '00' for i in range(7)])
#     plt.fill_between(normalized_mean)
plt.legend(loc=4)
plt.xlabel('Hour')
plt.ylabel('Normalized activity')
