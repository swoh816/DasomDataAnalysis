import pymysql
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# Korean font
import matplotlib.font_manager as fm
from sklearn.preprocessing import MinMaxScaler
path = '/Library/Fonts/Arial Unicode.ttf'
fontprop = fm.FontProperties(fname=path, size=10)


mysql = pymysql.connect(
    host='104.199.189.13',
    user='root',
    password='1thefull!',
    db='silverCare'
)



class sensor_data_analysis:
    ''' Module to recreate the dataframe and create figures
    Important parameters:
        eventType: 8, 73
        analysis_content:   if eventType==8, ['contentsRoom', 'contentsCurrAct', 'contentsPrevAct']
                            if eventType==73, ['contentsStep', 'contentsDistance', 'contentsCalory', 'contentsHrm', 'contentsBattery',
                                'contentsCondition', 'contentsTotalSleep', 'contentsTotalDeepSleep', 'contentsTotalLightSleep']
        period: regDate, hour, dayHour


    Module details:
        change2df:
        1. silverCare database contains important information (`contents`) in `list` format. Split the list to different columns, and put different column names according to the `eventType`. Drop `contents` column when the split is done.
        2. Change the type of `regDate` from string to datetime.
        3. Convert the type of contents from string to integer.

        phone_number_list: Show registered phone numbers in the database

        convertContents2Float: Change the type of contents from string to float.

        getPeriodicResult: Normalize contents data

        plotContent: Returns plot based on the getPeriodicResult





    Example:
    target_analysis = sensor_data_analysis(eventType, phone_number)
    target_df = target_analysis.target_df
    normalized_mean_content_time_data, std_content_time_data = target_analysis.getPeriodicResult(self, analysis_content, period)
    target_analysis.plotContent(self, analysis_content, period):
    '''

    def __init__(self, eventType, phone_number=None):
        self.eventType = eventType
        if eventType != 8 and eventType != 73:
            raise ValueError('Unknown eventType')

        self.phone_number = phone_number
        if phone_number:
            self.sensor_df = pd.read_sql_query('select * from SensorData where eventType = {} and phone = {}'.format(eventType, phone_number), mysql)
        else:
            self.sensor_df = pd.read_sql_query('select * from SensorData where eventType = {}'.format(eventType), mysql)

        self.target_df = self.change2df()



    def phone_number_list():
        """List of unique phone numbers"""
        return pd.read_sql_query('select distinct phone from SensorData', mysql)



    def convertContents2Float(self, contents):
        try:
            return float(contents)
        except:
            if contents == 'null':
                return np.nan
            else:
                return contents



    def change2df(self):
        if self.phone_number:
            target_df = self.sensor_df[(self.sensor_df.eventType == self.eventType) & (self.sensor_df.phone == self.phone_number)]
        else:
            target_df = self.sensor_df[self.sensor_df.eventType == self.eventType]
        target_df.reset_index(inplace=True)
        target_df.regDate = target_df.regDate.map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))

        if self.eventType == 8:
            converted_columns = ['contentsRoom', 'contentsCurrAct', 'contentsPrevAct']
        elif self.eventType == 73:
            converted_columns = ['contentsStep', 'contentsDistance', 'contentsCalory',
                                   'contentsHrm', 'contentsBattery',
                                   'contentsCondition', 'contentsTotalSleep',
                                   'contentsTotalDeepSleep', 'contentsTotalLightSleep']
        else:
            raise ValueError("Eventtype {} is not available".format(self.eventType))
            
        temp_df = pd.DataFrame(target_df.contents.map(lambda x: x.split(';')).values.tolist(), columns=converted_columns)
        for column in temp_df:
            temp_df[column] = temp_df[column].map(lambda x: self.convertContents2Float(x))
        target_df = pd.concat([target_df, temp_df], axis=1)
        target_df.drop(columns=['contents'], inplace=True)
        target_df = target_df[['id', 'phone', 'devType', 'macAddr', 'eventType'] + converted_columns +                         ['commState', 'powerState', 'batState', 'regDate', 'createdAt', 'updatedAt']]
        if self.eventType == 8:
            target_df.contentsRoom = target_df.contentsRoom.astype('category')
       
        return target_df



    def getPeriodicResult(self, analysis_content, period, normalize=True):
        if period != 'regDate':
            self.target_df['daily'] = self.target_df.createdAt.map(lambda x: datetime.strftime(x, '%H'))
            self.target_df['weekly'] = self.target_df.createdAt.map(lambda x: str(x.weekday()) + datetime.strftime(x, '%H'))
        content_time_data = self.target_df[[period, analysis_content]]
        mean_content_time_data = content_time_data.groupby(period).mean()
        if normalize == True:
            mean_content_time_data = mean_content_time_data/max(mean_content_time_data.values)
        if period != 'regDate':
            std_content_time_data = content_time_data.groupby(period).std()
            if normalize == True:
                std_content_time_data = std_content_time_data/max(std_content_time_data.values)
        else:
            std_content_time_data = np.nan
        
        # Insert empty time slots with `np.nan`
        idx_list = mean_content_time_data.index
        if period == 'daily':
            time_list = range(24)
        elif period == 'weekly':
            time_list = []
            for j in range(7):
                time_list.extend([str(j) + str(i).zfill(2) for i in range(24)])
            
        if period != 'regDate':
            for time in time_list:
                zfill_time = str(time).zfill(2)
                if zfill_time not in idx_list:
                    mean_content_time_data.loc[zfill_time] = np.nan
                    std_content_time_data.loc[zfill_time] = np.nan
            mean_content_time_data = mean_content_time_data.sort_index()
            std_content_time_data = std_content_time_data.sort_index()
            
        return mean_content_time_data, std_content_time_data



    def plotContent(self, analysis_content_list, period, std=True, normalize=True):
        if 'contentsRoom' in analysis_content_list:
            raise ValueError("Use `plotRoom` to plot room status")

        plt.figure(figsize=(16,8))
        for analysis_content in analysis_content_list:
            mean_content_time_data, std_content_time_data = self.getPeriodicResult(analysis_content, period, normalize)
            ymin = (mean_content_time_data-std_content_time_data)[analysis_content]
            ymax = (mean_content_time_data+std_content_time_data)[analysis_content]
            plt.plot(mean_content_time_data.index, mean_content_time_data, 'o-', label=analysis_content.split('contents')[1])
            if std:
                plt.fill_between(mean_content_time_data.index, ymin, ymax, alpha=0.3)
        if period == 'weekly':
            plt.xticks([str(i)+'00' for i in range(7)], ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
            # plt.vlines([str(i)+'00' for i in range(7)], ymin=min(ymin)/3,
            #           ymax=max(ymax)/3, color='red')
        plt.xlabel("Hour")
        plt.ylabel("Normalized Activity")
        plt.legend()



    def plotRoom(self):
        self.target_df['hour'] = self.target_df.regDate.dt.floor('12H')
        self.target_df['hour'] = self.target_df['hour'].map(lambda x: x.hour)
        self.target_df['weekday'] = self.target_df.regDate.dt.weekday
        data_of_interest = self.target_df.groupby(['hour', 'weekday', 'contentsRoom']).count()['id'].values.reshape(-1, 4)
        scaler = MinMaxScaler()
        scaler.fit(data_of_interest.T)

        plt.figure(figsize=(20,8))
        plt.imshow(scaler.transform(data_of_interest.T))
        plt.colorbar()
        plt.yticks([0, 1, 2, 3], self.target_df.contentsRoom.dtypes.categories, fontproperties=fontprop)
        plt.ylim(-.5, 3.5)
        plt.xticks([2*i+.5 for i in range(7)], ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
        plt.ylabel("Location")
