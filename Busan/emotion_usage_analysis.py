import pymysql
import pandas as pd
import datetime
import numpy as np
import random as rd

import matplotlib.pyplot as plt
import matplotlib


# Emotion data database
mysql = pymysql.connect(
    host='35.201.169.147',
    user='root',
    password='1thefull!',
    db='silverCare'
)


emotion_df = pd.read_sql_query("SELECT b.SORT,                                     a.REGISTRATION_DATE,       a.PUDDING_SERIALNUM,       CASE IFNULL(EMOTION, '')                  WHEN '긍정' THEN 1 * EMOTION_SCORE                  WHEN '부정' THEN -1 * EMOTION_SCORE                  ELSE 0                 END AS emotionScore FROM ROJA_HI_LOG a, PUDDING_SERIALNUM b WHERE a.PUDDING_SERIALNUM = b.PUDDING_SERIALNUM  AND a.SEND_SORT = 'asr'  AND a.SENDER <> 'ROJA'", mysql)


class investigate_emotion:
    """
    Use emotion/usage data collected by Dasom.
    Example:

    # Get the data of users by region
    busanjin_emotion = investigate_emotion('busanjin')
    gimpojin_emotion = investigate_emotion('Gimpo')

    # Plot the change of a user's emotion score by time
    plt.figure(figsize=(16, 8))
    random_user_plot = busanjin_emotion.userEmotionScore(rd.choice(busanjin_emotion.user_list), legend=True)

    plt.figure(figsize=(16, 8))
    for user in busanjin_emotion.user_list:
        random_user_plot = busanjin_emotion.userEmotionScore(user)

    # Plot the change of aggregated emotion score by time
    plt.figure(figsize=(16, 8))
    busanjin_emotion.totalEmotionScore()
    plt.hlines(y=0, xmin=busanjin_emotion.region_emotion_df.date.iloc[0],
             xmax=busanjin_emotion.region_emotion_df.date.iloc[-1], color="red")

    # Plot the change of a user's usage frequency by time
    busanjin_emotion.userFrequency(rd.choice(busanjin_emotion.user_list))

    # Plot the change of aggregated usage frequency by time
    busanjin_emotion.totalFrequency(normalize_by_people=True)
    """

    def __init__(self, region):
        self.region = region
        self.region_emotion_df = emotion_df[(emotion_df.SORT == self.region) &                                            (emotion_df.REGISTRATION_DATE > datetime.date(2019, 12, 1))]
        self.user_list = list(set(self.region_emotion_df.PUDDING_SERIALNUM))

        
        
    def userEmotionScore(self, user, legend=False):
        # self.region: Gimpo, busanjin, B2C
        user_specific_data = self.region_emotion_df[self.region_emotion_df.PUDDING_SERIALNUM == user]
        user_specific_data['date'] = user_specific_data.REGISTRATION_DATE.map(lambda x: x.date())
        mean_data = user_specific_data.groupby('date').mean()
        std_data = user_specific_data.groupby('date').std()
        mean_data = mean_data[mean_data.index>datetime.datetime(2019, 12, 1).date()]
        std_data = std_data[std_data.index>datetime.datetime(2019, 12, 1).date()]
        plt.plot(mean_data.index, mean_data.emotionScore, 'o-', label=user)
        plt.fill_between(mean_data.index, mean_data.emotionScore-std_data.emotionScore,
                         mean_data.emotionScore+std_data.emotionScore, alpha=0.3)
        plt.xticks(mean_data.index[[int((mean_data.index.__len__()-1)/3*i) for i in range(3)]])
        if legend == True:
            plt.legend()
            
        

    def totalEmotionScore(self, legend=False):
        # self.region: Gimpo, busanjin, B2C
        total_data = self.region_emotion_df
        total_data['date'] = total_data.REGISTRATION_DATE.map(lambda x: x.date())
        mean_data = total_data.groupby('date').mean()
        std_data = total_data.groupby('date').std()
        mean_data = mean_data[mean_data.index>datetime.datetime(2019, 12, 1).date()]
        std_data = std_data[std_data.index>datetime.datetime(2019, 12, 1).date()]
        plt.plot(mean_data.index, mean_data.emotionScore, 'o-')
        plt.fill_between(mean_data.index, mean_data.emotionScore-std_data.emotionScore,
                         mean_data.emotionScore+std_data.emotionScore, alpha=0.3)
        plt.xticks(mean_data.index[[int((mean_data.index.__len__()-1)/3*i) for i in range(3)]])
        if legend == True:
            plt.legend()
        

#     def emotionRatio(self, user): # Calculate the ratio of positive/negative ratio (dismiss emotion score)

        
    def userFrequency(self, user, alpha=None):
        user_specific_data = self.region_emotion_df[self.region_emotion_df.PUDDING_SERIALNUM == user]
        user_specific_data['date'] = user_specific_data.REGISTRATION_DATE.map(lambda x: x.date())
        count_per_date = user_specific_data.groupby('date').count()['REGISTRATION_DATE']
        count_per_date = count_per_date[count_per_date.index>datetime.datetime(2019, 12, 1).date()]
        
        plt.bar(count_per_date.index, count_per_date, alpha=alpha)
        plt.xticks([count_per_date.index[0], count_per_date.index[-1]])
        plt.ylabel("Number of use")
        plt.xlabel("Date")
        
    
    def totalFrequency(self, normalize_by_people=False):
        total_data = self.region_emotion_df
        total_data['date'] = total_data.REGISTRATION_DATE.map(lambda x: x.date())

        count_per_date = total_data.groupby('date').count()['REGISTRATION_DATE']
        count_per_date = count_per_date[count_per_date.index>datetime.datetime(2019, 12, 1).date()]

        if normalize_by_people:
            accumulate_count_df = pd.DataFrame(columns=['date', 'PUDDING_SERIALNUM'],
                               data=[list(i) for i in total_data.groupby(['date', 'PUDDING_SERIALNUM']).count().index.values])
            accumulate_count_df.set_index('date', inplace=True)
            accumulate_count_df = accumulate_count_df[accumulate_count_df.index > datetime.date(2019, 12, 1)]

            all_users = []
            prev_date = accumulate_count_df.index[0]
            accumulate_count = 0
            accumulate_count_list = []
            for count, (idx, row) in enumerate(accumulate_count_df.iterrows()):
                if idx != prev_date:
                    accumulate_count_list.append(accumulate_count)
                    prev_date = idx
                if row.PUDDING_SERIALNUM not in all_users:
                    all_users.append(row.PUDDING_SERIALNUM)
                    accumulate_count += 1
            accumulate_count_list.append(accumulate_count)

            plt.bar(count_per_date.index, count_per_date/np.array(accumulate_count_list))
        else:
            plt.bar(count_per_date.index, count_per_date)
        plt.xticks([count_per_date.index[0], count_per_date.index[-1]])
        plt.ylabel("Number of use")
        plt.xlabel("Date")
