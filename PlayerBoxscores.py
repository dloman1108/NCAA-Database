# -*- coding: utf-8 -*-
"""
Created on Sat Mar  3 18:52:00 2018

@author: DanLo1108
"""




from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import urllib2
import re
import string as st
import sqlalchemy as sa


def get_made(x,var):
    x_var=x[var]
    try:
        return int(x_var[:x_var.index('-')])
    except:
        return np.nan
            
def get_attempts(x,var):
    x_var=x[var]
    try:
        return int(x_var[x_var.index('-')+1:])
    except:
        return np.nan
                
                
def append_boxscores(game_id,engine):

    url='http://www.espn.com/mens-college-basketball/boxscore?gameId='+str(game_id)
    
    
    request=urllib2.Request(url)
    page = urllib2.urlopen(request)
    
    
    content=page.read()
    soup=BeautifulSoup(content,'lxml')
    
    
    tables=soup.find_all('table')
    
    results_head=[re.sub('\t|\n','',el.string) for el in tables[0].find_all('td')]        
    results_head_split=np.array_split(results_head,len(results_head)/5.)
            
    team_info=soup.find_all('a',{'class':'team-name'})
    
    for ind in [1,2]:
        results=[el.string for el in tables[ind].find_all('td')]
        
        try:
            ind_stop=min([i for i in range(len(results)) if pd.notnull(results[i]) and ('DNP-' in results[i] or 'Did not play' in results[i])])-1
        except:
            ind_stop=min([i for i in range(len(results)) if pd.notnull(results[i]) and results[i] == 'TEAM'])
            
        ind_team=min([i for i in range(len(results)) if pd.notnull(results[i]) and results[i] == 'TEAM'])
            
        
        player_stats_df=pd.DataFrame(np.array_split(results[:ind_stop],ind_stop/14.),
                        columns=['Player','MP','FG','3PT','FT',
                                 'OREB','DREB','REB','AST','STL','BLK',
                                 'TOV','PF','PTS'])
                                 
        for col in player_stats_df:
            try:
                player_stats_df[col]=map(lambda x: float(x),player_stats_df[col])
            except:
                continue
            
        if ind_stop != ind_team:
            dnp_df=pd.DataFrame(np.array_split(results[ind_stop:ind_team],(ind_team-ind_stop)/2.),
                   columns=['Player','DNP_Reason'])
        else:
            dnp_df=pd.DataFrame(columns=['Player','DNP_Reason'])
                
        player_stats_df=player_stats_df.append(dnp_df).reset_index(drop=True)
        
        player_stats_df['Player']=[el.string for el in tables[ind].find_all('span')][0::3][:len(player_stats_df)]
        player_stats_df['PlayerID']=[el['href'][el['href'].index('id/')+3:] for el in tables[ind].find_all('a',href=True)][:len(player_stats_df)][:len(player_stats_df)]      
        #player_stats_df['PlayerAbbr']=[el['href'][36:][el['href'][36:].index('/')+1:] for el in tables[ind].find_all('a',href=True)][:len(player_stats_df)]      
        
        try:
            player_stats_df['Position']=[el.string for el in tables[ind].find_all('span')][2::3][:len(player_stats_df)]
        except:
            spans=[el.string for el in tables[ind].find_all('span')]
            pos=[]
            for i in range(1,len(spans)):
                if spans[i] in ['PG','SG','SF','PF','C','G','F']:
                    pos.append(spans[i])
                elif spans[i-1] not in ['PG','SG','SF','PF','C','G','F'] and spans[i] not in ['PG','SG','SF','PF','C','G','F'] and spans[i] != spans[i-1]:
                    pos.append(None)
                
            if len(pos)==len(player_stats_df):
                player_stats_df['Position']=pos
            else:
                player_stats_df['Position']=pos+[None]
            
        player_stats_df=player_stats_df.replace('-----','0-0').replace('--',0)
        
        player_stats_df['Team']=team_info[ind-1].find_all('span',{'class':'abbrev'})[0].text
        player_stats_df['TeamID']=team_info[ind-1]['data-clubhouse-uid'][12:]
        player_stats_df['GameID']=game_id
                
                
        player_stats_df['FGM']=player_stats_df.apply(lambda x: get_made(x,'FG'), axis=1)
        player_stats_df['FGA']=player_stats_df.apply(lambda x: get_attempts(x,'FG'), axis=1)
        
        player_stats_df['3PTM']=player_stats_df.apply(lambda x: get_made(x,'3PT'), axis=1)
        player_stats_df['3PTA']=player_stats_df.apply(lambda x: get_attempts(x,'3PT'), axis=1)
        
        player_stats_df['FTM']=player_stats_df.apply(lambda x: get_made(x,'FT'), axis=1)
        player_stats_df['FTA']=player_stats_df.apply(lambda x: get_attempts(x,'FT'), axis=1)
        
        player_stats_df['StarterFLG']=[1.0]*5+[0.0]*(len(player_stats_df)-5)
        
        player_stats_df[['GameID','Player','PlayerID','Position','Team','TeamID','StarterFLG','MP','FG','FGM','FGA','3PT','3PTM','3PTA','FT','FTM','FTA',
                         'OREB','DREB','REB','AST','STL','BLK','TOV','PF','PTS','DNP_Reason']].to_sql('player_boxscores',con=engine,schema='ncaa',index=False,if_exists='append')
    


#Create PostgreSQL engine with SQL Alchemy. 
#Connects to database "BasketballStats" on AWS
username='dloman_bball'
password='gosaints'
endpoint='dlomandbinstance.cdusbgpuqzms.us-east-2.rds.amazonaws.com'
port='5432'
database='BasketballStats'

db_string = "postgres://{0}:{1}@{2}:{3}/{4}".format(username,password,endpoint,port,database)
engine=sa.create_engine(db_string)


#game_id_query='''
#
#select * from nba.game_summaries 
#where "Status"='Final'
#
#'''

game_id_query='''

select distinct
    gs."Season"
    ,gs."GameID"
from
    ncaa.game_summaries gs
left join
    ncaa.player_boxscores p on gs."GameID"=p."GameID" 
where
    p."GameID" is Null
    and gs."Status"='Final'
    and gs."Season"=(select max("Season") from ncaa.game_summaries)
order by
    gs."Season"

'''


game_ids=pd.read_sql(game_id_query,engine)


cnt=0
bad_gameids=[]
for game_id in game_ids.GameID.tolist():
    
    if np.mod(cnt,2000)==0:
        print 'CHECK: ',cnt,len(bad_gameids)

    try:
        append_boxscores(game_id,engine)
        cnt+=1
        if np.mod(cnt,100)==0:
            print str(round(float(cnt*100.0/len(game_ids)),2))+'%'
        
    except:
        bad_gameids.append(game_id)
        cnt+=1
        if np.mod(cnt,100) == 0:
            print str(round(float(cnt*100.0/len(game_ids)),2))+'%' 
        continue




