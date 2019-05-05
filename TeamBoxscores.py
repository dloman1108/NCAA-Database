#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 07:38:56 2018

@author: dh08loma
"""





from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import urllib2
import re
import os
import yaml
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
        
        
def get_possessions(x):
    return .5*((x.FGA+0.475*x.FTA-x.OREB+x.TOV)+
               (x.FGA_opp+0.475*x.FTA_opp-x.OREB_opp+x.TOV_opp))
    

def get_winner(x):
    if x.PTS > x.PTS_opp:
        return 1
    else:
        return 0
        
def append_team_boxscores(game_id,engine):
    #if 1==1:
        
    url='http://www.espn.com/mens-college-basketball/matchup?gameId='+game_id
    
    request=urllib2.Request(url)
    page = urllib2.urlopen(request)
    
    
    content=page.read()
    soup=BeautifulSoup(content,'lxml')

    tables=soup.find_all('table')
    
    try:
        results_head=[re.sub('\t|\n','',el.string) for el in tables[0].find_all('td')]
        results_body=[re.sub('\t|\n','',el.string) for el in tables[1].find_all('td')]
    except:
        results_head=[el.string for el in tables[0].find_all('td')]
        results_body=[el.string for el in tables[1].find_all('td')]
        
    results_head_split=np.array_split(results_head,2.)
    
    #results_body[0::3]
    #results_body[1::3]
    #results_body[2::3]
    
    team_info=soup.find_all('a',{'class':'team-name'})
    
    tm1=team_info[0].find_all('span',{'class':'abbrev'})[0].text
    tm2=team_info[1].find_all('span',{'class':'abbrev'})[0].text
    
    tm1_id=team_info[0]['data-clubhouse-uid'][12:]
    tm2_id=team_info[1]['data-clubhouse-uid'][12:]
    
    tm1_pts=results_head_split[0][-1]
    tm2_pts=results_head_split[1][-1]
    
    team_stats_df=pd.DataFrame([([tm1,tm1_id,tm1_pts]+results_body[1::3]),
                  ([tm2,tm2_id,tm2_pts]+results_body[2::3])],
                columns=['Team','TeamID','PTS','FG','FG_Pct','FG3','FG3_Pct',
                         'FT','FT_Pct','REB','OREB','DREB','TmREB','AST','STL',
                         'BLK','TOV','PF','TechF','FlagF'])
                         
    for col in team_stats_df:
        try:
            team_stats_df[col]=map(lambda x: int(x), team_stats_df[col])
        except:
            continue
    
    team_stats_df['GameID']=game_id
    
    team_stats_df['FGM']=team_stats_df.apply(lambda x: get_made(x,'FG'), axis=1)
    team_stats_df['FGA']=team_stats_df.apply(lambda x: get_attempts(x,'FG'), axis=1)
    
    team_stats_df['3PTM']=team_stats_df.apply(lambda x: get_made(x,'FG3'), axis=1)
    team_stats_df['3PTA']=team_stats_df.apply(lambda x: get_attempts(x,'FG3'), axis=1)
    
    team_stats_df['FTM']=team_stats_df.apply(lambda x: get_made(x,'FT'), axis=1)
    team_stats_df['FTA']=team_stats_df.apply(lambda x: get_attempts(x,'FT'), axis=1)
    
    team_stats_df['OppTeam']=team_stats_df.Team.tolist()[::-1]
    
    team_stats_df=team_stats_df.merge(team_stats_df.drop(['Team','GameID'],axis=1),left_on='Team',right_on='OppTeam',suffixes=('', '_opp')).drop('OppTeam_opp',axis=1)   
    
    team_stats_df['Poss']=team_stats_df.apply(lambda x: get_possessions(x),axis=1)
    team_stats_df=team_stats_df.merge(team_stats_df[['OppTeam','Poss']],left_on='Team',right_on='OppTeam',suffixes=('', '_opp')).drop('OppTeam_opp',axis=1)   
    
    team_stats_df['ORTG']=team_stats_df.PTS/team_stats_df.Poss*100
    team_stats_df['DRTG']=team_stats_df.PTS_opp/team_stats_df.Poss*100
    team_stats_df['NetRTG']=(team_stats_df.PTS-team_stats_df.PTS_opp)/team_stats_df.Poss
    
    team_stats_df['IsWinnerFLG']=team_stats_df.apply(lambda x: get_winner(x),axis=1)
    
    team_stats_df.to_sql('team_boxscores',con=engine,schema='ncaa',index=False,if_exists='append')

        
    
        

#Get credentials stored in sql.yaml file (saved in root directory)
if os.path.isfile('/Users/dh08loma/Documents/Projects/Bracket Voodoo/sql.yaml'):
    with open("/Users/dh08loma/Documents/Projects/Bracket Voodoo/sql.yaml", 'r') as stream:
        data_loaded = yaml.load(stream)
        
        #domain=data_loaded['SQL_DEV']['domain']
        user=data_loaded['BBALL_STATS']['user']
        password=data_loaded['BBALL_STATS']['password']
        endpoint=data_loaded['BBALL_STATS']['endpoint']
        port=data_loaded['BBALL_STATS']['port']
        database=data_loaded['BBALL_STATS']['database']

db_string = "postgres://{0}:{1}@{2}:{3}/{4}".format(user,password,endpoint,port,database)
engine=sa.create_engine(db_string)


game_id_query='''
    select distinct
        gs."Season"
        ,gs."GameID"
    from
        ncaa.game_summaries gs
    left join
        ncaa.team_boxscores tb on gs."GameID"=tb."GameID" 
    where 1=1
        and tb."GameID" is Null
        and gs."Status"='Final'
        and gs."Season"=(select max("Season") from ncaa.game_summaries)
    order by
        gs."Season"
'''



game_ids=pd.read_sql(game_id_query,engine)


cnt=0
bad_gameids=[]
for game_id in game_ids.GameID.tolist():

    try:
        append_team_boxscores(game_id,engine)
        cnt+=1
        if np.mod(cnt,100)==0:
            print str(round(float(cnt*100.0/len(game_ids)),2))+'%'
        
    except:
        bad_gameids.append(game_id)
        cnt+=1
        if np.mod(cnt,100) == 0:
            print str(round(float(cnt*100.0/len(game_ids)),2))+'%' 
        continue
    
    
    
    




    
    