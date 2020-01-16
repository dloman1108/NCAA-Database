# -*- coding: utf-8 -*-
"""
Created on Sat Mar  3 18:52:00 2018

@author: DanLo1108
"""




from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from urllib.request import urlopen
import re
import sqlalchemy as sa
import yaml
import os


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
    
    
    page = urlopen(url)
    
    content=page.read()
    soup=BeautifulSoup(content,'lxml')
    
    
    tables=soup.find_all('table')
    
    results_head=[re.sub('\t|\n','',el.string) for el in tables[0].find_all('td')]        
    
    teams=[]
    for res in results_head:
        try:
            a=int(res)
        except:
            teams.append(res)
            
    team_info=soup.find_all('a',{'class':'team-name'})
    
    for ind in [1,2]:
        results=[el.string for el in tables[ind].find_all('td')]
        
        try:
            ind_stop=min([i for i in range(len(results)) if pd.notnull(results[i]) and ('DNP-' in results[i] or 'Did not play' in results[i])])-1
        except:
            ind_stop=min([i for i in range(len(results)) if pd.notnull(results[i]) and results[i] == 'TEAM'])
            
        ind_team=min([i for i in range(len(results)) if pd.notnull(results[i]) and results[i] == 'TEAM'])
            
        
        player_stats_df=pd.DataFrame(np.array_split(results[:ind_stop],ind_stop/14.),
                        columns=['player','mp','fg','fg3','ft',
                                 'oreb','dreb','reb','ast','stl','blk',
                                 'tov','pf','pts'])
                                 
        for col in player_stats_df:
            try:
                player_stats_df[col]=list(map(lambda x: float(x),player_stats_df[col]))
            except:
                continue
            
        if ind_stop != ind_team:
            dnp_df=pd.DataFrame(np.array_split(results[ind_stop:ind_team],(ind_team-ind_stop)/2.),
                   columns=['player','dnp_reason'])
        else:
            dnp_df=pd.DataFrame(columns=['player','dnp_reason'])
                
        player_stats_df=player_stats_df.append(dnp_df).reset_index(drop=True)
        
        player_stats_df['player']=[el.string for el in tables[ind].find_all('span')][0::3][:len(player_stats_df)]
        
        try:
            player_stats_df['player_id']=[el['href'][el['href'].find('id')+3:el['href'].find('id')+3+el['href'][el['href'].find('id')+3:].find('/')] for el in tables[ind].find_all('a',href=True)][:len(player_stats_df)]
        except:
            player_stats_df['player_id']=[el['href'][36:] for el in tables[ind].find_all('a',href=True)][:len(player_stats_df)]          
        #player_stats_df['PlayerAbbr']=[el['href'][36:][el['href'][36:].index('/')+1:] for el in tables[ind].find_all('a',href=True)][:len(player_stats_df)]      
        
        try:
            player_stats_df['position']=[el.string for el in tables[ind].find_all('span')][2::3][:len(player_stats_df)]
        except:
            spans=[el.string for el in tables[ind].find_all('span')]
            pos=[]
            for i in range(1,len(spans)):
                if spans[i] in ['PG','SG','SF','PF','C','G','F']:
                    pos.append(spans[i])
                elif spans[i-1] not in ['PG','SG','SF','PF','C','G','F'] and spans[i] not in ['PG','SG','SF','PF','C','G','F'] and spans[i] != spans[i-1]:
                    pos.append(None)
                
            if len(pos)==len(player_stats_df):
                player_stats_df['position']=pos
            elif len(pos)==len(player_stats_df) - 1:
                player_stats_df['position']=pos+[None]
            else:
                player_stats_df['position']=[None]*len(player_stats_df)
            
        player_stats_df=player_stats_df.replace('-----','0-0').replace('--',0)
        
        if len(team_info) == 2:
            player_stats_df['team']=team_info[ind-1].find_all('span',{'class':'abbrev'})[0].text
            player_stats_df['team_id']=team_info[ind-1]['data-clubhouse-uid'][12:]
        else:
            if team_info[0].find_all('span',{'class':'abbrev'})[0].text == teams[ind-1]:
                player_stats_df['team']=team_info[0].find_all('span',{'class':'abbrev'})[0].text
                player_stats_df['team_id']=team_info[0]['data-clubhouse-uid'][12:]
            else:
                player_stats_df['team']=teams[ind-1]
                player_stats_df['team_id']=None
        
        
        player_stats_df['game_id']=game_id
                
                
        player_stats_df['fgm']=player_stats_df.apply(lambda x: get_made(x,'fg'), axis=1)
        player_stats_df['fga']=player_stats_df.apply(lambda x: get_attempts(x,'fg'), axis=1)
        
        player_stats_df['fg3m']=player_stats_df.apply(lambda x: get_made(x,'fg3'), axis=1)
        player_stats_df['fg3a']=player_stats_df.apply(lambda x: get_attempts(x,'fg3'), axis=1)
        
        player_stats_df['ftm']=player_stats_df.apply(lambda x: get_made(x,'ft'), axis=1)
        player_stats_df['fta']=player_stats_df.apply(lambda x: get_attempts(x,'ft'), axis=1)
        
        player_stats_df['starter_flg']=[1.0]*5+[0.0]*(len(player_stats_df)-5)
        
        column_order=['game_id','player','player_id','position','team','team_id','starter_flg',
                      'mp','fg','fgm','fga','fg3','fg3m','fg3a','ft','ftm','fta',
                      'oreb','dreb','reb','ast','stl','blk','tov','pf','pts','dnp_reason']
        
        player_stats_df[column_order].to_sql('player_boxscores',
                                             con=engine,
                                             schema='ncaa',
                                             index=False,
                                             if_exists='append',
                                             dtype={'game_id': sa.types.INTEGER(),
                                                    'player': sa.types.VARCHAR(length=255),
                                                    'player_id': sa.types.INTEGER(),
                                                    'position': sa.types.CHAR(length=5),
                                                    'team': sa.types.VARCHAR(length=255),
                                                    'team_id':sa.types.INTEGER(),
                                                    'starter_flg': sa.types.BOOLEAN(),
                                                    'mp': sa.types.INTEGER(),
                                                    'fg': sa.types.VARCHAR(length=255),
                                                    'fgm': sa.types.INTEGER(),
                                                    'fga': sa.types.INTEGER(),
                                                    'fg3': sa.types.VARCHAR(length=255),
                                                    'fg3m': sa.types.INTEGER(),
                                                    'fg3a': sa.types.INTEGER(),
                                                    'ft': sa.types.VARCHAR(length=255),
                                                    'ftm': sa.types.INTEGER(),
                                                    'fta': sa.types.INTEGER(),
                                                    'oreb': sa.types.INTEGER(),
                                                    'dreb': sa.types.INTEGER(),
                                                    'reb': sa.types.INTEGER(),
                                                    'ast': sa.types.INTEGER(),
                                                    'stl': sa.types.INTEGER(),
                                                    'blk': sa.types.INTEGER(),
                                                    'tov': sa.types.INTEGER(),
                                                    'pf': sa.types.INTEGER(),
                                                    'pts': sa.types.INTEGER(),
                                                    'dnp_reason': sa.types.VARCHAR(length=255)}) 


def get_engine():
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
    
    return engine



def get_gameids(engine):
    
    game_id_query='''

    select distinct
        gs.season
        ,gs.game_id
    from
        ncaa.game_summaries gs
    left join
        ncaa.player_boxscores p on gs.game_id=p.game_id 
    left join
        ncaa.bad_gameids b on gs.game_id=b.game_id and b.table='player_boxscores'
    where
        p.game_id is Null
        and b.game_id is Null
        and gs.status='Final'
    order by
        gs.season
        
    '''
    
    game_ids=pd.read_sql(game_id_query,engine)
    
    return game_ids.game_id.tolist()



def update_player_boxscores(engine,game_id_list):
    cnt=0
    print('Total Games: ',len(game_id_list))
    for game_id in game_id_list:
    
        try:
            append_boxscores(game_id,engine)
            cnt+=1
            if np.mod(cnt,100)==0:
                print(str(round(float(cnt*100.0/len(game_id_list)),2))+'%')
            
        except:
            bad_gameid_df=pd.DataFrame({'game_id':[game_id],'table':['player_boxscores']})
            bad_gameid_df.to_sql('bad_gameids',
                                  con=engine,
                                  schema='ncaa',
                                  index=False,
                                  if_exists='append',
                                  dtype={'game_id': sa.types.INTEGER(),
                                         'table': sa.types.VARCHAR(length=255)})
            cnt+=1
            if np.mod(cnt,100) == 0:
                print(str(round(float(cnt*100.0/len(game_id_list)),2))+'%')
            continue


def main():
    engine=get_engine()
    game_ids=get_gameids(engine)
    update_player_boxscores(engine,game_ids)
    
    
    
if __name__ == "__main__":
    main()


