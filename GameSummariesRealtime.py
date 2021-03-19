# -*- coding: utf-8 -*-
"""
Created on Sat Mar  3 16:48:35 2018

@author: DanLo1108
"""

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from urllib.request import urlopen
import re
import os
import yaml
import sqlalchemy as sa
from datetime import date


#Function which takes a date string and appends game summaries
#to PostGres database
def append_game_summary(engine):
    
    #Define URL from ESPN
    url='https://www.espn.com/mens-college-basketball/scoreboard'
    
    #Get URL page 
    page = urlopen(url)
    
    #Get content from URL page
    content=page.read()
    soup=BeautifulSoup(content,'lxml')
    
    #Get scripts
    scripts=soup.find_all('script')
    
    #Get results from scripts
    results=[script.contents[0] for script in scripts if len(script.contents) > 0 and '{"leagues"' in script.contents[0]][0]
    #results=scripts[9].contents[0]
    results=results[results.index('{"leagues"'):results.index(';window.')]
    results=re.sub('false','False',results)
    results=re.sub('true','True',results)
    
    events=eval(results)['events']
    
    #Iterate through "events" i.e. games
    scoreboard_results=[]
    for event in events:
        game_id=event['id'] #Game ID
        datey=date.today()
        season=event['season']['year'] #Season
        
        #Get venue/attendance
        if 'venue' in event['competitions'][0]:
            venue=event['competitions'][0]['venue']['fullName']
            if 'address' in event['competitions'][0]['venue']:
                if 'state' in event['competitions'][0]['venue']['address'] and 'city' in event['competitions'][0]['venue']['address']:
                    location=event['competitions'][0]['venue']['address']['city']+', '+event['competitions'][0]['venue']['address']['state']
                else:
                    location=None
            else:
                location=None
            venue_id=event['competitions'][0]['venue']['id']
        else:
            venue=None
            location=None
            venue_id=None
        attendance=event['competitions'][0]['attendance']
        neutral_site_flg=event['competitions'][0]['neutralSite']
        
        #Get game type (preseason/reg season/postseason)
        if 'type' in event['season']:
            if event['season']['type']==1:
                game_type='Preseason'
            elif event['season']['type']==2:
                game_type='Regular Season'
            elif event['season']['type']==3:
                game_type='Postseason'
        else:
            game_type=None
        
        #Get long and short headlines for game
        if 'headlines' in event['competitions'][0]:
            if 'description' in event['competitions'][0]['headlines'][0]:
                headline_long=re.sub('&#39;',"'",event['competitions'][0]['headlines'][0]['description'])
            else:
                headline_long=None
            if 'shortLinkText' in event['competitions'][0]['headlines'][0]:
                headline_short=re.sub('&#39;',"'",event['competitions'][0]['headlines'][0]['shortLinkText'])
            else:
                headline_short=None
        else:
            headline_long=None
            headline_short=None
            
        if len(event['competitions'][0]['notes']) > 0:#'headline' in event['competitions'][0]['notes'][0]:
            notes=event['competitions'][0]['notes'][0]['headline']
        else:
            notes=None
            
        if 'broadcasts' in event['competitions'][0] and len(event['competitions'][0]['broadcasts']) > 0:
            broadcast=event['competitions'][0]['broadcasts'][0]['names'][0]
        else:
            broadcast=None
            
        #Get home team details (name, abbreviation, ID, score, WinFLG)
        home_team=event['competitions'][0]['competitors'][0]['team']['displayName']
        if 'abbreviation' in event['competitions'][0]['competitors'][0]['team']:
            home_team_abbr=event['competitions'][0]['competitors'][0]['team']['abbreviation']
        else:
            home_team_abbr=None
        home_team_id=event['competitions'][0]['competitors'][0]['team']['id']
        
        
        if 'logo' in event['competitions'][0]['competitors'][0]['team']:
            home_team_d1_flg=1
        else:
            home_team_d1_flg=0
        
        if 'conferenceId' in event['competitions'][0]['competitors'][0]['team']:
            home_team_conference_id=event['competitions'][0]['competitors'][0]['team']['conferenceId']
        else:
            home_team_conference_id=None
        
        home_team_score=event['competitions'][0]['competitors'][0]['score']
        if 'winner' in event['competitions'][0]['competitors'][0]:
            home_team_winner=event['competitions'][0]['competitors'][0]['winner']
        else:
            home_team_winner=None
            
            
        #Get away team details (name, abbreviation, ID, score, WinFLG)    
        away_team=event['competitions'][0]['competitors'][1]['team']['displayName']
        if 'abbreviation' in event['competitions'][0]['competitors'][1]['team']:
            away_team_abbr=event['competitions'][0]['competitors'][1]['team']['abbreviation']
        else:
            away_team_abbr=None
        away_team_id=event['competitions'][0]['competitors'][1]['team']['id']
        
        if 'logo' in event['competitions'][0]['competitors'][1]['team']:
            away_team_d1_flg=1
        else:
            away_team_d1_flg=0
            
        if 'conferenceId' in event['competitions'][0]['competitors'][1]['team']:
            away_team_conference_id=event['competitions'][0]['competitors'][1]['team']['conferenceId']
        else:
            away_team_conference_id=None
            
        away_team_score=event['competitions'][0]['competitors'][1]['score']
        if 'winner' in event['competitions'][0]['competitors'][1]:
            away_team_winner=event['competitions'][0]['competitors'][1]['winner']
        else:
            away_team_winner=None
            
        #Get series summary, if postseason game
        if 'conferenceCompetition' in event['competitions'][0]:
            conference_game=event['competitions'][0]['conferenceCompetition']
        else:
            conference_game=None
            
        if 'groups' in event['competitions'][0]:
            group_conference_flg=event['competitions'][0]['groups']['isConference']
            group_id=event['competitions'][0]['groups']['id']
            group_name=event['competitions'][0]['groups']['name']
        else:
            group_conference_flg=None
            group_id=None
            group_name=None
        
           
        #Get team records
        home_team_overall_record=None
        home_team_home_record=None
        home_team_away_record=None
        home_team_conference_record=None
        if 'records' in event['competitions'][0]['competitors'][0]:
            if len(event['competitions'][0]['competitors'][0]['records'])==4:
                home_team_overall_record=event['competitions'][0]['competitors'][0]['records'][0]['summary']
                home_team_home_record=event['competitions'][0]['competitors'][0]['records'][1]['summary']
                home_team_away_record=event['competitions'][0]['competitors'][0]['records'][2]['summary']
                home_team_conference_record=event['competitions'][0]['competitors'][0]['records'][3]['summary']
            elif len(event['competitions'][0]['competitors'][0]['records'])==2:
                home_team_overall_record=event['competitions'][0]['competitors'][0]['records'][0]['summary']
                home_team_conference_record=event['competitions'][0]['competitors'][0]['records'][1]['summary']

            
        away_team_overall_record=None
        away_team_home_record=None
        away_team_away_record=None
        away_team_conference_record=None
        if 'records' in event['competitions'][0]['competitors'][1]:
            if len(event['competitions'][0]['competitors'][1]['records'])==4:
                away_team_overall_record=event['competitions'][0]['competitors'][1]['records'][0]['summary']
                away_team_home_record=event['competitions'][0]['competitors'][1]['records'][1]['summary']
                away_team_away_record=event['competitions'][0]['competitors'][1]['records'][2]['summary']
                away_team_conference_record=event['competitions'][0]['competitors'][1]['records'][3]['summary']
            elif len(event['competitions'][0]['competitors'][0]['records'])==2:
                away_team_overall_record=event['competitions'][0]['competitors'][0]['records'][0]['summary']
                away_team_conference_record=event['competitions'][0]['competitors'][0]['records'][1]['summary']

        
            
        #Get game statuses - Completion and OT
        if 'status' in event:
            status=event['status']['type']['description']
            display_clock=event['status']['displayClock']
            try:
                ot_status=event['status']['type']['altDetail']
                period=event['status']['period']
            except:
                ot_status='Reg'
                period=None
        else:
            status=None
            ot_status=None
            period=None

        if 'situation' in event['competitions'][0]:
            try:
                home_win_pct=event['competitions'][0]['situation']['lastPlay']['probability']['homeWinPercentage']
                away_win_pct=event['competitions'][0]['situation']['lastPlay']['probability']['awayWinPercentage']
            except:
                home_win_pct=None
                away_win_pct=None
        else:
            home_win_pct=None
            away_win_pct=None
        
        home_team_rank_seed=event['competitions'][0]['competitors'][0]['curatedRank']['current']
        away_team_rank_seed=event['competitions'][0]['competitors'][1]['curatedRank']['current']
        
        if home_team_rank_seed == 99:
            home_team_rank_seed=np.nan
        
        if away_team_rank_seed == 99:
            away_team_rank_seed=np.nan
        
        postseason_tourney = 'NCAA'
        ncaa_tournament_flg=1
            
        #Append game results to list   
        scoreboard_results.append((game_id,status,ot_status,game_type,neutral_site_flg,datey,season,
                                  home_team,away_team,home_team_rank_seed,away_team_rank_seed,home_team_score,
                                  away_team_score,location,venue,venue_id,attendance,broadcast,
                                  headline_long,headline_short,home_team_abbr,home_team_id,home_team_conference_id,home_team_d1_flg,
                                  home_team_winner,away_team_abbr,
                                  away_team_id,away_team_conference_id,away_team_d1_flg,away_team_winner,conference_game,notes,
                                  group_conference_flg,group_id,group_name,
                                  home_team_overall_record,home_team_conference_record,home_team_home_record,home_team_away_record,
                                  away_team_overall_record,away_team_conference_record,away_team_home_record,away_team_away_record,
                                  postseason_tourney,ncaa_tournament_flg,home_win_pct,away_win_pct,display_clock,period))
    
    #Define column names
    col_names=['game_id','status','status_detail','game_type','neutral_site_flg',
               'date','season','home_team','away_team','home_team_rank_seed','away_team_rank_seed',
               'home_team_score','away_team_score','location','venue','venue_id','attendance',
               'broadcast','headline_long','headline_short','home_team_abbr','home_team_id',
               'home_team_conference_id','home_team_d1_flg','home_team_winner','away_team_abbr',
               'away_team_id','away_team_conference_id','away_team_d1_flg','away_team_winner',
               'conference_game_flg','notes','group_conference_flg','group_id','group_name',
               'home_team_overall_record','home_team_conference_record','home_team_home_record',
               'home_team_away_record','away_team_overall_record','away_team_conference_record',
               'away_team_home_record','away_team_away_record','postseason_tourney','ncaa_tournament_flg',
               'home_team_win_pct','away_team_win_pct','display_clock','period']  
     
    #Save all games for date to DF                           
    scoreboard_results_df=pd.DataFrame(scoreboard_results,columns=col_names)
    
    #Append dataframe results to PostGres database
    scoreboard_results_df.to_sql('game_summaries_realtime',
                                 con=engine,schema='ncaa',
                                 index=False,
                                 if_exists='replace',
                                 dtype={'game_id': sa.types.INTEGER(),
                                        'status': sa.types.VARCHAR(length=255),
                                        'status_detail': sa.types.VARCHAR(length=255),
                                        'game_type': sa.types.VARCHAR(length=255),
                                        'neutral_site_flg': sa.types.BOOLEAN(), 
                                        'date': sa.types.Date(),
                                        'season': sa.types.INTEGER(),
                                        'home_team': sa.types.VARCHAR(length=255),
                                        'away_team': sa.types.VARCHAR(length=255),
                                        'home_team_rank_seed': sa.types.INTEGER(), 
                                        'away_team_rank_seed': sa.types.INTEGER(),
                                        'home_team_score': sa.types.INTEGER(),
                                        'away_team_score': sa.types.INTEGER(),
                                        'location': sa.types.VARCHAR(length=255),
                                        'venue': sa.types.VARCHAR(length=255),
                                        'venue_id': sa.types.INTEGER(),
                                        'attendance': sa.types.INTEGER(),
                                        'broadcast': sa.types.VARCHAR(length=255),
                                        'headline_long': sa.types.VARCHAR(length=255),
                                        'headline_short': sa.types.VARCHAR(length=255),
                                        'home_team_abbr': sa.types.VARCHAR(length=255),
                                        'home_team_id': sa.types.INTEGER(),
                                        'home_team_conference_id': sa.types.INTEGER(),
                                        'home_team_d1_flg': sa.types.BOOLEAN(), 
                                        'home_team_winner': sa.types.BOOLEAN(),
                                        'away_team_abbr': sa.types.VARCHAR(length=255),
                                        'away_team_id': sa.types.INTEGER(),
                                        'away_team_conference_id': sa.types.INTEGER(),
                                        'away_team_d1_flg': sa.types.BOOLEAN(), 
                                        'away_team_winner': sa.types.BOOLEAN(),
                                        'conference_game_flg': sa.types.BOOLEAN(),
                                        'notes': sa.types.VARCHAR(length=255),
                                        'group_conference_flg': sa.types.BOOLEAN(),
                                        'group_id': sa.types.INTEGER(),
                                        'group_name': sa.types.VARCHAR(length=255),
                                        'home_team_overall_record': sa.types.VARCHAR(length=255),
                                        'home_team_conference_record': sa.types.VARCHAR(length=255),
                                        'home_team_home_record': sa.types.VARCHAR(length=255),
                                        'home_team_away_record': sa.types.VARCHAR(length=255),
                                        'away_team_overall_record': sa.types.VARCHAR(length=255),
                                        'away_team_conference_record': sa.types.VARCHAR(length=255),
                                        'away_team_home_record': sa.types.VARCHAR(length=255),
                                        'away_team_away_record': sa.types.VARCHAR(length=255),
                                        'postseason_tourney': sa.types.VARCHAR(length=255),
                                        'ncaa_tournament_flg': sa.types.BOOLEAN(),
                                        'home_team_win_pct': sa.types.FLOAT(),
                                        'away_team_win_pct': sa.types.FLOAT(),
                                        'display_clock': sa.types.VARCHAR(length=255),
                                        'period': sa.types.INTEGER()}
                                 )
    

#Get credentials stored in sql.yaml file (saved in root directory)
def get_engine():
    if os.path.isfile('/sql.yaml'):
        with open("/sql.yaml", 'r') as stream:
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


#Get max dates of games that were scheduled but not completed
from datetime import datetime
from datetime import date
    
def main():
    engine=get_engine()
    append_game_summary(engine)
    
    
    
if __name__ == "__main__":
    main() 




