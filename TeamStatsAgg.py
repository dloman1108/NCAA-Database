#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 07:46:31 2018

@author: dh08loma
"""



import numpy as np
import pandas as pd
import sqlalchemy as sa 
import os
import yaml


def get_engine():
	#Get credentials stored in sql.yaml file (saved in root directory)
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


def calculate_team_stats(engine):
    team_stats_agg_query='''
    with possessions as (
        	select
        		tb.team_id
        		,gs.season
        		,gs.game_type
        		,.5*(sum(fga) + 0.475*sum(fta) - sum(orb) + sum(tov)) + .5*(sum(fga_opp) + 0.475*sum(fta_opp) - sum(orb_opp) + sum(tov_opp)) poss         
        	from
        		ncaa.team_boxscores tb
        	join
        		ncaa.game_summaries gs on tb.game_id=gs.game_id and gs.status='Final' and gs.ncaa_tournament_flg=False
        	group by
        		tb.team_id
        		,gs.season
        		,gs.game_type
        )
        
    select
    	tb.team
        ,tb.team_id
    	,gs.season
    	,gs.game_type
    	,count(*) gp
    	,sum(case when tb.pts > tb.pts_opp then 1 else 0 end) wins
    	,sum(case when tb.pts < tb.pts_opp then 1 else 0 end) losses
    	,avg(case when tb.pts > tb.pts_opp then 1.0 else 0 end) win_pct
    	,avg(tb.fgm*1.0) fgm
    	,avg(tb.fga*1.0) fga
    	,sum(tb.fgm)*1.0/sum(tb.fga) fg_pct
    	,avg(tb.fg3m*1.0) fg3m
    	,avg(tb.fg3a*1.0) fg3a
    	,sum(tb.fg3m)*1.0/sum(tb.fg3a) fg3_pct
    	,avg(tb.ftm*1.0) ftm
    	,avg(tb.fta*1.0) fta
    	,sum(tb.ftm)*1.0/sum(tb.fta) ft_pct
    	,avg(tb.pts) pts
    	,avg(tb.oreb)+avg(tb.dreb) reb
    	,avg(tb.oreb) oreb
    	,avg(tb.dreb) dreb
    	,avg(tb.ast) ast
    	,avg(tb.stl) stl
    	,avg(tb.blk) blk
    	,avg(tb.tov) tov
    	,avg(tb.pf) pf
    	,avg(tb.tech_fl) tech_fl
    	,avg(tb.flag_fl) flag_fl
    	,avg(tb.fgm_opp*1.0) fgm_opp
    	,avg(tb.fga_opp*1.0) fga_opp
    	,sum(tb.fgm_opp)*1.0/sum(tb.fga_opp) fg_pct_opp
    	,avg(tb.fg3m_opp*1.0) fg3m_opp
    	,avg(tb.fg3a_opp*1.0) fg3a_opp
    	,sum(tb.fg3m_opp)*1.0/sum(tb.fg3a_opp) fg3_pct_opp
    	,avg(tb.ftm_opp*1.0) ftm_opp
    	,avg(tb.fga_opp*1.0) fta_opp
    	,sum(tb.ftm_opp)*1.0/sum(tb.fta_opp) ft_pct_opp
    	,avg(tb.pts_opp) pts_opp
    	,avg(tb.oreb_opp)+avg(tb.dreb_opp) reb_opp
    	,avg(tb.oreb_opp) oreb_opp
    	,avg(tb.dreb_opp) dreb_opp
    	,avg(tb.ast_opp) ast_opp
    	,avg(tb.stl_opp) stl_opp
    	,avg(tb.blk_opp) blk_opp
    	,avg(tb.tov_opp) tov_opp
    	,avg(tb.pf_opp) pf_opp
    	,avg(tb.tech_fl_opp) tech_fl_opp
    	,avg(tb.flag_Fl_opp) flag_fl_opp
    	--Get 4 factors + ratings
    	,sum(pb.mp) mp
    	,p.poss
    	,case when sum(pb.mp) > 0 then 40*((p.poss*2)/(2*(sum(pb.mp)/5.0))) else null end pace
    	,sum(tb.pts)/p.poss*100 off_ftg
    	,sum(tb.pts_opp)*100.0/p.poss def_rtg
    	,(sum(tb.pts)-sum(tb.pts_opp))/p.poss*100 net_rtg
    	,sum(tb.fg3a)*1.0/sum(tb.fga) fg3_rate
    	,sum(tb.fta)*1.0/sum(tb.fga) ft_rate
    	,(sum(tb.fgm)+.5*sum(tb.fg3m))/sum(tb.fga) efg_pct
    	,sum(tb.tov)/(sum(tb.fga)+.475*sum(tb.fta)+sum(tb.tov)) tov_pct
    	,sum(tb.oreb)*1.0/(sum(tb.oreb)+sum(tb.dreb_opp)) oreb_pct
    	,sum(tb.ftm)*1.0/sum(tb.fga) ff_ft_rate
    	,sum(tb.fg3a_opp)*1.0/sum(tb.fga_opp) fg3_rate_opp
    	,sum(tb.fga_opp)*1.0/sum(tb.fga_opp) ft_rate_opp
    	,(sum(tb.fgm_opp)+.5*sum(tb.fg3m_opp))/sum(tb.fga_opp) efg_pct_opp
    	,sum(tb.tov_opp)/(sum(tb.fga_opp)+.475*sum(tb.fta_opp)+sum(tb.tov_opp)) tov_pct_opp
    	,sum(tb.oreb_opp)*1.0/(sum(tb.oreb_opp)+sum(tb.dreb)) oreb_pct_opp
    	,sum(tb.ftm_opp)*1.0/sum(tb.fga_opp) ff_ft_rate_opp
    from
    	ncaa.team_boxscores tb
    join
    	ncaa.game_summaries gs on tb.game_id=gs.game_id and gs.status='Final' and gs.ncaa_tournament_flg=False
    left join 
    	(select game_id,team_id,sum(cast(mp as float)) mp from ncaa.player_boxscores group by game_id,team_id) pb on tb.game_id=pb.game_id and tb.team_id=pb.team_id
    join
    	possessions p on tb.team_id=p.team_id and gs.season=p.season and gs.game_type=p.game_type
    group by
    	tb.team
    	,tb.team_id
    	,gs.season
    	,gs.game_type
    	,p.poss
    having
    	sum(tb.fta) > 0
    	and sum(tb.fga) > 0
    	and sum(tb.fg3a) > 0
    and sum(tb.fta_opp) > 0
	and sum(tb.fga_opp) > 0
	and sum(tb.fg3a_opp) > 0
    and (sum(tb.oreb_opp)+sum(tb.dreb)) > 0
	'''

    team_stats_agg=pd.read_sql(team_stats_agg_query,engine)

    team_stats_agg.to_sql('team_stats_agg',con=engine,schema='ncaa',index=False,if_exists='replace')
	

def main():
    engine=get_engine()
    calculate_team_stats(engine)
    
    
if __name__ == "__main__":
    main()
    
    
