import awswrangler as wr
import pandas as pd
import json
import urllib.parse
import os
import boto3
import datetime
s3=boto3.client('s3')
# Common variables
glue_catalog_db_name = os.environ['glue_catalog_db_name']
write_data_operation = os.environ['write_data_operation']
# fixtures
s3_fixtures_preprocessed = os.environ['s3_fixtures_preprocessed']
glue_catalog_fixtures = os.environ['glue_catalog_table_name_fixtures']

# match_played
s3_match_played_preprocessed = os.environ['s3_match_played_preprocessed']
glue_catalog_match_played = os.environ['glue_catalog_table_name_match_played']

#match_lineups
s3_match_lineups_preprocessed = os.environ['s3_match_lineups_preprocessed']
glue_catalog_match_lineups = os.environ['glue_catalog_table_name_match_lineups']

#match_formation_coach
s3_match_formation_coach_preprocessed = os.environ['s3_match_formation_coach_preprocessed']
glue_catalog_match_formation_coach = os.environ['glue_catalog_table_name_match_formation_coach']

#match_captain
s3_match_captain_preprocessed = os.environ['s3_match_captain_preprocessed']
glue_catalog_match_captain = os.environ['glue_catalog_table_name_match_captain']

#match_players_performances
s3_match_players_performance_preprocessed = os.environ['s3_match_players_performances_preprocessed']
glue_catalog_match_players_performance = os.environ['glue_catalog_table_name_match_players_performance']
#match_events_table
s3_match_events_preprocessed = os.environ['s3_match_events_preprocessed']
glue_catalog_match_events_performance = os.environ['glue_catalog_table_name_match_events']

def fixtures_extract(d):
    format_='%Y-%m-%d'
    fixtures={'id':[],'date':[],'league_id':[],'season':[],'round':[],'venue_id':[],'referee':[],'status':[]}
    for i in range(0,len(d['response'])):
        # Fixtures table
        fixture_id=d['response'][i]['fixture']['id']
        date_=d['response'][i]['fixture']['date']
        date_=date_.split('T')[0]
        date_=datetime.datetime.strptime(date_,format_)
        date_=date_.date()
        league_id=d['response'][i]['league']['id']
        season=d['response'][i]['league']['season']
        round_=d['response'][i]['league']['round']
        venue_id=d['response'][i]['fixture']['venue']['id']
        referee=d['response'][i]['fixture']['referee']
        if referee is not None:
            referee=referee.split(",")[0]
        time_zone=d['response'][i]['fixture']['timezone']
        status=d['response'][i]['fixture']['status']['short']
        fixtures['id'].append(fixture_id)
        fixtures['date'].append(date_)
        fixtures['league_id'].append(league_id)
        fixtures['season'].append(season)
        fixtures['round'].append(round_)
        fixtures['venue_id'].append(venue_id)
        fixtures['referee'].append(referee)
        fixtures['status'].append(status)
    return fixtures

def match_played_extract(d):
    match_played={'fixture_id':[],'league_id':[],'team_id':[],'winner':[],'halftime_goals':[],'fulltime_goals':[],
               'extratime_goals':[],'penalty':[],'total_goals':[],'home_away':[]}
    for i in range(0,len(d['response'])):
        # Fixtures table
        fixture_id=d['response'][i]['fixture']['id']
        league_id=d['response'][i]['league']['id']
        # Home team
        home_team_id=d['response'][i]['teams']['home']['id']
        home_winner=d['response'][i]['teams']['home']['winner']
        home_halftime_goals=d['response'][i]['score']['halftime']['home']          
        home_fulltime_goals=d['response'][i]['score']['fulltime']['home'] 
        home_extratime_goals=d['response'][i]['score']['extratime']['home'] 
        home_penalty=d['response'][i]['score']['penalty']['home'] 
        home_total_goals=d['response'][i]['goals']['home']
        match_played['fixture_id'].append(fixture_id)
        match_played['league_id'].append(league_id)
        match_played['team_id'].append(home_team_id)
        match_played['winner'].append(home_winner)
        match_played['halftime_goals'].append(home_halftime_goals)
        match_played['fulltime_goals'].append(home_fulltime_goals)
        match_played['extratime_goals'].append(home_extratime_goals)
        match_played['penalty'].append(home_penalty)
        match_played['total_goals'].append(home_total_goals)
        match_played['home_away'].append('home')
        # Away team
        away_team_id=d['response'][i]['teams']['away']['id']
        away_winner=d['response'][i]['teams']['away']['winner']
        away_halftime_goals=d['response'][i]['score']['halftime']['away']          
        away_fulltime_goals=d['response'][i]['score']['fulltime']['away'] 
        away_extratime_goals=d['response'][i]['score']['extratime']['away'] 
        away_penalty=d['response'][i]['score']['penalty']['away'] 
        away_total_goals=d['response'][i]['goals']['away']
        match_played['fixture_id'].append(fixture_id)
        match_played['league_id'].append(league_id)
        match_played['team_id'].append(away_team_id)
        match_played['winner'].append(away_winner)
        match_played['halftime_goals'].append(away_halftime_goals)
        match_played['fulltime_goals'].append(away_fulltime_goals)
        match_played['extratime_goals'].append(away_extratime_goals)
        match_played['penalty'].append(away_penalty)
        match_played['total_goals'].append(away_total_goals)
        match_played['home_away'].append('away')
    return match_played
def match_lineups_formation_coach_extract(d):
    match_lineups={'fixture_id':[],'league_id':[],'team_id':[],'player_id':[],'player_name':[],
               'position':[],'grid':[],'lineups_type':[]}
    match_formation_coach={'fixture_id':[],'league_id':[],'team_id':[],'coach_id':[],'coach_name':[],'formation':[]}
    for i in range(0,len(d['response'])):
        # Fixtures table
        fixture_id=d['response'][i]['fixture']['id']
        league_id=d['response'][i]['league']['id']
        for j in range(0,len(d['response'][i]['lineups'])):
            team_id=d['response'][i]['lineups'][j]['team']['id']
            coach_id=d['response'][i]['lineups'][j]['coach']['id']
            coach_name=d['response'][i]['lineups'][j]['coach']['name']
            formation=d['response'][i]['lineups'][j]['formation']
            match_formation_coach['fixture_id'].append(fixture_id)
            match_formation_coach['league_id'].append(league_id)
            match_formation_coach['team_id'].append(team_id)
            match_formation_coach['coach_id'].append(coach_id)
            match_formation_coach['coach_name'].append(coach_name)
            match_formation_coach['formation'].append(formation)          
            # StartXI
            for k in range(0,len(d['response'][i]['lineups'][j]['startXI'])):
                player_id=d['response'][i]['lineups'][j]['startXI'][k]['player']['id']
                player_name=d['response'][i]['lineups'][j]['startXI'][k]['player']['name']
                position=d['response'][i]['lineups'][j]['startXI'][k]['player']['pos']
                grid=d['response'][i]['lineups'][j]['startXI'][k]['player']['grid']
                match_lineups['fixture_id'].append(fixture_id)
                match_lineups['league_id'].append(league_id)
                match_lineups['team_id'].append(team_id)
                match_lineups['player_id'].append(player_id)
                match_lineups['player_name'].append(player_name)
                match_lineups['position'].append(position)
                match_lineups['grid'].append(grid)
                match_lineups['lineups_type'].append('startXI')
            for l in range(0,len(d['response'][i]['lineups'][j]['substitutes'])):
                #substitutes
                player_id=d['response'][i]['lineups'][j]['substitutes'][l]['player']['id']
                player_name=d['response'][i]['lineups'][j]['substitutes'][l]['player']['name']
                position=d['response'][i]['lineups'][j]['substitutes'][l]['player']['pos']
                grid=d['response'][i]['lineups'][j]['substitutes'][l]['player']['grid']
                match_lineups['fixture_id'].append(fixture_id)
                match_lineups['league_id'].append(league_id)
                match_lineups['team_id'].append(team_id)
                match_lineups['player_id'].append(player_id)
                match_lineups['player_name'].append(player_name)
                match_lineups['position'].append(position)
                match_lineups['grid'].append(grid)
                match_lineups['lineups_type'].append('substitutes')
    return (match_formation_coach,match_lineups)   

def match_player_performance_captain_extract(d):
    match_captain={'fixture_id':[],'league_id':[],'team_id':[],'player_id':[],'player_name':[]}
    match_players_performance={'fixture_id':[],'league_id':[],'team_id':[],'player_id':[],'player_name':[],
                           'offsides':[],'shots_total':[],'shots_on':[],'goals_total':[],'goals_conceded':[],
                           'goals_assists':[],'goals_saves':[],'goals_total':[],'passes_key':[],'passes_accuracy':[],
                           'tackles_total':[],'tackles_blocks':[],'tackles_interceptions':[],'duels_total':[],
                           'duels_won':[],'dribbles_attempts':[],'dribbles_success':[],'dribbles_past':[],
                           'fouls_drawn':[],'fouls_committed':[],'cards_yellow':[],'cards_red':[],'penalty_won':[],
                           'penalty_committed':[],'penalty_scored':[],'penalty_missed':[],'penalty_saved':[]}
    for i in range(0,len(d['response'])):
        fixture_id=d['response'][i]['fixture']['id']
        league_id=d['response'][i]['league']['id']
        for j in range(0,len(d['response'][i]['players'])):
            team_id=d['response'][i]['players'][j]['team']['id']
            for k in range(0,len(d['response'][i]['players'][j]['players'])):
                player_id=d['response'][i]['players'][j]['players'][k]['player']['id']
                player_name=d['response'][i]['players'][j]['players'][k]['player']['name']
                captain=d['response'][i]['players'][j]['players'][k]['statistics'][0]['games']['captain']
                # match_captain table
                if captain:
                    match_captain['fixture_id'].append(fixture_id)
                    match_captain['league_id'].append(league_id)
                    match_captain['team_id'].append(team_id)
                    match_captain['player_id'].append(player_id)
                    match_captain['player_name'].append(player_name)
                    
                #match_players_performance table
                offsides=d['response'][i]['players'][j]['players'][k]['statistics'][0]['offsides']
                shots_total=d['response'][i]['players'][j]['players'][k]['statistics'][0]['shots']['total']
                shots_on=d['response'][i]['players'][j]['players'][k]['statistics'][0]['shots']['on']
                goals_total=d['response'][i]['players'][j]['players'][k]['statistics'][0]['goals']['total']
                goals_conceded=d['response'][i]['players'][j]['players'][k]['statistics'][0]['goals']['conceded']
                goals_assists=d['response'][i]['players'][j]['players'][k]['statistics'][0]['goals']['assists']
                goals_saves=d['response'][i]['players'][j]['players'][k]['statistics'][0]['goals']['saves']
                passes_total=d['response'][i]['players'][j]['players'][k]['statistics'][0]['passes']['total']        
                passes_key=d['response'][i]['players'][j]['players'][k]['statistics'][0]['passes']['key']
                passes_accuracy=d['response'][i]['players'][j]['players'][k]['statistics'][0]['passes']['accuracy']
                tackles_total=d['response'][i]['players'][j]['players'][k]['statistics'][0]['tackles']['total']
                tackles_blocks=d['response'][i]['players'][j]['players'][k]['statistics'][0]['tackles']['blocks']
                tackles_interceptions=d['response'][i]['players'][j]['players'][k]['statistics'][0]['tackles']['interceptions']
                duels_total=d['response'][i]['players'][j]['players'][k]['statistics'][0]['duels']['total']
                duels_won=d['response'][i]['players'][j]['players'][k]['statistics'][0]['duels']['won']
                dribbles_attempts=d['response'][i]['players'][j]['players'][k]['statistics'][0]['dribbles']['attempts']
                dribbles_success=d['response'][i]['players'][j]['players'][k]['statistics'][0]['dribbles']['success']
                dribbles_past=d['response'][i]['players'][j]['players'][k]['statistics'][0]['dribbles']['past']
                fouls_drawn=d['response'][i]['players'][j]['players'][k]['statistics'][0]['fouls']['drawn']
                fouls_committed=d['response'][i]['players'][j]['players'][k]['statistics'][0]['fouls']['committed']
                cards_yellow=d['response'][i]['players'][j]['players'][k]['statistics'][0]['cards']['yellow']
                cards_red=d['response'][i]['players'][j]['players'][k]['statistics'][0]['cards']['red']
                penalty_won=d['response'][i]['players'][j]['players'][k]['statistics'][0]['penalty']['won']
                penalty_committed=d['response'][i]['players'][j]['players'][k]['statistics'][0]['penalty']['commited']
                penalty_scored=d['response'][i]['players'][j]['players'][k]['statistics'][0]['penalty']['scored']
                penalty_missed=d['response'][i]['players'][j]['players'][k]['statistics'][0]['penalty']['missed']
                penalty_saved=d['response'][i]['players'][j]['players'][k]['statistics'][0]['penalty']['saved']
                match_players_performance['fixture_id'].append(fixture_id)
                match_players_performance['league_id'].append(league_id)
                match_players_performance['team_id'].append(team_id)
                match_players_performance['player_id'].append(player_id)
                match_players_performance['player_name'].append(player_name)
                match_players_performance['offsides'].append(offsides)
                match_players_performance['shots_total'].append(shots_total)
                match_players_performance['shots_on'].append(shots_on)
                match_players_performance['goals_total'].append(goals_total)
                match_players_performance['goals_conceded'].append(goals_conceded)
                match_players_performance['goals_assists'].append(goals_assists)
                match_players_performance['goals_saves'].append(goals_saves)
                match_players_performance['passes_key'].append(passes_key)
                match_players_performance['passes_accuracy'].append(passes_accuracy)
                match_players_performance['tackles_total'].append(tackles_total)
                match_players_performance['tackles_blocks'].append(tackles_blocks)
                match_players_performance['tackles_interceptions'].append(tackles_interceptions)
                match_players_performance['duels_total'].append(duels_total)
                match_players_performance['duels_won'].append(duels_won)
                match_players_performance['dribbles_attempts'].append(dribbles_attempts)
                match_players_performance['dribbles_success'].append(dribbles_success)
                match_players_performance['dribbles_past'].append(dribbles_past)
                match_players_performance['fouls_drawn'].append(fouls_drawn)
                match_players_performance['fouls_committed'].append(fouls_committed)
                match_players_performance['cards_yellow'].append(cards_yellow)
                match_players_performance['cards_red'].append(cards_red)
                match_players_performance['penalty_won'].append(penalty_won)
                match_players_performance['penalty_committed'].append(penalty_committed)
                match_players_performance['penalty_scored'].append(penalty_scored)
                match_players_performance['penalty_missed'].append(penalty_missed)
                match_players_performance['penalty_saved'].append(penalty_saved)
    return (match_players_performance,match_captain)

def match_events_extract(d):
    match_events={'fixture_id':[],'league_id':[],'team_id':[],'time_elapsed':[],'time_extra':[],'player_id':[],
                  'player_name':[],'assisted_player_id':[],'assisted_player_name':[],'type':[],'detail':[]}
    for i in range(0,len(d['response'])):
        # Fixtures table
        fixture_id=d['response'][i]['fixture']['id']
        league_id=d['response'][i]['league']['id']
        for j in range(0,len(d['response'][i]['events'])):
            team_id=d['response'][i]['events'][j]['team']['id']
            time_elapsed=d['response'][i]['events'][j]['time']['elapsed']
            time_extra=d['response'][i]['events'][j]['time']['extra']
            player_id=d['response'][i]['events'][j]['player']['id']
            player_name=d['response'][i]['events'][j]['player']['name']
            assisted_player_id=d['response'][i]['events'][j]['assist']['id']
            assisted_player_name=d['response'][i]['events'][j]['assist']['name']
            type_=d['response'][i]['events'][j]['type']
            detail=d['response'][i]['events'][j]['detail']
            match_events['fixture_id'].append(fixture_id)
            match_events['league_id'].append(league_id)
            match_events['team_id'].append(team_id)
            match_events['time_elapsed'].append(time_elapsed)
            match_events['time_extra'].append(time_extra)
            match_events['player_id'].append(player_id)
            match_events['player_name'].append(player_name)
            match_events['assisted_player_id'].append(assisted_player_id)
            match_events['assisted_player_name'].append(assisted_player_name)
            match_events['type'].append(type_)
            match_events['detail'].append(detail)
                
    return match_events


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        s3_response = s3.get_object(Bucket=bucket,Key=key)
        # Get the Body object in the S3 get_object() response
        s3_object_body = s3_response.get('Body')
        # Read the data in bytes format
        content = s3_object_body.read()
        d = json.loads(content)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
    # Fixtures extractions
    fixtures=fixtures_extract(d)
    fixtures_df=pd.DataFrame(fixtures)
    # Handling None values
    string_column_set={'round','referee','status'}
    date_column_set={'date'}
    columns=list(fixtures.keys())
    for column in columns:
        if column in string_column_set:
            fixtures_df[column]=fixtures_df[column].astype('str')
        elif column in date_column_set:
            continue
        else:
            fixtures_df[column]=pd.to_numeric(fixtures_df[column], errors='coerce').astype('Int32')
    response_1=wr.s3.to_parquet(
            df=fixtures_df,
            path=s3_fixtures_preprocessed,
            dataset=True,
            database=glue_catalog_db_name,
            table=glue_catalog_fixtures,
            mode=write_data_operation)
    # match-played 
    match_played=match_played_extract(d)
    match_played_df=pd.DataFrame(match_played)
    # Handling Null in match_formation_coach
    string_column_set={'home_away'}
    boolean_column_set={'winner'}
    columns=list(match_played.keys())
    for column in columns:
        if column in string_column_set:
            match_played_df[column]=match_played_df[column].astype('str')
        elif column in boolean_column_set:
            match_played_df[column]=match_played_df[column].astype('boolean')
        else:
            match_played_df[column]=pd.to_numeric(match_played_df[column], errors='coerce').astype('Int32')
    response_2=wr.s3.to_parquet(
            df=match_played_df,
            path=s3_match_played_preprocessed,
            dataset=True,
            database=glue_catalog_db_name,
            table=glue_catalog_match_played,
            mode=write_data_operation)
    
    # match-lineups or match-formation-coach
    match_formation_coach,match_lineups=match_lineups_formation_coach_extract(d)
    match_formation_coach_df=pd.DataFrame(match_formation_coach)
    match_lineups_df=pd.DataFrame(match_lineups)
    # Handling Null in match lineups
    string_column_set={'player_name','position','grid','lineups_type'}
    columns=list(match_lineups.keys())
    for column in columns:
        if column in string_column_set:
            match_lineups_df[column]=match_lineups_df[column].astype('str')
        else:
            match_lineups_df[column]=pd.to_numeric(match_lineups_df[column], errors='coerce').astype('Int32')
    # Handling Null in match_formation_coach
    string_column_set={'coach_name','formation'}
    columns=list(match_formation_coach.keys())
    for column in columns:
        if column in string_column_set:
            match_formation_coach_df[column]=match_formation_coach_df[column].astype('str')
        else:
            match_formation_coach_df[column]=pd.to_numeric(match_formation_coach_df[column], errors='coerce').astype('Int32')
    response_3=wr.s3.to_parquet(
            df=match_lineups_df,
            path=s3_match_lineups_preprocessed,
            dataset=True,
            database=glue_catalog_db_name,
            table=glue_catalog_match_lineups,
            mode=write_data_operation)
    response_4=wr.s3.to_parquet(
            df=match_formation_coach_df,
            path=s3_match_formation_coach_preprocessed,
            dataset=True,
            database=glue_catalog_db_name,
            table=glue_catalog_match_formation_coach,
            mode=write_data_operation)
    # match-player-performance and captain tables
    match_players_performance,match_captain=match_player_performance_captain_extract(d)
    match_captain_df=pd.DataFrame(match_captain)
    match_players_performance_df=pd.DataFrame(match_players_performance)
    # Handling Null in match palyer performance
    columns=list(match_players_performance.keys())
    for column in columns:
        if column in string_column_set:
            match_players_performance_df[column]=match_players_performance_df[column].astype('str')
        else:
            match_players_performance_df[column]=pd.to_numeric(match_players_performance_df[column], errors='coerce').astype('Int32')
    # Handling Null in match captain
    string_column_set={'player_name'}
    columns=list(match_captain.keys())
    for column in columns:
        if column in string_column_set:
            match_captain_df[column]=match_captain_df[column].astype('str')
        else:
            match_captain_df[column]=pd.to_numeric(match_captain_df[column], errors='coerce').astype('Int32')
    response_5=wr.s3.to_parquet(
            df=match_captain_df,
            path=s3_match_captain_preprocessed,
            dataset=True,
            database=glue_catalog_db_name,
            table=glue_catalog_match_captain,
            mode=write_data_operation)
    response_6=wr.s3.to_parquet(
            df=match_players_performance_df,
            path=s3_match_players_performance_preprocessed,
            dataset=True,
            database=glue_catalog_db_name,
            table=glue_catalog_match_players_performance,
            mode=write_data_operation)
    # Match-events table
    match_events=match_events_extract(d)
    match_events_df=pd.DataFrame(match_events)
    # Handling the Null data 
    string_column_set={'player_name','type','assisted_player_name','detail'}
    columns=list(match_events.keys())
    for column in columns:
        if column in string_column_set:
            match_events_df[column]=match_events_df[column].astype('str')
        else:
            match_events_df[column]=pd.to_numeric(match_events_df[column], errors='coerce').astype('Int32')
    response_7=wr.s3.to_parquet(
            df=match_events_df,
            path=s3_match_events_preprocessed,
            dataset=True,
            database=glue_catalog_db_name,
            table=glue_catalog_match_events_performance,
            mode=write_data_operation)
    
    response=(response_1,response_2,response_3,response_4,response_5,response_6,response_7)
    return { 'status':200, 'body':response }