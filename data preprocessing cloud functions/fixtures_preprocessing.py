import functions_framework
import random, string
import time
import datetime
import json
import urllib.parse
import pandas as pd
from google.cloud import storage
import os


def output_path_generator(table_name):
    output_bucket='preprocessed_data_12345'
    x = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
    output_file_name=str(time.time()).split('.')[0]+"_"+x+'.parquet'
    output_path = 'gs://{}/{}/'.format(output_bucket,table_name)
    output_blob_name = os.path.join(output_path,output_file_name)
    return output_blob_name


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
    return fixtures_df

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
    return match_played_df
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
            if 'startXI' in d['response'][i]['lineups'][j]:
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
            if 'substitutes' in d['response'][i]['lineups'][j]:
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
    return (match_formation_coach_df,match_lineups_df)   

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
    match_captain_df=pd.DataFrame(match_captain)
    match_players_performance_df=pd.DataFrame(match_players_performance)
    # Handling Null in match palyer performance
    columns=list(match_players_performance.keys())
    string_column_set={'player_name'}
    for column in columns:
        if column in string_column_set:
            match_players_performance_df[column]=match_players_performance_df[column].astype('str')
        else:
            match_players_performance_df[column]=pd.to_numeric(match_players_performance_df[column], errors='coerce').astype('Int32')
    # Handling Null in match captain
    columns=list(match_captain.keys())
    for column in columns:
        if column in string_column_set:
            match_captain_df[column]=match_captain_df[column].astype('str')
        else:
            match_captain_df[column]=pd.to_numeric(match_captain_df[column], errors='coerce').astype('Int32')
    return (match_players_performance_df,match_captain_df)

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
    match_events_df=pd.DataFrame(match_events)
    # Handling the Null data 
    string_column_set={'player_name','type','assisted_player_name','detail'}
    columns=list(match_events.keys())
    for column in columns:
        if column in string_column_set:
            match_events_df[column]=match_events_df[column].astype('str')
        else:
            match_events_df[column]=pd.to_numeric(match_events_df[column], errors='coerce').astype('Int32')     
    return match_events_df

# CloudEvent function to be triggered by an Eventarc Cloud Audit Logging trigger
# Note: this is NOT designed for second-party (Cloud Audit Logs -> Pub/Sub) triggers!
@functions_framework.cloud_event
def hello_auditlog(cloudevent):
    # Google Cloud Storage client
    storage_client = storage.Client()
    x = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
    payload = cloudevent.data.get("protoPayload")
    bucket=cloudevent.data['resource']['labels']['bucket_name']
    #response=-1  # No processing happend
    if payload:
        key=payload.get('resourceName').split('objects')[-1][1:]
        blob = storage_client.bucket(bucket).get_blob(key)
        content = blob.download_as_text()
        d=json.loads(content)
        # Extract the data in tabular form 
        fixtures_df=fixtures_extract(d)
        match_played_df=match_played_extract(d)
        match_formation_coach_df,match_lineups_df=match_lineups_formation_coach_extract(d)
        match_players_performance_df,match_captain_df=match_player_performance_captain_extract(d)
        match_events_df=match_events_extract(d)
        # Save all the files
        fixtures_df.to_parquet(output_path_generator('fixtures'),index=False )
        match_played_df.to_parquet(output_path_generator('match-played'),index=False )
        match_formation_coach_df.to_parquet(output_path_generator('match-formation-coach'),index=False )
        match_lineups_df.to_parquet(output_path_generator('match-lineups'),index=False )
        match_players_performance_df.to_parquet(output_path_generator('match-players-performance'),index=False )
        match_captain_df.to_parquet(output_path_generator('match-captain'),index=False ) 
        match_events_df.to_parquet(output_path_generator('match-events'),index=False )