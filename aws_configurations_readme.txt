# AWS S3 Bucket Strctures: 
  1. Raw Data Bucket (To store raw .json data)
     Bucket Name: football-raw-data-12345
     Folders: fixtures/
              leagues/
              players/
              teams/
              venues/
  2.  Preprocess Data Bucket ( To store .parquet data)
     Bucket Name: football-analytics-preprocessed-12345
     Folders: fixtures/
              leagues-played/
              leagues/
              match-captain/
              match-events/
              match-formation-coach/
              match-lineups/
              match-played/
              match-players-performance/
              players/
              teams/
              venues/

# Glue Catalog : 
  Database Name: football_analytics_preprocessed
  Tables Names:  fixtures
                 leagues-played
                 leagues
                 match-captain
                 match-events
                 match-formation-coach
                 match-lineups
                 match-played
                 match-players-performance
                 players
                 teams
                 venues
  Note: For above tables, use the schema as per given "table_schema.png" or "table_schema and cloud functions.xlsx"


# Lambda Functions Configuratins:
1. teams_preprocessing.py 
  Environment variables: 
    glue_catalog_db_name : football_analytics_preprocessed
    glue_catalog_table_name : teams	
    s3_teams_preprocessed	 : s3://football-analytics-preprocessed-12345/teams/
    write_data_operation : append
  Layers: 
       AWSSDKPandas-Python39
  Run time: 
       Python39 
  trigger: 
      Bucket arn: arn:aws:s3:::football-raw-data-12345
      Event types: s3:ObjectCreated:Put
      Prefix: teams/
      Service principal: s3.amazonaws.com
      Suffix: .json

  2. leagues_preprocessing.py 	
    Environment variables: 
    glue_catalog_db_name:	football_analytics_preprocessed
    glue_catalog_table_name_leagues:	leagues
    glue_catalog_table_name_leagues_played:	leagues-played
    s3_players_preprocessed_leagues:	s3://football-analytics-preprocessed-12345/leagues/
    s3_players_preprocessed_leagues_played:	s3://football-analytics-preprocessed-12345/leagues-played/
    write_data_operation:	append
  Layers: 
       AWSSDKPandas-Python39
  Run time: 
       Python39 
  trigger: 
      Bucket arn: arn:aws:s3:::football-raw-data-12345
      Event types: s3:ObjectCreated:Put
      Prefix: leagues/
      Service principal: s3.amazonaws.com
      Suffix: .json


3. players_preprocessing.py 	
    Environment variables: 
    glue_catalog_db_name:	football_analytics_preprocessed
    glue_catalog_table_name:	players
    s3_players_preprocessed:	s3://football-analytics-preprocessed-12345/players
    write_data_operation:	append
  Layers: 
       AWSSDKPandas-Python39
  Run time: 
       Python39 
  trigger: 
      Bucket arn: arn:aws:s3:::football-raw-data-12345
      Event types: s3:ObjectCreated:Put
      Prefix: players/
      Service principal: s3.amazonaws.com
      Suffix: .json

4. players_preprocessing.py 	
    Environment variables:
      glue_catalog_db_name:	football_analytics_preprocessed
      glue_catalog_table_name_venues:	venues
      s3_venues_preprocessed:	s3://football-analytics-preprocessed-12345/venues
      write_data_operation:	append
  Layers: 
       AWSSDKPandas-Python39
  Run time: 
       Python39 
  trigger: 
      Bucket arn: arn:aws:s3:::football-raw-data-12345
      Event types: s3:ObjectCreated:Put
      Prefix: venues/
      Service principal: s3.amazonaws.com
      Suffix: .json 

5. fixture_preprocessing.py 	
    Environment variables:
      glue_catalog_db_name:	football_analytics_preprocessed
      glue_catalog_table_name_fixtures:	fixtures
      glue_catalog_table_name_match_captain:	match_captain
      glue_catalog_table_name_match_events:	match_events
      glue_catalog_table_name_match_formation_coach:	match_formation_coach
      glue_catalog_table_name_match_lineups:	match_lineups
      glue_catalog_table_name_match_played:	match_played
      glue_catalog_table_name_match_players_performance: match_players_performance
      s3_fixtures_preprocessed:	s3://football-analytics-preprocessed-12345/fixtures/
      s3_match_captain_preprocessed:	s3://football-analytics-preprocessed-12345/match-captain/
      s3_match_events_preprocessed:	s3://football-analytics-preprocessed-12345/match-events/
      s3_match_formation_coach_preprocessed:	s3://football-analytics-preprocessed-12345/match-formation-coach/
      s3_match_lineups_preprocessed:	s3://football-analytics-preprocessed-12345/match-lineups/
      s3_match_played_preprocessed:	s3://football-analytics-preprocessed-12345/match-played/
      s3_match_players_performances_preprocessed:	s3://football-analytics-preprocessed-12345/match-players-performance/
      write_data_operation:	append
  Layers: 
       AWSSDKPandas-Python39
  Run time: 
       Python39 
  trigger: 
      Bucket arn: arn:aws:s3:::football-raw-data-12345
      Event types: s3:ObjectCreated:Put
      Prefix: fixtures/
      Service principal: s3.amazonaws.com
      Suffix: .json