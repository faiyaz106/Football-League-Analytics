# GCP Cloud Storage Bucket Strctures: 
  1. Raw Data Bucket (To store raw .json data)
     Bucket Name: raw_data_12345
     Folders: fixtures/
              leagues/
              players/
              teams/
              venues/
  2.  Preprocess Data Bucket ( To store .parquet data)
     Bucket Name: preprocessed_data_12345
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

# Invoking the cloud function path
/projects/_/buckets/raw_data_12345/objects/venues/*
/projects/_/buckets/raw_data_12345/objects/fixtures/*
/projects/_/buckets/raw_data_12345/objects/leagues/*
/projects/_/buckets/raw_data_12345/objects/players/*
/projects/_/buckets/raw_data_12345/objects/teams/*

# Requirement.txt
functions-framework==3.*
pandas
pyarrow
google-cloud-storage 
