# Raw Data Folder names
fixtures
leagues
players
teams
venues


Folder names in  Preprocessed_data bucket:
fixtures
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


# Partitioned dataset: Duplicate is removed, partitioned by league id (reduce the querying cost)
# Venues: If duplicate available, drop based on capacity