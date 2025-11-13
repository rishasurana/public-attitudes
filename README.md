# Tweet Analysis Scripts

## Script 1: analysis_basic.py
Computes basic statistics for each CSV including:
rows
unique users
retweet, reply, quote counts
top influencers
field population rates
data cleaning checks

Outputs results to the terminal.

## Script 2: network_graphs.py
Builds directed networks of retweets, replies, and quotes using NetworkX.
Creates three graph files:
retweet_network.gpickle
reply_network.gpickle
quote_network.gpickle
Each graph contains user to user interactions.

## Script 3: cross_dataset_influencers.py
Extracts influencers from each dataset.
Combines influencer information across all CSV files.
Produces:
all_influencers.csv
influencer_dataset_counts.csv
influencers_with_cross_dataset_stats.csv

