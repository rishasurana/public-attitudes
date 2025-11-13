import pandas as pd
import networkx as nx
from pathlib import Path

DATA_DIR = Path("/project2/swabhas_1625/risha/homelessness-tweets/")

def load_all():
    csvs = sorted(DATA_DIR.glob("*.csv"))
    frames = []
    for f in csvs:
        try:
            df = pd.read_csv(f, low_memory=False)
            df["dataset"] = f.name
            frames.append(df)
        except Exception as e:
            print(f"Could not read {f}: {e}")
    if frames:
        return pd.concat(frames, ignore_index=True)
    return None

def add_edges(df, graph, source_col, target_col):
    if source_col in df.columns and target_col in df.columns:
        sub = df[[source_col, target_col]].dropna()
        for _, row in sub.iterrows():
            graph.add_edge(row[source_col], row[target_col])

def main():
    df = load_all()
    if df is None:
        print("No data found.")
        return

    G_retweet = nx.DiGraph()
    G_reply = nx.DiGraph()
    G_quote = nx.DiGraph()

    add_edges(df, G_retweet, "author_id", "retweeted_user_id")
    add_edges(df, G_reply, "author_id", "in_reply_to_user_id")
    add_edges(df, G_quote, "author_id", "quoted_user_id")

    print("Retweet network nodes:", G_retweet.number_of_nodes())
    print("Retweet network edges:", G_retweet.number_of_edges())

    print("Reply network nodes:", G_reply.number_of_nodes())
    print("Reply network edges:", G_reply.number_of_edges())

    print("Quote network nodes:", G_quote.number_of_nodes())
    print("Quote network edges:", G_quote.number_of_edges())

    nx.write_gpickle(G_retweet, "retweet_network.gpickle")
    nx.write_gpickle(G_reply, "reply_network.gpickle")
    nx.write_gpickle(G_quote, "quote_network.gpickle")

    print("Graphs saved to gpickle files.")

if __name__ == "__main__":
    main()
