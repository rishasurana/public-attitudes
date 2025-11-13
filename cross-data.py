import pandas as pd
from pathlib import Path

DATA_DIR = Path("/project2/swabhas_1625/risha/homelessness-tweets/")

def load_csvs(directory):
    csvs = sorted(directory.glob("*.csv"))
    data = {}
    for f in csvs:
        try:
            df = pd.read_csv(f, low_memory=False)
            data[f.name] = df
        except Exception as e:
            print(f"Failed to read {f}: {e}")
    return data

def extract_influencers(df, dataset_name):
    required = [
        "author_id",
        "author.username",
        "author.public_metrics.followers_count",
        "public_metrics.like_count",
        "public_metrics.reply_count",
        "public_metrics.retweet_count"
    ]

    if not all(c in df.columns for c in required):
        return None

    tmp = df.copy()
    tmp["engagement"] = (
        tmp["public_metrics.like_count"].fillna(0)
        + tmp["public_metrics.reply_count"].fillna(0)
        + tmp["public_metrics.retweet_count"].fillna(0)
    )

    agg = tmp.groupby(["author_id", "author.username"], as_index=False).agg({
        "author.public_metrics.followers_count": "max",
        "engagement": "sum"
    })

    agg["dataset"] = dataset_name
    return agg

def main():
    data = load_csvs(DATA_DIR)
    if not data:
        print("No datasets found.")
        return

    frames = []
    for name, df in data.items():
        inf = extract_influencers(df, name)
        if inf is not None:
            frames.append(inf)

    if not frames:
        print("No datasets provided influencer information.")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined.to_csv("all_influencers.csv", index=False)

    counts = combined.groupby("author_id")["dataset"].nunique().reset_index()
    counts = counts.rename(columns={"dataset": "num_datasets"})
    counts.to_csv("influencer_dataset_counts.csv", index=False)

    merged = combined.merge(counts, on="author_id", how="left")
    merged.to_csv("influencers_with_cross_dataset_stats.csv", index=False)

    print("Generated:")
    print("all_influencers.csv")
    print("influencer_dataset_counts.csv")
    print("influencers_with_cross_dataset_stats.csv")

if __name__ == "__main__":
    main()
