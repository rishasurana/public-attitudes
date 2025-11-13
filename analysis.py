import os
import pandas as pd
from pathlib import Path

DATA_DIR = Path("/project2/swabhas_1625/risha/homelessness-tweets/")

def load_csvs(directory):
    csv_files = sorted([f for f in directory.glob("*.csv")])
    data = {}
    for f in csv_files:
        try:
            df = pd.read_csv(f, low_memory=False)
            data[f.name] = df
        except Exception as e:
            print(f"Failed to load {f}: {e}")
    return data

def compute_basic_stats(df):
    stats = {}
    stats["rows"] = len(df)
    stats["unique_users"] = df["author_id"].nunique() if "author_id" in df.columns else None
    stats["tweets_with_hashtags"] = df["entities.hashtags"].notna().sum() if "entities.hashtags" in df.columns else None

    def count_non_null(col):
        return df[col].notna().sum() if col in df.columns else None

    stats["retweets"] = count_non_null("referenced_tweets.retweeted.id")
    stats["replies"] = count_non_null("referenced_tweets.replied_to.id")
    stats["quotes"] = count_non_null("referenced_tweets.quoted.id")

    return stats

def compute_influencers(df, top_n=10):
    if "author_id" not in df.columns or "author.public_metrics.followers_count" not in df.columns:
        return None

    inf = df.drop_duplicates(subset=["author_id"])[
        ["author_id", "author.username", "author.public_metrics.followers_count"]
    ]
    inf = inf.sort_values("author.public_metrics.followers_count", ascending=False)
    return inf.head(top_n)

def compute_field_population(df):
    result = {}
    for col in df.columns:
        result[col] = df[col].notna().mean()
    return result

def compute_cleaning_issues(df):
    issues = {}

    if "id" in df.columns:
        issues["duplicate_ids"] = df["id"].duplicated().sum()
    else:
        issues["duplicate_ids"] = None

    missing_key_fields = ["id", "author_id", "text", "created_at"]
    issues["missing_values"] = {col: df[col].isna().sum() for col in missing_key_fields if col in df.columns}

    if "created_at" in df.columns:
        try:
            pd.to_datetime(df["created_at"], errors="raise")
            issues["created_at_errors"] = 0
        except Exception:
            issues["created_at_errors"] = "datetime parsing errors detected"
    else:
        issues["created_at_errors"] = None

    return issues

def analyze_all(data):
    for name, df in data.items():
        print("=" * 80)
        print(f"File: {name}")
        print("=" * 80)

        basic = compute_basic_stats(df)
        print("\nBasic Stats:")
        for k, v in basic.items():
            print(f"{k}: {v}")

        influencers = compute_influencers(df)
        print("\nTop Influencers:")
        if influencers is None:
            print("Influencer fields not present.")
        else:
            print(influencers)

        cleaning = compute_cleaning_issues(df)
        print("\nCleaning Issues:")
        for k, v in cleaning.items():
            print(f"{k}: {v}")

        populations = compute_field_population(df)
        print("\nField Population (percent non-null):")
        for col, pct in populations.items():
            print(f"{col}: {pct:.3f}")

        print("\n")

def main():
    print("Loading CSV files...")
    data = load_csvs(DATA_DIR)
    if not data:
        print("No CSV files found.")
        return

    analyze_all(data)

if __name__ == "__main__":
    main()
