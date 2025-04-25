import pandas as pd
from os import makedirs, path

# Setup
input_path:str = "./src/_30/input/"
output_path:str = "./src/_30/output/"

repositories_path:str = "./src/_00/input/450-linux-pytorch.csv"
cloc_path:str = "./src/_01/output/"
large_files_path:str = "./src/_03/output/"
large_files_total_per_language_path:str = "./src/_05/output/#large_files.csv"
bus_path:str = "./src/_25/output/"

repositories:pd.DataFrame = pd.read_csv(repositories_path)
large_files_total_per_language:pd.DataFrame = pd.read_csv(large_files_total_per_language_path)

# script
large_list = []
small_list = []

for i in range(len(repositories)):
    # getting repository information
    repository, language = repositories.loc[i, ['url', 'main language']]

    # makedirs(f"{output_path}{language}", exist_ok=True)
    repo_path: str = f"{language}/{repository.split('/')[-2]}~{repository.split('/')[-1]}"
    print(repo_path)

    if path.exists(f"{bus_path}per_project/{repo_path}.csv"):
        bus_df: pd.DataFrame = pd.read_csv(f"{bus_path}per_project/{repo_path}.csv")

        if not bus_df.empty:
            large_project_bus_factor = bus_df.iloc[0]["70% Threshould Large"]
            large_mean = bus_df.iloc[0]["Large Mean"]
            large_median = bus_df.iloc[0]["Large Median"]
            large_min = bus_df.iloc[0]["Large Min"]
            large_max = bus_df.iloc[0]["Large Max"]

            large_list.append(
                {
                    "project": repo_path,
                    "bus": large_project_bus_factor,
                    "mean": large_mean,
                    "median": large_median,
                    "min": large_min,
                    "max": large_max
                }
            )

            if len(bus_df) > 1:
                small_project_bus_factor = bus_df.iloc[0]["70% Threshould Small"]
                small_mean = bus_df.iloc[1]["Small Mean"]
                small_median = bus_df.iloc[1]["Small Median"]
                small_min = bus_df.iloc[1]["Small Min"]
                small_max = bus_df.iloc[1]["Small Max"]

                small_list.append(
                    {
                        "project": repo_path,
                        "bus": small_project_bus_factor,
                        "mean": small_mean,
                        "median": small_median,
                        "min": small_min,
                        "max": small_max
                    }
                )

large_df = pd.DataFrame(large_list)
small_df = pd.DataFrame(small_list)

result: dict = {
    "Large Project Bus Factor Mean": [large_df["bus"].mean()],
    "Large Project Bus Factor Median": [large_df["bus"].mean()],
    "Large Mean": [large_df["mean"].mean()],
    "Large Median": [large_df["median"].median()],
    "Large Min Project": [large_df.loc[large_df["min"].idxmin(), "project"]],
    "Large Min": [large_df["min"].min()],
    "Large Max Project": [large_df.loc[large_df["max"].idxmax(), "project"]],
    "Large Max": [large_df["max"].max()],

    "Small Project Bus Factor Mean": [large_df["bus"].mean()],
    "Small Project Bus Factor Median": [large_df["bus"].mean()],
    "Small Mean": [small_df["mean"].mean()],
    "Small Median": [small_df["median"].median()],
    "Small Min Project": [small_df.loc[small_df["min"].idxmin(), "project"]],
    "Small Min": [small_df["min"].min()],
    "Small Max Project": [small_df.loc[small_df["max"].idxmax(), "project"]],
    "Small Max": [small_df["max"].max()]
}

makedirs(f"{output_path}", exist_ok=True)
pd.DataFrame(result).to_csv(f"{output_path}result.csv")
# Save a CSV with the top 50% highest means and medians
large_top_50 = large_df.nlargest(len(large_df) // 2, "mean")
small_top_50 = small_df.nlargest(len(small_df) // 2, "mean")

large_top_50_mean = large_top_50["mean"].mean()
small_top_50_mean = small_top_50["mean"].mean()
large_top_50_median = large_top_50["median"].median()
small_top_50_median = small_top_50["median"].median()

# Save the means and medians to a separate CSV
means_medians = pd.DataFrame({
    "Large Top 50% Mean": [large_top_50_mean],
    "Small Top 50% Mean": [small_top_50_mean],
    "Large Top 50% Median": [large_top_50_median],
    "Small Top 50% Median": [small_top_50_median]
})
means_medians.to_csv(f"{output_path}top_50_means_medians.csv", index=False)

# Save a CSV with the top 25% highest means and medians
large_top_25 = large_df.nlargest(len(large_df) * 25 // 100, "mean")
small_top_25 = small_df.nlargest(len(small_df) * 25 // 100, "mean")

large_top_25_mean = large_top_25["mean"].mean()
small_top_25_mean = small_top_25["mean"].mean()
large_top_25_median = large_top_25["median"].median()
small_top_25_median = small_top_25["median"].median()

# Save the means and medians to a separate CSV
means_medians_25 = pd.DataFrame({
    "Large Top 25% Mean": [large_top_25_mean],
    "Small Top 25% Mean": [small_top_25_mean],
    "Large Top 25% Median": [large_top_25_median],
    "Small Top 25% Median": [small_top_25_median]
})
means_medians_25.to_csv(f"{output_path}top_25_means_medians.csv", index=False)

# Save a CSV with the top 10% highest means and medians
large_top_10 = large_df.nlargest(len(large_df) * 10 // 100, "mean")
small_top_10 = small_df.nlargest(len(small_df) * 10 // 100, "mean")

large_top_10_mean = large_top_10["mean"].mean()
small_top_10_mean = small_top_10["mean"].mean()
large_top_10_median = large_top_10["median"].median()
small_top_10_median = small_top_10["median"].median()

# Save the means and medians to a separate CSV
means_medians_10 = pd.DataFrame({
    "Large Top 10% Mean": [large_top_10_mean],
    "Small Top 10% Mean": [small_top_10_mean],
    "Large Top 10% Median": [large_top_10_median],
    "Small Top 10% Median": [small_top_10_median]
})
means_medians_10.to_csv(f"{output_path}top_10_means_medians.csv", index=False)
