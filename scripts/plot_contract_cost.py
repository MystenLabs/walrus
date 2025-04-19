# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

import argparse
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os


def plot_metric_per_blob(df, metric, title, output_dir, label):
    plt.figure(figsize=(10, 6))
    sns.lineplot(
        data=df,
        x="n_blobs",
        y=metric,
        hue="epochs",
        style="epochs",
        markers=True,
        dashes=False,
    )
    plt.title(title)
    plt.xlabel("Number of Blobs")
    plt.ylabel(metric.replace("_", " ").title())
    plt.legend(title="Epochs")
    plt.tight_layout()
    filename = f"{label}_{metric}.png".replace(" ", "_")
    plt.savefig(os.path.join(output_dir, filename))
    plt.close()


def main(csv_path, output_dir):
    # Load and clean the CSV
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()

    # Normalize values
    df["computation_cost_per_blob"] = df["computation_cost"] / df["n_blobs"]
    df["net_gas_used_per_blob"] = df["net_gas_used"] / df["n_blobs"]

    # Treat epochs as categorical
    df["epochs"] = df["epochs"].astype(str)

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Generate plots per label
    for label in df["label"].unique():
        sub_df = df[df["label"] == label]
        plot_metric_per_blob(
            sub_df,
            "computation_cost_per_blob",
            f"{label} - Computation Cost per Blob",
            output_dir,
            label,
        )
        plot_metric_per_blob(
            sub_df,
            "net_gas_used_per_blob",
            f"{label} - Net Gas Used per Blob",
            output_dir,
            label,
        )

    print(f"Plots saved in: {output_dir}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Plot normalized contract costs from a CSV file."
    )
    parser.add_argument("csv_path", help="Path to the CSV file.")
    parser.add_argument(
        "-o",
        "--output-dir",
        default="plots",
        help="Directory to save plots (default: ./plots)",
    )
    args = parser.parse_args()
    main(args.csv_path, args.output_dir)
