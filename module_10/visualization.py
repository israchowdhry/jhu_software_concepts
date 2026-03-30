"""
This module generates visualizations for the diamonds dataset,
including static and interactive plots for price analysis.
"""

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import seaborn as sns

sns.set(style="whitegrid")


def plot_price_vs_carat(df):
    """Create a scatter plot of price vs carat."""
    plt.figure(figsize=(8, 6))
    sns.scatterplot(data=df, x="carat", y="price", alpha=0.1, s=6)
    plt.title("Diamond Price vs Carat")
    plt.xlabel("Carat")
    plt.ylabel("Price (USD)")
    plt.savefig("price_vs_carat.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_price_by_cut(df):
    """Create a boxplot showing price distribution by cut."""
    order = ["Fair", "Good", "Very Good", "Premium", "Ideal"]

    plt.figure(figsize=(8, 6))
    sns.boxplot(data=df, x="cut", y="price", order=order)
    plt.title("Price Distribution by Cut Quality")
    plt.xlabel("Cut")
    plt.ylabel("Price (USD)")
    plt.savefig("price_by_cut.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_interactive(df):
    """Create an interactive scatter plot using Plotly."""
    fig = px.scatter(
        df,
        x="carat",
        y="price",
        color="cut",
        title="Diamond Price vs Carat by Cut Quality",
        labels={"price": "Price (USD)", "carat": "Carat"}
    )

    fig.write_html("interactive_plot.html")
    fig.write_image("interactive_plot.png")


def main():
    """Load dataset and generate all visualizations."""
    df = pd.read_csv("diamonds.csv")
    plot_price_vs_carat(df)
    plot_price_by_cut(df)
    plot_interactive(df)


if __name__ == "__main__":
    main()
