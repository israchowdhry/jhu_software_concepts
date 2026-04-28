"""
This module generates visualizations for the diamonds dataset,
including static and interactive plots for price analysis.
"""

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import seaborn as sns

sns.set(style="whitegrid")


def plot_average_price_by_clarity(data):
    """Create a bar chart of average diamond price by clarity."""
    clarity_order = ["I1", "SI2", "SI1", "VS2", "VS1", "VVS2", "VVS1", "IF"]

    avg_price = (
        data.groupby("clarity", observed=True)["price"]
        .mean()
        .reindex(clarity_order)
        .reset_index()
    )

    figure, axis = plt.subplots(figsize=(8, 6))
    sns.barplot(data=avg_price, x="clarity", y="price", ax=axis)
    axis.set_title("Average Diamond Price by Clarity")
    axis.set_xlabel("Clarity")
    axis.set_ylabel("Average Price (USD)")
    figure.tight_layout()
    figure.savefig("average_price_by_clarity.png", dpi=300, bbox_inches="tight")
    return figure


def plot_price_by_cut(data):
    """Create a boxplot of price by cut, save it, and return the figure."""
    order = ["Fair", "Good", "Very Good", "Premium", "Ideal"]

    figure, axis = plt.subplots(figsize=(8, 6))
    sns.boxplot(data=data, x="cut", y="price", order=order, ax=axis)
    axis.set_title("Price Distribution by Cut Quality")
    axis.set_xlabel("Cut")
    axis.set_ylabel("Price (USD)")
    figure.tight_layout()
    figure.savefig("price_by_cut.png", dpi=300, bbox_inches="tight")
    return figure


def plot_interactive(data):
    """Create an interactive Plotly scatter plot, save it, and return the figure."""
    figure = px.scatter(
        data,
        x="carat",
        y="price",
        color="cut",
        title="Diamond Price vs Carat by Cut Quality",
        labels={"price": "Price (USD)", "carat": "Carat"},
    )
    figure.update_traces(marker={"size": 5})
    figure.update_layout(template="plotly_white")
    figure.write_html("interactive_plot.html")
    return figure


def main():
    """Load dataset and generate all required visualizations."""
    data = pd.read_csv("diamonds.csv")

    static_figure_one = plot_average_price_by_clarity(data)
    static_figure_two = plot_price_by_cut(data)
    plot_interactive(data)

    plt.close(static_figure_one)
    plt.close(static_figure_two)


if __name__ == "__main__":
    main()
