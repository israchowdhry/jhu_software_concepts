"""
This module creates a Dash dashboard for the diamonds dataset
using functionality imported from visualization.py.
"""

import base64
from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd
from dash import Dash, dcc, html

from visualization import plot_interactive, plot_price_by_cut, plot_price_vs_carat


def convert_figure_to_base64(figure):
    """Convert a seaborn figure to a base64 string for Dash display."""
    buffer = BytesIO()
    figure.savefig(buffer, format="png", dpi=300, bbox_inches="tight")
    buffer.seek(0)
    encoded_image = base64.b64encode(buffer.read()).decode("utf-8")
    buffer.close()
    return encoded_image


data = pd.read_csv("diamonds.csv")

price_vs_carat_figure = plot_price_vs_carat(data)
price_by_cut_figure = plot_price_by_cut(data)
interactive_figure = plot_interactive(data)

price_vs_carat_image = convert_figure_to_base64(price_vs_carat_figure)
price_by_cut_image = convert_figure_to_base64(price_by_cut_figure)

plt.close(price_vs_carat_figure)
plt.close(price_by_cut_figure)

app = Dash(__name__)

app.layout = html.Div(
    children=[
        html.H1("Can the Price of a Diamond Be Determined by Its Features?"),
        html.P(
            (
                "Diamond price is primarily driven by carat, with larger diamonds "
        "increasing in price at a nonlinear rate. While cut quality also "
        "influences price, there is significant overlap between categories, "
        "indicating it is a secondary factor. The visualizations show that "
        "size is the strongest predictor of value, with quality characteristics "
        "refining price differences among diamonds of similar carat."
            ),
            style={
                "maxWidth": "900px",
                "lineHeight": "1.6",
                "marginBottom": "30px"
            }
        ),
        html.H2("Visualization 1: Diamond Price vs Carat"),
        html.Img(
            src=f"data:image/png;base64,{price_vs_carat_image}",
            style={"width": "70%", "display": "block", "marginBottom": "30px"}
        ),
        html.H2("Visualization 2: Price Distribution by Cut Quality"),
        html.Img(
            src=f"data:image/png;base64,{price_by_cut_image}",
            style={"width": "70%", "display": "block", "marginBottom": "30px"}
        ),
        html.H2("Visualization 3: Interactive Diamond Price vs Carat by Cut Quality"),
        dcc.Graph(
            figure=interactive_figure,
            style={"width": "80%"}
        ),
    ],
    style={"fontFamily": "Arial, sans-serif", "padding": "40px"}
)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)
