"""
This module creates a Dash dashboard for the diamonds dataset
using functionality imported from visualization.py.
"""

import base64
from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd
from dash import Dash, dcc, html

from visualization import (
    plot_average_price_by_clarity,
    plot_interactive,
    plot_price_by_cut,
)


def convert_figure_to_base64(figure):
    """Convert a seaborn figure to a base64 string for Dash display."""
    buffer = BytesIO()
    figure.savefig(buffer, format="png", dpi=300, bbox_inches="tight")
    buffer.seek(0)
    encoded_image = base64.b64encode(buffer.read()).decode("utf-8")
    buffer.close()
    return encoded_image

# Load the dataset used for all visualizations
data = pd.read_csv("diamonds.csv")

# Generate static matplotlib visualizations
average_price_by_clarity_figure = plot_average_price_by_clarity(data)
price_by_cut_figure = plot_price_by_cut(data)

# Generate interactive Plotly visualization
interactive_figure = plot_interactive(data)

# Convert matplotlib figures to base64 so they can be displayed in Dash
average_price_by_clarity_image = convert_figure_to_base64(
    average_price_by_clarity_figure
)
price_by_cut_image = convert_figure_to_base64(price_by_cut_figure)

# Close figures to free memory after conversion
plt.close(average_price_by_clarity_figure)
plt.close(price_by_cut_figure)

# Initialize Dash application
app = Dash(__name__)

app.layout = html.Div(
    children=[
        html.H1("Can the Price of a Diamond Be Determined by Its Features?"),

        # Summary of insights derived from the visualizations
        html.P(
            (
                "Diamond price is strongly related to carat, but clarity and cut also "
        "help explain differences in value. The clarity visualization shows how "
        "average price varies across different clarity grades, while the cut "
        "distribution highlights how prices differ within each cut category. "
        "The interactive chart then illustrates the relationship between carat "
        "and price, with cut providing an additional dimension of detail."
            ),
            style={
                "maxWidth": "900px",
                "lineHeight": "1.6",
                "marginBottom": "30px"
            }
        ),
        html.H2("Visualization 1: Average Diamond Price by Clarity"),
        html.Img(
            src=f"data:image/png;base64,{average_price_by_clarity_image}",
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
    app.run(host='0.0.0.0', port=8050)
