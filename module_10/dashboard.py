"""
This module creates a Dash dashboard for the diamonds dataset
and displays static and interactive visualizations.
"""

import base64
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html


def encode_image(image_path):
    """Encode an image file for display in the Dash app."""
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")


def create_interactive_figure(data):
    """Create an interactive Plotly scatter plot."""
    fig = px.scatter(
        data,
        x="carat",
        y="price",
        color="cut",
        title="Diamond Price vs Carat by Cut Quality",
        labels={"price": "Price (USD)", "carat": "Carat"},
    )
    fig.update_layout(template="plotly_white")
    fig.update_traces(marker={"size": 5})
    return fig


app = Dash(__name__)

df = pd.read_csv("diamonds.csv")
price_vs_carat = encode_image("price_vs_carat.png")
price_by_cut = encode_image("price_by_cut.png")
interactive_fig = create_interactive_figure(df)

app.layout = html.Div(
    style={
        "fontFamily": "Arial, sans-serif",
        "padding": "30px",
        "backgroundColor": "#f8f9fa"
    },
    children=[
        html.H1(
            "Can the Price of a Diamond Be Determined by Its Features?",
            style={
                "textAlign": "center",
                "marginBottom": "15px"
            }
        ),
        html.P(
            (
                "Diamond price is strongly influenced by carat, with larger diamonds "
                "generally costing much more. Cut quality also affects price, although "
                "there is overlap between categories. Overall, carat appears to be the "
                "strongest predictor of diamond price."
            ),
            style={
                "textAlign": "center",
                "maxWidth": "900px",
                "margin": "0 auto 30px auto",
                "fontSize": "18px",
                "lineHeight": "1.6"
            }
        ),
        html.Div(
            style={
                "backgroundColor": "white",
                "padding": "20px",
                "marginBottom": "30px",
                "borderRadius": "10px",
                "boxShadow": "0 2px 8px rgba(0, 0, 0, 0.1)"
            },
            children=[
                html.H2(
                    "Scatter Plot: Price vs Carat",
                    style={"textAlign": "center"}
                ),
                html.Img(
                    src=f"data:image/png;base64,{price_vs_carat}",
                    style={
                        "width": "85%",
                        "display": "block",
                        "margin": "20px auto"
                    }
                )
            ]
        ),
        html.Div(
            style={
                "backgroundColor": "white",
                "padding": "20px",
                "marginBottom": "30px",
                "borderRadius": "10px",
                "boxShadow": "0 2px 8px rgba(0, 0, 0, 0.1)"
            },
            children=[
                html.H2(
                    "Boxplot: Price Distribution by Cut Quality",
                    style={"textAlign": "center"}
                ),
                html.Img(
                    src=f"data:image/png;base64,{price_by_cut}",
                    style={
                        "width": "85%",
                        "display": "block",
                        "margin": "20px auto"
                    }
                )
            ]
        ),
        html.Div(
            style={
                "backgroundColor": "white",
                "padding": "20px",
                "borderRadius": "10px",
                "boxShadow": "0 2px 8px rgba(0, 0, 0, 0.1)"
            },
            children=[
                html.H2(
                    "Interactive Plot: Diamond Price vs Carat by Cut Quality",
                    style={"textAlign": "center"}
                ),
                dcc.Graph(
                    figure=interactive_fig,
                    style={"width": "90%", "margin": "0 auto"}
                )
            ]
        )
    ]
)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)
