"""
Flask application for the Grad Cafe admissions website, including a new
page called "Will You Get In?" that runs the fine-tuned transformer model
on user-submitted applicant information.
"""

from __future__ import annotations

from flask import Flask, render_template, request

from inference import AdmissionsPredictor


DISCLAIMER_TEXT = (
    "Disclaimer: This is a course project model trained on scraped, "
    "self-reported admissions data. It is not a real admissions decision "
    "system and should not be treated as an authority."
)


def create_app() -> Flask:
    """
    Create and configure the Flask app.

    Returns:
        Flask application instance.
    """
    app = Flask(__name__)

    predictor = AdmissionsPredictor()

    @app.route("/", methods=["GET"])
    def index():
        """
        Homepage route.

        Returns:
            Rendered homepage template.
        """
        return render_template("index.html")

    @app.route("/will-you-get-in", methods=["GET", "POST"])
    def will_you_get_in():
        """
        Admissions prediction page.

        GET:
            Show a blank form.

        POST:
            Read form values, validate numeric fields, run model inference,
            and display prediction results.

        Returns:
            Rendered prediction page template.
        """
        result = None
        error_message = None

        form_data = {
            "program_name": "",
            "university": "",
            "comments": "",
            "start_term": "",
            "degree": "",
            "international_american": "",
            "gpa": "",
            "gre_score": "",
            "gre_v_score": "",
            "gre_aw": "",
        }

        if request.method == "POST":
            form_data = {
                "program_name": request.form.get("program_name", "").strip(),
                "university": request.form.get("university", "").strip(),
                "comments": request.form.get("comments", "").strip(),
                "start_term": request.form.get("start_term", "").strip(),
                "degree": request.form.get("degree", "").strip(),
                "international_american": request.form.get(
                    "international_american", ""
                ).strip(),
                "gpa": request.form.get("gpa", "").strip(),
                "gre_score": request.form.get("gre_score", "").strip(),
                "gre_v_score": request.form.get("gre_v_score", "").strip(),
                "gre_aw": request.form.get("gre_aw", "").strip(),
            }

            numeric_fields = {
                "GPA": form_data["gpa"],
                "GRE Total": form_data["gre_score"],
                "GRE Verbal": form_data["gre_v_score"],
                "GRE AW": form_data["gre_aw"],
            }

            invalid_numeric_fields = []

            for field_name, raw_value in numeric_fields.items():
                if raw_value != "":
                    try:
                        float(raw_value)
                    except ValueError:
                        invalid_numeric_fields.append(field_name)

            if invalid_numeric_fields:
                error_message = (
                    "These fields must be numeric if provided: "
                    + ", ".join(invalid_numeric_fields)
                )
            else:
                try:
                    result = predictor.predict(form_data)
                except Exception:
                    error_message = (
                        "Sorry, the prediction could not be generated right now. "
                        "Please make sure the trained model exists in the "
                        "'saved_model' folder and try again."
                    )

        return render_template(
            "will_you_get_in.html",
            result=result,
            error_message=error_message,
            form_data=form_data,
            disclaimer=DISCLAIMER_TEXT,
        )

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
