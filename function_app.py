"""
function_app.py

Main entry point for the Azure Function App.
Registers all function blueprints.
"""
from dotenv import load_dotenv

load_dotenv()

import azure.functions as func

from functions.bronze_nexudus import bp as bronze_bp
from functions.silver_nexudus import bp as silver_bp

app = func.FunctionApp()
app.register_functions(bronze_bp)
app.register_functions(silver_bp)
