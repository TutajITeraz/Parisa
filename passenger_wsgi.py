# Entry point for Passenger / cPanel "Setup Python App" (startup file: passenger_wsgi.py, entry point: application)
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server import application  # noqa: E402,F401
