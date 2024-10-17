# This is a config template file. Copy this file to config.py and fill in the values during production.
# !!! Do not commit the config.py file to the repository.

# Database configuration
DB_HOST = 'localhost'
DB_PORT = 5432
DB_DATABASE = 'your_database_name'
DB_USER = 'your_database_user'
DB_PASSWORD = 'your_database_password'

# Web API configuration
web_api_host = 'localhost'
web_api_port = 12345

# Spatial Computation configuration
crs = "EPSG:4326"  # Coordinate Reference System, default is WGS84
