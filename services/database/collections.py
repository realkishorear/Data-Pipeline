"""Database collections setup."""
from pymongo import MongoClient
from config.settings import MONGO_URI

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client.get_default_database()

# Collections
checklist_map_col = db["checklistmaps"]
inspections_col = db["inspections"]
file_uploads_col = db["checklistfileuploads"]
checklistresults_col = db["checklistresults"]

