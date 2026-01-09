from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import os

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("DB_URL")
DB_NAME = os.getenv("DB_NAME")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Collection name
collection = db["checklistmaps"]

# Current UTC timestamp
now = datetime.utcnow()

# Data with metadata
data = [
    {"acronym": "DA", "checklistRef": "68cbd3a293105c0c3fcdba52"},
    {"acronym": "IM", "checklistRef": "68cbcead93105c0c3fcdb97e"},
    {"acronym": "FM", "checklistRef": "68cbc2bd93105c0c3fcdb879"},
    {"acronym": "PR", "checklistRef": "68cbc21593105c0c3fcdb7e1"},
    {"acronym": "LD", "checklistRef": "68cbb95c93105c0c3fcdb6d5"}
]

# Add metadata to each document
for doc in data:
    doc.update({
        "createdAt": now,
        "updatedAt": now,
        "createdBy": "system",
        "updatedBy": "system",
        "isActive": True
    })

# Prevent inserting duplicates based on acronym
inserted_count = 0
for doc in data:
    existing = collection.find_one({"acronym": doc["acronym"]})
    if existing:
        print(f"[WARN] Skipping duplicate: {doc['acronym']}")
    else:
        collection.insert_one(doc)
        inserted_count += 1
        print(f"[INFO] Inserted: {doc['acronym']}")

print(f"\n[INFO] Done. Inserted {inserted_count} new documents into 'checklistmaps'.")
