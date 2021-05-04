from typing import Optional

import os
import motor.motor_asyncio
from fastapi import FastAPI, HTTPException
from pymongo.errors import DuplicateKeyError

app = FastAPI()

# Database
client = motor.motor_asyncio.AsyncIOMotorClient(os.environ["MONGODB_URL"])
db = client.metadata_server
collection = db.metadata


@app.post("/items/{key}")
async def create_item(key, value):
    """Create an item"""

    new_item = {"_id": key, "value": value}

    # Query db.
    try:
        result = await collection.insert_one(new_item)
    except DuplicateKeyError as e:
        raise HTTPException(status_code=409, detail="Duplicate key.")

    return new_item


@app.get("/items/{key}")
async def read_item(key):
    """Get value with specific key."""
    # Query db.
    result = await collection.find_one({"_id": key})

    # Return results or not found.
    if result:
        return result
    else:
        raise HTTPException(status_code=404, detail="Item not found.")


@app.put("/items/{key}")
async def update_item(key, value):
    """Update an existing key."""
    # Query db.
    result = await collection.update_one({"_id": key}, {"$set": {"value": value}})

    if result.matched_count:
        return result.raw_result
    else:
        raise HTTPException(status_code=404, detail="Item not found.")


@app.delete("/items/{key}")
async def delete_item(key):
    """Remove an item."""
    # Query db.
    result = await collection.delete_one({"_id": key})

    if result.deleted_count:
        return result.raw_result
    else:
        raise HTTPException(status_code=404, detail="Item not found.")






