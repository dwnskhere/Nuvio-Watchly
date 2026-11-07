"""Compatibility import for the historical service name; no Redis server is required."""

from app.services.mongo_store import mongo_store

redis_service = mongo_store
