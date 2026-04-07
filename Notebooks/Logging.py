# Databricks notebook source
import logging

# Clear existing handlers (important in notebooks)
logging.getLogger().handlers.clear()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

# Print logs in notebook
logging.getLogger().addHandler(logging.StreamHandler())

log_records = []

from datetime import datetime

def log_message(level, message):
    log_records.append((datetime.now(), level, message))
    print(f"{level}: {message}")