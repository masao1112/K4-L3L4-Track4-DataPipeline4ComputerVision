#!/usr/bin/env python3
import time
import os
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

OUT_DIR = Path("report/screenshots")
OUT_DIR.mkdir(parents=True, exist_ok=True)

opts = Options()
opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-gpu")
opts.add_argument("--window-size=1600,1000")

driver = webdriver.Chrome(options=opts)

try:
    # 1. Airflow Graph
    print("Capturing Airflow Graph...")
    driver.get("http://localhost:8080/login/")
    time.sleep(2)
    # Login
    try:
        user_input = driver.find_element(By.ID, "username")
        user_input.send_keys("admin")
        pass_input = driver.find_element(By.ID, "password")
        pass_input.send_keys("admin")
        driver.find_element(By.XPATH, "//input[@type='submit' or @value='Sign In']").click()
        time.sleep(3)
    except Exception as e:
        print("Airflow login step error or already logged in:", e)

    driver.get("http://localhost:8080/dags/day09_video_pipeline/graph")
    time.sleep(5)
    driver.save_screenshot(str(OUT_DIR / "airflow_graph.png"))
    print("Saved airflow_graph.png")

    # 2. MinIO Lake
    print("Capturing MinIO Lake...")
    driver.get("http://localhost:9001/login")
    time.sleep(2)
    try:
        access_key = driver.find_element(By.ID, "accessKey")
        access_key.send_keys("minioadmin")
        secret_key = driver.find_element(By.ID, "secretKey")
        secret_key.send_keys("minioadmin")
        driver.find_element(By.XPATH, "//button[@type='submit']").click()
        time.sleep(3)
    except Exception as e:
        print("MinIO login step error:", e)

    driver.get("http://localhost:9001/browser/lake")
    time.sleep(5)
    driver.save_screenshot(str(OUT_DIR / "minio_lake.png"))
    print("Saved minio_lake.png")

    # 3. Metabase Dashboard
    print("Capturing Metabase Dashboard...")
    driver.get("http://localhost:3000/auth/login")
    time.sleep(2)
    try:
        email_in = driver.find_element(By.NAME, "username")
        email_in.send_keys("lab@example.com")
        pass_in = driver.find_element(By.NAME, "password")
        pass_in.send_keys("Day9-lab-pass!")
        driver.find_element(By.XPATH, "//button[@type='submit']").click()
        time.sleep(4)
    except Exception as e:
        print("Metabase login step error:", e)

    driver.get("http://localhost:3000/dashboard/2")
    time.sleep(8)
    driver.save_screenshot(str(OUT_DIR / "metabase_dashboard.png"))
    print("Saved metabase_dashboard.png")

finally:
    driver.quit()

print("All screenshots taken successfully.")
