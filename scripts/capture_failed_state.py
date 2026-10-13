#!/usr/bin/env python3
import time
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

OUT_DIR = Path("report/screenshots")
opts = Options()
opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-gpu")
opts.add_argument("--window-size=1600,1000")

driver = webdriver.Chrome(options=opts)
try:
    driver.get("http://localhost:8080/login/")
    time.sleep(2)
    try:
        driver.find_element(By.ID, "username").send_keys("admin")
        driver.find_element(By.ID, "password").send_keys("admin")
        driver.find_element(By.XPATH, "//input[@type='submit' or @value='Sign In']").click()
        time.sleep(3)
    except Exception:
        pass

    # Capture failed graph
    driver.get("http://localhost:8080/dags/day09_video_pipeline/graph?dag_run_id=manual__2026-10-13T04:22:27%2B00:00")
    time.sleep(5)
    driver.save_screenshot(str(OUT_DIR / "airflow_task_failed.png"))
    print("Saved airflow_task_failed.png")
finally:
    driver.quit()
