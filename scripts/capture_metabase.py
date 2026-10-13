#!/usr/bin/env python3
import time
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

OUT_DIR = Path("report/screenshots")
OUT_DIR.mkdir(parents=True, exist_ok=True)

opts = Options()
opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-gpu")
opts.add_argument("--disable-software-rasterizer")
opts.add_argument("--window-size=1600,1000")

driver = webdriver.Chrome(options=opts)

try:
    print("Logging into Metabase...")
    driver.get("http://localhost:3000/auth/login")
    time.sleep(3)
    try:
        driver.find_element(By.NAME, "username").send_keys("lab@example.com")
        driver.find_element(By.NAME, "password").send_keys("Day9-lab-pass!")
        driver.find_element(By.XPATH, "//button[@type='submit']").click()
        time.sleep(3)
    except Exception as e:
        print("Login form note:", e)

    print("Navigating to dashboard 2...")
    driver.get("http://localhost:3000/dashboard/2")
    time.sleep(5)
    driver.save_screenshot(str(OUT_DIR / "metabase_dashboard.png"))
    print("Saved metabase_dashboard.png successfully!")
finally:
    driver.quit()
