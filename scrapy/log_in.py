import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from selenium.webdriver.edge.service import Service
import platform
import os
from base import get_system_path
button_text_xpath = "/html/body/div[2]/div/div/div/div/div[2]/div[1]/div/div/div/div[2]/div[1]/div[1]/button/div[1]"
webdriver_executable_path=get_system_path('webdriver_executable_path')
binary_location = get_system_path('binary_location')
password_file = get_system_path('password_file')
def init_driver():
    webdriver_service = Service(executable_path=webdriver_executable_path)
    options = webdriver.EdgeOptions()
    options.binary_location = binary_location
    driver = webdriver.Edge(options=options,service=webdriver_service)
    return driver

def load_account_info():
    df = pd.read_excel(password_file)
    username = df.iloc[0, 1]  # B2
    password = df.iloc[0, 2]  # C2
    return username, password

def login_to_website(driver, username, password):
    driver.get('https://partners.wayfair.com/')
    wait = WebDriverWait(driver, 10)
    username_input = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div/div[2]/div/div/div[1]/div[1]/form/div[1]/input')))
    password_input = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div/div[2]/div/div/div[1]/div[1]/form/div[2]/input')))
    username_input.send_keys(username)
    password_input.send_keys(password)
    login_button = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div/div[2]/div/div/div[1]/div[1]/form/div[4]/input[2]')))
    login_button.click()

def get_and_click_button(driver, wait, xpath):
    button = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
    button.click()

def get_cookies(driver):
    return "; ".join([f'{cookie["name"]}={cookie["value"]}' for cookie in driver.get_cookies()])

def start():
    driver = init_driver()
    username, password = load_account_info()
    login_to_website(driver, username, password)

    wait = WebDriverWait(driver, 10)

    wait.until(EC.visibility_of_element_located((By.XPATH, button_text_xpath)))
    button_text = driver.find_element(By.XPATH,button_text_xpath).text

    return driver,button_text,wait

if __name__ == '__main__':
    driver,button_text,wait = start()

    if button_text == "CAN_39FInc.":
        cookies_ca = get_cookies(driver)
        first_button_xpath = "/html/body/div[2]/div/div/div/div/div[2]/div[1]/div/div/div/div[2]/div[1]/div/button"
        get_and_click_button(driver, wait, first_button_xpath)
        second_button_xpath = "/html/body/div[2]/div/div/div/div/div[2]/div[1]/div/div/div/div[2]/div[1]/div[2]/div/ul/li[2]/button"
        wait.until(EC.visibility_of_element_located((By.XPATH, second_button_xpath)))
        get_and_click_button(driver, wait, second_button_xpath)
    elif button_text == "39fInc.":
        cookies_us = get_cookies(driver)
        first_button_xpath = "/html/body/div[2]/div/div/div/div/div[2]/div[1]/div/div/div/div[2]/div[1]/div/button"
        get_and_click_button(driver, wait, first_button_xpath)
        second_button_xpath = "/html/body/div[2]/div/div/div/div/div[2]/div[1]/div/div/div/div[2]/div[1]/div[2]/div/ul/li[1]/button"
        wait.until(EC.visibility_of_element_located((By.XPATH, second_button_xpath)))
        get_and_click_button(driver, wait, second_button_xpath)

    time.sleep(20)
    wait.until(EC.visibility_of_element_located((By.XPATH, button_text_xpath)))
    button_text = driver.find_element(By.XPATH,button_text_xpath).text
    if button_text == "CAN_39FInc.":
        cookies_ca = get_cookies(driver)
    elif button_text == "39fInc.":
        cookies_us = get_cookies(driver)

    driver.quit()
