##################################
# 0. Librerías
##################################
import os
import time
import json
import wget
import zipfile
import requests
import platform

import datetime
import pandas as pd
from browsermobproxy import Server

import psycopg2
from sqlalchemy import create_engine

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC

from utils.paths import PROJECT_PATH

##################################
# 0. Funciones de procesamiento
##################################

def process_date(date):
    try:
        return date.replace('T',' ').split('.')[0]
    except:
        return None

##################################
# 1. Funciones de PostgreSQL
##################################

def create_update_table(df, tabla, db_user, db_password, db_host, db_port, db_name):   
    # 1. Crear conexión a la base de datos
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)

    # 2. Guarda el DataFrame en la tabla de PostgreSQL
    df.to_sql(tabla, engine, if_exists='replace', index=False)
    print("El DataFrame `{}` se ha guardado correctamente en la tabla de PostgreSQL.".format(tabla))

    # 3. Cerar conexión de base de datos
    engine.dispose()

def insert_dataframe_to_table(dataframe, table_name, db_user, db_password, db_host, db_port, db_name):
    # 1. Conexión a la base de datos PostgreSQL
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)
    
    # 2. Insertar DataFrame en la tabla
    dataframe.to_sql(table_name, engine, if_exists='append', index=False)

    # 3. Cerar conexión de base de datos
    engine.dispose()

def select_from_table(query, db_user, db_password, db_host, db_port, db_name):
    # 1. Conexión a la base de datos PostgreSQL
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    conn = create_engine(connection_string)

    # 2. Ejecutar la consulta y obtener los resultados
    results = pd.read_sql(query, conn)

    # 3. Cerrar la conexión a la base de datos
    conn.dispose()

    return results

######################################
# 2. Funciones del driver y proxy
######################################

def get_chrome_driver(chromedriver_path=None, print_view=False, headless=False):
    
    # Iniciar el servidor proxy
    if os.name == 'nt':  # Para Windows
        browsermob_path = os.path.join(PROJECT_PATH, 'browsermob-proxy-2.1.4', 'bin', 'browsermob-proxy.bat')
        print("Se inicia el servidor en Windows")
    else:  # Para Mac o Linux
        browsermob_path = os.path.join(PROJECT_PATH, 'browsermob-proxy-2.1.4', 'bin', 'browsermob-proxy')
        print("Se inicia el servidor en Mac o Linux")
    
    server = Server(browsermob_path) 
    server.start()
    proxy = server.create_proxy(params={'trustAllServers': 'true'})
        
    # Configurar las opciones de Selenium
    options = webdriver.ChromeOptions()

    options.add_argument("--no-sandbox")
    # options.add_argument("--start-maximized")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--proxy-server={0}".format(proxy.proxy))

    if print_view:
        options.add_argument('--disable-print-preview')
    
    if headless:
        options.add_argument("--headless=new")

    if chromedriver_path is not None:
        print("Usando el chromedriver_path: {}".format(chromedriver_path))
        service = Service(executable_path=chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)
    else:
        print("Usando el chromedriver_path por defecto")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    return driver, server, proxy

def get_firefox_driver(geckodriver_path=None, print_view=False, headless=False):
    # Iniciar el servidor proxy
    if os.name == 'nt':  # Para Windows
        browsermob_path = os.path.join(PROJECT_PATH, 'browsermob-proxy-2.1.4', 'bin', 'browsermob-proxy.bat')
        print("Se inicia el servidor en Windows")
    else:  # Para Mac o Linux
        browsermob_path = os.path.join(PROJECT_PATH, 'browsermob-proxy-2.1.4', 'bin', 'browsermob-proxy')
        print("Se inicia el servidor en Mac o Linux")

    server = Server(browsermob_path)
    server.start()
    proxy = server.create_proxy(params={'trustAllServers': 'true'})

    # Configurar las opciones de Selenium
    options = webdriver.FirefoxOptions()

    options.add_argument("--no-sandbox")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--proxy-server={0}".format(proxy.proxy))

    if print_view:
        options.add_argument('--disable-print-preview')

    if headless:
        options.add_argument("--headless")

    print("Usando el geckodriver_path por defecto")
    service = Service(executable_path='geckodriver.exe')
    driver = webdriver.Firefox(service=service, options=options)

    return driver, server, proxy

def stop_chrome_driver(driver, server, proxy):
    driver.quit()
    server.stop()
    proxy.close()


##################################
# 3. Funciones de Selenium
##################################

def find_elements_decorator(func):
    def wrapper(driver, by, value, text):
        try:
            wait = WebDriverWait(driver, 10)
            wait.until(EC.presence_of_element_located((by, value)))
            func(driver, by, value, text)
            time.sleep(1)

        except:
            print(f"Ocurrió un error by: ({by}), value: ({value})")
    return wrapper

def find_element_decorator(func):
    def wrapper(driver, by, value):
        try:
            wait = WebDriverWait(driver, 10)
            wait.until(EC.presence_of_element_located((by, value)))
            func(driver, by, value)
            time.sleep(1)

        except:
            print(f"Ocurrió un error by: ({by}), value: ({value})")
    return wrapper

@find_elements_decorator
def find_elements_and_click(driver, by, value, text):
    elements = driver.find_elements(by, value)
    for element in elements:
        if text in element.text:
            element.click()
            break

@find_element_decorator
def find_element_and_click(driver, by, value):
    driver.find_element(by, value).click()
