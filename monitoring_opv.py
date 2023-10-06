#!/usr/bin/python3.9
#!/usr/bin/env python
# coding: utf-8

import datetime
import glob
import logging
import os
import pathlib
import urllib
import urllib.parse
import warnings
from collections import Counter
from sys import platform
from time import sleep

import numpy as np
import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

warnings.filterwarnings("ignore")

start_time = datetime.datetime.now()


print("# Monitoring OPV Start! #", start_time)

# Общий раздел

# Настройки для логера
if platform == "linux" or platform == "linux2":
    logging.basicConfig(
        filename="/var/log/log-execute/monitoring_opv.log.txt",
        level=logging.INFO,
        format=(
            "%(asctime)s - %(levelname)s - "
            "%(funcName)s: %(lineno)d - %(message)s"
        ),
    )
elif platform == "win32":
    logging.basicConfig(
        filename=(
            f"{pathlib.Path(__file__).parent.absolute()}"
            "/monitoring_opv.log.txt"
        ),
        level=logging.INFO,
        format=(
            "%(asctime)s - %(levelname)s - "
            "%(funcName)s: %(lineno)d - %(message)s"
        ),
    )

# Загружаем yaml файл с настройками
with open(
    f"{pathlib.Path(__file__).parent.absolute()}/settings.yaml", "r"
) as yaml_file:
    settings = yaml.safe_load(yaml_file)
telegram_settings = pd.DataFrame(settings["telegram"])


# Функция отправки уведомлений в telegram на любое количество каналов
# (указать данные в yaml файле настроек)
def telegram(i, text, media_path):
    try:
        msg = urllib.parse.quote(str(text))
        bot_token = str(telegram_settings.bot_token[i])
        channel_id = str(telegram_settings.channel_id[i])

        retry_strategy = Retry(
            total=3,
            status_forcelist=[101, 429, 500, 502, 503, 504],
            method_whitelist=["GET", "POST"],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)

        if media_path is not None:
            print(media_path)
            http.post(
                "https://api.telegram.org/bot" + bot_token + "/sendDocument?",
                data={
                    "chat_id": channel_id,
                    "parse_mode": "HTML",
                    "caption": text,
                },
                files={"document": open(media_path, "rb")},
            )
        if media_path is None:
            http.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={channel_id}&text={msg}",
                timeout=10,
            )
    except Exception as err:
        print(f"monitoring_opv: Ошибка при отправке в telegram -  {err}")
        logging.error(
            f"monitoring_opv: Ошибка при отправке в telegram -  {err}"
        )


# Функция сравнения двух датафреймов


def compare(
    latest_file,
    penult_file,
    download_path,
    download_path_yesterday,
    message_str,
    datetime_header,
):
    download_path_latest = download_path
    download_path_penult = download_path_yesterday
    message_str = message_str
    datetime_header = datetime_header
    latest_file = latest_file
    penult_file = penult_file
    print(latest_file)
    print(penult_file)

    # for sh_name in ('ВЭС', 'ГЭС', 'СЭС'):
    for sh_name in ("СЭС",):
        df1_ves = pd.read_excel(
            f"{download_path_latest}{latest_file}",
            sheet_name=sh_name,
            names=[
                "generating_object",
                "performance_indicator",
                "required_revenue",
                "application_time",
            ],
        )
        df1_ves["year"] = np.nan
        df1_ves.loc[
            df1_ves["generating_object"].str.contains(
                "Плановый год начала поставки мощности:"
            ),
            "year",
        ] = df1_ves["generating_object"].str[-4:]
        df1_ves["year"].fillna(method="ffill", inplace=True)
        print(df1_ves)

        df2_ves = pd.read_excel(
            f"{download_path_penult}{penult_file}",
            sheet_name=sh_name,
            names=[
                "generating_object",
                "performance_indicator",
                "required_revenue",
                "application_time",
            ],
        )
        df2_ves["year"] = np.nan
        df2_ves.loc[
            df2_ves["generating_object"].str.contains(
                "Плановый год начала поставки мощности:"
            ),
            "year",
        ] = df2_ves["generating_object"].str[-4:]
        df2_ves["year"].fillna(method="ffill", inplace=True)
        print(df2_ves)

        df3_ves = df1_ves[
            ~df1_ves.apply(tuple, 1).isin(df2_ves.apply(tuple, 1))
        ]
        print(df3_ves)

        text_diff = ""
        if len(df3_ves.index) > 0:
            df3_ves.reset_index(drop=True, inplace=True)
            df3_ves.replace(
                [
                    "Объект солнечной генерации",
                    "Объект ветровой генерации",
                    "Объект гидрогенерации",
                ],
                ["СЭС", "ВЭС", "ГЭС"],
                inplace=True,
            )
            print(df3_ves)

            for row_index in range(len(df3_ves.index)):
                text_diff = (
                    text_diff
                    + f"{df3_ves.year[row_index]} "
                    f"{df3_ves.generating_object[row_index]} "
                    f"{df3_ves.performance_indicator[row_index]} "
                    f"{df3_ves.required_revenue[row_index]} "
                    f"{df3_ves.application_time[row_index]}\n"
                )
        if text_diff != "":
            if sh_name == "ВЭС":
                sh_name += "🌪"
            if sh_name == "ГЭС":
                sh_name += "💦"
            if sh_name == "СЭС":
                sh_name += "☀"
            message_str = message_str + f"{sh_name}:\n" + text_diff
    if message_str != datetime_header:
        print(message_str)
        telegram(3, datetime_header, f"{download_path_latest}{latest_file}")
        telegram(3, message_str, None)


def main():
    last_timestamp = datetime.datetime.strptime(
        "05-04-2023 09:51", "%d-%m-%Y %H:%M"
    )

    while True:
        datetime_header = f"{datetime.datetime.now()}\n"
        message_str = datetime_header
        print(datetime_header)
        date_today = datetime.datetime.now().strftime("%Y%m%d")
        date_yesterday = (
            datetime.datetime.now() - datetime.timedelta(days=1)
        ).strftime("%Y%m%d")
        parent_path = str(pathlib.Path(__file__).parent.absolute())
        download_path = f"{parent_path}\\reestr_zayavki\\{date_today}\\"
        download_path_yesterday = (
            f"{parent_path}\\reestr_zayavki\\{date_yesterday}\\"
        )
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        gecko_path = (
            f"{pathlib.Path(__file__).parent.absolute()}/geckodriver.exe"
        )
        firefox_path = (
            f"{pathlib.Path(__file__).parent.absolute()}"
            "/FirefoxPortable/App/Firefox64/firefox.exe"
        )
        try:
            # 1
            # Открываем браузер, проверяем дату последнего файла
            # если она меньше чем сегодня, то пишем,что за сегодня отчетов ещё нет
            # если она больше чем время прошлого запуска, то
            # скачиваем новый, если нет, то ничего не делаем
            # или пишем в лог для мониторинга работы

            # Настройки для драйвера Firefox
            # (скрытый режим и установка драйвера(закоменчена),
            # берется geckodriver.exe из этой же папки и portable версия firefox
            options = Options()
            options.set_preference("browser.download.folderList", 2)
            options.set_preference(
                "browser.download.manager.showWhenStarting", False
            )
            options.set_preference("browser.download.dir", download_path)
            options.set_preference(
                "browser.helperApps.neverAsk.openFile",
                (
                    "application/vnd.ms-excel"
                    " application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                ),
            )
            options.set_preference(
                "browser.helperApps.neverAsk.saveToDisk",
                (
                    "application/vnd.ms-excel"
                    " application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                ),
            )
            options.headless = (
                True  # True - скрытый режим, False - показывая браузер.
            )
            # options.headless = False  # True - скрытый режим, False - показывая браузер.
            options.binary_location = firefox_path
            serv = Service(gecko_path)
            browser = webdriver.Firefox(options=options, service=serv)
            # browser.get('https://www.atsenergo.ru/vie/zainfo')
            browser.get("https://www.atsenergo.ru/vie/addopv")
            browser.set_window_size(1920, 3240)
            sleep(5)
            # dt_last_file = browser.find_element(By.XPATH, '//*[@id="block-views-ovp-block"]/div/div[2]/div[1]/div[1]/div/span').text
            ### для продленного этапа
            dt_last_file = browser.find_element(
                By.XPATH,
                '//*[@id="block-views-addopv-block"]/div/div/div[1]/div[1]/div/span',
            ).text
            file_name = browser.find_element(
                By.XPATH,
                '//*[@id="block-views-addopv-block"]/div/div/div[1]/div[2]/span/a',
            ).text
            print(file_name)
            ###
            dt_last_file = (
                dt_last_file.replace("(", "")
                .replace(")", "")
                .replace(".", "-")
            )
            dt_last_file = datetime.datetime.strptime(
                dt_last_file, "%d-%m-%Y %H:%M"
            )

            if dt_last_file.date() < datetime.date.today():
                print(f"За сегодня отчетов ещё нет")
                logging.info(f"За сегодня отчетов ещё нет")

            print(dt_last_file, type(dt_last_file))
            print(last_timestamp, type(last_timestamp))
            # if dt_last_file > last_timestamp:
            ### для продленного этапа
            if (
                dt_last_file > last_timestamp
                and "перечень принятых заявок" in file_name
            ):
                last_timestamp = dt_last_file
                print(f"Есть новый отчет от {dt_last_file}")
                # element = browser.find_element(By.XPATH, '//*[@id="block-views-ovp-block"]/div/div[2]/div[1]/div[2]/a').click()
                element = browser.find_element(
                    By.XPATH,
                    '//*[@id="block-views-addopv-block"]/div/div/div[1]/div[2]/span/a',
                ).click()
                sleep(2)

                # 2
                # Смотрим сколько файлов в папке
                # если один, то пишем, что за сегодня отчет появился
                # если больше чем один, то находим последний и предпоследний
                # и находим отличия и отправляем в телегу
                filenames = [
                    entry.name
                    for entry in sorted(
                        os.scandir(download_path),
                        key=lambda x: x.stat().st_mtime,
                        reverse=True,
                    )
                ]
                filenames_yesterday = [
                    entry.name
                    for entry in sorted(
                        os.scandir(download_path_yesterday),
                        key=lambda x: x.stat().st_mtime,
                        reverse=True,
                    )
                ]
                if len(filenames) == 1:
                    print(
                        "monitoring_opv: Появился первый отчет за сегодня от"
                        f" даты  {dt_last_file}"
                    )
                    logging.info(
                        "monitoring_opv: Появился первый отчет за сегодня от"
                        f" даты  {dt_last_file}"
                    )
                    latest_file = filenames[0]
                    penult_file = filenames_yesterday[0]
                    telegram(
                        3,
                        (
                            "monitoring_opv: Появился первый отчет за сегодня"
                            f" от даты  {dt_last_file}"
                        ),
                        f"{download_path}{latest_file}",
                    )
                    compare(
                        latest_file,
                        penult_file,
                        download_path,
                        download_path_yesterday,
                        message_str,
                        datetime_header,
                    )
                if len(filenames) > 1:
                    latest_file = filenames[0]
                    penult_file = filenames[1]
                    compare(
                        latest_file,
                        penult_file,
                        download_path,
                        download_path,
                        message_str,
                        datetime_header,
                    )
            else:
                logging.info(
                    f"С последней проверки нового отчета не появилось"
                )
        except Exception as err:
            print(f"monitoring_opv: Ошибка при выполнении -  {err}")
            logging.error(f"monitoring_opv: Ошибка при выполнении -  {err}")
            telegram(
                1, f"monitoring_opv: Ошибка при выполнении -  {err}", None
            )
        finally:
            browser.quit()
            sleep(60)


if __name__ == "__main__":
    main()

# print('Время выполнения:', datetime.datetime.now() - start_time)
