import os
import time
import zipfile
from pathlib import Path

from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.support.ui import Select
import typer


def download_real_estate(url, download_dir, target_cities, driver_path):
    chrome_option = ChromeOptions()
    chrome_option.add_experimental_option('prefs', {'download.default_directory': download_dir})
    driver = Chrome(executable_path=driver_path, options=chrome_option)

    driver.get(url)
    driver.implicitly_wait(5)

    # 非本期下載
    driver.find_element_by_id('ui-id-2').click()

    # CSV
    file_format_select = Select(driver.find_element_by_id('fileFormatId'))
    file_format_select.select_by_value('csv')

    # 108Q2
    publish_date = Select(driver.find_element_by_id('historySeason_id'))
    publish_date.select_by_value('108S2')

    # advance
    driver.find_element_by_id('downloadTypeId2').click()

    # check cities
    cities = driver.find_elements_by_class_name('advDownloadClass')
    for city in cities:
        if city.text.strip() in target_cities:
            city.find_elements_by_tag_name('input')[0].click()

    # download data
    driver.find_element_by_id('downloadBtnId').click()

    # unzip
    file_path = os.path.join(download_dir, 'download.zip')
    while True:
        if os.path.exists(file_path):
            with zipfile.ZipFile(file_path) as zf:
                zf.extractall(path=download_dir)
            break
        time.sleep(1)

    # close session
    driver.close()


def main(chrome_driver_path: str = None):
    root = Path(__file__).parents[1]
    data_dir = os.path.join(root, 'data', 'raw')
    chrome_driver_path = chrome_driver_path or os.path.join(root, 'libs', 'chromedriver')

    real_estate_url = 'http://plvr.land.moi.gov.tw/DownloadOpenData'
    check_cities = ['臺北市', '新北市', '桃園市', '臺中市', '高雄市']

    download_real_estate(url=real_estate_url,
                         download_dir=data_dir,
                         target_cities=check_cities,
                         driver_path=chrome_driver_path)


if __name__ == '__main__':
    typer.run(main)
