import os
import time
import zipfile

from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select

root = os.path.abspath(os.path.dirname(__file__))
data_dir = os.path.join(root, 'data')

chrome_option = Options()
chrome_option.add_experimental_option('prefs', {'download.default_directory': data_dir})
driver = Chrome(executable_path='libs/chromedriver', options=chrome_option)

url = 'http://plvr.land.moi.gov.tw/DownloadOpenData'
driver.get(url)
driver.implicitly_wait(5)

# 非本期下載
driver.find_element_by_id('ui-id-2').click()

# CSV
file_format_select = Select(driver.find_element_by_id('fileFormatId'))
file_format_select.select_by_value('csv')

# 108第二季
publish_date = Select(driver.find_element_by_id('historySeason_id'))
publish_date.select_by_value('108S2')

# advance
driver.find_element_by_id('downloadTypeId2').click()

# check cities
check_cities = ['臺北市', '新北市', '桃園市', '臺中市', '高雄市']
cities = driver.find_elements_by_class_name('advDownloadClass')
for city in cities:
    if city.text.strip() in check_cities:
        city.find_elements_by_tag_name('input')[0].click()

# download data
driver.find_element_by_id('downloadBtnId').click()

# unzip
file_path = os.path.join(data_dir, 'download.zip')
while True:
    if os.path.exists(file_path):
        with zipfile.ZipFile(file_path) as zf:
            zf.extractall(path=data_dir)
        break
    time.sleep(1)

driver.close()
