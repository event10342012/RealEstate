# 使用說明

1. 下載 Chrome driver. [here](https://chromedriver.chromium.org/downloads)


2. 解壓縮後將檔案放置libs目錄下面


3. 安裝python依賴套件 
```shell
$ pip install -r requirements.txt
```

4. 執行爬蟲程式

```shell
# use absolute path instead of relative, otherwise download directory would be difference
# add --chrome-driver-path argus, if your chrome driver path is difference
$ python3 /{project_path}/src/real_estate_crawler.py
```

5. 執行spark處理並輸出結果到 ./data/result
```shell
$ python3 /{project_path}/src/spark_processing.py
```
