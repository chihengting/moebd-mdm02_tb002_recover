# ==============================================================================
# 腳本名稱：trigger_silver.py
# 目的：透過呼叫 Cloud Run API，觸發 MDM 資料的 Bronze to Silver 轉換。
# 邏輯：
#   1. 遍歷指定日期範圍。
#   2. 模擬 Airflow 呼叫 Cloud Run 介面。
#   3. 將補全後的 tb002 JSON 檔案 (Bronze) 轉換為 Parquet 檔案 (Silver)。
#   4. 僅處理 tb002 (tablet_usage)，確保 tb001 (tablet_status) 資料維持舊版不動。
# ==============================================================================

import requests
import json
import os
import google.auth
import google.auth.transport.requests
import google.oauth2.id_token
from datetime import datetime, timedelta

# ------------------------------------------------------------------------------
# 1. 環境設定
# ------------------------------------------------------------------------------
PROJECT_ID = "twmoe-bigdata-prod"       # GCP 專案 ID
DRY_RUN = False                        # True: 僅印出 Payload; False: 真正執行請求
# Cloud Run 服務的端點 (Base URL)
BASE_URL = "https://twmoe-bigdata-cdp-cr-data-transport-01-771805491747.asia-east1.run.app"
SA_NAME = "mdm02"                      # 服務分類名稱
BRONZE_BUCKET = "moebd-bronze-prod"    # 來源 JSON Bucket
SILVER_BUCKET = "moebd-silver-prod"    # 輸出 Parquet Bucket
START_DATE = "2026-02-01"              # 開始執行任務的日期
END_DATE = "2026-02-08"                # 結束執行任務的日期

# 確保認證程式庫能夠識別當前專案，避免配額 (Quota) 錯誤
os.environ["GOOGLE_CLOUD_PROJECT"] = PROJECT_ID

# ------------------------------------------------------------------------------
# 2. 輔助函式：取得身分驗證 Token
# ------------------------------------------------------------------------------
def get_id_token():
    """
    獲取呼叫 Cloud Run 所需的 OIDC ID Token。
    1. 優先嘗試從 Metadata Server 獲取 (若在 GCP 環境執行)。
    2. 若失敗，則嘗試從本地授權認證 (ADC) 獲取。
    """
    try:
        # 建立一個針對特定 Audience (BASE_URL) 的身分請求
        auth_req = google.auth.transport.requests.Request()
        return google.oauth2.id_token.fetch_id_token(auth_req, BASE_URL)
    except Exception as e:
        # 本地開發測試時常見的例外處理
        print(f"身分驗證提示：{e}。將嘗試使用本地 Application Default Credentials...")
        creds, _ = google.auth.default(quota_project_id=PROJECT_ID)
        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        # 部分 SDK 版本下 token 存放在 id_token 或 token 屬性中
        return getattr(creds, 'id_token', creds.token)

# ------------------------------------------------------------------------------
# 3. 核心處理函式：觸發單日任務
# ------------------------------------------------------------------------------
def process_date(date_str, header):
    """
    建構 Payload 並發送 API 請求。
    """
    # 這裡的 file_name 維持為原本的 .zip 格式字串，以便讓 Silver 的 Partition 路徑與舊版 tb001 一致
    file_name = f"{date_str}.zip"
    bronze_uri = f"gs://{BRONZE_BUCKET}/{SA_NAME}/{date_str}"
    
    # 定義要處理的表格：目前僅限定處理 tb002 (tablet_usage)
    tasks = [
        {
            "table": "tablet_usage",                   # Silver 區的資料表目錄
            "prefix": "tb002_",                        # 掃描 Bronze 區檔案的前綴
            "dtype_mapping": {"timeperiod": "float64"}, # 指定特定欄位的資料型別
            "logic": "mdm02"                           # 特殊業務處理邏輯
        }
    ]

    for task in tasks:
        # 構建輸出 Parquet 的目標路徑，採用 batch_m 分區 (Partitioning) 策略
        silver_uri = f"gs://{SILVER_BUCKET}/{SA_NAME}/{task['table']}/batch_m={file_name}/{task['table']}_{date_str}.parquet"
        
        # 建構傳送給 Cloud Run API 的參數
        payload = {
            "from_dir": bronze_uri,
            "prefix": task["prefix"],
            "add_created_date_m": True,  # 在 Silver 資料中加入「建立日期」欄位
            "add_batch_m": False,        # batch_m 已透過 to_dir 的 partition 路徑定義，不需額外重複
            "format": "json",            # 來源檔案格式
            "to_dir": silver_uri,        # 輸出目標路徑
            "dtype_mapping": task["dtype_mapping"],
            "data_source_specific_logic": task["logic"],
        }

        if DRY_RUN:
            # 預檢模式：僅列印 Payload，不實際執行 API 呼叫
            print(f"\n[預檢] Table: {task['table']} - 日期: {date_str}")
            print(f"目標 URL: {BASE_URL}/multi_file_bronze_to_silver")
            print(f"Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")
        else:
            # 執行模式：發送正式 HTTP POST 請求
            print(f"正在觸發 {task['table']} (日期：{date_str})...")
            resp = requests.post(
                f"{BASE_URL}/multi_file_bronze_to_silver",
                json=payload,
                headers=header,
            )
            
            # API 回傳 201 代表任務已成功於後端執行完畢並建立檔案
            if resp.status_code == 201:
                print(f"成功處理：{task['table']} (日期：{date_str})")
            else:
                # 記錄失敗的原因 (錯誤碼與錯誤訊息)
                print(f"處理失敗：{task['table']} (日期：{date_str}) | 錯誤碼：{resp.status_code}")
                print(f"錯誤內容：{resp.text}")

# ------------------------------------------------------------------------------
# 4. 腳本入口點 (Main Loop)
# ------------------------------------------------------------------------------
def main():
    token = None
    if not DRY_RUN:
        # 正式執行前先取得一次 Token (通常 ID Token 有一小時效能，若日期範圍過大需在 loop 中重新取得)
        token = get_id_token()
    
    header = {"Authorization": f"Bearer {token}"} if token else {}
    
    # 將日期字串轉換為 datetime 物件進行計算
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    delta = timedelta(days=1)
    
    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        process_date(date_str, header)
        current += delta
    
    if DRY_RUN:
        print("\n------------------------------------------")
        print("預檢測試結束。請確認 Payload 的 URI 與參數無誤。")
        print("若正確，將 DRY_RUN 設為 False 即可執行補資料。")

if __name__ == "__main__":
    main()
