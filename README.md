# MDM tb002 資料恢復專案 (mdm-tb002-recover)

本專案旨在解決 moebd-mdm02 MDM (Mobile Device Management) 系統中 `tb002` (設備使用記錄/tablet_usage) 資料遺漏的問題。

## 專案背景

在 2026-02-09 至 2026-03-01 期間，系統雖然成功匯入了 `tb001` (設備狀態)，但遺漏了對應的 `tb002` 使用資料。
*   **tb001 (tablet_status)**：必須保留原始上傳版本，不可覆蓋。
*   **tb002 (tablet_usage)**：需從帶有時間戳記的修復資料夾中補全至 Bronze 區，再經過 Silver 區轉換，最後入庫至 Storage 層。

## 環境需求

1.  **作業系統**：Windows (支援 PowerShell)。
2.  **Google Cloud SDK (gcloud CLI)**：需已安裝並完成認證。
3.  **BigQuery 權限**：需具備執行預存程序 (Stored Procedures) 與修改目標表格的權限。
4.  **Python 3.10+**：用於執行 Cloud Run 觸發腳本。

## 檔案說明

### 核心操作腳本
*   `cp_bronze_data.ps1`：將缺失的 `tb002_*.json` 從修復目錄複製到原始 Bronze Bucket。
*   `trigger_silver.py`：呼叫 Cloud Run API，將補全後的 JSON 轉換為 Silver 區的 Parquet。
*   `backfill_mdm_usage.sql`：BigQuery 自動化回補腳本，將資料從 Silver 載入 Storage 與 Dashboard 層。

---

## 操作流程 (三步走)

### 第一步：補全 Bronze 區 JSON 檔案
執行 `cp_bronze_data.ps1`。該腳本會掃描日期範圍，將修復資料夾中的 `tb002` 檔案補進原始目錄。
**關鍵安全性**：使用 `--no-clobber` 參數，絕對不覆蓋已存在的檔案，確保 `tb001` 安全。
```powershell
.\cp_bronze_data.ps1
```

### 第二步：觸發 Silver 區 Parquet 轉換
執行 `trigger_silver.py`。該腳本會呼叫 Cloud Run，針對補全後的 `tb002_` 檔案進行 Parquet 轉換。
```powershell
python .\trigger_silver.py
```

### 第三步：BigQuery 資料入庫與報表修復
開啟 `backfill_mdm_usage.sql`，將內容貼上至 **BigQuery Console** 並執行。
此腳本會自動循環執行 **2026-02-09 至 2026-03-01** 的每一天：
1.  執行 `stp_mdm02_ToWS_tablet_usage`。
2.  更新 `dw_storage.mdm02_tb_tablet_usage_new_schema`。
3.  更新 `dw_dashboard.mdm02_tb_tablet_usage` (報表數據即時生效)。

---

## 重要注意事項

1.  **廢棄表格警示**：
    *   `dw_storage.mdm02_tb_tablet_usage` (實體表) 已於 2025-07 停止更新，**請勿嘗試修復此表**。
    *   目前的真相來源為 `dw_storage.mdm02_tb_tablet_usage_new_schema`。
2.  **SN 更換邏輯**：
    *   回補過程中會自動套用 `Replace Mapping` 邏輯，確保維修換機後的數據連續性。
3.  **效能與權限**：
    *   執行 SQL 腳本時，若遇到 `Already Exists: Table ... min_max_date` 錯誤，請確保使用的是最新版具備 `DROP TABLE` 邏輯的 SQL 腳本。
