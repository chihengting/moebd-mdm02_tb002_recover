# MDM tb002 資料恢復專案 (mdm-tb002-recover)

本專案旨在解決 moebd-mdm02 MDM (Mobile Device Management) 系統中 `tb002` (設備連線記錄/tablet_usage) 資料遺漏的問題。透過從修復目錄補全 JSON 檔案至 Bronze 區，並重新觸發 Cloud Run API 產生 Silver 區的 Parquet 檔案。

## 專案背景

在 2026-02-09 至 2026-03-01 期間，系統雖然成功匯入了 `tb001` (設備狀態)與，但遺漏了對應的 `tb002` 資料。
*   **tb001 (tablet_status)**：必須保留原始上傳版本，不可覆蓋。
*   **tb002 (tablet_usage)**：需從帶有時間戳記的修復資料夾中補全。

## 環境需求

1.  **作業系統**：Windows (支援 PowerShell)。
2.  **Google Cloud SDK (gcloud CLI)**：需已安裝並完成認證。
3.  **Python 3.10+**：建議使用 Conda 建立獨立環境。

### 安裝步驟

```powershell
# 1. 建立並啟用 Conda 環境
conda create -n mdm-recover python=3.10 -y
conda activate mdm-recover

# 2. 安裝 Python 依賴套件
pip install -r requirements.txt

# 3. 進行 GCP 身分驗證
gcloud auth login
gcloud auth application-default login
```

## 檔案說明

*   `cp_bronze_data.ps1`：PowerShell 腳本，負責將缺失的 `tb002_*.json` 從修復目錄複製到原始目錄。
*   `trigger_silver.py`：Python 腳本，負責呼叫 Cloud Run API，將補全後的 JSON 轉換為 Silver 區的 Parquet。
*   `requirements.txt`：Python 套件清單。
*   `.gitignore`：排除不需進入版本控制的檔案（如 `GEMINI.md`、`__pycache__`）。

## 操作流程

### 第一步：補全 Bronze 區 JSON 檔案

執行 `cp_bronze_data.ps1`。該腳本會掃描日期範圍，尋找對應的修復資料夾，並將缺失的 `tb002` 檔案補進原始資料夾。

**關鍵特性**：使用 `--no-clobber` 參數，若原始目錄已存在檔案則絕對不覆蓋，確保 `tb001` 的安全。

```powershell
.\cp_bronze_data.ps1
```

### 第二步：觸發 Silver 區 Parquet 轉換

執行 `trigger_silver.py`。該腳本會呼叫正式區的 Cloud Run API，針對補全後的資料進行轉換。

**關鍵特性**：
*   僅針對 `prefix: tb002_` 進行處理。
*   `DRY_RUN` 模式：預設可先設為 `True` 檢查 Payload。
*   支援跨專案權限處理（需確保正式區服務帳號具備測試區 Bucket 權限）。

```powershell
python .\trigger_silver.py
```

## 注意事項

1.  **認證權限**：若在執行時遇到 403 錯誤，請確認 Cloud Run 的服務帳號具備目標 Bucket 的 `storage.objectViewer` 與 `storage.objectAdmin` 權限。
2.  **日期範圍**：執行前請務必檢查腳本頂部的 `$START_DATE`、`$END_DATE` (PowerShell) 以及 `START_DATE`、`END_DATE` (Python) 設定。
3.  **測試環境**：強烈建議先在 `moebd-bronze-test` 等測試 Bucket 完成驗證後，再將腳本中的變數切換至 `prod` 環境。

