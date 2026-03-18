# ==============================================================================
# 腳本名稱：cp_bronze_data.ps1
# 目的：補全 Google Cloud Storage (GCS) Bronze 區缺失的 tb002 (設備連線記錄) 資料。
# 邏輯：
#   1. 遍歷指定的日期範圍。
#   2. 尋找帶有時間戳記後綴的「修復資料夾」(例如 2026-03-01_20260305_190854)。
#   3. 將修復資料夾中的 tb002_*.json 檔案複製回原始的日期資料夾。
#   4. 使用 --no-clobber 確保不會覆蓋原始目錄中已存在的任何檔案。
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. 環境設定
# ------------------------------------------------------------------------------
$TEST_PROJECT_ID = "twmoe-bigdata-prod"   # 目標 GCP 專案 ID
$BRONZE_BUCKET = "moebd-bronze-prod"      # Bronze 區資料存放的 Bucket 名稱
$SA_NAME = "mdm02"                        # 服務帳號或資料夾分類名稱
$START_DATE = Get-Date "2026-02-09"       # 補漏任務的開始日期
$END_DATE = Get-Date "2026-03-01"         # 補漏任務的結束日期

# 確保 gcloud CLI 的專案上下文正確，避免將資料複製到錯誤的專案
Write-Host "正在將 gcloud 專案切換至：$TEST_PROJECT_ID..." -ForegroundColor Cyan
gcloud config set project "$TEST_PROJECT_ID"

# ------------------------------------------------------------------------------
# 2. 核心邏輯：遍歷日期範圍
# ------------------------------------------------------------------------------
$current = $START_DATE
while ($current -le $END_DATE) {
    # 格式化日期為字串 (例如 "2026-02-09")
    $date_str = $current.ToString("yyyy-MM-dd")
    Write-Host "------------------------------------------"
    Write-Host "正在處理日期：$date_str"
    
    # 原始資料夾路徑 (這是當初匯入 tb001 但遺漏 tb002 的位置)
    $ORIGINAL_DIR = "gs://$BRONZE_BUCKET/$SA_NAME/$date_str"
    
    # 尋找帶有時間戳記的修復資料夾
    # 使用 gcloud storage ls 搜尋匹配 "日期_*" 的路徑
    # Sort-Object | Select-Object -Last 1 確保取得最新產生的修復目錄
    $recovery_output = gcloud storage ls "gs://$BRONZE_BUCKET/$SA_NAME/${date_str}_*" 2>$null | Sort-Object | Select-Object -Last 1

    if ($recovery_output) {
        $RECOVERY_PATH = $recovery_output.Trim()
        
        # 使用正規表達式提取目錄前綴
        # 這是為了處理 ls 可能回傳檔案路徑的情況，確保取得的是目錄路徑 (結尾帶斜線)
        if ($RECOVERY_PATH -match "(gs://.*/${date_str}_[^/]+/)") {
            $RECOVERY_DIR = $Matches[1]
        } else {
            # 備用方案：直接截取最後一個斜線之前的字串
            $RECOVERY_DIR = $RECOVERY_PATH.Substring(0, $RECOVERY_PATH.LastIndexOf('/') + 1)
        }

        Write-Host "找到修復資料夾：$RECOVERY_DIR" -ForegroundColor Green
        
        # 執行複製動作
        # --no-clobber 是關鍵參數：
        #   - 如果原始目錄 ($ORIGINAL_DIR) 已存在同名檔案，則跳過該檔案，絕對不覆蓋。
        #   - 只有在原始目錄缺少的檔案才會從修復目錄複製過去。
        Write-Host "[執行] 正在補全缺失的 tb002 檔案至 $ORIGINAL_DIR..." -ForegroundColor Yellow
        gcloud storage cp "${RECOVERY_DIR}tb002_*.json" "$ORIGINAL_DIR/" --no-clobber
        
    } else {
        # 如果找不到帶有時間戳記的目錄，代表該日期可能沒有對應的修復資料
        Write-Host "未找到 $date_str 的修復目錄，跳過..." -ForegroundColor Gray
    }

    # 日期遞增一天
    $current = $current.AddDays(1)
}

Write-Host "------------------------------------------"
Write-Host "資料補全任務完成 (僅針對 tb002)。" -ForegroundColor Cyan
