/* 
  專案名稱：MDM02 (TB002) 資料修復與回補 SQL 腳本
  檔案名稱：backfill_mdm_usage.sql
  功能說明：
    1. 針對 2026-02-09 至 2026-03-01 範圍，自動生成日期批次。
    2. 解決 BigQuery Scripting 迴圈中 TEMP TABLE 衝突問題 (Already Exists)。
    3. 逐日呼叫 dw_stage.stp_mdm02_ToWS_tablet_usage 進行資料入庫。
  使用方式：直接在 BigQuery Console 貼上並執行。
*/

-- 使用 BigQuery Scripting 語法進行迴圈處理
BEGIN
  -- 遍歷指定日期範圍並格式化為 YYYY-MM-DD.zip
  FOR record IN (
    SELECT FORMAT_DATE('%Y-%m-%d.zip', d) AS bm
    FROM UNNEST(GENERATE_DATE_ARRAY('2026-02-09', '2026-03-01', INTERVAL 1 DAY)) AS d
  )
  DO
    BEGIN
      DROP TABLE IF EXISTS `_SESSION.min_max_date`;
    EXCEPTION WHEN ERROR THEN
      -- 若 DROP 失敗（例如表不存在），則忽略錯誤繼續執行
      SELECT 'Notice: Skip drop table' AS msg;
    END;

    -- 呼叫核心 ETL 預存程序
    CALL `twmoe-bigdata-prod.dw_stage.stp_mdm02_ToWS_tablet_usage`(record.bm);
    
  END FOR;

  SELECT '所有回補任務執行完畢！' AS final_status;
END;
