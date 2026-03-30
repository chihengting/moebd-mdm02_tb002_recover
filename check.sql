SELECT
  *
FROM
  dw_storage.mdm02_tb_tablet_usage_new_schema u
LEFT JOIN dw_storage.mdm02_tablet_replace_mapping crd
  ON u.serialNumber = crd.serialNumber
WHERE
  dates BETWEEN '2026-02-01' AND '2026-03-20'
  AND end_date_m BETWEEN '2026-02-01' AND '2026-03-20'
  AND crd.serialNumber IS NULL;