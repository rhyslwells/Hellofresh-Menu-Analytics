SELECT
  t.`transactionID`,
  t.`customerID`,
  t.`franchiseID`,
  f.`name` AS franchise_name,
  t.`dateTime`,
  t.`product`,
  t.`totalPrice`
FROM
  `samples`.`bakehouse`.`sales_transactions` t
    JOIN `samples`.`bakehouse`.`sales_franchises` f
      ON t.`franchiseID` = f.`franchiseID`
WHERE
  t.`franchiseID` IS NOT NULL
  AND f.`franchiseID` IS NOT NULL
LIMIT 10;