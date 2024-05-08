SELECT
    SiteCode,
    SUM(CASE WHEN FinalTestResult = 'Positive' THEN 1 ELSE 0 END) AS HTSPos_total,
    TestDate
FROM ODS.dbo.Intermediate_EncounterHTSTests link
where link.TestDate  between  DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0) and DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1) and FinalTestResult='Positive' and SiteCode is not null and TestType in ('Initial Test', 'Initial')
GROUP BY SiteCode, TestDate