SELECT
    PatientPkHash,
    Sitecode,
    visitdate as ScreenedDepressionDate,
    PHQ_9_rating
FROM DepressionScreening
WHERE Num=1