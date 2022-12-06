Select
    Patient.PatientID,
    Patient.PatientPK,
    cast (Patient.SiteCode as nvarchar) As SiteCode,
    DATEDIFF(yy,Patient.DOB,Patient.RegistrationAtCCC) AgeAtEnrol,
    DATEDIFF(yy,Patient.DOB,ART.StartARTDate) AgeAtARTStart,
    ART.StartARTAtThisfacility,
    ART.PreviousARTStartDate,
    ART.PreviousARTRegimen,
    StartARTDate,
    LastARTDate,
    CASE WHEN [DateConfirmedHIVPositive] IS NOT NULL AND ART.StartARTDate IS NOT NULL
    THEN CASE WHEN DateConfirmedHIVPositive<= ART.StartARTDate THEN DATEDIFF(DAY,DateConfirmedHIVPositive,ART.StartARTDate)
					ELSE NULL END
				ELSE NULL END AS TimetoARTDiagnosis,
    CASE WHEN Patient.RegistrationAtCCC IS NOT NULL AND ART.StartARTDate IS NOT NULL
				THEN CASE WHEN Patient.RegistrationAtCCC<=ART.StartARTDate  THEN DATEDIFF(DAY,Patient.[RegistrationAtCCC],ART.StartARTDate)
				ELSE NULL END
				ELSE NULL END AS TimetoARTEnrollment,
        Pre.PregnantARTStart,
        Pre.PregnantAtEnrol,
        las.LastEncounterDate As LastVisitDate,
        las.NextAppointmentDate,
        datediff(yy, patient.DOB, las.LastEncounterDate) as AgeLastVisit,
        lastRegimen,
        StartRegimen,
        lastRegimenline,
        StartRegimenline

from
ODS.dbo.CT_Patient Patient
left join ODS.dbo.CT_ARTPatients ART on ART.PatientPK=Patient.Patientpk and ART.SiteCode=Patient.SiteCode
left join ODS.dbo.PregnancyAsATInitiation   Pre on Pre.Patientpk= Patient.PatientPK and Pre.SiteCode=Patient.SiteCode
left join ODS.dbo.Intermediate_LastPatientEncounter las on las.PatientPK collate Latin1_General_CI_AS=Patient.PatientPK collate Latin1_General_CI_AS and las.SiteCode collate Latin1_General_CI_AS=Patient.SiteCode collate Latin1_General_CI_AS