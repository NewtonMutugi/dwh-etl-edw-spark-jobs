Select
    -- Factkey = IDENTITY(INT, 1, 1),
    pat.PatientKey,
    fac.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    StartARTDate.DateKey As StartARTDateKey,
    LastARTDate.DateKey  as LastARTDateKey,
    DateConfirmedPos.DateKey as DateConfirmedPosKey,
    ARTOutcome.ARTOutcomeKey,
    lastRegimen As CurrentRegimen,
    LastRegimenLine As CurrentRegimenline,
    StartRegimen,
    StartRegimenLine,
    AgeAtEnrol,
    AgeAtARTStart,
    AgeLastVisit,
    CASE
    WHEN floor( AgeLastVisit ) < 15 THEN
    'Child'
    WHEN floor( AgeLastVisit ) >= 15 THEN
    'Adult' ELSE 'Aii'
END AS Agegrouping,
            TimetoARTDiagnosis,
            TimetoARTEnrollment,
            PregnantARTStart,
            PregnantAtEnrol,
            Patient.LastVisitDate,
            Patient.NextAppointmentDate,
            StartARTAtThisfacility,
            PreviousARTStartDate,
            PreviousARTRegimen,
            WhoStage,
            PHQ_9_rating,
            case when LatestDepressionScreening.Patientpkhash is not null then 1 else 0 End as ScreenedForDepression,
            coalesce(ncd_screening.ScreenedBPLastVisit, 0) as ScreenedBPLastVisit,
            coalesce(ncd_screening.ScreenedDiabetes, 0) as ScreenedDiabetes,
			ScreenedDepressionDate,
            AppointmentsCategory,

            Pregnant,
            Breastfeeding,
            case
              when rtt_within_last_12_months.PatientPkHash is not null then 1
              else 0
end as IsRTTLast12MonthsAfter3monthsIIT,
            end_month.DateKey as AsOfDateKey,
            cast(getdate() as date) as LoadDate
INTO NDWH.dbo.FACTART
from  Patient
left join NDWH.dbo.DimPatient as Pat on pat.PatientPKHash=Patient.PatientPkHash and Pat.SiteCode=Patient.SiteCode
left join NDWH.dbo.Dimfacility fac on fac.MFLCode=Patient.SiteCode
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code  = Patient.SiteCode
left join NDWH.dbo.DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
left join NDWH.dbo.DimAgeGroup as age_group on age_group.Age = Patient.AgeLastVisit
left join NDWH.dbo.DimDate as StartARTDate on StartARTDate.Date = Patient.StartARTDate
left join NDWH.dbo.DimDate as LastARTDate on  LastARTDate.Date=Patient.LastARTDate
left join NDWH.dbo.DimDate as DateConfirmedPos on  DateConfirmedPos.Date=Patient.DateConfirmedHIVPositive
left join NDWH.dbo.DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join ODS.dbo.Intermediate_ARTOutcomes As IOutcomes  on IOutcomes.PatientPKHash = Patient.PatientPkHash  and IOutcomes.SiteCode = Patient.SiteCode
left join LatestDepressionScreening on LatestDepressionScreening.Patientpkhash=patient.patientpkhash and LatestDepressionScreening.sitecode=patient.sitecode
left join NDWH.dbo.DimARTOutcome ARTOutcome on ARTOutcome.ARTOutcome=IOutcomes.ARTOutcome
left join ncd_screening on ncd_screening.PatientPKHash = patient.PatientPKHash
  and ncd_screening.SiteCode = patient.SiteCode
left join NDWH.dbo.DimDate as end_month on end_month.Date = eomonth(dateadd(mm,-1,getdate()))
left join rtt_within_last_12_months on rtt_within_last_12_months.PatientPKHash = Patient.PatientPKHash
  and rtt_within_last_12_months.MFLCode = Patient.SiteCode
WHERE pat.voided =0;