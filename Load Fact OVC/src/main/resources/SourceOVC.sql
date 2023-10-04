select
    distinct row_number () over (partition by ovc.SiteCode, ovc.PatientPK order by ovc.OVCEnrollmentDate desc) as rank,ovc.PatientPKHash,
             ovc.PatientIDHash,
             ovc.SiteCode,
             OVCEnrollmentDate,
             RelationshipToClient,
             EnrolledinCPIMS,
             CPIMSUniqueIdentifierHash,
             PartnerOfferingOVCServices,
             OVCExitReason,
             ExitDate,
             datediff(yy, patient.DOB, last_encounter.LastEncounterDate) as AgeLastVisit
from dbo.CT_OVC as ovc
    left join dbo.Intermediate_LastPatientEncounter as last_encounter on last_encounter.PatientPK = ovc.PatientPK and last_encounter.SiteCode = ovc.SiteCode
    left join dbo.CT_Patient as patient on patient.PatientPK = ovc.PatientPK and patient.SiteCode = ovc.SiteCode