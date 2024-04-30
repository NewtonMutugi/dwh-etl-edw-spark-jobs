select
    -- Factkey = IDENTITY(INT, 1, 1),
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    source_CD4.CD4atEnrollment,
    source_CD4.CD4atEnrollmentDate,
    source_CD4.BaselineCD4,
    source_CD4.BaselineCD4Date,
    source_CD4.LastCD4,
    source_CD4.LastCD4Date,
    source_CD4.LastCD4Percentage,
    cast(getdate() as date) as LoadDate
into NDWH.dbo.FactCD4
from source_CD4 as source_CD4
    left join NDWH.dbo.DimPatient as patient on patient.PatientPKHash = source_CD4.PatientPKHash
    and patient.SiteCode = source_CD4.SiteCode
    left join NDWH.dbo.DimFacility as facility on facility.MFLCode = source_CD4.SiteCode
    left join NDWH.dbo.DimDate as cd4_enrollment on cd4_enrollment.Date = source_CD4.CD4atEnrollmentDate
    left join NDWH.dbo.DimDate as last_cd4 on last_cd4.Date = source_CD4.LastCD4Date
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = source_CD4.SiteCode
    left join NDWH.dbo.DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
    left join NDWH.dbo.DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join NDWH.dbo.DimAgeGroup as age_group on age_group.Age = source_CD4.AgeLastVisit
WHERE patient.voided =0;