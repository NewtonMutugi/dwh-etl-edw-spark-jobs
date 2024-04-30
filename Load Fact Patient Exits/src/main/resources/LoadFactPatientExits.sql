Select
    Patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    dtDead.DateKey As dtDeadKey,
    dtLTFU.DateKey As dtLFTUKey,
    dtTO.DateKey As dtTOKey,
    dtARTStop.DateKey As dtARTStopKey,
    current_date() as LoadDate
from Exits
    Left join patient on Patient.PatientPKHash= Exits.PatientPKHash and Patient.SiteCode=Exits.SiteCode
    Left join facility on facility.MFLCode=Exits.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code=Exits.SiteCode
    Left join partner on partner.PartnerName=MFL_partner_agency_combination.SDP
    Left join agency on agency.AgencyName=MFL_partner_agency_combination.agency
    left join Died on Died.PatientPKHash=Exits.PatientPKHash and Died.SiteCode=Exits.SiteCode
    left join [Stopped] on [Stopped].PatientPKHash=Exits.PatientPKHash and [Stopped].SiteCode=Exits.SiteCode
    left join TransferOut on TransferOut.PatientPK=Exits.PatientPKHash and TransferOut.SiteCode=Exits.SiteCode
    left join LTFU on LTFU.PatientPKHash=Exits.PatientPKHash and LTFU.SiteCode=Exits.SiteCode
    left join DimDate as dtARTStop on dtARTStop.Date= dtARTStop
    left join DimDate as dtLTFU on dtLTFU.Date= dtLTFU
    left join DimDate as dtTO on dtTO.Date= dtTO
    left join DimDate as dtDead on dtDead.Date= dtDead
WHERE patient.voided =0;