Select
    FactKey = IDENTITY(INT, 1, 1),
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey ,
    Summary.EMR,
    KHIS_HTSPos,
    DWH_HTSPos,
    EMR_HTSPos,
    Diff_EMR_DWH,
    DiffKHISDWH,
    DiffKHISEMR,
    Percent_variance_EMR_DWH as Proportion_variance_EMR_DWH,
    Percent_variance_KHIS_DWH as Proportion_variance_KHIS_DWH,
    Percent_variance_KHIS_EMR as Proportion_variance_KHIS_EMR,
    EOMONTH(DATEADD(mm,-1,GETDATE())) as Reporting_Month,
    dwapi.DwapiVersion,
    Cast(getdate() as date) as LoadDate
into NDWH.dbo.FactHTSPosConcordance
from Summary
    left join NDWH.dbo.DimFacility as facility on facility.MFLCode = Summary.MFLCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = Summary.MFLCode
    left join NDWH.dbo.DimPartner as partner on partner.PartnerName = Summary.SDP
    left join NDWH.dbo.DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join DWAPI on DWAPI.SiteCode=Summary.MFLCode
ORDER BY Percent_variance_EMR_DWH DESC