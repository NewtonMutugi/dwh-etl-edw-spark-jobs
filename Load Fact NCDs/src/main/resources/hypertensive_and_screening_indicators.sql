select
    ncd_source_data.PatientPKHash,
    ncd_source_data.SiteCode,
    case
        when ncd_controlled_status.Controlled in  ('Yes', 'No') then 1
        else 0
        end as IsHyperTensiveAndScreenedBPLastVisit,
    case
        when ncd_controlled_status.Controlled = 'Yes' then 1
        else 0
        end as IsHyperTensiveAndBPControlledAtLastVisit
from ncd_source_data
left join Intermediate_NCDControlledStatusLastVisit as ncd_controlled_status on ncd_controlled_status.PatientPKHash = ncd_source_data.PatientPKHash
    and ncd_controlled_status.SiteCode = ncd_source_data.SiteCode
    and ncd_source_data.Hypertension = 1
    and ncd_controlled_status.Disease = 'Hypertension'