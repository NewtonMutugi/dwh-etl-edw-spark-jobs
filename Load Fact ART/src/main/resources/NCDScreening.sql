select
    patient.PatientPKHash,
    patient.SiteCode,
    case
        when latest_diabetes.Controlled in ('Yes', 'No') then  1
        else 0
        end as ScreenedDiabetes,
    case
        when latest_hypertension.Controlled in ('Yes', 'No') then  1
        else 0
        end as ScreenedBPLastVisit
from Patient
left join Intermediate_NCDControlledStatusLastVisit as latest_diabetes on latest_diabetes.PatientPKHash = Patient.PatientPKHash
    and latest_diabetes.SiteCode = Patient.SiteCode and latest_diabetes.Disease = 'Diabetes'
left join Intermediate_NCDControlledStatusLastVisit as latest_hypertension on latest_hypertension.PatientPKHash = Patient.PatientPKHash
    and latest_hypertension.SiteCode = Patient.SiteCode and latest_hypertension.Disease = 'Hypertension'