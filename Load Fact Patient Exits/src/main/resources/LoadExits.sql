select
    PatientID,
    PatientPKHash,
    SiteCode
from ODS.dbo.CT_PatientStatus
where ExitReason is not null
    ),
	Died As (select 
		PatientID,
		PatientPKHash,
		SiteCode,
		ExitDate as dtDead
	from ODS.dbo.CT_PatientStatus
	where ExitReason in ('Died','death')
	),

	Stopped As (select 
		PatientID,
		PatientPKHash,
		SiteCode,
		ExitDate as dtARTStop
	from ODS.dbo.CT_PatientStatus
	where ExitReason in ('Stopped','Stopped Treatment')
	),
	LTFU AS ( Select
		PatientID,
		PatientPKHash,
		SiteCode,
		ExitDate as dtLTFU
	from ODS.dbo.CT_PatientStatus
	where ExitReason in ('Lost','Lost to followup','LTFU')
	),  
	TransferOut AS ( Select
		PatientID,
		PatientPK,
		SiteCode,
		ExitDate as dtTO
	from ODS.dbo.CT_PatientStatus
	where ExitReason in ('Transfer Out','transfer_out','Transferred out','Transfer')
	),
	MFL_partner_agency_combination As (
	Select 
		MFL_code,
		SDP,
		[SDP_Agency]  as agency
		from [ODS].dbo.All_EMRSites
		)