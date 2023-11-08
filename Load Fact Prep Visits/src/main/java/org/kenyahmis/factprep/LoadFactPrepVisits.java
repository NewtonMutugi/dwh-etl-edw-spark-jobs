package org.kenyahmis.factprep;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import static org.apache.spark.sql.functions.row_number;

public class LoadFactPrepVisits {
    private static final Logger logger = LoggerFactory.getLogger(LoadFactPrepVisits.class);
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("Load Fact Prep");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();
        LoadFactPrepVisits loadFactPrep = new LoadFactPrepVisits();

        Dataset<Row> dimMFLPartnerAgencyCombinationDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select distinct MFL_Code,SDP,[SDP_Agency] as Agency from dbo.All_EMRSites")
                .load();
        dimMFLPartnerAgencyCombinationDataFrame.persist(StorageLevel.DISK_ONLY());
        dimMFLPartnerAgencyCombinationDataFrame.createOrReplaceTempView("MFL_partner_agency_combination");

        // Prep Patients
        Dataset<Row> prepPatientDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", " select\n" +
                        "            distinct PatientPKHash PatientPK,\n" +
                        "            SiteCode\n" +
                        "        from dbo.PrEP_Patient " +
                        "where PrepNumber is not null")
                .load();

        prepPatientDataDf.createOrReplaceTempView("prep_patients");
        prepPatientDataDf.persist(StorageLevel.DISK_ONLY());

        // Prep Visits
        Dataset<Row> prepVisitsDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select\n" +
                        "            PatientPKHash PatientPK,\n" +
                        "            SiteCode,\n" +
                        "            VisitID,\n" +
                        "            VisitDate,\n" +
                        "            BloodPressure,\n" +
                        "            Temperature,\n" +
                        "            Weight,\n" +
                        "            Height,\n" +
                        "            BMI,\n" +
                        "            STIScreening,\n" +
                        "            STISymptoms,\n" +
                        "            STITreated,\n" +
                        "            Circumcised,\n" +
                        "            VMMCReferral,\n" +
                        "            cast(LMP as Date) as LMP,\n" +
                        "            MenopausalStatus,\n" +
                        "            PregnantAtThisVisit,\n" +
                        "            cast(EDD as Date) as EDD,\n" +
                        "            PlanningToGetPregnant,\n" +
                        "            PregnancyPlanned,\n" +
                        "            PregnancyEnded,\n" +
                        "            cast(PregnancyEndDate as Date) as PregnancyEndDate,\n" +
                        "            PregnancyOutcome,\n" +
                        "            BirthDefects,\n" +
                        "            Breastfeeding,\n" +
                        "            FamilyPlanningStatus,\n" +
                        "            FPMethods,\n" +
                        "            AdherenceDone,\n" +
                        "            AdherenceOutcome,\n" +
                        "            AdherenceReasons,\n" +
                        "            SymptomsAcuteHIV,\n" +
                        "            ContraindicationsPrep,\n" +
                        "            PrepTreatmentPlan,\n" +
                        "            PrepPrescribed,\n" +
                        "            RegimenPrescribed,\n" +
                        "            MonthsPrescribed,\n" +
                        "            CondomsIssued,\n" +
                        "            Tobegivennextappointment,\n" +
                        "            Reasonfornotgivingnextappointment,\n" +
                        "            HepatitisBPositiveResult,\n" +
                        "            HepatitisCPositiveResult,\n" +
                        "            VaccinationForHepBStarted,\n" +
                        "            TreatedForHepB,\n" +
                        "            VaccinationForHepCStarted,\n" +
                        "            TreatedForHepC,\n" +
                        "            cast(NextAppointment as Date) NextAppointment,\n" +
                        "            ClinicalNotes\n" +
                        "        from dbo.PrEP_Visits\n" +
                        "        where VisitDate is not null")
                .load();

        prepVisitsDataDf.createOrReplaceTempView("PrepVisits");
        prepVisitsDataDf.persist(StorageLevel.DISK_ONLY());


        Dataset<Row> dimDateDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimDate")
                .load();
        dimDateDataFrame.persist(StorageLevel.DISK_ONLY());
        dimDateDataFrame.createOrReplaceTempView("DimDate");

        Dataset<Row> dimFacilityDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimFacility")
                .load();
        dimFacilityDataFrame.persist(StorageLevel.DISK_ONLY());
        dimFacilityDataFrame.createOrReplaceTempView("Dimfacility");

        Dataset<Row> dimPatientDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimPatient")
                .load();
        dimPatientDataFrame.persist(StorageLevel.DISK_ONLY());
        dimPatientDataFrame.createOrReplaceTempView("DimPatient");

        Dataset<Row> dimPartnerDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimPartner")
                .load();
        dimPartnerDataFrame.persist(StorageLevel.DISK_ONLY());
        dimPartnerDataFrame.createOrReplaceTempView("DimPartner");

        Dataset<Row> dimAgencyDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimAgency")
                .load();
        dimAgencyDataFrame.persist(StorageLevel.DISK_ONLY());
        dimAgencyDataFrame.createOrReplaceTempView("DimAgency");


        Dataset<Row> dimAgeGroupDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimAgeGroup")
                .load();
        dimAgeGroupDataFrame.persist(StorageLevel.DISK_ONLY());
        dimAgeGroupDataFrame.createOrReplaceTempView("DimAgeGroup");

        String factPrepQuery = loadFactPrep.loadQuery("LoadFactPrep.sql");
        Dataset<Row> factPrepDf = session.sql(factPrepQuery);

        // Add FactKey Column
//        WindowSpec window = Window.orderBy("PatientKey");
//        factPrepDf = factPrepDf.withColumn("FactKey",  row_number().over(window));
        factPrepDf.printSchema();
        factPrepDf
                .repartition(50)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("truncate", "true")
                .option("dbtable", "dbo.FactPrepVisits")
                .mode(SaveMode.Overwrite)
                .save();

    }

    private String loadQuery(String fileName) {
        String query;
        InputStream inputStream = LoadFactPrepVisits.class.getClassLoader().getResourceAsStream(fileName);
        if (inputStream == null) {
            logger.error(fileName + " not found");
            throw new RuntimeException(fileName + " not found");
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load query from file", e);
            throw new RuntimeException("Failed to load query from file");
        }
        return query;

    }
}
