package org.kenyahmis.loadfactncds;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class LoadFactNCDs {
    private static final Logger logger = LoggerFactory.getLogger(LoadFactNCDs.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("Load NCDs Fact");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        RuntimeConfig rtConfig = session.conf();

        LoadFactNCDs loadFactNCDs = new LoadFactNCDs();
        String ncdSourceQuery = loadFactNCDs.loadQuery("NCDSource.sql");

        Dataset<Row> ncdSourceDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", ncdSourceQuery)
                .load();
        ncdSourceDataFrame.createOrReplaceTempView("ncd_source_data");
        ncdSourceDataFrame.persist(StorageLevel.DISK_ONLY());

        ncdSourceDataFrame.printSchema();
        ncdSourceDataFrame.show();

        String visitOrderingQuery = loadFactNCDs.loadQuery("visits_ordering.sql");

        Dataset<Row> visitOrderingDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", visitOrderingQuery)
                .load();
        visitOrderingDataFrame.createOrReplaceTempView("visits_ordering");
        visitOrderingDataFrame.persist(StorageLevel.DISK_ONLY());

        visitOrderingDataFrame.printSchema();
        visitOrderingDataFrame.show();

        Dataset<Row> CTPatientDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select * from dbo.CT_Patient")
                .load();
        CTPatientDataFrame.persist(StorageLevel.DISK_ONLY());
        CTPatientDataFrame.createOrReplaceTempView("CT_Patient");

        String ageLastVisitQuery = loadFactNCDs.loadQuery("age_as_of_last_visit.sql");

        Dataset<Row> ageLastVisitDataFrame = session.sql(ageLastVisitQuery);
        ageLastVisitDataFrame.createOrReplaceTempView("age_as_of_last_visit");
        ageLastVisitDataFrame.persist(StorageLevel.DISK_ONLY());

        ageLastVisitDataFrame.printSchema();
        ageLastVisitDataFrame.show();

        String hypertensivesOrderingQuery = loadFactNCDs.loadQuery("hypertensives_ordering.sql");

        Dataset<Row> hypertensivesOrderingDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", hypertensivesOrderingQuery)
                .load();
        hypertensivesOrderingDataFrame.createOrReplaceTempView("hypertensives_ordering");
        hypertensivesOrderingDataFrame.persist(StorageLevel.DISK_ONLY());

        String diabetesOrderingQuery = loadFactNCDs.loadQuery("diabetes_ordering.sql");

        Dataset<Row> diabetesOrderingDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", diabetesOrderingQuery)
                .load();
        diabetesOrderingDataFrame.createOrReplaceTempView("diabetes_ordering");
        diabetesOrderingDataFrame.persist(StorageLevel.DISK_ONLY());

        String dyslipidemiaOrderingQuery = loadFactNCDs.loadQuery("dyslipidemia_ordering.sql");

        Dataset<Row> dyslipidemiaOrderingDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", dyslipidemiaOrderingQuery)
                .load();
        dyslipidemiaOrderingDataFrame.createOrReplaceTempView("dyslipidemia_ordering");
        dyslipidemiaOrderingDataFrame.persist(StorageLevel.DISK_ONLY());

        String earliestHypertensionQuery = loadFactNCDs.loadQuery("earliest_hpertension_recorded.sql");

        Dataset<Row> earliestHypertensionDataFrame = session.sql(earliestHypertensionQuery);
        earliestHypertensionDataFrame.createOrReplaceTempView("earliest_hpertension_recorded");
        earliestHypertensionDataFrame.persist(StorageLevel.DISK_ONLY());

        earliestHypertensionDataFrame.printSchema();
        earliestHypertensionDataFrame.show();

        String earliestDiabetesQuery = loadFactNCDs.loadQuery("earliest_diabetes_recorded.sql");

        Dataset<Row> earliestDiabetesDataFrame = session.sql(earliestDiabetesQuery);
        earliestDiabetesDataFrame.createOrReplaceTempView("earliest_diabetes_recorded");
        earliestDiabetesDataFrame.persist(StorageLevel.DISK_ONLY());

        earliestDiabetesDataFrame.printSchema();
        earliestDiabetesDataFrame.show();

        String earliestDyslipidemiaQuery = loadFactNCDs.loadQuery("earliest_dyslipidemia_recorded.sql");

        Dataset<Row> earliestDyslipidemiaDataFrame = session.sql(earliestDyslipidemiaQuery);
        earliestDyslipidemiaDataFrame.createOrReplaceTempView("earliest_dyslipidemia_recorded");
        earliestDyslipidemiaDataFrame.persist(StorageLevel.DISK_ONLY());

        Dataset<Row> IntermediateNCDControlledStatusLVDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select * from dbo.Intermediate_NCDControlledStatusLastVisit")
                .load();
        IntermediateNCDControlledStatusLVDataFrame.persist(StorageLevel.DISK_ONLY());
        IntermediateNCDControlledStatusLVDataFrame.createOrReplaceTempView("Intermediate_NCDControlledStatusLastVisit");

        String hypertensiveScreeningQuery = loadFactNCDs.loadQuery("hypertensive_and_screening_indicators.sql");

        Dataset<Row> hypertensiveScreeningDataFrame = session.sql(hypertensiveScreeningQuery);
        hypertensiveScreeningDataFrame.createOrReplaceTempView("hypertensive_and_screening_indicators");
        hypertensiveScreeningDataFrame.persist(StorageLevel.DISK_ONLY());

        String diabetesScreeningIndicatorsQuery = loadFactNCDs.loadQuery("diabetes_and_screening_indicators.sql");

        Dataset<Row> diabetesScreeningIndicatorsDataFrame = session.sql(diabetesScreeningIndicatorsQuery);
        diabetesScreeningIndicatorsDataFrame.createOrReplaceTempView("diabetes_and_screening_indicators");
        diabetesScreeningIndicatorsDataFrame.persist(StorageLevel.DISK_ONLY());

        Dataset<Row> dimMFLPartnerAgencyCombinationDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select MFL_Code,SDP,[SDP_Agency] as Agency from dbo.All_EMRSites")
                .load();
        dimMFLPartnerAgencyCombinationDataFrame.persist(StorageLevel.DISK_ONLY());
        dimMFLPartnerAgencyCombinationDataFrame.createOrReplaceTempView("MFL_partner_agency_combination");

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

        String factNCDQuery = loadFactNCDs.loadQuery("LoadFactNCDs.sql");
        Dataset<Row> factNCDDf = session.sql(factNCDQuery);
        factNCDDf.printSchema();
        long factNCDCount = factNCDDf.count();
        logger.info("Fact NCDs Count is: " + factNCDCount);
        final int writePartitions = 50;
        factNCDDf
                .repartition(writePartitions)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("truncate", "true")
                .option("dbtable", "dbo.FactNCD")
                .mode(SaveMode.Overwrite)
                .save();
    }

    private String loadQuery(String fileName) {
        String query;
        InputStream inputStream = LoadFactNCDs.class.getClassLoader().getResourceAsStream(fileName);
        if (inputStream == null) {
            logger.error(fileName + " not found");
            return null;
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load query from file", e);
            return null;
        }
        return query;
    }
}