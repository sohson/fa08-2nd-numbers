-- MySQL dump 10.13  Distrib 8.0.44, for Win64 (x86_64)
--
-- Host: localhost    Database: kospi_db
-- ------------------------------------------------------
-- Server version	8.0.44

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `feature_krx`
--

DROP TABLE IF EXISTS `feature_krx`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `feature_krx` (
  `period` varchar(10) NOT NULL,
  `ticker` varchar(10) NOT NULL,
  `avg_mktcap` bigint DEFAULT NULL,
  `float_ratio` float DEFAULT NULL,
  `gics_sector` varchar(50) DEFAULT NULL,
  `krx_group` varchar(50) DEFAULT NULL,
  `period_rank` int DEFAULT NULL,
  `turnover_ratio` float DEFAULT NULL,
  PRIMARY KEY (`period`,`ticker`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `filter_flag`
--

DROP TABLE IF EXISTS `filter_flag`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `filter_flag` (
  `ticker` varchar(10) NOT NULL,
  `managed_date` date DEFAULT NULL,
  `warning_date` date DEFAULT NULL,
  `is_managed` int DEFAULT '0',
  `is_warning` int DEFAULT '0',
  `flag_date` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`ticker`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `foreign_holding`
--

DROP TABLE IF EXISTS `foreign_holding`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `foreign_holding` (
  `ym` int NOT NULL,
  `ticker` varchar(10) NOT NULL,
  `foreign_holding_qty` bigint DEFAULT NULL,
  `foreign_holding_ratio` float DEFAULT NULL,
  `foreign_limit_qty` bigint DEFAULT NULL,
  `foreign_limit_exhaustion_rate` float DEFAULT NULL,
  PRIMARY KEY (`ym`,`ticker`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kospi_friday_daily`
--

DROP TABLE IF EXISTS `kospi_friday_daily`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `kospi_friday_daily` (
  `date` int DEFAULT NULL,
  `ticker` varchar(10) DEFAULT NULL,
  `company` varchar(50) DEFAULT NULL,
  `close` int DEFAULT NULL,
  `volume` bigint DEFAULT NULL,
  `trading_value` double DEFAULT NULL,
  `mktcap` double DEFAULT NULL,
  `shares` bigint DEFAULT NULL,
  `mktcap_rank` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `labels`
--

DROP TABLE IF EXISTS `labels`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `labels` (
  `period` varchar(10) NOT NULL,
  `ticker` varchar(10) NOT NULL,
  `was_member` int DEFAULT NULL,
  `label_in` int DEFAULT NULL,
  `label_out` int DEFAULT NULL,
  `actual_rank` int DEFAULT NULL,
  `is_member` int DEFAULT '0',
  PRIMARY KEY (`period`,`ticker`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `major_holder`
--

DROP TABLE IF EXISTS `major_holder`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `major_holder` (
  `period` varchar(10) NOT NULL,
  `ticker` varchar(10) NOT NULL,
  `major_holder_shares` bigint DEFAULT NULL,
  `major_holder_ratio` float DEFAULT NULL,
  `treasury_shares` bigint DEFAULT NULL,
  `treasury_ratio` float DEFAULT NULL,
  `non_float_ratio` float DEFAULT NULL,
  `float_rate` float DEFAULT NULL,
  PRIMARY KEY (`period`,`ticker`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `period`
--

DROP TABLE IF EXISTS `period`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `period` (
  `period` varchar(10) NOT NULL,
  `period_start` date NOT NULL,
  `period_end` date NOT NULL,
  PRIMARY KEY (`period`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `predictions`
--

DROP TABLE IF EXISTS `predictions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `predictions` (
  `id` int NOT NULL AUTO_INCREMENT,
  `run_date` date NOT NULL,
  `period` varchar(20) NOT NULL,
  `ticker` varchar(20) NOT NULL,
  `company` varchar(100) DEFAULT NULL,
  `score` double DEFAULT NULL,
  `pred_rank` int DEFAULT NULL,
  `period_rank` int DEFAULT NULL,
  `pred_top200` tinyint DEFAULT '0',
  `strong_in` tinyint DEFAULT '0',
  `strong_out` tinyint DEFAULT '0',
  `prev_member` tinyint DEFAULT '0',
  `model_version` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_pred` (`run_date`,`period`,`ticker`)
) ENGINE=InnoDB AUTO_INCREMENT=2984 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sector_map`
--

DROP TABLE IF EXISTS `sector_map`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `sector_map` (
  `ksic_sector` varchar(50) NOT NULL,
  `gics_sector_pre2023` varchar(50) NOT NULL,
  `gics_sector_2023` varchar(50) NOT NULL,
  `krx_group` varchar(50) NOT NULL,
  PRIMARY KEY (`ksic_sector`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stock_meta`
--

DROP TABLE IF EXISTS `stock_meta`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `stock_meta` (
  `ticker` varchar(10) NOT NULL,
  `list_date` date DEFAULT NULL,
  `is_not_common` int DEFAULT '0',
  `is_reits` int DEFAULT '0',
  `ksic_sector` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`ticker`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping routines for database 'kospi_db'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-03-20 15:34:11
