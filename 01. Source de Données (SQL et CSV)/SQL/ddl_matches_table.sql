CREATE TABLE dbo.matches (
    match_id VARCHAR(20) NOT NULL,          -- Identifiant du match (ex : AFCON2025_M36_F_03)

    match_date DATE NULL,                   -- Date du match
    match_time TIME NULL,                   -- Heure de début

    stage VARCHAR(30) NULL,                 -- Phase de la compétition
    group_code CHAR(1) NULL,                -- Code du groupe (A à F)

    stadium_id VARCHAR(12) NULL,            -- Identifiant du stade

    home_team_code CHAR(3) NULL,            -- Équipe à domicile
    away_team_code CHAR(3) NULL,            -- Équipe à l’extérieur

    referee_id VARCHAR(10) NULL,            -- Identifiant de l’arbitre
    weather_notes VARCHAR(50) NULL          -- Conditions météo
);
GO

INSERT INTO dbo.matches (
    match_id,
    match_date,
    match_time,
    stage,
    group_code,
    stadium_id,
    home_team_code,
    away_team_code,
    referee_id,
    weather_notes
)
VALUES
('AFCON2025_M1_A_01','2025-12-21','20:00','Group Stage','A','STAD_RAB_01','MAR','COM','REF01','Clear'),
('AFCON2025_M2_A_01','2025-12-22','15:00','Group Stage','A','STAD_CAS_01','MLI','ZMB','REF23','Sunny'),
('AFCON2025_M3_B_01','2025-12-22','18:00','Group Stage','B','STAD_MAR_01','RSA','ANG','REF25','Partly Cloudy'),
('AFCON2025_M4_B_01','2025-12-22','21:00','Group Stage','B','STAD_AGA_01','EGY','ZWE','REF11','Clear'),
('AFCON2025_M5_D_01','2025-12-23','13:30','Group Stage','D','STAD_RAB_02','COD','BEN','REF09','Partly Cloudy'),
('AFCON2025_M6_D_01','2025-12-23','16:00','Group Stage','D','STAD_TAN_01','SEN','BOT','REF18','Windy'),
('AFCON2025_M7_C_01','2025-12-23','18:30','Group Stage','C','STAD_FEZ_01','NGR','TAN','REF15','Clear'),
('AFCON2025_M8_C_01','2025-12-23','21:00','Group Stage','C','STAD_RAB_04','TUN','UGA','REF20','Clear'),
('AFCON2025_M9_E_01','2025-12-24','13:30','Group Stage','E','STAD_CAS_01','BFA','EQG','REF03','Sunny'),
('AFCON2025_M10_E_01','2025-12-24','16:00','Group Stage','E','STAD_RAB_03','ALG','SDN','REF17','Sunny'),
('AFCON2025_M11_F_01','2025-12-24','18:30','Group Stage','F','STAD_MAR_01','CIV','MOZ','REF08','Partly Cloudy'),
('AFCON2025_M12_F_01','2025-12-24','20:00','Group Stage','F','STAD_AGA_01','CMR','GAB','REF12','Clear'),
('AFCON2025_M13_B_02','2025-12-26','13:30','Group Stage','B','STAD_MAR_01','ANG','ZWE','REF26','Sunny'),
('AFCON2025_M14_B_02','2025-12-26','16:00','Group Stage','B','STAD_AGA_01','EGY','RSA','REF23','Partly Cloudy'),
('AFCON2025_M15_A_02','2025-12-26','18:30','Group Stage','A','STAD_CAS_01','ZMB','COM','REF22','Clear'),
('AFCON2025_M16_A_02','2025-12-26','21:00','Group Stage','A','STAD_RAB_01','MAR','MLI','REF16','Clear'),
('AFCON2025_M17_D_02','2025-12-27','13:30','Group Stage','D','STAD_RAB_04','BEN','BOT','REF04','Sunny'),
('AFCON2025_M18_D_02','2025-12-27','16:00','Group Stage','D','STAD_TAN_01','SEN','COD','REF01','Partly Cloudy'),
('AFCON2025_M19_C_02','2025-12-27','18:30','Group Stage','C','STAD_RAB_02','UGA','TAN','REF28','Clear'),
('AFCON2025_M20_C_02','2025-12-27','21:00','Group Stage','C','STAD_FEZ_01','NGR','TUN','REF11','Clear'),
('AFCON2025_M21_F_02','2025-12-28','13:30','Group Stage','F','STAD_AGA_01','GAB','MOZ','REF13','Sunny'),
('AFCON2025_M22_E_02','2025-12-28','16:00','Group Stage','E','STAD_CAS_01','EQG','SDN','REF06','Partly Cloudy'),
('AFCON2025_M23_E_02','2025-12-28','18:30','Group Stage','E','STAD_RAB_03','ALG','BFA','REF18','Clear'),
('AFCON2025_M24_F_02','2025-12-28','21:00','Group Stage','F','STAD_MAR_01','CIV','CMR','REF09','Clear'),
('AFCON2025_M25_B_03','2025-12-29','17:00','Group Stage','B','STAD_AGA_01','ANG','EGY','REF20','Partly Cloudy'),
('AFCON2025_M26_B_03','2025-12-29','17:00','Group Stage','B','STAD_MAR_01','ZWE','RSA','REF24','Partly Cloudy'),
('AFCON2025_M27_A_03','2025-12-29','20:00','Group Stage','A','STAD_CAS_01','COM','MLI','REF14','Clear'),
('AFCON2025_M28_A_03','2025-12-29','20:00','Group Stage','A','STAD_RAB_01','ZMB','MAR','REF15','Clear'),
('AFCON2025_M29_C_03','2025-12-30','17:00','Group Stage','C','STAD_RAB_04','TAN','TUN','REF02','Partly Cloudy'),
('AFCON2025_M30_C_03','2025-12-30','17:00','Group Stage','C','STAD_FEZ_01','UGA','NGR','REF25','Partly Cloudy'),
('AFCON2025_M31_D_03','2025-12-30','20:00','Group Stage','D','STAD_RAB_02','BEN','SEN','REF26','Clear'),
('AFCON2025_M32_D_03','2025-12-30','20:00','Group Stage','D','STAD_TAN_01','BOT','COD','REF19','Clear'),
('AFCON2025_M33_E_03','2025-12-31','17:00','Group Stage','E','STAD_RAB_03','EQG','ALG','REF10','Sunny'),
('AFCON2025_M34_E_03','2025-12-31','17:00','Group Stage','E','STAD_CAS_01','SDN','BFA','REF07','Sunny'),
('AFCON2025_M35_F_03','2025-12-31','20:00','Group Stage','F','STAD_MAR_01','GAB','CIV','REF16','Clear'),
('AFCON2025_M36_F_03','2025-12-31','20:00','Group Stage','F','STAD_AGA_01','MOZ','CMR','REF05','Clear');
