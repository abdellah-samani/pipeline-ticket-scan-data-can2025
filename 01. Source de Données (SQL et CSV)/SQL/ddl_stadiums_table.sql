CREATE TABLE dbo.stadiums (
    stadium_id VARCHAR(12) NOT NULL PRIMARY KEY,       
    stadium_name VARCHAR(100) NOT NULL,
    city_code VARCHAR(10) NOT NULL,
    full_address VARCHAR(200) NOT NULL,
    seating_capacity INT NOT NULL,
    construction_year INT NOT NULL,
    last_renovation INT NULL,
    pitch_type VARCHAR(50) NOT NULL,
    vip_capacity INT NULL,
    media_capacity INT NULL,
    parking_spaces INT NULL,
    accessibility_rating INT NULL CHECK (accessibility_rating BETWEEN 1 AND 10),
    safety_certificate_expiry DATE NULL,
    geo_latitude FLOAT NULL,
    geo_longitude FLOAT NULL,
    contact_phone VARCHAR(20) NULL,
    email VARCHAR(100) NULL
);

GO
INSERT INTO dbo.stadiums (
    stadium_id,
    stadium_name,
    city_code,
    full_address,
    seating_capacity,
    construction_year,
    last_renovation,
    pitch_type,
    vip_capacity,
    media_capacity,
    parking_spaces,
    accessibility_rating,
    safety_certificate_expiry,
    geo_latitude,
    geo_longitude,
    contact_phone,
    email
)
VALUES
('STAD_RAB_01','Prince Moulay Abdellah Stadium','RAB','Avenue Hassan II, Rabat',68700,1983,2025,'Hybrid Grass',2500,1800,10000,9,'2026-12-31',33.9592,-6.8851,'+212-537-700-000','contact@sonarges.ma'),
('STAD_RAB_02','Al Barid Stadium','RAB','Agdal District, Rabat',18500,1950,2025,'Natural Grass',250,100,800,7,'2026-12-31',33.9980,-6.8480,'+212-537-700-002','albarid@sonarges.ma'),
('STAD_RAB_03','Moulay Hassan Stadium','RAB','El Youssoufia, Rabat',22000,2024,2025,'Natural Grass',500,200,1200,8,'2026-12-31',33.9755,-6.8243,'+212-537-600-000','rabat-city@sonarges.ma'),
('STAD_RAB_04','Olympic Annex Stadium','RAB','Complex Moulay Abdellah, Rabat',21000,2025,2025,'Natural Grass',300,150,2000,7,'2026-12-31',33.9575,-6.8870,'+212-537-700-001','contact@sonarges.ma'),
('STAD_CAS_01','Mohammed V Stadium','CAS','Rue Al-Azrak Ahmed, Maârif, Casablanca',67000,1955,2025,'Natural Grass',2000,650,1500,7,'2026-12-31',33.5828,-7.6483,'+212-522-200-000','casa@sonarges.ma'),
('STAD_MAR_01','Marrakesh Stadium','MAR','RN9, Northern Outskirts, Marrakesh',45240,2011,2025,'Natural Grass',1130,1000,4500,8,'2026-12-31',31.7061,-7.9806,'+212-524-400-000','kech@sonarges.ma'),
('STAD_AGA_01','Adrar Stadium','AGA','Hay Mohammadi, Agadir',45480,2013,2025,'Natural Grass',1200,500,5000,8,'2026-12-31',30.4267,-9.5317,'+212-528-200-000','agadir@sonarges.ma'),
('STAD_TAN_01','Ibn Batouta Stadium','TAN','Avenue des Forces Armées Royales, Tangier',75600,2011,2025,'Hybrid Grass',3500,250,4100,8,'2026-12-31',35.7368,-5.8458,'+212-539-300-000','tangier@sonarges.ma'),
('STAD_FEZ_01','Fez Stadium','FEZ','Route de Sefrou, Fez',45000,2003,2025,'Natural Grass',1000,400,8000,7,'2026-12-31',34.0044,-4.9769,'+212-535-600-000','fez@sonarges.ma');
