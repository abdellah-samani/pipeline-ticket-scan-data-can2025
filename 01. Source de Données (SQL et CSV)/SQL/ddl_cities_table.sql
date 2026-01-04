CREATE TABLE dbo.cities (
    city_code VARCHAR(10) NOT NULL PRIMARY KEY,         -- abréviation de la ville
    city_name VARCHAR(50) NOT NULL,                     -- nom complet de la ville
    region VARCHAR(50) NOT NULL,                        -- région administrative
    population INT NULL,                                -- population totale
    altitude_meters INT NULL,                            -- altitude au-dessus du niveau de la mer
    airport_code VARCHAR(10) NULL,                       -- code IATA
    train_station_count INT NULL,                        -- nombre de gares ferroviaires
    total_hotels INT NULL,                               -- nombre total d'hôtels
    hotel_capacity INT NULL,                              -- nombre total de lits d'hôtel
    avg_hotel_price_eur DECIMAL(10,2) NULL,            -- prix moyen en EUR
    police_stations INT NULL,                             -- nombre de postes de police
    hospitals INT NULL,                                   -- nombre d'hôpitaux
    tourist_attraction_score INT NULL,                   -- notation 1–10
    public_transport_score INT NULL,                     -- notation 1–10
    climate_type VARCHAR(50) NULL                        -- description du climat
);

GO
INSERT INTO dbo.cities (
    city_code,
    city_name,
    region,
    population,
    altitude_meters,
    airport_code,
    train_station_count,
    total_hotels,
    hotel_capacity,
    avg_hotel_price_eur,
    police_stations,
    hospitals,
    tourist_attraction_score,
    public_transport_score,
    climate_type
)
VALUES
('CAS','Casablanca','Casablanca-Settat',3450000,25,'CMN',6,280,32000,95,45,22,8,9,'Mediterranean'),
('RAB','Rabat','Rabat-Salé-Kénitra',1200000,30,'RBA',3,140,15000,110,25,12,9,8,'Mediterranean'),
('MAR','Marrakesh','Marrakesh-Safi',1050000,460,'RAK',1,650,85000,120,30,15,10,7,'Semi-arid'),
('TAN','Tangier','Tanger-Tetouan-Al Hoceima',1000000,15,'TNG',2,180,22000,85,20,10,9,7,'Mediterranean'),
('FEZ','Fez','Fez-Meknes',1150000,410,'FEZ',1,160,18000,75,22,11,10,6,'Mediterranean/Continental'),
('AGA','Agadir','Souss-Massa',502000,75,'AGA',0,190,35000,80,15,8,8,6,'Semi-arid/Oceanic');
