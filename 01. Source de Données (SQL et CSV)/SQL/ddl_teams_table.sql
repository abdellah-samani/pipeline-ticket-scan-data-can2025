CREATE TABLE dbo.teams (
    team_code CHAR(3) NULL,                -- Code de l’équipe (MAR, SEN, CIV, etc.)

    team_name VARCHAR(50) NULL,             -- Nom complet de l’équipe (Morocco, Ivory Coast…)

    federation VARCHAR(20) NULL,            -- Fédération nationale (FRMF, CAF, FECAFOOT…)

    world_ranking INT NULL,                 -- Classement FIFA mondial

    coach_name VARCHAR(100) NULL            -- Nom du sélectionneur (accents inclus)
);

GO

INSERT INTO dbo.teams (
    team_code,
    team_name,
    federation,
    world_ranking,
    coach_name
)
VALUES
('MAR','Morocco','FRMF',11,'Walid Regragui'),
('COM','Comoros','FFC',108,'Stefano Cusin'),
('MLI','Mali','FEMAFOOT',54,'Tom Saintfiet'),
('ZMB','Zambia','FAZ',83,'Moses Sichone'),
('RSA','South Africa','SAFA',61,'Hugo Broos'),
('ANG','Angola','FAF',89,'Patrice Beaumelle'),
('EGY','Egypt','EFA',34,'Hossam Hassan'),
('ZWE','Zimbabwe','ZIFA',117,'Marian Mario Marinică'),
('COD','DR Congo','FECOFA',56,'Sébastien Desabre'),
('BEN','Benin','FBF',92,'Gernot Rohr'),
('SEN','Senegal','FSF',19,'Pape Thiaw'),
('BOT','Botswana','BFU',148,'Morena Ramoreboli'),
('NGR','Nigeria','NFF',38,'Éric Chelle'),
('TAN','Tanzania','TFF',112,'Miguel Gamondi'),
('TUN','Tunisia','FTF',40,'Sami Trabelsi'),
('UGA','Uganda','FUFA',85,'Paul Put'),
('BFA','Burkina Faso','FBF',62,'Brama Traoré'),
('EQG','Equatorial Guinea','FEGUIFUT',97,'Juan Micha'),
('ALG','Algeria','FAF',35,'Vladimir Petković'),
('SDN','Sudan','SFA',118,'James Kwesi Appiah'),
('CIV','Ivory Coast','FIF',42,'Emerse Faé'),
('MOZ','Mozambique','FMF',102,'Chiquinho Conde'),
('CMR','Cameroon','FECAFOOT',57,'David Pagou'),
('GAB','Gabon','FEGAFOOT',78,'Thierry Mouyouma');