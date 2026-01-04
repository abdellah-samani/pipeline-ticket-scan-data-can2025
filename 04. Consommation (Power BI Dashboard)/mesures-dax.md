# Mesures DAX – Tableau de Bord Billetterie - CAN 2025 PHASE DE GROUPES

Ce fichier documente toutes les mesures DAX utilisées dans le dashboard **Tableau de Bord Billetterie - CAN 2025 PHASE DE GROUPES**.  
Il accompagne les captures d’écran.

---

## 1️⃣ Tickets scannés

Tickets = 
CALCULATE(
    COUNTROWS('fact_ticket_scan'),
    FILTER('fact_ticket_scan', 'fact_ticket_scan'[scan_status] = "SCANNED")
)
Description :
Compte le nombre total de billets effectivement scannés dans les stades.

2️⃣ Capacité maximale du stade

Capacite = 
CALCULATE(
    MAX('dim_stadium'[seating_capacity]),
    CROSSFILTER('fact_ticket_scan'[stadium_key], 'dim_stadium'[stadium_key], Both)
)
Description :
Récupère la capacité maximale du stade correspondant aux tickets scannés, en activant une relation bidirectionnelle temporaire pour le calcul.

3️⃣ Taux de remplissage par match

Remplissage = 
DIVIDE([Tickets], [Capacite], 0)
Description :
Calcule le taux de remplissage d’un stade pour un match donné en divisant les tickets scannés par la capacité du stade.

4️⃣ Taux de remplissage global (Gauge)

Taux Remplissage Gauge = 
VAR TicketsVendus = DISTINCTCOUNT('fact_ticket_scan'[ticket_id])
VAR CapaciteParStade = SUM('dim_stadium'[seating_capacity])  
VAR NbMatchs = 36
VAR NbStades = 9
VAR CapaciteTotale = CapaciteParStade * (NbMatchs / NbStades)  
RETURN
MIN(1, DIVIDE(TicketsVendus, CapaciteTotale, 0))
Description :
Calcule le taux de remplissage global sur l’ensemble des matchs pour le dashboard.

TicketsVendus : nombre de tickets uniques scannés

CapaciteParStade : somme des capacités des stades

NbMatchs / NbStades : proportion pour ajuster la capacité totale

La fonction MIN(1, ...) limite le résultat à 100% pour la visualisation gauge
