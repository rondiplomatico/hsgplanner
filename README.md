# HSG Planning App

## Import von Spielplänen
1. Für HVW-Scope die Seite http://spo.handball4all.de/Spielbetrieb/mannschaftsspielplaene.php?orgGrpID=3 aufrufen,
und mit Vereinsnummer 114 den Spielplan herunterladen.
2. Die Datei "HSG_Leinfelden-Echterdingen.csv" hat wahrscheinlich eine doppelte .csv Endung, Achtung beim ablegen.
3. Falls in Liga außerhalb HVW, ggf. die Spielpläne von F1 manuell in die Spielliste einfügen.
Achtung, die Hallenbezeichnung muss auch auf "4066" angepasst werden!

## Import von Spielerlisten
1. Aktuelle Arbeitsdienstzeiten je Spieler aus der Saisonvorzeit in den "Export" Reiter der Stammdaten kopieren, Spalte I
2. Den Reiter "Export" als csv herunterladen.
3. In der CSV schauen, dass unten keine "leeren" Einträge ohne Namen vorn sind 

## Generelle ToDos
1. Wischerdienste: Die Leistungsfaktoren der C1-Jugenden sind manuell auf 0.27 angepasst, um in der Gesamtkonstellation jeweils ~600 min arbeitslast zu bekommen, was
den Stunden für Wischerdienste in 22/23 entspricht. (Automatisches "Cap" der Zeiten auf die leistbaren zeiten ist eine etwas schwierigere Angelegenheit..)