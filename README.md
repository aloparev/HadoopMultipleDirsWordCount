# Häufigkeitsberechnung mit Hadoop

Die Aufgabe ist, in einem zur Verfügung gestellten Datensatz eine Vorkommenshäufigkeit
der Wörter zu ermitteln. Anschließend sollen die Top-10 davon ausgegeben werden. Der
Datensatz besteht aus acht Ordner mit literarischen Werken verschiedener Sprachen. Jeder
Ordner repräsentiert eine konkrete Sprache aus der folgenden Liste: Niederländisch,
Englisch, Französisch, Deutsch, Italienisch, Russisch, Ukrainisch und Spanisch. Um die
Ergebnisse vergleichbar zu machen, werden zusätzlich alle Stoppwörter aus den Werken
entfernt.


Die Herausforderung besteht dabei in der Aufteilung der Sprachen auf mehrere Ordner,
sodass die Suche nicht ohne weitere Tricks durchführbar ist. Wir müssen also beim Zählen
zwischen den Sprachen differenzieren können, um im Nachhinein eine Top-10 Aufstellung
pro Sprache zu ermöglichen.