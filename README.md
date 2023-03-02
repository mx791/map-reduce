# Map-Reduce
Implémentation de Map-Reduce en Golang.
Permet l'execution de taches en mode distribué, avec l'algorithme Map-Shuffle-Reduce.
Les calculs sont répartis sur différents serveurs, ces derniers communiquants à travers le reseau via le protocole TCP.

## Lancer un serveur
Pour lancer un noeud, il faut spécifier son port d'écoute en arguement.
On utilisera par exemple :
```
go run ServerWordCount.go 1234
```
Il faut également rendre accessible au programme un fichier JSON contenant la liste des noeuds du cluster, ainsi que leur port d'écoute.

## Interface MapReducer
Cette interface permet de créer des classes exécutées dans un contexte distribué. Il suffit pour cela d'implémenter les méthodes requises.
- Split : réparti les calculs sur differents noeuds
- Map : commence le travail sur les données reçues après le split
- ShuffleReceiver : est appelé quand le noeud reçoit les donénes du shuffle
- GetReduceData : opération reduce
- ReduceReceiver : est appelé quand le noeud reçoit les données de reduce des autres noeuds
- ReduceDone : est appelé quand tous les noeuds ont terminé le reduce
- Reset : ré-initialise le contexte

## Word Count
L'implémentation permet ici de faire un word-count sur un text de Jule Vernes.