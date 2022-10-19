# Map-Reduce
Implémentation de Map-Reduce en Golang.
Permet l'execution de taches en mode distribué, avec l'algorithme Map-Shuffle-Reduce.
Les calculs sont répartis sur différents serveurs, ces derniers communiquants à travers le reseau via le protocole TCP.

## Template
Un template de l'implémentation est mis à disposition : il suffit de remplir les fonctions vides avec notre code pour bénéficier d'une implémentation fonctionelle.
Les fonctions à considérer sont :
- Split
- Map
- ShuffleReceiver
- GetReduceData
- ReduceReceiver
- ReduceDone

## Word Count
Utilisation du template décrit ci-dessus pour compter les mots d'un texte. Ici, nous utiliserons un texte de Jules-Verne.