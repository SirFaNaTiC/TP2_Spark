# Exercice 1 - Explications détaillées

## Objectif
Calculer le nombre de secondes écoulées depuis minuit (00:00:00) pour une heure donnée au format `hh:mm:ss`.

## Exemple
L'heure `8:19:22` correspond à :
- 8 heures × 3600 secondes/heure = 28800 secondes
- 19 minutes × 60 secondes/minute = 1140 secondes  
- 22 secondes = 22 secondes
- **Total : 28800 + 1140 + 22 = 29962 secondes**

## Solutions proposées

### 1. Version itérative structurée

```python
def calcul_secondes_iterative(heure_str):
    parties = heure_str.split(':')
    heures = int(parties[0])
    minutes = int(parties[1]) 
    secondes = int(parties[2])
    
    total_secondes = 0
    total_secondes += heures * 3600
    total_secondes += minutes * 60
    total_secondes += secondes
    
    return total_secondes
```

**Caractéristiques :**
- Approche séquentielle classique
- Variables intermédiaires explicites
- Facile à comprendre et déboguer

### 2. Version fonctionnelle avec map() et reduce()

```python
def calcul_secondes_fonctionnelle(heure_str):
    parties = list(map(int, heure_str.split(':')))
    coefficients = [3600, 60, 1]
    produits = list(map(lambda x: x[0] * x[1], zip(parties, coefficients)))
    total_secondes = reduce(lambda a, b: a + b, produits)
    return total_secondes
```

**Étapes de la programmation fonctionnelle :**

1. **`map(int, heure_str.split(':'))`** : Transforme `["8", "19", "22"]` en `[8, 19, 22]`

2. **`zip(parties, coefficients)`** : Associe chaque valeur à son coefficient
   - `(8, 3600)`, `(19, 60)`, `(22, 1)`

3. **`map(lambda x: x[0] * x[1], ...)`** : Calcule les produits
   - `[28800, 1140, 22]`

4. **`reduce(lambda a, b: a + b, ...)`** : Somme tous les éléments
   - `28800 + 1140 + 22 = 29962`

### 3. Version fonctionnelle optimisée

```python
def calcul_secondes_fonctionnelle_v2(heure_str):
    parties = list(map(int, heure_str.split(':')))
    coefficients = [3600, 60, 1]
    return reduce(lambda acc, x: acc + x[0] * x[1], 
                  zip(parties, coefficients), 0)
```

**Avantages :**
- Plus concise
- Une seule expression `reduce()`
- Évite la création de listes intermédiaires

## Concepts de programmation fonctionnelle utilisés

### `map(function, iterable)`
- Applique une fonction à chaque élément d'un itérable
- Retourne un objet map (transformé en liste avec `list()`)

### `reduce(function, iterable, initializer)`
- Applique une fonction de façon cumulative aux éléments d'un itérable
- Réduit l'itérable à une seule valeur
- Nécessite `from functools import reduce`

### `zip(iterable1, iterable2, ...)`
- Associe les éléments de plusieurs itérables
- Crée des tuples avec les éléments correspondants

### `lambda`
- Fonctions anonymes courtes
- `lambda arguments: expression`

## Comparaison des approches

| Aspect | Itérative | Fonctionnelle |
|--------|-----------|---------------|
| Lisibilité | ★★★★★ | ★★★☆☆ |
| Concision | ★★☆☆☆ | ★★★★☆ |
| Performance | ★★★★☆ | ★★★☆☆ |
| Maintenance | ★★★★☆ | ★★★★☆ |
| Style fonctionnel | ★☆☆☆☆ | ★★★★★ |

## Tests de validation
Tous les cas de test passent avec succès, confirmant la correction des algorithmes.