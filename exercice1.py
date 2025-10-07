# Exercice 1 : Calcul du nombre de secondes à partir d'une heure
# Format d'entrée : hh:mm:ss
# Exemple : 8:19:22 donnera 29962 secondes

from functools import reduce

def calcul_secondes_iterative(heure_str):
    """
    Version itérative structurée pour calculer les secondes
    """
    # Découper la chaîne en heures, minutes, secondes
    parties = heure_str.split(':')
    heures = int(parties[0])
    minutes = int(parties[1])
    secondes = int(parties[2])
    
    # Calcul itératif
    total_secondes = 0
    total_secondes += heures * 3600  # 1 heure = 3600 secondes
    total_secondes += minutes * 60   # 1 minute = 60 secondes
    total_secondes += secondes       # secondes restantes
    
    return total_secondes

def calcul_secondes_fonctionnelle(heure_str):
    """
    Version fonctionnelle utilisant map() et reduce()
    """
    # Découper et convertir en entiers avec map()
    parties = list(map(int, heure_str.split(':')))
    
    # Coefficients pour heures, minutes, secondes
    coefficients = [3600, 60, 1]
    
    # Multiplier chaque partie par son coefficient avec map()
    produits = list(map(lambda x: x[0] * x[1], zip(parties, coefficients)))
    
    # Sommer tous les produits avec reduce()
    total_secondes = reduce(lambda a, b: a + b, produits)
    
    return total_secondes

def calcul_secondes_fonctionnelle_v2(heure_str):
    """
    Version fonctionnelle alternative plus concise
    """
    parties = list(map(int, heure_str.split(':')))
    coefficients = [3600, 60, 1]
    
    # Calcul direct avec reduce() et zip()
    return reduce(lambda acc, x: acc + x[0] * x[1], 
                  zip(parties, coefficients), 0)

# Tests et démonstrations
if __name__ == "__main__":
    # Test avec l'exemple donné
    heure_test = "8:19:22"
    
    print(f"Heure d'entrée : {heure_test}")
    print()
    
    # Version itérative
    resultat_iteratif = calcul_secondes_iterative(heure_test)
    print(f"Version itérative : {resultat_iteratif} secondes")
    
    # Version fonctionnelle
    resultat_fonctionnel = calcul_secondes_fonctionnelle(heure_test)
    print(f"Version fonctionnelle : {resultat_fonctionnel} secondes")
    
    # Version fonctionnelle v2
    resultat_fonctionnel_v2 = calcul_secondes_fonctionnelle_v2(heure_test)
    print(f"Version fonctionnelle v2 : {resultat_fonctionnel_v2} secondes")
    
    print()
    
    # Vérification que toutes les versions donnent le même résultat
    if resultat_iteratif == resultat_fonctionnel == resultat_fonctionnel_v2:
        print("✓ Toutes les versions donnent le même résultat !")
    else:
        print("✗ Erreur : les versions donnent des résultats différents")
    
    print()
    
    # Tests supplémentaires
    tests = ["0:0:0", "1:0:0", "0:1:0", "0:0:1", "23:59:59", "12:30:45"]
    
    print("Tests supplémentaires :")
    for test in tests:
        resultat = calcul_secondes_fonctionnelle(test)
        print(f"{test} → {resultat} secondes")