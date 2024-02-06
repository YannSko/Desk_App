from database_utils import * 

# Créer la table si elle n'existe pas
create_table()

# Insérer des données dans la table
insert_data("Alice", 25)
insert_data("Bob", 30)
insert_data("Charlie", 35)

# Obtenir toutes les données
all_data = get_data_x()
print("Toutes les données :")
print(all_data)

# Obtenir les deux premières lignes
limited_data = get_data_x(x=2)
print("\nDeux premières lignes :")
print(limited_data)

# Mettre à jour les données de la deuxième ligne
update_data_x(id=2, new_name="Robert", new_age=32)

# Supprimer la première ligne
delete_data_x(id=1)

drop_table("test_table")

# Agréger l'âge moyen des trois premières personnes
average_age = aggregate_data("AVG", "age", x=3)
print("\nÂge moyen des trois premières personnes :", average_age)