import unittest
from database_utils import *



class TestDatabaseFunctions(unittest.TestCase):

    def setUp(self):
        # Créer une table de test
        create_table()

    def tearDown(self):
        # Supprimer la table de test
        drop_table('test_table')

    def test_insert_and_get_data(self):
        # Insérer des données dans la table
        insert_data("Alice", 25)
        insert_data("Bob", 30)
        insert_data("Charlie", 35)

        # Obtenir toutes les données
        all_data = get_data_x()
        expected_data = [(1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)]
        self.assertEqual(all_data, expected_data)

    def test_update_data(self):
        # Insert some data into the table
        insert_data("Alice", 25)
        insert_data("Bob", 30)
        
        # Update the data
        update_data_x(id=2, new_name="Robert", new_age=32)
        
        # Check that the update was done correctly
        updated_data = get_data_x()
        expected_data = [(1, 'Alice', 25), (2, 'Robert', 32)]
        self.assertEqual(updated_data, expected_data)


    def test_delete_data(self):
        # Insérer des données dans la table
        insert_data("Alice", 25)
        insert_data("Bob", 30)
        
        # Supprimer une ligne
        delete_data_x(id=1)
        
        # Vérifier que la suppression a été effectuée correctement
        remaining_data = get_data_x()
        expected_data = [(2, 'Bob', 30)]
        self.assertEqual(remaining_data, expected_data)

    def test_aggregate_data(self):
        # Insérer des données dans la table
        insert_data("Alice", 25)
        insert_data("Bob", 30)
        insert_data("Charlie", 35)

        # Agréger l'âge moyen
        average_age = aggregate_data("AVG", "age", x=3)
        expected_average_age = (25 + 30 + 35) / 3
        self.assertEqual(average_age, expected_average_age)

if __name__ == '__main__':
    unittest.main()
