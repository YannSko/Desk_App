# Desk_App_Application

Ci_joint se trouve mon application CustomTkinter concernant la récupération , traitements et plot des données sur mon application Tkinter.

### Les techs utilisées:
Python == 10
PostgreSql = 15
Custom Tkinter 

### Pour utiliser :
- cloner  : https://github.com/YannSko/Desk_App.git

-  creer votre environnement

- pip install requirements.txt

- lancer main.py du directory App (léger temps de chargement lors du launch de lappli)

- pour traitement de text de livre  :text > Treatments_txt > data_txt > test_text.iypnb



## Comment le projet est construit:

### A la partie traitement de texte :

dans  le dossier text > Treatments_txt.
Lancer le notebook et changer le txt dentrée avec votre path afin davoir votre analyse de text ( livre).

Condition : le livre doit être en anglais.

Vous pouvez insérer votre logo afin d'avoir une analyse sur pdf à l'output.


#### L 'application Desktop :
#### Dans le dossier Logs se trouve le decorateur de log. 
Insérer le décorateur où vous avez besoin de log concernant le déroulement des functions.

#### Dans le dossier utils : 

Se trouve les 2 class threads et multiprocess :
Si votre Ordinateur est puissant insérer multiprocess dans les parties qui vont suivre .
Sinon conserver thread.

#### Dossier Data :

Se trouve dans Api : les Call Api avec leurs test ( tout est fonctionnel) :
dans le dossier se trouve les datas récupérées

Se trouve dans Database :

- Data_process.py = traite les dataframes dans leurs bon type et applique un clean simple

- Database_decorator : decore les fonctions critiques du Crud BDD afin de créer une back up automatique dans une bdd annexe et des csv dans le dossier " Back_up"( est automatiquement lancé au lancement de lapp)

-database_utils : Toutes les fonctions avec le CRUD et les scripts pour gerer différents types de json ( dont les times series et certains nested) : les scripts complexes nont pas été  implemntés dans le desk app ( soucis de performance de mon pc)
 
- dans test_opty.py : se trouve le script réalisant le backup et celui permettant de charger le backup ( les 2 sont implementes dans lapp avec un bouton)

- dans le notebook se trouve les traitements de data vérifiant le script de data process

## Dans le directory App:

main.py : pour lancer lapplication

dans le directory Components:

Dataviz.py = onglet permettant de realiser les taches de plot en fonctions de parametres que le user peut modifier

db_management = onglet permettant dinteragir avec la bdd : option de roll back lors des taches "critiques"

export_tab = log de lapplication en temps reels avec possiblités de les exporter en local

file select = possibilité de lire un fichier avant de linserer dans la bbdd .

Settings > Settings .py = gestion de settings ui ux + restaure backup + make backup
            settings.json ( load les settings de la sessions actuelles et precedentes lors du launch)



