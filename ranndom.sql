SELECT titre , auteur , annee FROM livres 
ORDER BY annee DESC 
LIMIT 10 

SLECT * FROM livres 
WHERE titre LIKE '%Histoire%' 

SELECT * FROM livres 
WHERE nb_pages > 400
ORDER BY nb_pages DESC

/* Niveau 2  */ 

/**Ex 2.1** : Compte le nombre total de livres dans la bibliothèque */ 

SELECT count(DISTINCT id)  AS  nb_total_livre FROM livres 

/**Ex 2.2** : Calcule le prix moyen des livres par catégorie */

SELECT categorie , avg(prix) FROM livres 
GROUP BY categorie

/**Ex 2.3** : Trouve le nombre de membres par ville */

SELECT ville , count(*) AS nb_membres FROM membres 
GROUP BY ville 

/**Ex 2.4** : Affiche les catégories qui ont plus de 5 livres */

SELECT categorie , count(id) >  FROM livres 
GROUP BY categorie
HAVING count(id) > 5 

/**Ex 2.5** : Calcule le nombre moyen de pages par décennie de publication */

SELECT (annee_publication / 10 ) * 10 AS decennie , avg(nb_pages) FROM livres 
GROUP BY (annee_publication / 10 ) * 10



/* Ex 3.1 : Affiche tous les emprunts en cours (non rendus) avec le nom du membre et le titre du livre */

SELECT m.nom , l.titre FROM livres l
JOIN emprunts e ON l.id = e.livre_id 
JOIN membres m ON m.id = e.membre_id
WHERE e.date_retour_effective  IS NULL

/* Ex 3.2 : Liste les livres qui ont été empruntés au moins une fois, avec le nombre total d'emprunts */ 

SELECT l.titre , count(*) FROM livres l
JOIN emprunts e ON l.id = e.livre_id 
GROUP BY e.livre_id
HAVING count(*) >= 1

-- /* Ex 3.3 : Trouve les membres qui ont laissé au moins un avis, avec le nombre d'avis par membre */

SELECT mb.nom , count(av.id) FROM membres mb JOIN avis av 
ON mb.id = av.membre_id 
GROUP BY av.memebre_id , mb.nom 

/* Ex 3.4 : Affiche la note moyenne de chaque livre (seulement les livres qui ont des avis) */ 

SELECT lv.titre, avg(av.note) FROM livres lv JOIN avus av
ON lv.id = av.livre_id
GROUP BY av.livre_id , lv.titre

/* Ex 3.5 : Liste les emprunts avec pénalité supérieure à 5€, en affichant : nom du membre, titre du livre, montant de la pénalité */


SELECT m.nom , l.titre , e.penalite  FROM livres l
JOIN emprunts e ON l.id = e.livre_id 
JOIN membres m ON m.id = e.membre_id
WHERE e.penalite  >5 


/* Ex 4.1 : Liste TOUS les livres avec le nombre d'emprunts (même ceux jamais empruntés → 0) */ 

SELECT lv.titre , count(em.id)  AS nb_emprunts FROM livres lv LEFT JOIN emprunts em
ON lv.id = em.livre_id 
GROUP BY lv.id , lv.titre

/* Ex 4.2 : Affiche tous les membres avec le nombre de livres qu'ils ont empruntés (même ceux qui n'ont jamais emprunté) */

SELECT mb.nom , count(em.livre_id) FROM membres mb
LEFT JOIN emprunts em
ON mb.id = em.membre_id 
GROUP BY mb.id , mb.nom 


/* Ex 4.3 : Pour chaque livre, affiche : titre, nombre d'emprunts, note moyenne (NULL si pas d'avis)*/

SELECT lv.titre, count(em.id), avg(av.note) FROM livres 
LEFT JOIN emprunts em ON 
lv.id = em.livre_id
LEFT JOIN avis av ON
lv.id = av.livre_id
GROUP BY lv.id , lv.titre

/* Ex 4.4 : Liste les membres qui se sont inscrits mais n'ont jamais emprunté de livre */

SELECT mb.nom , mb.prenom FROM membres 
LEFT JOIN emprunts em ON 
mb.id=em.membre_id 
WHERE em.id IS NULL

-- Ex 4.5 : Trouve les livres qui ont été empruntés mais n'ont jamais reçu d'avis

SELECT DISTINCT lv.titre FROM livres 
INNER JOIN emprunts em ON 
lv.id = em.livre_id
LEFT JOIN avis av ON
lv.id = av.livre_id
WHERE av.id IS NULL


-- Niveau 5 
/* Ex 5.1 : Trouve les livres dont le prix est supérieur à la moyenne */

SELECT titre FROM livres 
WHERE prix > (SELECT avg(prix) FROM livres) 

/* Ex 5.2 : Liste les membres qui ont emprunté plus de livres que la moyenne */

SELECT mb.nom , mb.prenom  FROM membres mb
JOIN emprunts em ON 
mb.id = em.membre_id 
GROUP BY mb.id , mb.nom , mb.prenom 
HAVING count(em.livre_id) > (SELECT avg(count(livre_id)) FROM emprunts ) 

/* Ex 5.3 : Catégorise les membres selon leur activité : */ 


SELECT mb.nom , mb.prenom , count(em.id) AS nb_emprunts , CASE
    WHEN count(em.id) > 10 THEN "Lecteur assidu"
    WHEN count(em.id) < 3 THEN "Peu actif"
    ELSE "Lecteur moyen"
    END AS categorie 
FROM membres mb
JOIN emprunts em ON 
mb.id = em.membre_id 
GROUP BY mb.id, mb.nom , mb.prenom

/* Ex 5.4 : Calcule le nombre de jours moyen entre la date d'emprunt et la date de retour effective (seulement pour les livres rendus) */

SELECT DATEDIFF(day, date_emprunt ,date_retour_effective) AS nb_jours 
FROM emprunts
WHERE date_retour_effective IS NOT NULL

/* Ex 5.5 : Trouve les livres en retard (date_retour_effective > date_retour_prevue OU date_retour_effective NULL et date_retour_prevue < aujourd'hui)*/

SELECT lv.titre FROM livres lv 
JOIN emprunts em 
ON lv.id = em.livre_id 
WHERE (em.date_retour_prevue < em.date_retour_effective) OR (date_retour_effective IS NULL AND date_retour_prevue < CURDATE() )

/* Ex 6.1 : Trouve les 3 auteurs les plus prolifiques (avec le plus de livres) */ 

SELECT auteur , count(id) FROM livres 
GROUP BY auteur 
ORDER BY count(id)
LIMIT 3 

/* Ex 6.2 : Affiche le TOP 5 des membres qui ont généré le plus de pénalités (somme totale) */

SELECT membre ,sum(em.penalite) AS total_penalites FROM membres mb 
LEFT JOIN emprunts em 
ON mb.id = em.membre_id 
GROUP BY mb.id 
ORDER BY sum(em.penalite) 
LIMIT 5

-- Ex 6.3 : Liste les 5 livres les mieux notés (note moyenne > 4) avec au moins 3 avis

SELECT lv.titre , AVG(av.note) AS note_moy , COUNT(av.id) AS nb_avis FROM livres lv
JOIN avis av ON 
lv.id = av.livre_id 
GROUP BY lv.id , lv.titre
HAVING note_moy > 4 AND nb_avis >=3
ORDER BY note_moy 
LIMIT 3 



-- Ex 6.4 : Pour chaque catégorie, trouve le livre le plus cher

SELECT categorie,titre, prix
   ( SELECT titre , categorie ,prix , DENSE_RANK() OVER ( PARTITION BY categorie ORDER BY prix DESC) AS rang
   FROM livres )
   WHERE rang = 1

-- Ex 6.5 : Calcule le taux d'emprunt par mois pour l'année 2024 
SELECT mois , count(id) OVER( ORDER BY mois desc) FROM (
SELECT id , EXTRACT(YEAR FROM date_emprunt) AS annee , DATE_FORMAT(date_emprunt, '%Y-%m') AS mois FROM emprunst )
WHERE year = '2024'


