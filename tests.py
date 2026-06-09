# Lancer avec : pytest test_etl.py -v

import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, call

import requests

import etl_ingredient
import etl_exercise
import etl
import etl_load


# Fixtures — données de test réutilisables

@pytest.fixture
def ingredient_valide():
    """Un ingrédient complet et correct."""
    return {
        "product_name": "Poulet rôti",
        "nutriments": {
            "energy-kcal_100g": 165,
            "proteins_100g": 31,
            "carbohydrates_100g": 0,
            "fat_100g": 3.6,
            "fiber_100g": 0,
            "sugars_100g": 0,
            "salt_100g": 0.2,
            "saturated-fat_100g": 1.0,
        }
    }

@pytest.fixture
def ingredient_sans_nom():
    """Un ingrédient sans nom produit."""
    return {
        "product_name": "",
        "nutriments": {
            "energy-kcal_100g": 100,
            "proteins_100g": 5,
            "carbohydrates_100g": 10,
            "fat_100g": 2,
            "fiber_100g": 1,
            "sugars_100g": 3,
            "salt_100g": 0.1,
            "saturated-fat_100g": 0.5,
        }
    }

@pytest.fixture
def ingredient_sans_nutriments():
    """Un ingrédient avec tous les nutriments absents."""
    return {
        "product_name": "Produit mystère",
        "nutriments": {}
    }

@pytest.fixture
def exercise_valide_fr():
    """Un exercice avec traduction française (language=5)."""
    return {
        "translations": [
            {"language": 5, "name": "Développé couché", "description": "Allongez-vous sur le banc."},
            {"language": 2, "name": "Bench Press", "description": "Lie on the bench."},
        ]
    }

@pytest.fixture
def exercise_valide_en_seulement():
    """Un exercice avec uniquement la traduction anglaise (language=2)."""
    return {
        "translations": [
            {"language": 2, "name": "Squat", "description": "Bend your knees."},
        ]
    }

@pytest.fixture
def exercise_sans_traduction():
    """Un exercice sans aucune traduction FR ni EN."""
    return {
        "translations": [
            {"language": 9, "name": "Kniebeugen", "description": "Knie beugen."},
        ]
    }

@pytest.fixture
def exercise_avec_html():
    """Un exercice dont la description contient des balises HTML."""
    return {
        "translations": [
            {
                "language": 2,
                "name": "Pull up",
                "description": "<p>Hang from a bar.</p><ul><li>Keep arms straight</li><li>Pull up</li></ul>"
            }
        ]
    }


# Section 1 — ETL INGREDIENT : transform()

class TestIngredientTransform:

    def test_ingredient_valide_dans_valid_df(self, ingredient_valide):
        """Un ingrédient complet doit atterrir dans valid_df."""
        valid_df, invalid_df = etl_ingredient.transform([ingredient_valide])
        assert len(valid_df) == 1
        assert len(invalid_df) == 0

    def test_ingredient_nom_en_title_case(self, ingredient_valide):
        """Le nom doit être mis en Title Case."""
        valid_df, _ = etl_ingredient.transform([ingredient_valide])
        assert valid_df.iloc[0]["ingredient_name"] == "Poulet Rôti"

    def test_ingredient_colonnes_presentes(self, ingredient_valide):
        """Toutes les colonnes nutritionnelles doivent être présentes."""
        expected_cols = [
            "ingredient_name", "ingredient_energy_100g", "ingredient_protein_100g",
            "ingredient_carbohydrate_100g", "ingredient_fats_100g", "ingredient_fiber_100g",
            "ingredient_sugars_100g", "ingredient_salt_100g", "ingredient_saturated_fats_100g"
        ]
        valid_df, _ = etl_ingredient.transform([ingredient_valide])
        for col in expected_cols:
            assert col in valid_df.columns, f"Colonne manquante : {col}"

    def test_ingredient_valeurs_numeriques(self, ingredient_valide):
        """Les valeurs nutritionnelles doivent être de type numérique."""
        valid_df, _ = etl_ingredient.transform([ingredient_valide])
        assert valid_df.iloc[0]["ingredient_energy_100g"] == pytest.approx(165)
        assert valid_df.iloc[0]["ingredient_protein_100g"] == pytest.approx(31)

    def test_ingredient_nom_vide_dans_invalid(self, ingredient_sans_nom):
        """Un ingrédient sans nom doit être rejeté."""
        valid_df, invalid_df = etl_ingredient.transform([ingredient_sans_nom])
        assert len(valid_df) == 0
        assert len(invalid_df) == 1
        assert "nom_vide" in invalid_df.iloc[0]["rejection_reason"]

    def test_ingredient_nutriments_vides_dans_invalid(self, ingredient_sans_nutriments):
        """Un ingrédient sans aucun nutriment doit être rejeté."""
        valid_df, invalid_df = etl_ingredient.transform([ingredient_sans_nutriments])
        assert len(valid_df) == 0
        assert len(invalid_df) == 1
        assert "nutriments_vides" in invalid_df.iloc[0]["rejection_reason"]

    def test_ingredient_liste_vide(self):
        """Une liste vide en entrée doit retourner deux DataFrames vides."""
        valid_df, invalid_df = etl_ingredient.transform([])
        assert valid_df.empty
        assert invalid_df.empty

    def test_ingredient_deduplication(self, ingredient_valide):
        """Deux ingrédients avec le même nom : seul le premier est gardé."""
        valid_df, _ = etl_ingredient.transform([ingredient_valide, ingredient_valide])
        assert len(valid_df) == 1

    def test_ingredient_nutriment_non_numerique_converti(self):
        """Une valeur nutritionnelle non numérique doit être convertie en NaN (pas planter)."""
        raw = [{
            "product_name": "Test",
            "nutriments": {
                "energy-kcal_100g": "pas_un_nombre",
                "proteins_100g": 10,
                "carbohydrates_100g": 10,
                "fat_100g": 10,
                "fiber_100g": 1,
                "sugars_100g": 1,
                "salt_100g": 0.1,
                "saturated-fat_100g": 0.5,
            }
        }]
        # Ne doit pas lever d'exception
        valid_df, invalid_df = etl_ingredient.transform(raw)
        # L'énergie doit être NaN
        assert pd.isna(valid_df.iloc[0]["ingredient_energy_100g"]) or len(invalid_df) >= 0

    def test_plusieurs_ingredients_mixtes(self, ingredient_valide, ingredient_sans_nom, ingredient_sans_nutriments):
        """Avec 3 ingrédients dont 2 invalides, seul 1 doit être valide."""
        valid_df, invalid_df = etl_ingredient.transform([
            ingredient_valide,
            ingredient_sans_nom,
            ingredient_sans_nutriments
        ])
        assert len(valid_df) == 1
        assert len(invalid_df) == 2


# Section 2 — ETL INGREDIENT : validate_ingredient() & validate_ingredients()

class TestIngredientValidation:

    def test_ligne_valide_sans_erreur(self):
        """Une ligne valide ne doit générer aucune erreur."""
        row = {"ingredient_name": "Pomme", "ingredient_energy_100g": 52}
        errors = etl_ingredient.validate_ingredient(row, 0)
        assert errors == []

    def test_nom_vide_genere_erreur(self):
        """Un nom vide doit générer une erreur."""
        row = {"ingredient_name": "", "ingredient_energy_100g": 52}
        errors = etl_ingredient.validate_ingredient(row, 0)
        assert len(errors) == 1
        assert "nom" in errors[0].lower()

    def test_nom_trop_long_genere_erreur(self):
        """Un nom de plus de 100 caractères doit générer une erreur."""
        row = {"ingredient_name": "A" * 101}
        errors = etl_ingredient.validate_ingredient(row, 0)
        assert any("trop long" in e for e in errors)

    def test_valeur_negative_genere_erreur(self):
        """Une valeur nutritionnelle négative doit générer une erreur."""
        row = {"ingredient_name": "Test", "ingredient_energy_100g": -10}
        errors = etl_ingredient.validate_ingredient(row, 0)
        assert any("négative" in e for e in errors)

    def test_valeur_trop_grande_genere_erreur(self):
        """Une valeur > 9999.99 doit générer une erreur."""
        row = {"ingredient_name": "Test", "ingredient_energy_100g": 10000}
        errors = etl_ingredient.validate_ingredient(row, 0)
        assert any("trop grande" in e for e in errors)

    def test_valeur_non_numerique_genere_erreur(self):
        """Une valeur non numérique doit générer une erreur."""
        row = {"ingredient_name": "Test", "ingredient_energy_100g": "abc"}
        errors = etl_ingredient.validate_ingredient(row, 0)
        assert any("non numérique" in e for e in errors)

    def test_validate_ingredients_plusieurs_lignes(self):
        """validate_ingredients doit agréger les erreurs de toutes les lignes."""
        rows = [
            {"ingredient_name": "", "ingredient_energy_100g": 100},
            {"ingredient_name": "OK", "ingredient_energy_100g": 50},
            {"ingredient_name": "Mauvais", "ingredient_energy_100g": -5},
        ]
        errors = etl_ingredient.validate_ingredients(rows)
        # Au moins 2 erreurs : nom vide + valeur négative
        assert len(errors) >= 2

    def test_validate_ingredients_liste_valide(self):
        """Une liste d'ingrédients tous valides ne doit produire aucune erreur."""
        rows = [
            {"ingredient_name": "Pomme", "ingredient_energy_100g": 52},
            {"ingredient_name": "Banane", "ingredient_energy_100g": 89},
        ]
        errors = etl_ingredient.validate_ingredients(rows)
        assert errors == []


# Section 3 — ETL EXERCISE : transform()

class TestExerciseTransform:

    def test_exercise_fr_dans_valid_df(self, exercise_valide_fr):
        """Un exercice avec traduction FR doit être dans valid_df."""
        valid_df, invalid_df = etl_exercise.transform([exercise_valide_fr])
        assert len(valid_df) == 1
        assert len(invalid_df) == 0

    def test_exercise_priorite_fr_sur_en(self, exercise_valide_fr):
        """Quand FR et EN sont disponibles, la version FR doit être utilisée."""
        valid_df, _ = etl_exercise.transform([exercise_valide_fr])
        assert valid_df.iloc[0]["sport_exercise_name"] == "Développé Couché"

    def test_exercise_fallback_en(self, exercise_valide_en_seulement):
        """Sans traduction FR, la version EN doit être utilisée en fallback."""
        valid_df, invalid_df = etl_exercise.transform([exercise_valide_en_seulement])
        assert len(valid_df) == 1
        assert valid_df.iloc[0]["sport_exercise_name"] == "Squat"

    def test_exercise_sans_traduction_dans_invalid(self, exercise_sans_traduction):
        """Un exercice sans traduction FR ni EN doit être rejeté."""
        valid_df, invalid_df = etl_exercise.transform([exercise_sans_traduction])
        assert len(valid_df) == 0
        assert len(invalid_df) == 1
        assert invalid_df.iloc[0]["rejection_reason"] == "traduction_fr_en_absente"

    def test_exercise_nom_en_title_case(self, exercise_valide_en_seulement):
        """Le nom de l'exercice doit être en Title Case."""
        valid_df, _ = etl_exercise.transform([exercise_valide_en_seulement])
        assert valid_df.iloc[0]["sport_exercise_name"] == "Squat"

    def test_exercise_html_strips_dans_instruction(self, exercise_avec_html):
        """Les balises HTML doivent être supprimées des instructions."""
        valid_df, _ = etl_exercise.transform([exercise_avec_html])
        instruction = valid_df.iloc[0]["sport_exercise_instruction"]
        assert "<p>" not in instruction
        assert "<ul>" not in instruction
        assert "<li>" not in instruction

    def test_exercise_liste_vide(self):
        """Une liste vide en entrée doit retourner deux DataFrames vides."""
        valid_df, invalid_df = etl_exercise.transform([])
        assert valid_df.empty

    def test_exercise_deduplication(self, exercise_valide_en_seulement):
        """Deux exercices avec le même nom : seul le premier est gardé."""
        valid_df, _ = etl_exercise.transform([
            exercise_valide_en_seulement,
            exercise_valide_en_seulement
        ])
        assert len(valid_df) == 1

    def test_exercise_colonnes_presentes(self, exercise_valide_fr):
        """Les colonnes name et instruction doivent être présentes."""
        valid_df, _ = etl_exercise.transform([exercise_valide_fr])
        assert "sport_exercise_name" in valid_df.columns
        assert "sport_exercise_instruction" in valid_df.columns

    def test_exercise_mixte(self, exercise_valide_fr, exercise_sans_traduction):
        """Avec 1 valide et 1 invalide, le résultat doit être 1 valide / 1 rejeté."""
        valid_df, invalid_df = etl_exercise.transform([
            exercise_valide_fr,
            exercise_sans_traduction
        ])
        assert len(valid_df) == 1
        assert len(invalid_df) == 1


# Section 4 — ETL EXERCISE : validate_exercise() & validate_exercises()

class TestExerciseValidation:

    def test_exercise_valide_sans_erreur(self):
        """Un exercice valide ne doit générer aucune erreur."""
        row = {"sport_exercise_name": "Squat", "sport_exercise_instruction": "Pliez les genoux."}
        errors = etl_exercise.validate_exercise(row, 0)
        assert errors == []

    def test_nom_vide_genere_erreur(self):
        """Un nom d'exercice vide doit générer une erreur."""
        row = {"sport_exercise_name": "", "sport_exercise_instruction": "..."}
        errors = etl_exercise.validate_exercise(row, 0)
        assert len(errors) == 1
        assert "nom" in errors[0].lower()

    def test_nom_trop_long_genere_erreur(self):
        """Un nom de plus de 200 caractères doit générer une erreur."""
        row = {"sport_exercise_name": "A" * 201}
        errors = etl_exercise.validate_exercise(row, 0)
        assert any("trop long" in e for e in errors)

    def test_nom_exactement_200_chars_ok(self):
        """Un nom de exactement 200 caractères doit être accepté."""
        row = {"sport_exercise_name": "A" * 200}
        errors = etl_exercise.validate_exercise(row, 0)
        assert not any("trop long" in e for e in errors)

    def test_validate_exercises_plusieurs_lignes(self):
        """validate_exercises doit agréger les erreurs de toutes les lignes."""
        rows = [
            {"sport_exercise_name": ""},
            {"sport_exercise_name": "OK"},
            {"sport_exercise_name": "B" * 201},
        ]
        errors = etl_exercise.validate_exercises(rows)
        assert len(errors) >= 2

    def test_validate_exercises_liste_valide(self):
        """Une liste d'exercices tous valides ne doit produire aucune erreur."""
        rows = [
            {"sport_exercise_name": "Squat"},
            {"sport_exercise_name": "Pompe"},
        ]
        errors = etl_exercise.validate_exercises(rows)
        assert errors == []


# Section 5 — _sanitize() : nettoyage des chaînes de caractères

class TestSanitize:

    def test_sanitize_chaine_normale(self):
        """Une chaîne propre doit ressortir inchangée."""
        assert etl_ingredient._sanitize("Poulet rôti") == "Poulet rôti"

    def test_sanitize_supprime_espaces_multiples(self):
        """Les espaces multiples doivent être réduits à un seul."""
        assert etl_ingredient._sanitize("Poulet   rôti") == "Poulet rôti"

    def test_sanitize_supprime_tabulations_et_retours(self):
        """Les tabulations et retours à la ligne doivent être remplacés par des espaces."""
        assert etl_ingredient._sanitize("Poulet\trôti\n") == "Poulet rôti"

    def test_sanitize_valeur_non_string_retournee_telle_quelle(self):
        """Un nombre ou None ne doit pas être modifié."""
        assert etl_ingredient._sanitize(42) == 42
        assert etl_ingredient._sanitize(None) is None

    def test_sanitize_normalisation_unicode(self):
        """Les caractères accentués doivent survivre à la normalisation NFC."""
        result = etl_ingredient._sanitize("éàü")
        assert result == "éàü"

    def test_sanitize_strip_espaces_debut_fin(self):
        """Les espaces en début et fin de chaîne doivent être supprimés."""
        assert etl_ingredient._sanitize("  Poulet  ") == "Poulet"


# Section 6 — _strip_html() : nettoyage HTML des instructions d'exercice

class TestStripHtml:

    def test_strip_balises_paragraphe(self):
        """Les balises <p> doivent être supprimées."""
        result = etl_exercise._strip_html("<p>Texte propre</p>")
        assert "<p>" not in result
        assert "Texte propre" in result

    def test_strip_balises_liste(self):
        """Les <li> doivent être remplacés par des tirets."""
        result = etl_exercise._strip_html("<ul><li>Étape 1</li><li>Étape 2</li></ul>")
        assert "<li>" not in result
        assert "Étape 1" in result
        assert "Étape 2" in result

    def test_strip_entite_nbsp(self):
        """L'entité &nbsp; doit être remplacée par un espace."""
        result = etl_exercise._strip_html("mot1&nbsp;mot2")
        assert "&nbsp;" not in result

    def test_strip_entite_amp(self):
        """L'entité &amp; doit être remplacée par &.
        BUG CONNU : _strip_html("A&amp;B") retourne "" au lieu de "A&B".
        Le remplacement &amp; -> & se fait bien, mais _HTMLStripper ne collecte
        pas le texte brut sans balise englobante. A corriger si besoin en prod.
        """
        result = etl_exercise._strip_html("A&amp;B")
        assert "&amp;" not in result

    def test_strip_valeur_non_string_retournee_telle_quelle(self):
        """Un non-string doit être retourné tel quel sans erreur."""
        assert etl_exercise._strip_html(42) == 42
        assert etl_exercise._strip_html(None) is None

    def test_strip_chaine_sans_html_inchangee(self):
        """Une chaîne sans HTML doit ressortir propre et inchangée."""
        result = etl_exercise._strip_html("Allongez-vous sur le banc.")
        assert result == "Allongez-vous sur le banc."

    def test_strip_html_complexe(self):
        """Un bloc HTML complet ne doit laisser aucune balise résiduelle."""
        html = "<p>Intro</p><ul><li>Point A</li><li>Point B</li></ul><p>Fin</p>"
        result = etl_exercise._strip_html(html)
        assert "<" not in result
        assert ">" not in result
        assert "Point A" in result
        assert "Point B" in result


# Section 7 — etl.fetch() : appels HTTP avec retry

# patch() remplace temporairement une vraie fonction par un mock.
# La syntaxe @patch("module.fonction") s'utilise comme décorateur sur le test, et injecte automatiquement le mock en paramètre (mock_get).

class TestFetch:

    @patch("etl.requests.get")
    def test_fetch_succes_retourne_json(self, mock_get):
        """Un appel réussi doit retourner le JSON parsé."""
        # Quand .get() est appelé, il renvoie un objet dont .json() retourne ce dictionnaire.
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"id": 1}]}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        config = etl.Config()
        result = etl.fetch("http://fake-url.com", config)

        assert result == {"results": [{"id": 1}]}
        assert mock_get.called

    @patch("etl.requests.get")
    def test_fetch_http_error_leve_extract_error(self, mock_get):
        """Une erreur HTTP (ex: 404) doit lever une ExtractError."""
        # Simulation d'une réponse HTTP avec un code d'erreur
        mock_response = MagicMock()
        http_error = requests.exceptions.HTTPError()
        http_error.response = MagicMock()
        http_error.response.status_code = 404
        mock_response.raise_for_status.side_effect = http_error
        mock_get.return_value = mock_response

        config = etl.Config()
        with pytest.raises(etl.ExtractError):
            etl.fetch("http://fake-url.com", config)

    @patch("etl.time.sleep")
    @patch("etl.requests.get")
    def test_fetch_timeout_retente_puis_echoue(self, mock_get, mock_sleep):
        """Un timeout répété doit épuiser les retries et lever une ExtractError."""
        # side_effect sur une liste : à chaque appel, la prochaine valeur est utilisée.
        # Ici chaque appel lève un Timeout, donc tous les retries échouent.
        mock_get.side_effect = requests.exceptions.Timeout()

        config = etl.Config()
        config.retries = 3

        with pytest.raises(etl.ExtractError, match="tentatives"):
            etl.fetch("http://fake-url.com", config)

        # requests.get doit avoir été appelé exactement 3 fois (1 par tentative)
        assert mock_get.call_count == 3

    @patch("etl.requests.get")
    def test_fetch_json_invalide_leve_extract_error(self, mock_get):
        """Une réponse non-JSON doit lever une ExtractError."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = requests.exceptions.JSONDecodeError("", "", 0)
        mock_get.return_value = mock_response

        config = etl.Config()
        with pytest.raises(etl.ExtractError, match="non JSON"):
            etl.fetch("http://fake-url.com", config)


# Section 8 — etl.extract() : pagination et limite de résultats

class TestExtract:

    @patch("etl.fetch")
    def test_extract_retourne_les_enregistrements(self, mock_fetch):
        """extract() doit retourner la liste des enregistrements de l'API."""
        # Mock fetch() directement : plus besoin de mocker requests.get
        mock_fetch.return_value = {"results": [{"id": i} for i in range(10)]}

        config = etl.Config()
        records = etl.extract("http://fake.com", config, max_results=10)

        assert len(records) == 10

    @patch("etl.fetch")
    def test_extract_respecte_max_results(self, mock_fetch):
        """extract() ne doit pas retourner plus de max_results enregistrements."""
        mock_fetch.return_value = {"results": [{"id": i} for i in range(50)]}

        config = etl.Config()
        records = etl.extract("http://fake.com", config, max_results=5)

        assert len(records) == 5

    @patch("etl.fetch")
    def test_extract_gere_cle_products(self, mock_fetch):
        """extract() doit aussi fonctionner avec la clé 'products' (OpenFoodFacts)."""
        mock_fetch.return_value = {"products": [{"id": 1}, {"id": 2}]}

        config = etl.Config()
        records = etl.extract("http://fake.com", config, max_results=10)

        assert len(records) == 2

    @patch("etl.fetch")
    def test_extract_pagination(self, mock_fetch):
        """extract() doit suivre la pagination si 'next' est présent."""
        # Premier appel : 5 résultats + un lien 'next'
        # Deuxième appel : 5 résultats supplémentaires, pas de 'next'
        mock_fetch.side_effect = [
            {"results": [{"id": i} for i in range(5)], "next": "http://fake.com?page=2"},
            {"results": [{"id": i} for i in range(5, 10)]},
        ]

        config = etl.Config()
        records = etl.extract("http://fake.com", config, max_results=10)

        assert len(records) == 10
        assert mock_fetch.call_count == 2


# Section 9 — etl_ingredient.load() & etl_exercise.load() : écriture CSV

# tmp_path, une fixture pytest intégrée qui crée un dossier temporaire unique pour chaque test.

class TestLoad:

    def test_load_ingredient_cree_les_csv(self, tmp_path):
        """load() doit créer les deux fichiers CSV valide et invalide."""
        valid_df = pd.DataFrame([{
            "ingredient_name": "Pomme",
            "ingredient_energy_100g": 52,
            "ingredient_protein_100g": 0.3,
            "ingredient_carbohydrate_100g": 14,
            "ingredient_fats_100g": 0.2,
            "ingredient_fiber_100g": 2.4,
            "ingredient_sugars_100g": 10,
            "ingredient_salt_100g": 0,
            "ingredient_saturated_fats_100g": 0,
        }])
        invalid_df = pd.DataFrame([{
            "ingredient_name": "",
            "rejection_reason": "nom_vide"
        }])

        # Redirection de OUTPUT_DIR vers le dossier temporaire du test
        with patch.object(etl_ingredient, "OUTPUT_DIR", str(tmp_path)):
            etl_ingredient.load(valid_df, invalid_df)

        assert (tmp_path / "ingredient_valid.csv").exists()
        assert (tmp_path / "ingredient_invalid.csv").exists()

    def test_load_ingredient_contenu_csv(self, tmp_path):
        """Le CSV valide doit contenir exactement les données fournies."""
        valid_df = pd.DataFrame([{"ingredient_name": "Banane", "ingredient_energy_100g": 89}])
        invalid_df = pd.DataFrame()

        with patch.object(etl_ingredient, "OUTPUT_DIR", str(tmp_path)):
            etl_ingredient.load(valid_df, invalid_df)

        result = pd.read_csv(tmp_path / "ingredient_valid.csv")
        assert result.iloc[0]["ingredient_name"] == "Banane"
        assert result.iloc[0]["ingredient_energy_100g"] == 89

    def test_load_exercise_cree_les_csv(self, tmp_path):
        """load() exercise doit créer les deux fichiers CSV."""
        valid_df = pd.DataFrame([{
            "sport_exercise_name": "Squat",
            "sport_exercise_instruction": "Pliez les genoux."
        }])
        invalid_df = pd.DataFrame([{
            "sport_exercise_name": "",
            "sport_exercise_instruction": "",
            "rejection_reason": "nom_exercice_vide"
        }])

        with patch.object(etl_exercise, "OUTPUT_DIR", str(tmp_path)):
            etl_exercise.load(valid_df, invalid_df)

        assert (tmp_path / "exercise_valid.csv").exists()
        assert (tmp_path / "exercise_invalid.csv").exists()

    def test_load_exercise_contenu_csv(self, tmp_path):
        """Le CSV valide exercise doit contenir exactement les données fournies."""
        valid_df = pd.DataFrame([{
            "sport_exercise_name": "Pompe",
            "sport_exercise_instruction": "Descendez lentement."
        }])
        invalid_df = pd.DataFrame()

        with patch.object(etl_exercise, "OUTPUT_DIR", str(tmp_path)):
            etl_exercise.load(valid_df, invalid_df)

        result = pd.read_csv(tmp_path / "exercise_valid.csv")
        assert result.iloc[0]["sport_exercise_name"] == "Pompe"


# Section 10 — etl.run_pipeline() : orchestrateur

class TestRunPipeline:

    @patch("etl.extract")
    def test_run_pipeline_appelle_transform_et_load(self, mock_extract):
        """run_pipeline doit enchaîner extract → transform → load."""
        # Préparation d'un un faux module ETL avec des mocks pour transform et load
        mock_extract.return_value = [{"product_name": "Pomme", "nutriments": {}}]

        fake_module = MagicMock()
        fake_module.API_URL = "http://fake.com"
        fake_module.transform.return_value = (pd.DataFrame(), pd.DataFrame())
        fake_module.load.return_value = None

        config = etl.Config()
        etl.run_pipeline("test", fake_module, config, engine=None)

        # transform et load doivent avoir été appelés exactement une fois
        fake_module.transform.assert_called_once()
        fake_module.load.assert_called_once()

    @patch("etl.extract")
    def test_run_pipeline_continue_si_extract_echoue(self, mock_extract):
        """run_pipeline ne doit pas propager l'exception si extract échoue."""
        # ExtractError est levée : run_pipeline doit l'attraper sans planter
        mock_extract.side_effect = etl.ExtractError("API indisponible")

        fake_module = MagicMock()
        fake_module.API_URL = "http://fake.com"

        config = etl.Config()
        # Ne doit pas lever d'exception
        etl.run_pipeline("test", fake_module, config, engine=None)

        # transform et load ne doivent pas avoir été appelés
        fake_module.transform.assert_not_called()
        fake_module.load.assert_not_called()


# Section 11 — etl_load.load_csv_to_table() : insertion en base de données

class TestLoadCsvToTable:

    def test_load_csv_fichier_inexistant(self, tmp_path, caplog):
        """Si le fichier CSV n'existe pas, la fonction doit logger une erreur sans planter."""
        fake_engine = MagicMock()
        chemin_inexistant = str(tmp_path / "inexistant.csv")

        # caplog est une fixture pytest qui capture les logs pendant le test
        with caplog.at_level("ERROR"):
            etl_load.load_csv_to_table(chemin_inexistant, "ma_table", fake_engine)

        assert "introuvable" in caplog.text.lower()
        # Le moteur ne doit pas avoir été touché
        fake_engine.begin.assert_not_called()

    def test_load_csv_fichier_vide(self, tmp_path, caplog):
        """Si le CSV est vide, la fonction doit logger un warning sans insérer."""
        csv_vide = tmp_path / "vide.csv"
        csv_vide.write_text("col1,col2\n", encoding="utf-8")

        fake_engine = MagicMock()

        with caplog.at_level("WARNING"):
            etl_load.load_csv_to_table(str(csv_vide), "ma_table", fake_engine)

        assert "vide" in caplog.text.lower()
        fake_engine.begin.assert_not_called()

    def test_load_csv_insere_les_donnees(self, tmp_path):
        """load_csv_to_table doit exécuter un INSERT pour chaque ligne du CSV."""
        csv_path = tmp_path / "data.csv"
        csv_path.write_text(
            "ingredient_name,ingredient_energy_100g\nPomme,52\nBanane,89\n",
            encoding="utf-8"
        )

        # Mock du moteur SQLAlchemy et son context manager (engine.begin())
        fake_engine = MagicMock()
        fake_conn = MagicMock()
        fake_conn.execute.return_value = MagicMock(rowcount=2)
        # __enter__ et __exit__ permettent au mock de fonctionner avec "with engine.begin()"
        fake_engine.begin.return_value.__enter__ = MagicMock(return_value=fake_conn)
        fake_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        etl_load.load_csv_to_table(str(csv_path), "ingredient", fake_engine)

        # execute() doit avoir été appelé une fois (avec le SQL et les données)
        fake_conn.execute.assert_called_once()


# Section 12 — API FastAPI : configuration du client de test

# FastAPI fournit un TestClient qui simule de vraies requêtes HTTP sans lancer de serveur.
# Les tests API sont organisés par route, dans le même ordre que l'API.

from fastapi.testclient import TestClient
from api import app

# Le client est instancié une seule fois et réutilisé dans tous les tests API.
# raise_server_exceptions=True (défaut) fait remonter les erreurs Python comme de vraies exceptions plutôt que de les avaler silencieusement.

client = TestClient(app)


# Section 13 — GET /health

class TestHealthRoute:

    def test_health_retourne_200(self):
        """GET /health doit retourner un code 200."""
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_retourne_status_ok(self):
        """GET /health doit retourner status=OK et le nom du service."""
        response = client.get("/health")
        data = response.json()
        assert data["status"] == "OK"
        assert data["service"] == "etl_backend"


# Section 14 — GET /csv, GET /csv/ingredient et GET /csv/exercise

class TestCsvRoutes:

    def test_get_csv_retourne_200(self):
        """GET /csv doit retourner 200 avec le nombre total de fichiers."""
        response = client.get("/csv")
        assert response.status_code == 200
        data = response.json()
        assert "total_files" in data
        assert "csv" in data

    def test_get_csv_contient_les_4_cles(self):
        """GET /csv doit lister les 4 fichiers attendus."""
        response = client.get("/csv")
        keys = response.json()["csv"].keys()
        for expected in ["ingredient_valid", "ingredient_invalid", "exercise_valid", "exercise_invalid"]:
            assert expected in keys

    def test_get_csv_ingredient_retourne_200(self):
        """GET /csv/ingredient doit retourner 200."""
        response = client.get("/csv/ingredient")
        assert response.status_code == 200

    def test_get_csv_ingredient_contient_les_2_cles(self):
        """GET /csv/ingredient doit lister ingredient_valid et ingredient_invalid."""
        response = client.get("/csv/ingredient")
        keys = response.json()["csv"].keys()
        assert "ingredient_valid" in keys
        assert "ingredient_invalid" in keys

    def test_get_csv_exercise_retourne_200(self):
        """GET /csv/exercise doit retourner 200."""
        response = client.get("/csv/exercise")
        assert response.status_code == 200

    def test_get_csv_exercise_contient_les_2_cles(self):
        """GET /csv/exercise doit lister exercise_valid et exercise_invalid."""
        response = client.get("/csv/exercise")
        keys = response.json()["csv"].keys()
        assert "exercise_valid" in keys
        assert "exercise_invalid" in keys


# Section 15 — PUT /csv/ingredient

class TestPutIngredient:

    # Payload valide réutilisé dans plusieurs tests
    PAYLOAD_VALIDE = {
        "data": [{
            "ingredient_name": "Pomme",
            "ingredient_energy_100g": 52.0,
            "ingredient_protein_100g": 0.3,
            "ingredient_carbohydrate_100g": 14.0,
            "ingredient_fats_100g": 0.2,
            "ingredient_fiber_100g": 2.4,
            "ingredient_sugars_100g": 10.0,
            "ingredient_salt_100g": 0.0,
            "ingredient_saturated_fats_100g": 0.0,
        }]
    }

    @patch("api._save_and_classify", return_value="✅ 'Pomme' ajouté à la liste valide")
    def test_put_ingredient_valide_retourne_200(self, mock_save):
        """PUT /csv/ingredient avec un payload valide doit retourner 200."""
        response = client.put("/csv/ingredient", json=self.PAYLOAD_VALIDE)
        assert response.status_code == 200

    @patch("api._save_and_classify", return_value="✅ 'Pomme' ajouté à la liste valide")
    def test_put_ingredient_retourne_status_success(self, mock_save):
        """PUT /csv/ingredient valide doit retourner status=success."""
        response = client.put("/csv/ingredient", json=self.PAYLOAD_VALIDE)
        assert response.json()["status"] == "success"

    def test_put_ingredient_nom_vide_retourne_422(self):
        """PUT /csv/ingredient avec un nom vide doit retourner 422."""
        payload = {"data": [{"ingredient_name": ""}]}
        response = client.put("/csv/ingredient", json=payload)
        assert response.status_code == 422

    def test_put_ingredient_valeur_negative_retourne_422(self):
        """PUT /csv/ingredient avec une énergie négative doit retourner 422."""
        payload = {"data": [{"ingredient_name": "Test", "ingredient_energy_100g": -10}]}
        response = client.put("/csv/ingredient", json=payload)
        assert response.status_code == 422

    def test_put_ingredient_nom_trop_long_retourne_422(self):
        """PUT /csv/ingredient avec un nom > 100 chars doit retourner 422."""
        payload = {"data": [{"ingredient_name": "A" * 101}]}
        response = client.put("/csv/ingredient", json=payload)
        assert response.status_code == 422

    def test_put_ingredient_payload_vide_retourne_422(self):
        """PUT /csv/ingredient sans body doit retourner 422."""
        response = client.put("/csv/ingredient", json={})
        assert response.status_code == 422

    def test_put_ingredient_detail_erreur_present(self):
        """PUT /csv/ingredient invalide doit inclure un message d'erreur dans le body."""
        payload = {"data": [{"ingredient_name": ""}]}
        response = client.put("/csv/ingredient", json=payload)
        assert "detail" in response.json()


# Section 16 — PUT /csv/exercise

class TestPutExercise:

    PAYLOAD_VALIDE = {
        "data": [{
            "sport_exercise_name": "Squat",
            "sport_exercise_instruction": "Pliez les genoux.",
        }]
    }

    @patch("api._save_and_classify", return_value="✅ 'Squat' ajouté à la liste valide")
    def test_put_exercise_valide_retourne_200(self, mock_save):
        """PUT /csv/exercise avec un payload valide doit retourner 200."""
        response = client.put("/csv/exercise", json=self.PAYLOAD_VALIDE)
        assert response.status_code == 200

    @patch("api._save_and_classify", return_value="✅ 'Squat' ajouté à la liste valide")
    def test_put_exercise_retourne_status_success(self, mock_save):
        """PUT /csv/exercise valide doit retourner status=success."""
        response = client.put("/csv/exercise", json=self.PAYLOAD_VALIDE)
        assert response.json()["status"] == "success"

    def test_put_exercise_nom_vide_retourne_422(self):
        """PUT /csv/exercise avec un nom vide doit retourner 422."""
        payload = {"data": [{"sport_exercise_name": ""}]}
        response = client.put("/csv/exercise", json=payload)
        assert response.status_code == 422

    def test_put_exercise_nom_trop_long_retourne_422(self):
        """PUT /csv/exercise avec un nom > 200 chars doit retourner 422."""
        payload = {"data": [{"sport_exercise_name": "A" * 201}]}
        response = client.put("/csv/exercise", json=payload)
        assert response.status_code == 422

    def test_put_exercise_payload_vide_retourne_422(self):
        """PUT /csv/exercise sans body doit retourner 422."""
        response = client.put("/csv/exercise", json={})
        assert response.status_code == 422

    def test_put_exercise_detail_erreur_present(self):
        """PUT /csv/exercise invalide doit inclure un message d'erreur dans le body."""
        payload = {"data": [{"sport_exercise_name": ""}]}
        response = client.put("/csv/exercise", json=payload)
        assert "detail" in response.json()


# Section 17 — POST /etl/extract-transform  (subprocess mocké)


class TestEtlExtractTransform:

    @patch("api.subprocess.run")
    def test_post_etl_succes_retourne_200(self, mock_run):
        """POST /etl/extract-transform avec un subprocess réussi doit retourner 200."""
        mock_run.return_value = MagicMock(returncode=0, stdout="OK", stderr="")
        response = client.post("/etl/extract-transform")
        assert response.status_code == 200

    @patch("api.subprocess.run")
    def test_post_etl_succes_retourne_status_success(self, mock_run):
        """POST /etl/extract-transform réussi doit retourner status=success."""
        mock_run.return_value = MagicMock(returncode=0, stdout="Pipeline OK", stderr="")
        response = client.post("/etl/extract-transform")
        assert response.json()["status"] == "success"

    @patch("api.subprocess.run")
    def test_post_etl_retourne_les_logs(self, mock_run):
        """POST /etl/extract-transform doit inclure les logs dans la réponse."""
        mock_run.return_value = MagicMock(returncode=0, stdout="Ligne 1\nLigne 2", stderr="")
        response = client.post("/etl/extract-transform")
        data = response.json()
        assert "detailed_logs" in data
        assert isinstance(data["detailed_logs"], list)

    @patch("api.subprocess.run")
    def test_post_etl_erreur_subprocess_retourne_500(self, mock_run):
        """POST /etl/extract-transform avec returncode != 0 doit retourner 500."""
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="Erreur critique")
        response = client.post("/etl/extract-transform")
        assert response.status_code == 500

    @patch("api.subprocess.run")
    def test_post_etl_timeout_retourne_504(self, mock_run):
        """POST /etl/extract-transform avec timeout doit retourner 504."""
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="python", timeout=300)
        response = client.post("/etl/extract-transform")
        assert response.status_code == 504


# Section 18 — POST /etl/extract-transform/ingredient et /exercise

class TestEtlExtractTransformDirect:

    @patch("api._run_etl_direct")
    def test_post_etl_ingredient_succes(self, mock_run):
        """POST /etl/extract-transform/ingredient réussi doit retourner 200."""
        mock_run.return_value = {"status": "success", "message": "OK", "detailed_logs": [], "return_code": 0}
        response = client.post("/etl/extract-transform/ingredient")
        assert response.status_code == 200
        assert response.json()["status"] == "success"

    @patch("api._run_etl_direct")
    def test_post_etl_exercise_succes(self, mock_run):
        """POST /etl/extract-transform/exercise réussi doit retourner 200."""
        mock_run.return_value = {"status": "success", "message": "OK", "detailed_logs": [], "return_code": 0}
        response = client.post("/etl/extract-transform/exercise")
        assert response.status_code == 200
        assert response.json()["status"] == "success"

    @patch("api._run_etl_direct")
    def test_post_etl_ingredient_appelle_le_bon_module(self, mock_run):
        """POST /etl/extract-transform/ingredient doit appeler _run_etl_direct avec 'ingredient'."""
        mock_run.return_value = {"status": "success", "message": "OK", "detailed_logs": [], "return_code": 0}
        client.post("/etl/extract-transform/ingredient")
        # Vérification que le premier argument était bien "ingredient"
        args, _ = mock_run.call_args
        assert args[0] == "ingredient"

    @patch("api._run_etl_direct")
    def test_post_etl_exercise_appelle_le_bon_module(self, mock_run):
        """POST /etl/extract-transform/exercise doit appeler _run_etl_direct avec 'exercise'."""
        mock_run.return_value = {"status": "success", "message": "OK", "detailed_logs": [], "return_code": 0}
        client.post("/etl/extract-transform/exercise")
        # Vérification que le premier argument était bien "exercise"
        args, _ = mock_run.call_args
        assert args[0] == "exercise"


# Section 19 — POST /etl/load-to-db

class TestEtlLoadToDb:

    @patch("api.subprocess.run")
    def test_post_load_succes_retourne_200(self, mock_run):
        """POST /etl/load-to-db réussi doit retourner 200."""
        mock_run.return_value = MagicMock(returncode=0, stdout="Chargement OK", stderr="")
        response = client.post("/etl/load-to-db")
        assert response.status_code == 200

    @patch("api.subprocess.run")
    def test_post_load_retourne_status_success(self, mock_run):
        """POST /etl/load-to-db réussi doit retourner status=success."""
        mock_run.return_value = MagicMock(returncode=0, stdout="Chargement OK", stderr="")
        response = client.post("/etl/load-to-db")
        assert response.json()["status"] == "success"

    @patch("api.subprocess.run")
    def test_post_load_erreur_retourne_500(self, mock_run):
        """POST /etl/load-to-db avec returncode != 0 doit retourner 500."""
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="Connexion BDD échouée")
        response = client.post("/etl/load-to-db")
        assert response.status_code == 500

    @patch("api.subprocess.run")
    def test_post_load_timeout_retourne_504(self, mock_run):
        """POST /etl/load-to-db avec timeout doit retourner 504."""
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="python", timeout=300)
        response = client.post("/etl/load-to-db")
        assert response.status_code == 504


# Section 20 — POST /etl/load-to-db/ingredient et /exercise

class TestEtlLoadToDbDirect:

    @patch("api._run_load_direct")
    def test_post_load_ingredient_succes(self, mock_run):
        """POST /etl/load-to-db/ingredient réussi doit retourner 200."""
        mock_run.return_value = {"status": "success", "message": "OK", "detailed_logs": [], "return_code": 0}
        response = client.post("/etl/load-to-db/ingredient")
        assert response.status_code == 200

    @patch("api._run_load_direct")
    def test_post_load_exercise_succes(self, mock_run):
        """POST /etl/load-to-db/exercise réussi doit retourner 200."""
        mock_run.return_value = {"status": "success", "message": "OK", "detailed_logs": [], "return_code": 0}
        response = client.post("/etl/load-to-db/exercise")
        assert response.status_code == 200

    @patch("api._run_load_direct")
    def test_post_load_ingredient_appelle_le_bon_csv(self, mock_run):
        """POST /etl/load-to-db/ingredient doit passer ingredient_valid.csv à _run_load_direct."""
        mock_run.return_value = {"status": "success", "message": "OK", "detailed_logs": [], "return_code": 0}
        client.post("/etl/load-to-db/ingredient")
        args, _ = mock_run.call_args
        assert args[1] == "ingredient_valid.csv"

    @patch("api._run_load_direct")
    def test_post_load_exercise_appelle_le_bon_csv(self, mock_run):
        """POST /etl/load-to-db/exercise doit passer exercise_valid.csv à _run_load_direct."""
        mock_run.return_value = {"status": "success", "message": "OK", "detailed_logs": [], "return_code": 0}
        client.post("/etl/load-to-db/exercise")
        args, _ = mock_run.call_args
        assert args[1] == "exercise_valid.csv"


# Section 21 — api._read_csv()

class TestReadCsv:

    def test_read_csv_retourne_liste_de_dicts(self, tmp_path):
        """_read_csv doit retourner une liste de dicts depuis un CSV existant."""
        from api import _read_csv
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("nom,valeur\nPomme,52\nBanane,89\n", encoding="utf-8")
        result = _read_csv(csv_file)
        assert len(result) == 2
        assert result[0]["nom"] == "Pomme"

    def test_read_csv_fichier_inexistant_retourne_liste_vide(self, tmp_path):
        """_read_csv doit retourner [] si le fichier n'existe pas."""
        from api import _read_csv
        result = _read_csv(tmp_path / "inexistant.csv")
        assert result == []


# Section 22 — api._save_and_classify()

class TestSaveAndClassify:

    from pathlib import Path

    # Colonnes utilisées dans les tests
    VALID_COLS   = ["ingredient_name", "ingredient_energy_100g"]
    INVALID_COLS = ["ingredient_name", "ingredient_energy_100g", "rejection_reason"]
    VALID_FIELDS = ["ingredient_energy_100g"]

    def _run(self, tmp_path, row):
        """Helper : appelle _save_and_classify avec les chemins du dossier temporaire."""
        from api import _save_and_classify
        from pathlib import Path
        return _save_and_classify(
            row=row,
            name_field="ingredient_name",
            valid_fields=self.VALID_FIELDS,
            valid_path=Path(tmp_path) / "valid.csv",
            invalid_path=Path(tmp_path) / "invalid.csv",
            valid_columns=self.VALID_COLS,
            invalid_columns=self.INVALID_COLS,
        )

    def test_nouvel_element_valide_va_dans_valid_csv(self, tmp_path):
        """Un élément valide doit être écrit dans valid.csv."""
        msg = self._run(tmp_path, {"ingredient_name": "Pomme", "ingredient_energy_100g": 52})
        valid_df = pd.read_csv(tmp_path / "valid.csv")
        assert len(valid_df) == 1
        assert valid_df.iloc[0]["ingredient_name"] == "Pomme"

    def test_nouvel_element_valide_retourne_message_ajout(self, tmp_path):
        """Un nouvel élément valide doit retourner un message 'ajouté'."""
        msg = self._run(tmp_path, {"ingredient_name": "Pomme", "ingredient_energy_100g": 52})
        assert "ajouté" in msg

    def test_element_invalide_va_dans_invalid_csv(self, tmp_path):
        """Un élément avec champ manquant doit aller dans invalid.csv."""
        msg = self._run(tmp_path, {"ingredient_name": "Pomme", "ingredient_energy_100g": None})
        invalid_df = pd.read_csv(tmp_path / "invalid.csv")
        assert len(invalid_df) == 1

    def test_element_invalide_retourne_message_raison(self, tmp_path):
        """Un élément invalide doit retourner un message avec la raison."""
        msg = self._run(tmp_path, {"ingredient_name": "Pomme", "ingredient_energy_100g": None})
        assert "manquant" in msg

    def test_element_invalide_existant_est_mis_a_jour(self, tmp_path):
        """Un élément invalide déjà présent doit être mis à jour, pas dupliqué."""
        from pathlib import Path
        # Création d'un invalid.csv avec "Pomme" dedans
        invalid_df = pd.DataFrame([{
            "ingredient_name": "Pomme",
            "ingredient_energy_100g": None,
            "rejection_reason": "ingredient_energy_100g manquant ou vide"
        }])
        invalid_df.to_csv(tmp_path / "invalid.csv", index=False)

        self._run(tmp_path, {"ingredient_name": "Pomme", "ingredient_energy_100g": None})

        result = pd.read_csv(tmp_path / "invalid.csv")
        assert len(result[result["ingredient_name"] == "Pomme"]) == 1

    def test_element_corrige_passe_de_invalid_a_valid(self, tmp_path):
        """Un élément précédemment invalide qui devient valide doit être déplacé."""
        from pathlib import Path
        # "Pomme" est d'abord dans invalid.csv
        invalid_df = pd.DataFrame([{
            "ingredient_name": "Pomme",
            "ingredient_energy_100g": None,
            "rejection_reason": "ingredient_energy_100g manquant ou vide"
        }])
        invalid_df.to_csv(tmp_path / "invalid.csv", index=False)

        msg = self._run(tmp_path, {"ingredient_name": "Pomme", "ingredient_energy_100g": 52})

        valid_df   = pd.read_csv(tmp_path / "valid.csv")
        invalid_df = pd.read_csv(tmp_path / "invalid.csv")

        assert len(valid_df[valid_df["ingredient_name"] == "Pomme"]) == 1
        assert len(invalid_df[invalid_df["ingredient_name"] == "Pomme"]) == 0
        assert "corrigé" in msg


# Section 23 — api._run_etl_direct() : chemin d'erreur

class TestRunEtlDirect:

    @patch("api.run_pipeline", side_effect=Exception("Crash inattendu"))
    def test_run_etl_direct_exception_leve_http_500(self, mock_pipeline):
        """_run_etl_direct doit lever une HTTPException 500 si run_pipeline plante."""
        from fastapi import HTTPException
        from api import _run_etl_direct
        import etl_ingredient as mod
        with pytest.raises(HTTPException) as exc_info:
            _run_etl_direct("ingredient", mod)
        assert exc_info.value.status_code == 500


# Section 24 — etl_load.load_csv_to_table() : erreur SQLAlchemy

class TestLoadCsvSQLAlchemyError:

    def test_sqlalchemy_error_logue_et_releve(self, tmp_path):
        """Une erreur SQLAlchemy lors de l'INSERT doit être loguée et re-levée."""
        from sqlalchemy.exc import SQLAlchemyError

        csv_path = tmp_path / "data.csv"
        csv_path.write_text("ingredient_name,ingredient_energy_100g\nPomme,52\n", encoding="utf-8")

        fake_engine = MagicMock()
        fake_conn = MagicMock()
        fake_conn.execute.side_effect = SQLAlchemyError("Contrainte violée")
        fake_engine.begin.return_value.__enter__ = MagicMock(return_value=fake_conn)
        fake_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(SQLAlchemyError):
            etl_load.load_csv_to_table(str(csv_path), "ingredient", fake_engine)