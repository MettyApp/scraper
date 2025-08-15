import glob
import json
import os
import pandas as pd
import spacy
from spacy.tokens import DocBin
from spacy.training import Example
from sklearn.model_selection import train_test_split


class DataPreparator:
    def __init__(self):
        self.rules = None

    def load_rules(self):
        """Charge les règles de classification depuis les fichiers JSON"""
        self.rules = {
            e.split("/")[-1].removesuffix(".json"): self.load_rule(e)
            for e in glob.glob("classifier/rules/*.json")
        }

    def load_rule(self, filename):
        """Charge une règle depuis un fichier JSON"""
        with open(filename, "r") as f:
            return json.load(f)

    def get_category_from_rules(self, product, rule):
        """Applique les règles pour déterminer la catégorie d'un produit"""
        if not rule or (
            rule.get("ignore_if_empty_categories", False)
            and len(product.get("categories", [])) == 0
        ):
            return None
        elif (
            len(
                [
                    v
                    for v in rule.get("ignore", [])
                    if v in product.get("categories", [])
                ]
            )
            > 0
        ):
            return None
        else:
            for candidate in rule.get("categories", []):
                name = candidate[0]
                keywords = candidate[1]
                if len([v for v in keywords if v in product.get("categories", [])]) > 0:
                    return name
        return None

    def featurize(self, product):
        """Crée le texte de features à partir d'un produit"""
        return " ".join(product["categories"] + [product["title"]])

    def prepare_training_data(self):
        """Prépare les données d'entraînement depuis items.json"""
        # Charger les règles
        self.load_rules()

        # Charger les données
        train_df = pd.read_json("items.json")

        # Appliquer les règles pour obtenir les catégories
        train_df["product_category"] = train_df.apply(
            lambda x: self.get_category_from_rules(x, self.rules.get(x["host"], {})),
            axis=1,
        )

        # Supprimer les lignes sans catégorie
        train_df.dropna(subset=["product_category"], inplace=True)

        print("== Statistiques des catégories ==")
        print(train_df.value_counts("product_category"))
        print("\n== Statistiques des hôtes ==")
        print(train_df.value_counts("host"))

        return train_df

    def create_spacy_data(self, df, output_dir="./data"):
        """Convertit les données en format spaCy et les sauvegarde"""
        # Créer le répertoire de sortie
        os.makedirs(output_dir, exist_ok=True)

        # Préparer les données
        texts = df.apply(lambda row: self.featurize(row), axis=1).values.tolist()
        categories = df["product_category"].values.tolist()

        # Obtenir toutes les catégories uniques
        unique_categories = set(categories)
        print(f"\nCatégories détectées: {sorted(unique_categories)}")

        # Créer les données d'entraînement
        training_data = []
        for text, category in zip(texts, categories):
            cats = {cat: cat == category for cat in unique_categories}
            training_data.append((text, {"cats": cats}))

        # Diviser train/dev
        train_data, dev_data = train_test_split(
            training_data, test_size=0.2, random_state=42, stratify=categories
        )

        print(f"Données d'entraînement: {len(train_data)}")
        print(f"Données de validation: {len(dev_data)}")

        # Créer un modèle spaCy temporaire pour la conversion
        try:
            nlp = spacy.load("fr_core_news_sm")
        except OSError:
            try:
                nlp = spacy.load("en_core_web_sm")
            except OSError:
                nlp = spacy.blank("fr")

        # Sauvegarder les données d'entraînement
        self._save_spacy_data(nlp, train_data, f"{output_dir}/train.spacy")
        self._save_spacy_data(nlp, dev_data, f"{output_dir}/dev.spacy")

        # Sauvegarder la liste des catégories
        with open(f"{output_dir}/categories.json", "w") as f:
            json.dump(sorted(unique_categories), f, indent=2)

        print(f"Données sauvegardées dans {output_dir}/")

    def _save_spacy_data(self, nlp, data, filename):
        """Sauvegarde les données au format spaCy"""
        db = DocBin()

        for text, annotations in data:
            doc = nlp.make_doc(text)
            example = Example.from_dict(doc, annotations)
            db.add(example.reference)

        db.to_disk(filename)


if __name__ == "__main__":
    preparator = DataPreparator()
    df = preparator.prepare_training_data()
    preparator.create_spacy_data(df)
    print("\n✅ Préparation des données terminée!")
    print("Vous pouvez maintenant entraîner le modèle avec:")
    print(
        "python -m spacy train spacy.cfg --output ./models --paths.train ./data/train.spacy --paths.dev ./data/dev.spacy"
    )
