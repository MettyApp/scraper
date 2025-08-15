import json
import os
import spacy
from urllib.parse import urlparse


class ProductClassifier:
    def __init__(self, model_path=None):
        """
        Initialise le classificateur avec un modèle pré-entraîné

        Args:
            model_path: Chemin vers le modèle spaCy entraîné
        """
        self.model_path = model_path or os.environ.get(
            "PRODUCT_MODEL_PATH", "./models/model-best"
        )

        self.nlp = None

        self._load_model()

    def _load_model(self):
        """Charge le modèle spaCy"""
        if os.path.exists(self.model_path):
            try:
                self.nlp = spacy.load(self.model_path)
            except Exception as e:
                print(f"Erreur lors du chargement du modèle: {e}")
                self.nlp = None
        else:
            print(f"Modèle non trouvé: {self.model_path}")
            self.nlp = None

    def featurize(self, product):
        """Crée le texte de features à partir d'un produit"""
        return " ".join(product.get("categories", []) + [product.get("title", "")])

    def predict(self, products):
        """
        Prédit les catégories pour une liste de produits

        Args:
            products: Liste de dictionnaires représentant les produits

        Returns:
            Liste des catégories prédites
        """
        if not self.nlp or "textcat" not in self.nlp.pipe_names:
            return ["_unknown" for _ in products]

        predictions = []
        for product in products:
            text = self.featurize(product)
            doc = self.nlp(text)

            if doc.cats:
                predicted_category = max(doc.cats, key=doc.cats.get)
                predictions.append(predicted_category)
            else:
                predictions.append("_unknown")

        return predictions

    def predict_with_confidence(self, products, threshold=0.5):
        """
        Prédit avec scores de confiance

        Args:
            products: Liste de produits
            threshold: Seuil de confiance minimum

        Returns:
            Liste de tuples (catégorie, confiance)
        """
        if not self.nlp or "textcat" not in self.nlp.pipe_names:
            return [("_unknown", 0.0) for _ in products]

        results = []
        for product in products:
            text = self.featurize(product)
            doc = self.nlp(text)

            if doc.cats:
                best_category = max(doc.cats, key=doc.cats.get)
                confidence = doc.cats[best_category]

                if confidence >= threshold:
                    results.append((best_category, confidence))
                else:
                    results.append(("_unknown", confidence))
            else:
                results.append(("_unknown", 0.0))

        return results

    def predict_all_scores(self, products):
        """
        Retourne tous les scores pour chaque catégorie

        Args:
            products: Liste de produits

        Returns:
            Liste de dictionnaires {catégorie: score}
        """
        if not self.nlp or "textcat" not in self.nlp.pipe_names:
            return [{"_unknown": 1.0} for _ in products]

        results = []
        for product in products:
            text = self.featurize(product)
            doc = self.nlp(text)
            results.append(dict(doc.cats) if doc.cats else {"_unknown": 1.0})

        return results


if __name__ == "__main__":
    import sys
    import pandas as pd

    classifier = ProductClassifier()

    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            if arg.startswith("https://"):
                # Prédire pour tous les produits d'un site
                df = pd.read_json("items.json")
                df = df[df["host"] == urlparse(arg).hostname]
                products = [json.loads(row.to_json()) for _, row in df.iterrows()]
                predictions = classifier.predict(products)
                df["predicted_category"] = predictions
                print(df[["title", "categories", "predicted_category"]])
            else:
                # Prédire pour une catégorie donnée
                product = {"categories": [arg], "title": ""}
                prediction = classifier.predict([product])[0]
                confidence = classifier.predict_with_confidence([product])[0]
                all_scores = classifier.predict_all_scores([product])[0]

                print(f"Produit: {arg}")
                print(f"Prédiction: {prediction}")
                print(f"Confiance: {confidence[1]:.3f}")
                print("Tous les scores:")
                for cat, score in sorted(
                    all_scores.items(), key=lambda x: x[1], reverse=True
                ):
                    print(f"  {cat}: {score:.3f}")
    else:
        print("Usage:")
        print("  python product_classifier.py <category>")
        print("  python product_classifier.py https://example.com")
