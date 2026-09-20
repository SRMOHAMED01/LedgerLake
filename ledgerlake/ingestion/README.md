# ingestion/ — Producers Kafka (Palier A.1) + writer Bronze (Palier A.2)

Simule les événements financiers d'une banque et les publie dans Kafka.
Le bronze_writer.py consomme ces topics et écrit brut dans le data lake (bronze/).

## Garanties du writer Bronze

- Lots par topic et partition : 100 messages ou vidage toutes les 10 secondes.
- Les offsets Kafka sont validés de manière synchrone, uniquement après une
  écriture MinIO réussie, et uniquement pour la partition écrite.
- Les validations automatiques sont désactivées. Une erreur de stockage ou de
  commit arrête le processus avec une erreur ; aucun lot suivant n'est traité.
- Après rétablissement du service, relancer `python ledgerlake/ingestion/bronze_writer.py`
  depuis la racine du dépôt. Le même groupe Kafka reprend depuis ses offsets
  validés. Cette récupération dépend de la conservation des événements dans Kafka.
- Ctrl+C tente de persister les lots restants, puis ferme le consumer, même si
  le vidage échoue. Les erreurs inattendues ne déclenchent pas de nouveau vidage.
- Lors d'une réaffectation de partitions, les buffers concernés sont abandonnés
  sans commit : Kafka les remet à disposition du prochain consumer.

La garantie est **at-least-once**, pas exactly-once : une écriture réussie suivie
d'un crash ou d'un commit échoué peut être rejouée dans un nouvel objet Bronze.
La déduplication Silver reste nécessaire et n'est pas corrigée par cette étape.
Les payloads restent inchangés, séparés par un saut de ligne. Les tombstones
(valeur Kafka nulle) arrêtent le writer sans acknowledgment ; ils nécessitent
une convention de stockage avant de pouvoir être pris en charge.

## Tests hors ligne

Depuis la racine du dépôt :

```bash
python -B -m unittest discover -s ledgerlake/ingestion/tests -v
```

Ces tests simulent Kafka et MinIO : ordre écriture/commit, isolation des partitions,
pannes, replay après redémarrage, timeout, réaffectation et arrêt. Ils ne remplacent
pas un test d'intégration avec les services réels.
