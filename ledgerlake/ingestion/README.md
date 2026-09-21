# ingestion/ — Producers Kafka (Palier A.1) + writer Bronze (Palier A.2)

Simule les événements financiers d'une banque et les publie dans Kafka.
Le bronze_writer.py consomme ces topics et écrit brut dans le data lake (bronze/).

## Contrats et données liées

Les contrats JSON Schema versionnés se trouvent dans `contracts/`. Les trois
producers reconstruisent le même registre clients à partir de `EVENT_SEED` et
`CUSTOMER_POPULATION`. Les transactions et crédits référencent donc des clients
existants, et les mises à jour profil réutilisent leur identifiant.

Chaque événement porte `event_id`, `event_type`, `schema_version`,
`event_timestamp` et `producer`. Les événements valides sont vérifiés contre
leur contrat avant publication. Les crédits suivent un cycle ordonné et gardent
le même `loan_id`.

Exemples de runs finis et reproductibles depuis la racine du dépôt :

```bash
python ledgerlake/ingestion/producers/customer_producer.py --count 2000 --customers 1000 --seed 42
python ledgerlake/ingestion/producers/transaction_producer.py --count 5000 --customers 1000 --seed 42
python ledgerlake/ingestion/producers/loan_producer.py --count 500 --customers 1000 --seed 42
```

`--count 0` conserve le mode continu. Le producer transactions accepte les
scénarios `valid`, `duplicate`, `invalid-amount`, `missing-customer`,
`late-event` et `conflicting-event`. Les deux scénarios invalides contournent
volontairement la validation uniquement lorsqu'ils sont demandés explicitement.
Ils servent aux tests de qualité en aval.

Tests des contrats, relations, cycles et erreurs de publication :

```bash
python -B -m unittest ledgerlake/ingestion/tests/test_event_factory.py -v
```

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
