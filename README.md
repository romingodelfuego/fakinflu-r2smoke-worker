# fakinflu-r2smoke-worker

Worker RunPod Serverless **CPU, sans modèle** qui sert aux smoke tests de la Phase 0 du plan
« Motion » : URL pré-signées Cloudflare R2, événements `/stream` en direct, repli en morceaux
de 4 Mo, parallélisme, reprise via `outputs.json`.

- `transfer.py` : bibliothèque de transfert **canonique** (stdlib). Chaque worker du plan
  Motion en embarque une copie identique (`make transfer-lib-check`).
- `handler.py` : handler générateur (`return_aggregate_stream = False`), modes
  `roundtrip | preview | fallback | sleep`.
- Le worker n'a **aucune clé R2** : il ne reçoit que des URL pré-signées.

Test local sans RunPod ni R2 (repli en morceaux) :

```bash
cd r2smoke/serverless && python3 handler.py --test_input "$(cat test_input.json)"
```

Côté client : `make smoke-r2 T=a|b|c|d|e` (voir `app/README.md`).

## Licences
Code seul, aucun modèle tiers.
