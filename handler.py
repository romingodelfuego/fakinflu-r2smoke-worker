#!/usr/bin/env python3
"""fakinflu — worker de smoke tests de la Phase 0 (plan motion 0.2). CPU, aucun modele.

Handler GENERATEUR : chaque `yield` est lisible en direct via GET /stream/<job_id>
(return_aggregate_stream = False, sinon RunPod recolle tout et on retombe sur la limite de taille).

params.mode :
  roundtrip  (a, e) lit l'entree "src" (URL GET pre-signee), produit "out" de out_bytes octets,
                    la depose via l'URL PUT + outputs.json ; delay_s avant la sortie pour (e)
  preview    (b)    progress x steps puis un apercu de preview_bytes octets (<= 1 Mio)
  fallback   (c)    produit "out" sans R2 -> morceaux de 1 Mio dans le stream
  sleep      (d)    occupe un worker seconds secondes (test de parallelisme)
"""
import hashlib, os, shutil, tempfile, time, traceback

import runpod

from transfer import Outputs, fetch_inputs, preview_event


def _blob(path, size, seed):
    """Contenu deterministe (le client peut recalculer l'empreinte attendue)."""
    block = hashlib.sha256(str(seed).encode()).digest() * (1 << 15)  # 1 Mio
    with open(path, "wb") as f:
        left = size
        while left > 0:
            f.write(block[:min(left, len(block))])
            left -= len(block)


def handler(job):
    inp = job.get("input") or {}
    params = inp.get("params") or {}
    mode = params.get("mode", "roundtrip")
    work = tempfile.mkdtemp(prefix="smoke_")
    t0 = time.time()
    try:
        outputs = Outputs(inp)
        paths, events = fetch_inputs(inp, os.path.join(work, "in"), outputs)
        yield from events
        if "src" in paths:
            yield {"type": "progress", "step": f"entree lue ({os.path.getsize(paths['src'])} octets)", "pct": 20}

        if mode == "preview":
            steps = int(params.get("steps", 5))
            for i in range(steps):
                time.sleep(float(params.get("step_s", 1)))
                yield {"type": "progress", "step": f"etape {i + 1}/{steps}", "pct": round(100 * (i + 1) / steps)}
            data = os.urandom(int(params.get("preview_bytes", 1_000_000)))
            yield preview_event(data, "application/octet-stream")
        elif mode == "sleep":
            secs = float(params.get("seconds", 60))
            end = time.time() + secs
            while time.time() < end:
                time.sleep(min(10, end - time.time()))
                yield {"type": "progress", "step": "occupe", "pct": round(100 * (1 - (end - time.time()) / secs))}
        else:  # roundtrip | fallback
            time.sleep(float(params.get("delay_s", 0)))
            out = os.path.join(work, "out.bin")
            _blob(out, int(params.get("out_bytes", 30_000_000)), params.get("seed", 0))
            yield {"type": "progress", "step": "sortie produite", "pct": 80}
            yield from outputs.emit("out", out, "out.bin")
            outputs.write_summary()
        yield {"type": "done", "seed": params.get("seed"), "timings": {"total_s": round(time.time() - t0, 2)}}
    except Exception as e:
        traceback.print_exc()
        yield {"type": "error", "message": f"{type(e).__name__}: {e}"}
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    runpod.serverless.start({"handler": handler, "return_aggregate_stream": False})
