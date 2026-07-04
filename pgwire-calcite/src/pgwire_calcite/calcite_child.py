# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Runnable Calcite JVM sidecar child (PGW-033).

``python -m pgwire_calcite.calcite_child --model M --port P`` boots JPype+Calcite
in this process and serves the Arrow-IPC socket bridge (see sidecar.py). The
supervisor (Phase 5) runs this as a recyclable child with its own heap; the
pgwire process reaches it via ``BridgeBackend``.
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pgwire-calcite-child", description=__doc__)
    parser.add_argument("--model", default=None, help="Calcite model JSON path")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5533)
    parser.add_argument("--lex", default="ORACLE")
    parser.add_argument("--fun", default="standard")
    parser.add_argument("--schema", default=None)
    parser.add_argument("--xmx", default=None, help="JVM max heap, e.g. 2g")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    from pgwire_calcite.calcite_backend import CalciteBackend
    from pgwire_calcite.sidecar import serve_calcite_child

    jvm_args = [f"-Xmx{args.xmx}"] if args.xmx else []
    backend = CalciteBackend(
        model_path=args.model,
        lex=args.lex,
        fun=args.fun,
        default_schema=args.schema,
        jvm_args=jvm_args,
    )
    serve_calcite_child(backend, host=args.host, port=args.port)
    logging.getLogger(__name__).info(
        "Calcite child ready on %s:%d (model=%s)", args.host, args.port, args.model
    )
    # Print a machine-readable ready marker so a supervisor/readiness probe can gate.
    print(f"CALCITE_CHILD_READY {args.host}:{args.port}", flush=True)
    threading.Event().wait()
    return 0


if __name__ == "__main__":
    sys.exit(main())
