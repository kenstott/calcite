# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Compiler-model shims for the copied catalog intercept.

provisa's ``catalog.py`` was written against provisa's compilation model
(``provisa.compiler.naming`` / ``provisa.compiler.sql_gen`` and its
CompilationContext/TableMeta). pgwire-calcite reuses ``catalog.py`` verbatim but
feeds it a Calcite-metadata-derived model, so this package provides the minimal
compatible types + naming helpers the catalog expects. See catalog_populate.py
for the JDBC-metadata -> CompilationContext builder.
"""
