#!/bin/bash
set -euo pipefail

# Optional: set CLOUDFLARE_ACCOUNT_ID in the environment to force a specific account.
# If unset, wrangler will use the default account associated with your login/token.

# Ensure dist exists and is populated
mkdir -p dist
cp visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html dist/index.html
cp visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html dist/spec_evolution.html
cp spec_evolution_v1.sqlite3 dist/
cp spec_evolution_v1.sqlite3.config.json dist/
cp og-image.png dist/
cp twitter-image.png dist/
cp frankensqlite_illustration.webp dist/
cp frankensqlite_diagram.webp dist/

npx wrangler pages deploy dist --project-name frankensqlite-spec-evolution --branch main --commit-dirty=true
