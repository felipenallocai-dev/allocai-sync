name: AllocAI Sync

on:
  schedule:
    - cron: '*/15 * * * *'
  workflow_dispatch:

jobs:
  sync:
    runs-on: ubuntu-latest

    env:
      SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
      SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
      CIDNEFRO_URL: ${{ secrets.CIDNEFRO_URL }}
      CIDNEFRO_USER: ${{ secrets.CIDNEFRO_USER }}
      CIDNEFRO_PASS: ${{ secrets.CIDNEFRO_PASS }}

    steps:
      - name: Checkout código
        uses: actions/checkout@v4

      - name: Instalar Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Instalar dependências
        run: |
          pip install supabase==2.15.0 python-dotenv playwright
          playwright install chromium --with-deps

      - name: Rodar sync
        run: python sync.py
