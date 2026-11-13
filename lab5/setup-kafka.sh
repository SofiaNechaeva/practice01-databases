#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Lab5 uses the shared Kafka Connect from Lab4.
bash lab4/setup-kafka.sh
