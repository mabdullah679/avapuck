#!/usr/bin/env bash
# Install Docker Engine + the Compose plugin on Ubuntu, from Docker's own
# repository (Ubuntu's `docker.io` package ships an older Engine and does NOT
# include the `docker compose` plugin this project needs).
#
#   ./scripts/install-prereqs-ubuntu.sh
#
# Needs sudo. Run once per machine, then log out and back in so your new
# docker group membership takes effect, then run ./setup.sh from the repo root.
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

[ -f /etc/os-release ] || { echo "not a Linux with /etc/os-release" >&2; exit 1; }
. /etc/os-release
case "${ID:-}" in
  ubuntu|debian) ;;
  *) echo "This helper targets Ubuntu/Debian; found '${ID:-unknown}'." >&2
     echo "Install Docker Engine + Compose plugin manually:" >&2
     echo "  https://docs.docker.com/engine/install/" >&2
     exit 1 ;;
esac

if docker compose version >/dev/null 2>&1; then
  echo "Docker and the Compose plugin are already installed."
else
  echo "==> Installing Docker Engine and the Compose plugin"
  sudo apt-get update
  sudo apt-get install -y ca-certificates curl gnupg

  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL "https://download.docker.com/linux/${ID}/gpg" \
    | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  sudo chmod a+r /etc/apt/keyrings/docker.gpg

  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/${ID} ${VERSION_CODENAME} stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null

  sudo apt-get update
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io \
                          docker-buildx-plugin docker-compose-plugin
fi

sudo systemctl enable --now docker

# Running docker without sudo. Without this every command needs sudo, and a
# stack started as root writes root-owned files into ./data.
if ! id -nG "$USER" | tr ' ' '\n' | grep -qx docker; then
  echo "==> Adding $USER to the docker group"
  sudo usermod -aG docker "$USER"
  echo
  echo "  LOG OUT AND BACK IN (or run: newgrp docker) before continuing,"
  echo "  otherwise docker commands will fail with a permission error."
fi

echo
echo "Docker:  $(sudo docker version --format '{{.Server.Version}}' 2>/dev/null || echo installed)"
echo "Compose: $(sudo docker compose version --short 2>/dev/null || echo installed)"
echo
echo "Next:  ./setup.sh"
