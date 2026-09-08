# T-Trader

Run t_order_manager.py in Docker on a remote server.

## Server prerequisites
- Docker with the compose plugin, and network egress to PyPI, the T-Bank package
  index (`opensource.tbank.ru`), and the sandbox API.
- The repo cloned onto the server, with a `.env` file present in the repo root
  (provides `T_INVEST_TOKEN_SANDBOX`). `.env` and `invest_token` are gitignored
  and must be copied onto the server manually — never commit or bake them in.

## Start
```sh
docker compose up -d --build        # build + start detached
docker compose attach               # dashboard + command console; Ctrl-C detaches only
```
Control-C in `attach` detaches without stopping the container (`restart: unless-stopped` keeps it up). Use `docker compose logs -f` to watch, and `docker compose stop` / `docker compose down` to actually stop it.

## Config & state
The repo dir is bind-mounted at `/app`, mirroring a local run:
- `ticker_settings.json` — edit on the host, then use the `reload` command inside the console.
- `t_trader.log` (rotated at 10 MB) and `ticker_figi_cache.txt` are written back into the repo dir on the host.
- Timezone is `Europe/Moscow` (MOEX session clock) via `TZ`.

## Notes
- Rebuild (`docker compose up -d --build`) only when `pyproject.toml` / `poetry.lock` change; code and settings come from the bind mount.
- The dashboard repaints at 5 fps, so `docker compose logs` is noisy; the durable record is the host `t_trader.log`.
- Running as root keeps bind-mount writes trivial. To harden, run as a non-root user (e.g. `user: "1000:1000"` in the service) with the repo owned by that UID.
- On a non-amd64 server add `platform: linux/amd64` to the service or verify arm64 wheels build.
