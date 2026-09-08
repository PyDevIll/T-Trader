# syntax=docker/dockerfile:1
# t_order_manager.py runner (two-stage build).
#
# t-tech-investments is only published on the T-Bank index:
#   https://opensource.tbank.ru/api/v4/projects/238/packages/pypi/simple
# (the URL the official SDK page documents for `pip install --index-url ...`).
# That index is also declared in pyproject.toml [tool.poetry.source]. Stage 1
# turns the poetry.lock into a requirements file (poetry export already emits the
# --extra-index-url for it), and stage 2 pip-installs from that file. All runtime
# deps ship as wheels for linux/amd64 + arm64 on Python 3.12, so no compiler is
# needed and Poetry never ends up in the final image.

FROM python:3.12-slim AS lock-export

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHON_KEYRING_BACKEND=keyring.backends.null

WORKDIR /lock

RUN pip install "poetry==2.4.2" poetry-plugin-export

COPY pyproject.toml poetry.lock ./
RUN poetry export --without-hashes -f requirements.txt -o requirements.txt

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    # MOEX session/regime logic runs on the Moscow clock
    TZ=Europe/Moscow

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=lock-export /lock/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# The app keeps its config/state next to the working dir; secrets are excluded
# via .dockerignore and must be provided at runtime (.env / environment).
COPY . .

# Running the module as a plain script from the repo root keeps the intra-package
# top-level imports (from t_services import ...) resolvable and CWD-relative
# files (ticker_settings.json, t_trader.log, ticker_figi_cache.txt) in place.
CMD ["python", "src/t_trader/t_order_manager.py"]
