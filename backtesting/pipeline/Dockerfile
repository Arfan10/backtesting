# ── Build stage ───────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# Install deps into a venv inside the image
RUN python -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# Copy venv from builder (keeps final image small)
COPY --from=builder /app/venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# Copy application code
COPY config.py   .
COPY pipeline.py .

# Don't run as root
RUN useradd --no-create-home --shell /bin/false appuser
USER appuser

# All config is injected via environment variables at runtime
ENV DELTA_API_KEY=""
ENV DELTA_API_SECRET=""
ENV POSTGRES_DSN=""

CMD ["python", "-u", "pipeline.py"]