FROM python:3.11-slim AS builder

WORKDIR /build
COPY pyproject.toml README.md LICENSE ./
COPY src/ src/

RUN pip install --no-cache-dir hatch && hatch build

FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /build/dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

EXPOSE 8080 9090

ENTRYPOINT ["rfobserver"]
CMD ["run"]
