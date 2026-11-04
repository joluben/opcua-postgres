# ============================================================================
# Makefile — Tareas de build, push y operación del conector OPC-UA.
#
# Variables configurables:
#   VERSION   — tag de imagen semántico   (por defecto: dev)
#   REGISTRY  — registro Docker privado   (por defecto: vacío → Docker Hub local)
#   SERVICE   — nombre del servicio compose a operar (por defecto: connector-01)
#
# Uso básico:
#   make build                    # construye imagen local con tag 'dev'
#   make build VERSION=1.3.0      # construye y etiqueta como 1.3.0
#   make push  VERSION=1.3.0 REGISTRY=registry.example.com
#   make up                       # levanta el stack de producción
#   make test                     # levanta el pipeline de test
# ============================================================================

VERSION  ?= dev
REGISTRY ?=
IMAGE    := opcua-connector

# Si REGISTRY está definido, prefijamos el nombre completo
ifneq ($(REGISTRY),)
FULL_IMAGE := $(REGISTRY)/$(IMAGE):$(VERSION)
else
FULL_IMAGE := $(IMAGE):$(VERSION)
endif

.PHONY: build push run up down test test-down acceptance monitoring-up monitoring-down logs ps clean help

## build — construye la imagen con ARG VERSION incrustado en los LABEL OCI
build:
	docker build \
		--build-arg VERSION=$(VERSION) \
		-t $(IMAGE):$(VERSION) \
		-t $(IMAGE):latest \
		.
	@echo "✓ Imagen construida: $(IMAGE):$(VERSION)"

## tag — (re)etiqueta la imagen local para el registro privado
tag: build
ifneq ($(REGISTRY),)
	docker tag $(IMAGE):$(VERSION) $(FULL_IMAGE)
	@echo "✓ Tag añadido: $(FULL_IMAGE)"
else
	@echo "⚠  REGISTRY no definido. Usa: make tag REGISTRY=registry.example.com"
endif

## push — construye, etiqueta y sube al registro privado
push: tag
ifneq ($(REGISTRY),)
	docker push $(FULL_IMAGE)
	@echo "✓ Imagen publicada: $(FULL_IMAGE)"
else
	@echo "⚠  REGISTRY no definido. Usa: make push REGISTRY=registry.example.com VERSION=1.3.0"
	@exit 1
endif

## up — levanta el stack de producción (requiere .env y secrets/)
up:
	docker compose up -d --build
	@echo "✓ Stack de producción levantado"

## down — para y elimina los contenedores de producción
down:
	docker compose down

## test — levanta el pipeline completo de pruebas (BD + simulador + conector)
test:
	docker compose -f docker-compose.test.yml up --build

## test-down — para el pipeline de pruebas y limpia volúmenes
test-down:
	docker compose -f docker-compose.test.yml down -v

## acceptance — prueba de aceptación con PostgreSQL real y rol restringido
acceptance:
	./scripts/acceptance_pg.sh

## monitoring-up — levanta Prometheus + Grafana (stack opcional)
monitoring-up:
	docker compose -f docker-compose.monitoring.yml up -d
	@echo "Prometheus: http://localhost:9090  Grafana: http://localhost:3000"

## monitoring-down — para el stack de monitorización (conserva volúmenes)
monitoring-down:
	docker compose -f docker-compose.monitoring.yml down

## logs — tail de logs del conector (producción)
logs:
	docker compose logs -f $(SERVICE)

## ps — estado de los contenedores
ps:
	docker compose ps

## clean — elimina imágenes locales del conector
clean:
	docker rmi -f $(IMAGE):$(VERSION) $(IMAGE):latest 2>/dev/null || true
	@echo "✓ Imágenes locales eliminadas"

## help — muestra esta ayuda
help:
	@grep -E '^## ' Makefile | sed 's/## /  make /'
