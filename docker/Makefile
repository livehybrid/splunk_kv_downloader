# Local docker loop for the kv_downloader integration suite.
.PHONY: up test down
up:
	docker compose up -d --wait --wait-timeout 360
test:
	SPLUNK_MGMT_URL=https://127.0.0.1:8089 SPLUNK_PASSWORD=Changeme1! SPLUNK_CONTAINER=kv_downloader_splunk pytest ../tests/ -v -ra
down:
	docker compose down -v || true
