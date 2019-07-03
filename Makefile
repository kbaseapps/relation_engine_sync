.PHONY: test

test:
	docker-compose down
	docker-compose run --entrypoint sh app scripts/run_tests.sh
	docker-compose down
