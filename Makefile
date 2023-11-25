SHELL:=/usr/bin/env bash

.DEFAULT_GOAL := git-commit-and-push

.PHONY: git-commit-and-push
git-commit-and-push:
	git add . && git commit -m 'update' && git push