
bump.patch:
	bumpversion patch --allow-dirty

bump.minor:
	bumpversion minor --allow-dirty

bump.major:
	bumpversion major --allow-dirty

push:
	git push && git push --tags