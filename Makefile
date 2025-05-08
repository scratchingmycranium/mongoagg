.PHONY: version-patch version-minor version-major release

# Helper function to get current version
define get_version
$(shell grep -m 1 "__version__" mongoagg/__init__.py | cut -d'"' -f2)
endef

# Helper function to update version in a file
define update_version
sed -i '' 's/version = ".*"/version = "$(1)"/' $(2)
endef

version-patch:
	@current=$$(grep -m 1 "__version__" mongoagg/__init__.py | cut -d'"' -f2); \
	major=$$(echo $$current | cut -d. -f1); \
	minor=$$(echo $$current | cut -d. -f2); \
	patch=$$(echo $$current | cut -d. -f3); \
	new_version="$$major.$$minor.$$((patch + 1))"; \
	sed -i '' 's/__version__ = ".*"/__version__ = "'$$new_version'"/' mongoagg/__init__.py; \
	sed -i '' 's/version=".*"/version="'$$new_version'"/' setup.py; \
	sed -i '' 's/version = ".*"/version = "'$$new_version'"/' pyproject.toml; \
	echo "Bumped version to $$new_version"

version-minor:
	@current=$$(grep -m 1 "__version__" mongoagg/__init__.py | cut -d'"' -f2); \
	major=$$(echo $$current | cut -d. -f1); \
	minor=$$(echo $$current | cut -d. -f2); \
	new_version="$$major.$$((minor + 1)).0"; \
	sed -i '' 's/__version__ = ".*"/__version__ = "'$$new_version'"/' mongoagg/__init__.py; \
	sed -i '' 's/version=".*"/version="'$$new_version'"/' setup.py; \
	sed -i '' 's/version = ".*"/version = "'$$new_version'"/' pyproject.toml; \
	echo "Bumped version to $$new_version"

version-major:
	@current=$$(grep -m 1 "__version__" mongoagg/__init__.py | cut -d'"' -f2); \
	major=$$(echo $$current | cut -d. -f1); \
	new_version="$$((major + 1)).0.0"; \
	sed -i '' 's/__version__ = ".*"/__version__ = "'$$new_version'"/' mongoagg/__init__.py; \
	sed -i '' 's/version=".*"/version="'$$new_version'"/' setup.py; \
	sed -i '' 's/version = ".*"/version = "'$$new_version'"/' pyproject.toml; \
	echo "Bumped version to $$new_version"

release:
	@version=$$(grep -m 1 "__version__" mongoagg/__init__.py | cut -d'"' -f2); \
	echo "Creating release for version $$version"; \
	git add mongoagg/__init__.py setup.py pyproject.toml; \
	git commit -m "Release v$$version" || true; \
	git tag -a "v$$version" -m "Release v$$version"; \
	git push origin "v$$version"; \
	echo "Release v$$version created! GitHub Actions will handle building and publishing." 