@_list:
	just --list --unsorted

# Perform all verifications (compile, test, lint etc.)
verify: test lint

# Watch the source files and run `just verify` when sources change
watch:
	cargo watch -- just verify

# Run the tests
test:
	cargo test

# Run the static code analysis
lint:
	cargo fmt -- --check
	cargo clippy --all-targets

# Build the documentation
doc *args:
	cargo doc --all-features --no-deps {{args}}

# Open the documentation page
doc-open: (doc "--open")

# Clean up compilation output
clean:
	rm -rf target
	rm -rf node_modules

# Install cargo dev-tools used by the `verify` recipe (requires rustup to be already installed)
install-dev-tools:
	rustup install stable
	rustup override set stable
	cargo install cargo-hack cargo-watch

# Install a git hook to run tests before every commit
install-git-hooks:
	echo '#!/usr/bin/env sh' > .git/hooks/pre-commit
	echo 'just verify' >> .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit
