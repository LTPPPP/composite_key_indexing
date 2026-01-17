# Contributing to Composite Key Indexing

First off, thank you for considering contributing to this project! 🎉

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [How Can I Contribute?](#how-can-i-contribute)
- [Development Setup](#development-setup)
- [Pull Request Process](#pull-request-process)
- [Style Guidelines](#style-guidelines)
- [Running Tests](#running-tests)

## Code of Conduct

By participating in this project, you are expected to uphold our Code of Conduct:

- Use welcoming and inclusive language
- Be respectful of differing viewpoints and experiences
- Gracefully accept constructive criticism
- Focus on what is best for the community
- Show empathy towards other community members

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/composite-key-indexing.git`
3. Create a new branch: `git checkout -b feature/your-feature-name`
4. Make your changes
5. Push to your fork and submit a pull request

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check existing issues to avoid duplicates. When creating a bug report, include:

- **Clear title** describing the issue
- **Steps to reproduce** the behavior
- **Expected behavior** vs **actual behavior**
- **Go version** (`go version`)
- **Operating system** and version
- **Relevant code snippets** or error messages

### Suggesting Enhancements

Enhancement suggestions are welcome! Please include:

- **Clear title** describing the enhancement
- **Detailed description** of the proposed functionality
- **Use case** explaining why this would be useful
- **Possible implementation** approach (if you have ideas)

### Pull Requests

We actively welcome your pull requests! See the [Pull Request Process](#pull-request-process) section below.

## Development Setup

### Prerequisites

- Go 1.21 or higher
- Make (optional, for using Makefile commands)

### Setup

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/composite-key-indexing.git
cd composite-key-indexing

# Download dependencies
go mod download

# Verify everything works
go test ./...
```

### Optional Dependencies

For running with specific backends:

- **BadgerDB**: `go get github.com/dgraph-io/badger/v4`
- **RocksDB**: Requires CGO and RocksDB installed on system

## Pull Request Process

1. **Ensure tests pass**: Run `go test ./...` before submitting
2. **Update documentation**: If you change functionality, update relevant docs
3. **Add tests**: New features should include tests
4. **Follow code style**: Run `go fmt` and `go vet`
5. **Write meaningful commits**: Use clear, descriptive commit messages
6. **Reference issues**: Link related issues in PR description

### Commit Message Format

```
<type>: <short description>

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code style (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `perf`: Performance improvements
- `chore`: Maintenance tasks

Example:
```
feat: add support for custom key separators

Allow users to specify custom separators for composite keys
instead of using the default null byte separator.

Closes #123
```

## Style Guidelines

### Go Code Style

- Follow [Effective Go](https://golang.org/doc/effective_go.html) guidelines
- Run `go fmt` on all code
- Run `go vet` to catch common errors
- Use `golint` or `golangci-lint` for additional checks

```bash
# Format code
go fmt ./...

# Run vet
go vet ./...

# Run linter (if installed)
golangci-lint run
```

### Code Organization

- Keep functions focused and small
- Add comments for exported functions and types
- Use meaningful variable and function names
- Handle errors explicitly

### Documentation

- Document all exported types, functions, and methods
- Include examples in documentation where helpful
- Keep README.md up to date

## Running Tests

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests with coverage
go test -cover ./...

# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html

# Run benchmarks
go test -bench=. ./...

# Run specific test
go test -run TestCompositeKey ./...
```

## Questions?

Feel free to open an issue with the "question" label if you have any questions about contributing.

Thank you for your contributions! 🙏
