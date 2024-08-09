#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

pyenv global $PYTHON_VERSION
make install
echo "Python version:"
bin/python --version
make test
