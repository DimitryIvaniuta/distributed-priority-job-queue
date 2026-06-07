#!/usr/bin/env sh
set -eu

APP_HOME=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
GRADLE_VERSION=9.5.1
GRADLE_DIR="$APP_HOME/.gradle/bootstrap/gradle-$GRADLE_VERSION"
GRADLE_BIN="$GRADLE_DIR/bin/gradle"

if [ -x "$GRADLE_BIN" ]; then
  exec "$GRADLE_BIN" "$@"
fi

if command -v gradle >/dev/null 2>&1; then
  exec gradle "$@"
fi

ZIP="$APP_HOME/.gradle/bootstrap/gradle-$GRADLE_VERSION-bin.zip"
mkdir -p "$APP_HOME/.gradle/bootstrap"
if command -v curl >/dev/null 2>&1; then
  curl -fsSLo "$ZIP" "https://services.gradle.org/distributions/gradle-$GRADLE_VERSION-bin.zip"
elif command -v wget >/dev/null 2>&1; then
  wget -qO "$ZIP" "https://services.gradle.org/distributions/gradle-$GRADLE_VERSION-bin.zip"
else
  echo "curl, wget, or a local Gradle installation is required." >&2
  exit 1
fi

unzip -q "$ZIP" -d "$APP_HOME/.gradle/bootstrap"
chmod +x "$GRADLE_BIN"
exec "$GRADLE_BIN" "$@"
