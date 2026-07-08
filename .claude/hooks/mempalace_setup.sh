#!/bin/bash
# MEMPALACE SETUP HOOK — Auto-install and init on first run.
#
# Runs once per session. If mempalace is already installed, ensures
# state directory exists and exits fast (~10ms).
# If NOT installed, attempts auto-install via pip in the background.
# First session won't have memory, but subsequent sessions will.
#
# Never blocks session start — errors are logged, never shown.

STATE_DIR="$HOME/.mempalace/hook_state"
LOG="$STATE_DIR/setup.log"
mkdir -p "$STATE_DIR"

# Already installed + initialized? Fast exit.
if command -v mempalace > /dev/null 2>&1 && [ -f "$STATE_DIR/.initialized" ]; then
  exit 0
fi

# First run: init state
echo "[$(date '+%Y-%m-%d %H:%M:%S')] agentcode: checking mempalace..." >> "$LOG"

if command -v mempalace > /dev/null 2>&1; then
  touch "$STATE_DIR/.initialized"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] agentcode: mempalace ready" >> "$LOG"
  exit 0
fi

# Auto-install in background (non-blocking)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] agentcode: installing mempalace..." >> "$LOG"
(
  if command -v uv > /dev/null 2>&1; then
    uv pip install mempalace -q >> "$LOG" 2>&1
  elif command -v pip3 > /dev/null 2>&1; then
    pip3 install mempalace -q >> "$LOG" 2>&1
  elif command -v pip > /dev/null 2>&1; then
    pip install mempalace -q >> "$LOG" 2>&1
  fi

  if command -v mempalace > /dev/null 2>&1; then
    touch "$STATE_DIR/.initialized"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] agentcode: mempalace installed successfully" >> "$LOG"
  else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] agentcode: mempalace install failed (no Python/pip?)" >> "$LOG"
  fi
) &
