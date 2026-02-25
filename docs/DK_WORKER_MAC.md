# DraftKings Sync Worker (macOS)

Goal: allow the web UI to queue DraftKings sync jobs from any device. A persistent worker running on your Mac will pick up jobs, open Chrome when needed, and ingest bets.

This uses the repo's local worker:

- `scripts/sync_worker.py --loop --provider draftkings`

## Prereqs

- Mac stays on
- You are logged in to the Mac user session
- Repo exists at: `~/clawd/repos/basement-bets`
- Python venv exists: `./.venv311`
- Persistent DK profile directory exists (recommended): `./chrome_profile`

## Environment

These env vars should be set for the LaunchAgent:

- `DK_PROFILE_PATH=./chrome_profile`
- `DK_KEEP_OPEN_ON_AUTH=1`
- `DK_WAIT_FOR_LOGIN_SECONDS=60`
- `DK_AUTO_RETRY_SECONDS=600`

## Install (LaunchAgent)

1) Create the LaunchAgent file:

```bash
mkdir -p ~/Library/LaunchAgents
cp docs/com.basementbets.dkworker.plist ~/Library/LaunchAgents/
```

2) Load it:

```bash
launchctl unload ~/Library/LaunchAgents/com.basementbets.dkworker.plist 2>/dev/null || true
launchctl load ~/Library/LaunchAgents/com.basementbets.dkworker.plist
launchctl start com.basementbets.dkworker
```

3) Check status:

```bash
launchctl list | grep basementbets.dkworker
```

## Logs

The agent writes logs to:

- `~/Library/Logs/basement-bets-dkworker.out.log`
- `~/Library/Logs/basement-bets-dkworker.err.log`

Tail logs:

```bash
tail -f ~/Library/Logs/basement-bets-dkworker.out.log
```

## Uninstall

```bash
launchctl stop com.basementbets.dkworker || true
launchctl unload ~/Library/LaunchAgents/com.basementbets.dkworker.plist
rm -f ~/Library/LaunchAgents/com.basementbets.dkworker.plist
```
