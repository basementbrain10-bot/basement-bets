import os

# Base execution switches
AGENTS_ENABLED = str(os.getenv('AGENTS_ENABLED', 'false')).lower() in ('true', '1', 't', 'y', 'yes')
AGENTS_IDEMPOTENCY_WINDOW_SECONDS = int(os.getenv('AGENTS_IDEMPOTENCY_WINDOW_SECONDS', '90'))

# Timeout / Limits / Cache caps
AGENTS_CACHE_TTL_SECONDS = int(os.getenv('AGENTS_CACHE_TTL_SECONDS', '60'))
AGENTS_MAX_EVENTS_PER_RUN = int(os.getenv('AGENTS_MAX_EVENTS_PER_RUN', '25'))
AGENTS_MAX_EXTERNAL_CALLS_PER_RUN = int(os.getenv('AGENTS_MAX_EXTERNAL_CALLS_PER_RUN', '25'))

# Timeboxing thresholds
AGENTS_AGENT_TIMEOUT_SECONDS = int(os.getenv('AGENTS_AGENT_TIMEOUT_SECONDS', '5'))
AGENTS_TOTAL_TIMEOUT_SECONDS = int(os.getenv('AGENTS_TOTAL_TIMEOUT_SECONDS', '20'))

# Target filters, sizing strategies
AGENTS_MAX_PLAYS_PER_DAY = int(os.getenv('AGENTS_MAX_PLAYS_PER_DAY', '5'))
AGENTS_MIN_EDGE = float(os.getenv('AGENTS_MIN_EDGE', '0.02'))
AGENTS_MIN_EV_PER_UNIT = float(os.getenv('AGENTS_MIN_EV_PER_UNIT', '0.02'))
AGENTS_SIZING_MODE = str(os.getenv('AGENTS_SIZING_MODE', 'fractional_kelly')).lower()
AGENTS_KELLY_FRACTION = float(os.getenv('AGENTS_KELLY_FRACTION', '0.25'))
AGENTS_MAX_KELLY_PCT = float(os.getenv('AGENTS_MAX_KELLY_PCT', '0.02'))
AGENTS_MAX_EVENT_EXPOSURE_PCT = float(os.getenv('AGENTS_MAX_EVENT_EXPOSURE_PCT', '0.05'))
AGENTS_CORRELATION_HAIRCUT_ENABLED = str(os.getenv('AGENTS_CORRELATION_HAIRCUT_ENABLED', 'true')).lower() in ('true', '1')

# Output / HITL constraints
AGENTS_REVIEW_CONFIDENCE_THRESHOLD = float(os.getenv('AGENTS_REVIEW_CONFIDENCE_THRESHOLD', '0.55'))
AGENTS_REVIEW_ON_FLAGS = str(os.getenv('AGENTS_REVIEW_ON_FLAGS', 'true')).lower() in ('true', '1')
